"""Functions and classes for indexing dicom files in a folder

Ties together modules
"""
from typing import Dict, Optional, Set

from pydicom import Dataset
from sqlalchemy.exc import StatementError
from tqdm import tqdm

from dicomindex.core import read_dicom_file
from dicomindex.dataset import RequiredDataset, RequiredTagNotFound
from dicomindex.exceptions import NoObjectIDFoundError, NotDICOMError
from dicomindex.iterators import AllFiles, Folder
from dicomindex.logs import get_module_logger
from dicomindex.orm import (
    DICOMFileDuplicate,
    Instance,
    NonDICOMFile,
    Patient,
    Series,
    Study,
)
from dicomindex.statistics import PathStatuses, Statistics
from dicomindex.threading import EagerIterator

logger = get_module_logger("processing")


def process_dataset(ds, session, index):
    """Add information from dataset. Add Patient,Study,Series,Instance objects if
    possible
    """
    to_add = index.add_dataset(ds, add_to_index=False)
    session.add_all(to_add)
    try:
        session.commit()
    except StatementError as e:
        session.rollback()
        # Skip error, continue
        logger.exception(e)
    else:
        # commit has succeeded. Now you can add
        index.add_to_index(ds)


def process_path(path, session, index):
    """Add this path to db and take care of all bookkeeping

    Returns
    -------
    str
        One of the values in PathStatuses
    """

    path = str(path)
    if path in index.paths:
        return PathStatuses.SKIPPED_ALREADY_VISITED
    try:
        ds = read_dicom_file(path)
    except NotDICOMError:
        index.set_path_status(path, status=PathStatuses.SKIPPED_NON_DICOM)
        session.add(NonDICOMFile(path=path))
        session.commit()
        return PathStatuses.SKIPPED_NON_DICOM

    try:
        to_add = index.add_file_dataset(RequiredDataset(ds), path, add_to_index=False)
    except (RequiredTagNotFound, NoObjectIDFoundError) as e:
        # a basic tag like PatientID is missing from this file. Don't process.
        logger.exception(e)
        logger.error(f"DICOM file at {path} was missing essential tags. skipping")
        index.set_path_status(path, PathStatuses.SKIPPED_FAILED)
        return PathStatuses.SKIPPED_FAILED

    session.add_all(to_add)
    try:
        session.commit()
    except StatementError as e:
        session.rollback()
        # Skip error, continue
        logger.exception(e)
        logger.error(f"Error processing {path}. skipping")
        return PathStatuses.SKIPPED_FAILED

    index.add_to_index(ds)  # commit has succeeded. Now you can add
    index.set_path_status(path, PathStatuses.PROCESSED)
    return PathStatuses.PROCESSED


def update_progress(
    iter_in: EagerIterator, stats: Statistics, progress_bar: Optional[tqdm]
):
    """Updates both count and total length of given tqdm progress bar"""
    if progress_bar:
        progress_bar.total = len(iter_in)
        progress_bar.update()
        progress_bar.set_postfix_str(stats.summary())


def index_folder_full(base_folder, session, progress_bar: Optional[tqdm] = None):
    """Iterate over all files in base folder, add them to session"""
    index = DICOMIndex.init_from_session(session)
    statistics = Statistics()
    logger.debug(f"Found {len(index.paths)} instances already in index")

    with EagerIterator(AllFiles(base_folder)) as paths:
        for path in paths:
            statistics.add(path, process_path(path, session, index))
            update_progress(paths, statistics, progress_bar)

    return statistics


def index_one_file_per_folder(
    base_folder, session, progress_bar: Optional[tqdm] = None
):
    """Add one file from each folder to index. This assumes the structure of base_folder
    somehow corresponds to patient/study/series.

    Notes
    -----
    This method will keep visiting files in a folder until a single DICOM file has
    been registered. Any non-dicom files or erroring files encountered during this
    search are registered in DB. This means a variable number of files per folder
    will be in db.
    """
    index = DICOMIndex.init_from_session(session)
    statistics = Statistics()
    logger.debug(f"Found {len(index.paths)} instances already in index")

    with EagerIterator(Folder(base_folder).folders()) as folders:
        for folder in folders:
            logger.debug(f"Finding single file in {folder}")
            folder_processed = False
            folder_skipped = False
            for path in (x for x in folder.glob("./*") if x.is_file()):
                if index.paths.get(str(path)) == PathStatuses.PROCESSED:
                    logger.debug(
                        f"Path {path} already processed. Skipping this " f"folder"
                    )
                    folder_skipped = True
                    folder_processed = True
                    break
                status = process_path(path, session, index)
                if status == PathStatuses.PROCESSED:
                    # added one., We're done with this folder
                    statistics.add(path, status)
                    update_progress(folders, statistics, progress_bar)
                    folder_processed = True
                    break
                else:
                    continue
            if not folder_processed:
                logger.debug(f"Found no DICOM files in {folder}")
                statistics.add(folder, PathStatuses.SKIPPED_NON_DICOM)
                update_progress(folders, statistics, progress_bar)
            elif folder_skipped:
                statistics.add(path, PathStatuses.SKIPPED_NON_DICOM)
                update_progress(folders, statistics, progress_bar)

    return statistics


class DICOMIndex:
    """Keeps track DICOM object ids to minimize database traffic

    A single DICOM Patient can be referenced in hundreds of DICOM instances. A
    Patient object will only have to be added to database once. Instead of
    checking the database hundreds of times for patients, keep track here.

    Design
    ------
    DICOMIndex does not use or maintain any database connection. It is an index of
    what has been added to avoid duplicate calls.

    """

    def __init__(
        self,
        patient_ids: Set[str],
        study_uids: Set[str],
        series_uids: Set[str],
        instance_uids: Set[str],
        paths: Dict[str, str],
    ):
        self.patient_ids = patient_ids
        self.study_uids = study_uids
        self.series_uids = series_uids
        self.instance_uids = instance_uids
        self.paths = paths

    @classmethod
    def init_from_session(cls, session):
        """Populate indexes by reading ids from given session"""

        return cls(
            patient_ids={pid for pid, in session.query(Patient.PatientID)},
            study_uids={pid for pid, in session.query(Study.StudyInstanceUID)},
            series_uids={pid for pid, in session.query(Series.SeriesInstanceUID)},
            instance_uids={pid for pid, in session.query(Instance.SOPInstanceUID)},
            paths=dict(
                **{
                    str(path): PathStatuses.PROCESSED
                    for path, in session.query(Instance.path)
                },
                **{
                    str(path): PathStatuses.SKIPPED_FAILED
                    for path, in session.query(DICOMFileDuplicate.path)
                },
                **{
                    str(path): PathStatuses.SKIPPED_NON_DICOM
                    for path, in session.query(NonDICOMFile.path)
                },
            ),
        )

    def add_file_dataset(self, dataset: Dataset, path: str, add_to_index=True):
        """Add the information from a file-based DICOM dataset to index

        Add original path and patient/study/series/instance objects if possible,
        skips existing based on DICOM object UIDs. Duplicate SOPinstanceUIDs are
        registered as duplicates.

        Notes
        -----
        Any file with previously seen SOPInstanceUID will be stored as duplicate
        and further processing will be skipped. This means any patient/study/series
        information in such a file is not recorded. Having duplicate SOPInstanceUIDs
        in an archive should never happen, but does.

        Parameters
        ----------
        dataset: Dataset
            Copy relevant fields from this dataset to db objects
        path: str
            Set Instance.path to this
        add_to_index: Bool, optional
            If True, automatically add ids and paths of any db objects to this index.
            If False, just return the objects that would have been added.
            Defaults to true.


        Returns
        -------
        List[Base]
            Patient/Study/Series/Instance objects for ids that have not been

        """
        path = str(path)  # handle Path type input

        if dataset.SOPInstanceUID in self.instance_uids:
            logger.debug(
                f"SOPInstanceUID in '{path}' already exists. This is a"
                f" duplicate file. Storing in duplicate instances"
            )
            self.set_path_status(path)
            return [
                DICOMFileDuplicate(path=path, SOPInstanceUID=dataset.SOPInstanceUID)
            ]

        db_objects = [Instance.init_from_dataset(dataset, path)]

        if dataset.PatientID not in self.patient_ids:
            db_objects.append(Patient.init_from_dataset(dataset))
        if dataset.StudyInstanceUID not in self.study_uids:
            db_objects.append(Study.init_from_dataset(dataset))
        if dataset.SeriesInstanceUID not in self.series_uids:
            db_objects.append(Series.init_from_dataset(dataset))

        if add_to_index:
            self.add_to_index(dataset)
            self.set_path_status(path)
        logger.debug(f"Created {len(db_objects)} db objects for {path}")
        return db_objects

    def add_dataset(self, dataset: Dataset, add_to_index=True):  # noqa: C901
        """Add the information from any DICOM dataset to index. Auto-detect
        whether this contains Instance, Series or Study information.

        Parameters
        ----------
        dataset: Dataset
            Copy relevant fields from this dataset to db objects
        add_to_index: Bool, optional
            If True, automatically add ids and paths of any db objects to this index.
            If False, just return the objects that would have been added.
            Defaults to true.


        Returns
        -------
        List[Base]
            Patient/Study/Series/Instance objects for ids that have not been

        """

        db_objects = []

        if dataset.get("PatientID") not in self.patient_ids:
            try:
                db_objects.append(Patient.init_from_dataset(dataset))
            except NoObjectIDFoundError:
                pass
        if dataset.get("StudyInstanceUID") not in self.study_uids:
            try:
                db_objects.append(Study.init_from_dataset(dataset))
            except NoObjectIDFoundError:
                pass
        if dataset.get("SeriesInstanceUID") not in self.series_uids:
            try:
                db_objects.append(Series.init_from_dataset(dataset))
            except NoObjectIDFoundError:
                pass
        if dataset.get("SOPInstanceUID") not in self.instance_uids:
            try:
                db_objects.append(Instance.init_from_dataset(dataset, path="None"))
            except NoObjectIDFoundError:
                pass

        if add_to_index:
            self.add_to_index(dataset)
        logger.debug(f"Created {len(db_objects)} db objects")
        return db_objects

    def set_path_status(self, path, status: str = PathStatuses.VISITED):
        """Register the given path as visited"""
        self.paths[str(path)] = status

    def add_to_index(self, dataset):
        """Add patient/study/series/instance ids to index"""
        if "PatientID" in dataset:
            self.patient_ids.add(dataset.PatientID)
        if "StudyInstanceUID" in dataset:
            self.study_uids.add(dataset.StudyInstanceUID)
        if "SeriesInstanceUID" in dataset:
            self.series_uids.add(dataset.SeriesInstanceUID)
        if "SOPInstanceUID" in dataset:
            self.instance_uids.add(dataset.SOPInstanceUID)
