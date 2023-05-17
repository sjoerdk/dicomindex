"""Functions and classes for indexing dicom files in a folder

Ties together modules
"""
from pathlib import Path
from typing import Optional, Set

from pydicom import Dataset
from sqlalchemy.exc import StatementError
from tqdm import tqdm

from dicomindex.core import read_dicom_file
from dicomindex.exceptions import NotDICOMError
from dicomindex.iterators import AllFiles
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
        index.paths.add(Path(path))
        session.add(NonDICOMFile(path=path))
        session.commit()
        return PathStatuses.SKIPPED_NON_DICOM

    to_add = index.create_new_db_objects(ds, path, add_to_index=False)
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
    index.paths.add(str(path))
    return PathStatuses.PROCESSED


def update_progress(iter_in: EagerIterator, stats: Statistics,
                    progress_bar: Optional[tqdm]):
    """Updates both count and total length of given tqdm progress bar"""
    if progress_bar:
        progress_bar.total = len(iter_in)
        progress_bar.update()
        progress_bar.set_postfix_str(stats.summary())


def index_folder(base_folder, session, progress_bar: Optional[tqdm] = None):
    """Iterate over all files in base folder, add them to session"""
    index = DICOMIndex.init_from_session(session)
    statistics = Statistics()
    logger.debug(f"Found {len(index.paths)} instances already in index")

    with EagerIterator(AllFiles(base_folder)) as paths:
        for path in paths:
            statistics.add(path, process_path(path, session, index))
            update_progress(paths, statistics, progress_bar)

    return statistics


class DICOMIndex:
    """Keeps track DICOM object ids to minimize database traffic

    A single DICOM Patient can be referenced in hundreds of DICOM instances. A
    Patient object will only have to be added to database once. Instead of
    checking the database hundreds of times for patients, keep track here.
    """

    def __init__(
        self,
        patient_ids: Set[str],
        study_uids: Set[str],
        series_uids: Set[str],
        instance_uids: Set[str],
        paths: Set[str],
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
            paths={path for path, in session.query(Instance.path)}
            | {path for path, in session.query(DICOMFileDuplicate.path)}
            | {path for path, in session.query(NonDICOMFile.path)},
        )

    def create_new_db_objects(self, dataset: Dataset, path: str, add_to_index=True):
        """Create patient/study/series/instance objects from dataset, ignore existing

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
            If True, automatically add ids and paths of any db objects to this index
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
            self.paths.add(path)
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
            self.paths.add(path)
        logger.debug(f"Created {len(db_objects)} db objects for {path}")
        return db_objects

    def add_to_index(self, dataset):
        """Add patient/study/series/instance ids to index"""
        self.patient_ids.add(dataset.PatientID)
        self.study_uids.add(dataset.StudyInstanceUID)
        self.series_uids.add(dataset.SeriesInstanceUID)
        self.instance_uids.add(dataset.SOPInstanceUID)
