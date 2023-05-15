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


def index_folder(base_folder, session, progress_bar: Optional[tqdm] = None):
    """Iterate over all files in base folder, add them to session

    progress bar is a tqdm progress bar object
    """
    index = DICOMIndex.init_from_session(session)
    statistics = Statistics()
    logger.debug(f"Found {len(index.paths)} instances already in index")

    def update_progress(paths_in, stats):
        if progress_bar:
            progress_bar.total = len(paths_in)
            progress_bar.update()
            progress_bar.set_postfix_str(stats.summary())

    with EagerIterator(AllFiles(base_folder)) as paths:
        for path in paths:
            path = str(path)
            if path in index.paths:
                statistics.add(path, PathStatuses.SKIPPED_ALREADY_VISITED)
                update_progress(paths, statistics)
                continue  # skip this
            try:
                ds = read_dicom_file(path)
            except NotDICOMError:
                statistics.add(path, PathStatuses.SKIPPED_NON_DICOM)
                index.paths.add(Path(path))
                session.add(NonDICOMFile(path=path))
                session.commit()
                update_progress(paths, statistics)
                continue
            statistics.add(path, PathStatuses.PROCESSED)
            to_add = index.create_new_db_objects(ds, path)
            session.add_all(to_add)
            try:
                session.commit()
            except StatementError as e:
                session.rollback()
                # Skip error, continue
                logger.exception(e)
                logger.error(f"Error processing {path}. skipping")
                statistics.add(path, PathStatuses.SKIPPED_FAILED)
                update_progress(paths, statistics)
                continue

            update_progress(paths, statistics)

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

    def create_new_db_objects(self, dataset: Dataset, path: str):
        """Create patient/study/series/instance objects from dataset, ignore existing

        Notes
        -----
        Any file with previously seen SOPInstanceUID will be stored as duplicate
        and further processing will be skipped. This means any patient/study/series
        information in such a file is not recorded. Having duplicate SOPInstanceUIDs
        in an archive should never happen, but does.

        Parameters
        ----------
        dataset:
            Copy relevant fields from this dataset to db objects
        path:
            Set Instance.path to this

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
