from dataclasses import dataclass
from typing import Iterator, Set

import pydicom
from pydicom import Dataset
from pydicom.errors import InvalidDicomError

from dicomindex.exceptions import DICOMIndexError
from dicomindex.logs import get_module_logger
from dicomindex.orm import Instance, Patient, Series, Study

logger = get_module_logger("core")


def read_dicom_file(path):
    """

    Parameters
    ----------
    path: Path
        Read this path as dicom file

    Raises
    ------
    DICOMIndexError
        When given path cannot be opened as DICOM
    FileNotFoundError
        When given path does not exist

    """
    try:
        return pydicom.filereader.dcmread(str(path), stop_before_pixels=True)
    except InvalidDicomError as e:
        raise DICOMIndexError(e) from e


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
            paths={path for path, in session.query(Instance.path)},
        )

    def create_new_db_objects(self, dataset: Dataset, path: str):
        """Create patient/study/series/instance objects from dataset, ignore existing


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
        db_objects = []
        if dataset.PatientID not in self.patient_ids:
            db_objects.append(Patient.init_from_dataset(dataset))
        if dataset.StudyInstanceUID not in self.study_uids:
            db_objects.append(Study.init_from_dataset(dataset))
        if dataset.SeriesInstanceUID not in self.series_uids:
            db_objects.append(Series.init_from_dataset(dataset))
        if dataset.SOPInstanceUID not in self.instance_uids:
            db_objects.append(Instance.init_from_dataset(dataset, path))

        logger.debug(f"Added {len(db_objects)} db objects for {path}")
        self.add_to_index(dataset, path)
        return db_objects

    def add_to_index(self, dataset, path):
        """Add patient/study/series/instance ids to index"""
        self.patient_ids.add(dataset.PatientID)
        self.study_uids.add(dataset.StudyInstanceUID)
        self.series_uids.add(dataset.SeriesInstanceUID)
        self.instance_uids.add(dataset.SOPInstanceUID)
        self.paths.add(path)


class DICOMFileIterator:
    """Something that yields file paths to valid DICOM files"""

    def __iter__(self) -> Iterator[str]:
        pass


@dataclass
class RootPathLevel:
    """Determines whether a folder is at archive/patient/study or series level"""

    name: str  # what to call this level
    series_glob: str  # how can you find all series folders


class ArchiveLevel(RootPathLevel):
    """A folder containing patient folders"""

    name = "ArchiveLevel"
    series_glob = "*/*/*"


class PatientLevel(RootPathLevel):
    """A folder containing study folders for a single patient"""

    name = "PatientLevel"
    series_glob = "*/*"


class StudyLevel(RootPathLevel):
    """A folder containing series folders for a single study"""

    name = "StudyLevel"
    series_glob = "*"


@dataclass
class RootPathLevels:
    archive = ArchiveLevel
    patient = PatientLevel
    study = StudyLevel


class NewDicomFiles(DICOMFileIterator):
    def __init__(self, file_iterator: DICOMFileIterator, index: DICOMIndex):
        """Yields file paths from iterator, unless dicom index already contains
        this path

        Parameters
        ----------
        file_iterator:
            Iterate over DICOM file paths from this iterator
        index:
            Check this index for SOPInstanceUIDs
        """
        self.file_iterator = file_iterator
        self.index = index

    def __iter__(self):
        return iter(x for x in self.file_iterator if str(x) not in self.index.paths)
