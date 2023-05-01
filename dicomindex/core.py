from pathlib import Path
from typing import Set

import pydicom
from pydicom import Dataset
from pydicom.errors import InvalidDicomError
from pydicom.misc import is_dicom
from sqlalchemy.orm import Session

from dicomindex.exceptions import DICOMIndexError
from dicomindex.orm import Instance, Patient, Series, Study


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


def index_dicom_dir(dicom_dir: Path, session: Session):
    """Go through dicom dir and build a patient/study/series structure

    Parameters
    ----------
    dicom_dir: Path
        Index this folder
    session: Session
        The session to add patient/study/series to

    Returns
    -------
    None

    """
    files = []
    index = DICOMIndex.init_from_session(session)
    for file in (x for x in dicom_dir.rglob("*") if x.is_file() and is_dicom(x)):
        files.append(file)
        to_add = index.create_new_db_objects(read_dicom_file(file), str(file))
        session.add_all(to_add)


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
    ):
        self.patient_ids = patient_ids
        self.study_uids = study_uids
        self.series_uids = series_uids
        self.instance_uids = instance_uids

    @classmethod
    def init_from_session(cls, session):
        """Populate indexes by reading ids from given session"""
        return cls(
            patient_ids={pid for pid, in session.query(Patient.PatientID)},
            study_uids={pid for pid, in session.query(Study.StudyInstanceUID)},
            series_uids={pid for pid, in session.query(Series.SeriesInstanceUID)},
            instance_uids={pid for pid, in session.query(Instance.SOPInstanceUID)},
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

        self.add_to_index(dataset)
        return db_objects

    def add_to_index(self, dataset):
        """Add patient/study/series/instance ids to index"""
        self.patient_ids.add(dataset.PatientID)
        self.study_uids.add(dataset.StudyInstanceUID)
        self.series_uids.add(dataset.SeriesInstanceUID)
        self.instance_uids.add(dataset.SOPInstanceUID)


class AllDICOMFiles:
    def __init__(self, path):
        """All DICOM files in path recursively"""
        self.path = Path(path)

    def __iter__(self):
        return iter(
            file
            for file in (x for x in self.path.rglob("*") if x.is_file() and is_dicom(x))
        )


class DICOMFilePerSeries:
    def __init__(self, root_path):
        """One DICOM file per series in a patient/study/series structured
        folder

        Can iterate over an archive much quicker
        """
        self.root_path = Path(root_path)

    def __iter__(self):
        for series_path in self.root_path.glob("*/*/*"):
            try:
                yield next(
                    x for x in series_path.glob("*") if x.is_file() and is_dicom(x)
                )
            except StopIteration:
                continue
