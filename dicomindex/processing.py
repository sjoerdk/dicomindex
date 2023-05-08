"""Functions and classes for indexing dicom files in a folder

Ties together modules
"""
from typing import Set

from pydicom import Dataset

from dicomindex.iterators import AllFiles
from dicomindex.logs import get_module_logger
from dicomindex.orm import Instance, Patient, Series, Study
from dicomindex.statistics import PathStatuses, Statistics
from dicomindex.threading import DICOMDatasetsThreaded, var_len_tqdm

logger = get_module_logger("processing")


def index_folder(base_folder, session, use_progress_bar=False):
    """Iterate over all files in base folder, add them to session

    use_progress_bar=True will show a progress bar at stdout using tqdm
    """
    index = DICOMIndex.init_from_session(session)
    stats = Statistics()
    logger.debug(f"Found {len(index.paths)} instances already in index")

    def path_iter():
        for path in AllFiles(base_folder):
            if str(path) in index.paths or not path.is_file():
                stats.add(path, PathStatuses.SKIPPED_ALREADY_VISITED)
                continue  # skip this path
            else:
                yield path

    # progress bar added with var_len_tqdm
    ds_generator = DICOMDatasetsThreaded(path_iter())
    if use_progress_bar:
        ds_generator = var_len_tqdm(ds_generator)
    for ds in ds_generator:
        stats.add(ds.filename, PathStatuses.PROCESSED)
        to_add = index.create_new_db_objects(ds, ds.filename)
        session.add_all(to_add)
        session.commit()
    return stats


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
