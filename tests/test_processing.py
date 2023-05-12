from dicomgenerator.export import export
from dicomgenerator.templates import CTDatasetFactory
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dicomindex.core import read_dicom_file
from dicomindex.processing import DICOMIndex, index_folder
from dicomindex.iterators import AllDICOMFiles, AllFiles
from dicomindex.orm import Base, Instance, Patient
from tests.conftest import generate_full_stack_patient


def test_index_dicom_dir(example_dicom_folder, a_db_file):

    with SQLiteSession(a_db_file) as session:
        index = DICOMIndex.init_from_session(session)
        for file in AllDICOMFiles(example_dicom_folder):
            session.add_all(
                index.create_new_db_objects(read_dicom_file(file), str(file))
            )
            session.commit()

        patients = session.query(Patient).all()
        instances = session.query(Instance).all()

        assert len(patients) == 2
        assert len(instances) == 14
        assert instances[4].ManufacturerModelName == "Aquilion"


def test_index_dirty_dicom_dir(example_dicom_folder, a_db_file):
    """Non-dicom files in directory should be handled gracefully"""
    with open(example_dicom_folder / "patient1" / "non_dicom.txt", "w") as f:
        f.write("No dicom content!")

    with SQLiteSession(a_db_file) as session:
        index = DICOMIndex.init_from_session(session)
        for file in AllDICOMFiles(example_dicom_folder):
            session.add_all(
                index.create_new_db_objects(read_dicom_file(file), str(file))
            )
            session.commit()


def test_dicom_index_initial_db(use_mem_db_session):
    """You can pre-populate a DICOMIndex with data from a db session"""
    use_mem_db_session.add(generate_full_stack_patient("patient1", randomize=False))
    use_mem_db_session.commit()

    index = DICOMIndex.init_from_session(session=use_mem_db_session)
    assert len(index.patient_ids) == 1
    assert len(index.study_uids) == 2
    assert len(index.series_uids) == 6
    assert len(index.instance_uids) == 30


def test_folder_iterator(example_dicom_folder):
    files = [x for x in AllDICOMFiles(example_dicom_folder)]
    assert len(files) == 14


def test_folder_iterator_skip_existing_instances(example_dicom_folder, a_db_file):
    """If an instance is in db, don't try to open again"""

    # a folder with some dicom
    files = [x for x in AllDICOMFiles(example_dicom_folder)]
    assert len(files) == 14

    # now add 5 of those files to db using create_new_db_objects
    with SQLiteSession(a_db_file) as session:
        index = DICOMIndex.init_from_session(session)
        for file in files[0:5]:
            session.add_all(
                index.create_new_db_objects(read_dicom_file(file), str(file))
            )
        session.commit()

    # run again on all files in folder
    with SQLiteSession(a_db_file) as session:
        index = DICOMIndex.init_from_session(session)
        new_files = list(
            x for x in AllDICOMFiles(example_dicom_folder) if str(x) not in index.paths
        )

        assert len(new_files) == 9
        #  5 should have been skipped


def test_index_duplicate_files(tmp_path, a_mem_db_session):
    """Handle multiple files with identical SOPInstanceUIDs"""

    # a folder with two files with the same SOPInstanceUID
    export(dataset=CTDatasetFactory(SOPInstanceUID="111"), path=tmp_path / "file1")
    export(dataset=CTDatasetFactory(SOPInstanceUID="111"), path=tmp_path / "file2")
    export(dataset=CTDatasetFactory(SOPInstanceUID="222"), path=tmp_path / "file3")

    index = DICOMIndex.init_from_session(a_mem_db_session)
    for file in tmp_path.glob("*"):
        a_mem_db_session.add_all(
            index.create_new_db_objects(read_dicom_file(file), file)
        )
        a_mem_db_session.commit()

    assert len(index.paths) == 3
    assert len(index.instance_uids) == 2

    new_index = DICOMIndex.init_from_session(a_mem_db_session)
    assert len(new_index.paths) == 3


def test_index_folder(example_dicom_folder, a_db_file):

    with open(example_dicom_folder / "patient1" / "non_dicom.txt", "w") as f:
        f.write("No dicom content!")

    assert len(list(AllFiles(example_dicom_folder))) == 15

    with SQLiteSession(a_db_file) as session:
        stats = index_folder(example_dicom_folder, session)

    assert len(stats.processed()) == 14
    assert len(stats.skipped_non_dicom()) == 1

    with open(example_dicom_folder / "patient1" / "non_dicom2.txt", "w") as f:
        f.write("No dicom content!")

    stats2 = index_folder(example_dicom_folder, session)
    assert len(stats2.processed()) == 0
    assert len(stats2.skipped_non_dicom()) == 1
    assert len(stats2.skipped_visited()) == 15


class SQLiteSession:
    """A database session on a sqlite file

    Examples
    --------
    with SQLiteSession('my_file.sql') as session:
        session.do_things()

    # closes after leaving closure
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.session = None

    def __enter__(self):
        engine = create_engine(f"sqlite:///{self.db_path}", echo=False)
        Base.metadata.create_all(engine, checkfirst=True)  # Create if needed
        self.session = Session(engine)
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()