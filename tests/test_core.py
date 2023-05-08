from _pytest.fixtures import fixture

from dicomindex.core import DICOMIndex, read_dicom_file
from dicomindex.iterators import AllDICOMFiles
from dicomindex.orm import Instance, Patient
from dicomindex.persistence import SQLiteSession
from tests.conftest import generate_full_stack_patient
from tests.factories import generate_dicom_file_structure


@fixture
def example_dicom_folder(tmp_path):
    """An example folder filled with dicom files. Folder has patient/study/series
    folder structure. Contains 2 files per series
    """
    structure = {
        "patient1": {"11111.1": ["2222.1", "2222.2"], "11111.2": ["2222.1"]},
        "patient2": {"11111.1": ["2222.1"], "11111.2": ["2222.1", "2222.2", "2222.3"]},
    }

    dicom_path = tmp_path / "dicomfolder"
    dicom_path.mkdir()
    generate_dicom_file_structure(structure=structure, output_dir=dicom_path)
    return dicom_path


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
