import logging
import uuid

from dicomgenerator.export import export
from dicomgenerator.templates import CTDatasetFactory

from dicomindex.core import read_dicom_file
from dicomindex.persistence import SQLiteSession
from dicomindex.processing import (
    DICOMIndex,
    index_folder_full,
    index_one_file_per_folder,
)
from dicomindex.iterators import AllDICOMFiles, AllFiles
from dicomindex.orm import Instance, Patient
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
        stats = index_folder_full(example_dicom_folder, session)

    assert len(stats.processed()) == 14
    assert len(stats.skipped_non_dicom()) == 1

    with open(example_dicom_folder / "patient1" / "non_dicom2.txt", "w") as f:
        f.write("No dicom content!")

    stats2 = index_folder_full(example_dicom_folder, session)
    assert len(stats2.processed()) == 0
    assert len(stats2.skipped_non_dicom()) == 1
    assert len(stats2.skipped_visited()) == 15


def test_index_one_file_per_folder(example_dicom_folder, a_mem_db_session, caplog):
    caplog.set_level(logging.DEBUG)
    stats = index_one_file_per_folder(example_dicom_folder, a_mem_db_session)
    assert len(stats.status_list) == 13


def test_index_one_file_per_folder_skip(example_dicom_folder, a_mem_db_session, caplog):
    """When appending to existing index, skip entire folder if there is one correctly
    processed file in that folder
    """
    caplog.set_level(logging.DEBUG)
    stats = index_one_file_per_folder(example_dicom_folder, a_mem_db_session)
    assert len(stats.status_list) == 13

    # Index again, This should skip all folders as all were processed before
    caplog.clear()
    stats = index_one_file_per_folder(example_dicom_folder, a_mem_db_session)
    assert len(stats.processed()) == 0


def test_index_folder_missing_tags(a_mem_db_session, tmp_path):
    """DICOM Tags used in processing might not be there"""
    dicom_dir = tmp_path / "dicom"
    dicom_dir.mkdir(parents=True)
    ds = CTDatasetFactory(PatientID="a_patient")
    del ds["StudyInstanceUID"]
    export(ds, path=(dicom_dir / str(uuid.uuid4())))

    stats = index_folder_full(dicom_dir, a_mem_db_session)
    assert len(stats.processed()) == 1
