from dicomindex.cli import main
from dicomindex.processing import index_folder
from click.testing import CliRunner

from tests.test_processing import SQLiteSession


def test_cli_base():
    """Just invoking root cli command should not crash"""
    runner = CliRunner()
    assert runner.invoke(main).exit_code == 0


def test_index_folder(example_dicom_folder, a_db_file):
    with open(example_dicom_folder / "patient1" / "non_dicom.txt", "w") as f:
        f.write("No dicom content!")

    with SQLiteSession(a_db_file) as session:
        stats = index_folder(example_dicom_folder, session)

    assert len(stats.processed()) == 14
