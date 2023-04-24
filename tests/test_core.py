from _pytest.fixtures import fixture

from dicomindex.core import index_dicom_dir
from tests.factories import generate_dicom_structure


@fixture
def example_dicom_folder(tmp_path):
    """An example folder filled with dicom files. Folder has patient/study/series
    folder structure. Contains 2 files per series
    """
    structure = {'patient1': {'11111.1': ['2222.1', '2222.2'],
                              '11111.2': ['2222.1']},
                 'patient2': {'11111.1': ['2222.1'],
                              '11111.2': ['2222.1', '2222.2', '2222.3']}, }

    dicom_path = tmp_path / "dicomfolder"
    dicom_path.mkdir()
    generate_dicom_structure(structure=structure, output_dir=dicom_path)
    return dicom_path


def test_index_dicom_dir(example_dicom_folder):

    result = index_dicom_dir(example_dicom_folder)
    test = 1
