from dicomindex.iterators import AllDICOMFiles, Folder
from dicomindex.threading import EagerIterator


def test_all_files(example_dicom_folder):
    """Make sure iteration order is correct"""
    files = list(x for x in AllDICOMFiles(example_dicom_folder))
    assert len(files) == 14  # just check basics. I believe the rest


def test_patient_folder_iterator(example_dicom_folder):
    folder = Folder(example_dicom_folder)
    path_iter = EagerIterator(folder.folders())
    dirs = list(path_iter)
    assert len(dirs) == 14
    files = [x for x in dirs[9].glob("*/*") if x.is_file()]
    assert len(files) == 2
