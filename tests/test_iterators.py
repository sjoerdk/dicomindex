from dicomindex.iterators import AllDICOMFiles


def test_all_files(example_dicom_folder):
    """Make sure iteration order is correct"""
    files = list(x for x in AllDICOMFiles(example_dicom_folder))
    assert len(files) == 14  # just check basics. I believe the rest
