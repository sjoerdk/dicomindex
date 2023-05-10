from dicomindex.exceptions import NotDICOMError
from dicomindex.iterators import AllFiles
from dicomindex.threading import DICOMDatasetOpener, EagerIterator, FileProcessor


def test_eager_iterator():
    iterator = EagerIterator(iter(range(100)))
    assert len(iterator) == 0
    items = [x for x in iterator]
    assert len(iterator) == 100
    assert len(items) == 100


def test_eager_iterator_context():
    with EagerIterator(iter(range(100))) as iterator:
        assert len(iterator) == 0
        items = [x for x in iterator]
        assert len(iterator) == 100
        assert len(items) == 100


def test_dicom_data_set_opener(example_dicom_folder, a_mem_db_session):
    # Create a file that cannot be opened
    with open(example_dicom_folder / "patient1" / "non_dicom.txt", "w") as f:
        f.write("No dicom content!")

    path_iter = AllFiles(example_dicom_folder)
    futures = [x for x in DICOMDatasetOpener(path_iter=path_iter)]

    results = []
    errors = []
    for future in futures:
        try:
            results.append(future.result())
        except NotDICOMError:
            errors.append(future)
    assert len(results) == 14
    assert len(errors) == 1


def test_file_processor():
    def do_work(var):
        return f"{var}processed"

    results = [
        x
        for x in FileProcessor(
            path_generator=iter(str(x) for x in range(10)), process_function=do_work
        )
    ]

    assert len(results) == 10
