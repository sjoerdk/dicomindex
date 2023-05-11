from os import environ

from stopwatch import Stopwatch
from tqdm import tqdm

from dicomindex.core import read_dicom_file
from dicomindex.iterators import AllDICOMDatasets, AllDICOMFiles
from dicomindex.threading import AllDICOMDatasetsOpener, var_len_tqdm

folder_to_index = environ["FOLDER"]


def run_test(iterator, tqdm_func=tqdm):
    stopwatch = Stopwatch()

    def myprint(msg):
        print(f"{stopwatch.elapsed} : {msg}")

    stopwatch.start()
    myprint("start")
    for _ in tqdm_func(iterator):
        pass
    myprint("done")


print("Find dicom files by quick check, then read_dicom")
all_dicom_datasets = iter(read_dicom_file(x) for x in AllDICOMFiles(folder_to_index))
run_test(all_dicom_datasets)

print("Find all files, then just read dicom and catch exceptions if not dicom")
run_test(AllDICOMDatasets(folder_to_index))

print("Use multithreading")
run_test(AllDICOMDatasetsOpener(folder_to_index), tqdm_func=var_len_tqdm)
