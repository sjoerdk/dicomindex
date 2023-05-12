"""Run through a folder with patient/study/series/ folder structure.
Read 1 dicom file inside each series to index all patients/studies/series in the
folder
"""
from os import environ

from tqdm import tqdm

from dicomindex.core import read_dicom_file
from dicomindex.processing import DICOMIndex, index_folder
from dicomindex.iterators import AllDICOMFiles
from tests.test_processing import SQLiteSession

index_file = "/tmp/archive.sql"
folder_to_index = environ["FOLDER"]

with SQLiteSession(index_file) as session:
    with tqdm(total=1) as pbar:
        index_folder(base_folder=folder_to_index, session=session, progress_bar=pbar)