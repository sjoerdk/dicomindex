"""Run through a folder with patient/study/series/ folder structure.
Read 1 dicom file inside each series to index all patients/studies/series in the
folder
"""
import logging
from os import environ

from tqdm import tqdm

from dicomindex.processing import index_folder
from dicomindex.persistence import SQLiteSession

index_file = "/tmp/archive.sql"
folder_to_index = environ["FOLDER"]

logging.basicConfig(level=logging.DEBUG)


with SQLiteSession(index_file) as session:
    with tqdm(total=1) as pbar:
        index_folder(base_folder=folder_to_index, session=session, progress_bar=pbar)
