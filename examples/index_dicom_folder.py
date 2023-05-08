"""Run through a folder with patient/study/series/ folder structure.
Read 1 dicom file inside each series to index all patients/studies/series in the
folder
"""

from dicomindex.core import read_dicom_file
from dicomindex.processing import DICOMIndex
from dicomindex.iterators import AllDICOMFiles
from dicomindex.persistence import SQLiteSession


index_file = "/tmp/archive.sql"
folder_to_index = "/folder/with/dicom"

with SQLiteSession("/tmp/archive2.sql") as session:
    index = DICOMIndex.init_from_session(session)
    for count, file in enumerate(AllDICOMFiles(folder_to_index)):
        if count > 1:
            break
        to_add = index.create_new_db_objects(read_dicom_file(file), str(file))

        session.add_all(to_add)
        print(f"{count} - {file}")
