"""Run through a folder with patient/study/series/ folder structure.
Read 1 dicom file inside each series to index all patients/studies/series in the
folder
"""

from dicomindex.core import DICOMFilePerSeries, DICOMIndex, read_dicom_file
from dicomindex.persistence import SQLiteSession


index_file = "/tmp/archive.sql"
folder_to_index = "/share/dicoms/"

with SQLiteSession("/tmp/archive2.sql") as session:
    index = DICOMIndex.init_from_session(session)
    for count, file in enumerate(DICOMFilePerSeries(folder_to_index)):
        if count > 1:
            break
        to_add = index.create_new_db_objects(read_dicom_file(file), str(file))
        session.add_all(to_add)
        session.commit()
        print(f"{count} - {file}")

    test = 1
