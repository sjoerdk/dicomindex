from dicomindex.orm import Patient, Study
from tests.test_processing import SQLiteSession


def test_write_db(a_db_file):
    """Write some data to a sqlite file, read again. Just checking basics"""

    patient1 = Patient(
        PatientID="Patient1",
        studies=[Study(StudyInstanceUID="1111.1"), Study(StudyInstanceUID="1111.2")],
    )

    with SQLiteSession(a_db_file) as session:
        session.add_all([patient1])
        session.commit()

    with SQLiteSession(a_db_file) as loaded:
        patients = loaded.query(Patient).all()
        assert len(patients[0].studies) == 2
        assert patients[0].studies[0].StudyInstanceUID == "1111.1"
