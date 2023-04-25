from pathlib import Path
from random import randint

import pytest

from dicomindex.orm import Instance, Patient, Series, Study
from dicomindex.persistence import SQLiteSession, get_session
from tests.conftest import set_factory_db_session
from tests.factories import InstanceFactory, PatientFactory, SeriesFactory, \
    StudyFactory


@pytest.fixture
def a_db_file(tmpdir):
    return Path(tmpdir) / 'a_db_file.db'


def test_write_db(a_db_file):
    """Write some data to a sqlite file, read again. Just checking basics"""

    patient1 = Patient(PatientID='Patient1',
                       studies=[Study(StudyInstanceUID='1111.1'),
                                Study(StudyInstanceUID='1111.2')])

    with SQLiteSession(a_db_file) as session:
        session.add_all([patient1])
        session.commit()

    with SQLiteSession(a_db_file) as loaded:
        patients = loaded.query(Patient).all()
        assert len(patients[0].studies) == 2
        assert patients[0].studies[0].StudyInstanceUID == '1111.1'


def generate_full_stack_patient(patient_id: str):
    """Create a patient containing one or two studies, which contain
    two or three series each, which contain 2 instances each

    Notes
    -----
    Must be called from a db-enabled test which calls
    `tests.conftest.set_factory_db_session()` somewhere to make Factory
    instances write to the db session
    """

    patient = PatientFactory(PatientID=patient_id)
    studies = [StudyFactory(PatientID=patient.PatientID) for _ in range(randint(1, 4))]
    for study in studies:
        seriess = [SeriesFactory(StudyInstanceUID=study.StudyInstanceUID)
                    for _ in range(randint(1, 8))]
        for series in seriess:
            [InstanceFactory(SeriesInstanceUID=series.SeriesInstanceUID)
             for _ in range(randint(1, 10))]

    return patient
