from pathlib import Path
from random import randint

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dicomindex.orm import Base
from tests.factories import InstanceFactory, PatientFactory, SeriesFactory, \
    StudyFactory


@pytest.fixture
def a_db_file(tmpdir):
    return Path(tmpdir) / 'a_db_file.db'


@pytest.fixture
def a_mem_db_session():
    """A self-closing memory-only db session"""
    engine = create_engine(f'sqlite://', echo=False)
    Base.metadata.create_all(engine, checkfirst=True)  # Create if needed
    session = Session(engine)
    yield session
    session.rollback()
    session.close()


def set_factory_db_session(session):
    """Make all db Factory classes use the given session"""
    PatientFactory._meta.sqlalchemy_session = session
    StudyFactory._meta.sqlalchemy_session = session
    SeriesFactory._meta.sqlalchemy_session = session
    InstanceFactory._meta.sqlalchemy_session = session


@pytest.fixture
def use_mem_db_session(a_mem_db_session):
    """Make all SQLAlchemy models use the same memory-only db session"""
    set_factory_db_session(a_mem_db_session)
    return a_mem_db_session


def generate_full_stack_patient(patient_id: str, randomize=False):
    """Create a patient containing one or two studies, which contain
    two or three series each, which contain 2 instances each

    Notes
    -----
    Must be called from a db-enabled test which calls
    `tests.conftest.set_factory_db_session()` somewhere to make Factory
    instances write to the db session
    """
    ranges = {'study': range(2), 'series': range(3),
              'instance': range(5)}
    if randomize:
        ranges = {'study': range(randint(1, 4)),
                  'series': range(randint(1, 8)),
                  'instance': range(randint(1, 10))}

    patient = PatientFactory(PatientID=patient_id)
    studies = [StudyFactory(PatientID=patient.PatientID) for _ in ranges['study']]
    for study in studies:
        seriess = [SeriesFactory(StudyInstanceUID=study.StudyInstanceUID)
                    for _ in ranges['series']]
        for series in seriess:
            [InstanceFactory(SeriesInstanceUID=series.SeriesInstanceUID)
             for _ in ranges['instance']]

    return patient
