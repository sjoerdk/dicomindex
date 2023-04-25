import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dicomindex.orm import Base
from tests.factories import InstanceFactory, PatientFactory, SeriesFactory, \
    StudyFactory

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
