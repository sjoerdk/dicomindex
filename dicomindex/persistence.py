"""Functions and classes for persisting state"""
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dicomindex.orm import Base


class SQLiteSession:
    """A database session on a sqlite file

    Examples
    --------
    with SQLiteSession('my_file.sql') as session:
        session.do_things()

    # closes after leaving closure
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.session = None

    def __enter__(self):
        engine = create_engine(f"sqlite:///{self.db_path}", echo=False)
        Base.metadata.create_all(engine, checkfirst=True)  # Create if needed
        self.session = Session(engine)
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()


def get_session(db_filename):
    """Returns a session on a anonqa sqlite database in the given file.
    Creates db if it does not exist.

    Returns
    -------
    sqlalchemy.orm.session.Session
        A session on the database in db_filename
    """
    engine = create_engine(f"sqlite:///{db_filename}", echo=False)
    Base.metadata.create_all(engine, checkfirst=True)  # Create if needed
    session = Session(engine)
    yield session
    session.close()
