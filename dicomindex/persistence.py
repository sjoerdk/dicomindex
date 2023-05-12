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
