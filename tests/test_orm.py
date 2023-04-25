from select import select

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dicomindex.orm import Base, Patient, Series, Study


def test_orm():
    engine = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:

        patient1 = Patient(PatientID='Patient1', studies=[
            Study(StudyInstanceUID='1111.1'),
            Study(StudyInstanceUID='1111.2')
        ])
        session.add_all([patient1])
        session.commit()
        test = 1

        studies = session.query(Patient).all()
        test = 1
