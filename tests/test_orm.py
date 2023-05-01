from dicomgenerator.templates import CTDatasetFactory
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from dicomindex.orm import Base, Instance, Patient, Series, Study


def test_orm():
    engine = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:

        patient1 = Patient(
            PatientID="Patient1",
            studies=[
                Study(StudyInstanceUID="1111.1"),
                Study(StudyInstanceUID="1111.2"),
            ],
        )
        session.add_all([patient1])
        session.commit()

        studies = session.query(Study).all()
        assert len(studies) == 2


def test_init_from_dataset():
    dataset = CTDatasetFactory()
    instance = Instance.init_from_dataset(dataset, "a_path")
    assert instance.SOPInstanceUID
    assert instance.SeriesInstanceUID
    assert instance.path

    series = Series.init_from_dataset(dataset)
    assert series.SeriesInstanceUID
    assert series.StudyInstanceUID

    study = Study.init_from_dataset(dataset)
    assert study.StudyInstanceUID
    assert study.PatientID

    patient = Patient.init_from_dataset(dataset)
    assert patient.PatientID
