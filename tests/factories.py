import uuid
from pathlib import Path

import factory
from dicomgenerator.export import export
from dicomgenerator.generators import DICOMVRProvider
from dicomgenerator.templates import CTDatasetFactory

from dicomindex.orm import Instance, Patient, Series, Study


def generate_dicom_file_structure(structure, output_dir):
    """Generate dicom files corresponding to given Patient/study/series
    stucture. Generate 2 files per series.

    Parameters
    ----------
    structure: Dict[str,Dict[str,List[str]]]
        Patient/study/series structure to generate. Like
        'Patient1':['Study1':['series1','series2']
    output_dir: Path
        Write files to this directory in /patient/study/series folder structure

    Returns
    -------
    None

    """

    output_dir = Path(output_dir)

    count = 0
    for patient, studies in structure.items():
        for study, seriess in studies.items():
            for series in seriess:
                for instance in range(2):
                    count = count + 1
                    output_dir_full = output_dir / patient / study / series
                    output_dir_full.mkdir(parents=True, exist_ok=True)
                    export(dataset=CTDatasetFactory(PatientID=patient,
                                                    StudyInstanceUID=study,
                                                    SeriesInstanceUID=series),
                           path=output_dir_full / str(
                               uuid.uuid4()))


# used to generate DICOM uids and dates
factory.Faker.add_provider(DICOMVRProvider)


class PatientFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Patient
        sqlalchemy_session_persistence = "commit"

    PatientID = factory.Sequence(lambda n: f"Patient{n}")


class StudyFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Study
        sqlalchemy_session_persistence = "commit"

    StudyInstanceUID = factory.Faker("dicom_ui")
    PatientID = factory.SubFactory(PatientFactory)


class SeriesFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Series
        sqlalchemy_session_persistence = "commit"

    SeriesInstanceUID = factory.Faker("dicom_ui")
    StudyInstanceUID = factory.SubFactory(StudyFactory)


class InstanceFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = Instance
        sqlalchemy_session_persistence = "commit"

    SOPInstanceUID = factory.Faker("dicom_ui")
    SeriesInstanceUID = factory.SubFactory(SeriesFactory)
    path = factory.Sequence(lambda n: f"/tmp/some/path/file{n}")



