"""
Create some dicom data presenting a small Patient -> Study -> Series -> instance tree
"""
import uuid
from pathlib import Path

from dicomgenerator.export import export
from dicomgenerator.templates import CTDatasetFactory

from dicomindex.persistence import SQLiteSession
from tests.conftest import set_factory_db_session
from tests.test_persistence import generate_full_stack_patient


def generate_dicom_structure(structure, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for patient, studies in structure.items():
        for study, seriess in studies.items():
            for series in seriess:
                for instance in range(2):
                    count = count + 1
                    export(dataset=CTDatasetFactory(PatientID=patient,
                                                    StudyInstanceUID=study,
                                                    SeriesInstanceUID=series),
                           path=output_dir / str(uuid.uuid4()))

    print(f"Wrote {count} files to {output_dir}")


structure = {'patient1':
                {'11111.1': ['2222.1', '2222.2'],
                 '11111.2': ['2222.1']},
            'patient2':
                {'11111.1': ['2222.1'],
                 '11111.2': ['2222.1', '2222.2', '2222.3']},
            }

#generate_dicom_structure(structure, "/tmp/dummy_dicom")


def write_large_db(a_db_file):
    """Write 1000 dummy patients including studies, series instances"""
    with SQLiteSession(a_db_file) as session:
        set_factory_db_session(session)
        for i in range(1000):
            if i % 10 == 0:
                print(f'{i} of 1000')
            generate_full_stack_patient(f'patient{i}')
            session.commit()

    print(a_db_file)


write_large_db("/tmp/a_db.sql")