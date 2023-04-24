"""
Create some dicom data presenting a small Patient -> Study -> Series -> instance tree
"""
import uuid
from pathlib import Path

from dicomgenerator.export import export
from dicomgenerator.templates import CTDatasetFactory


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

generate_dicom_structure(structure, "/tmp/dummy_dicom")
