import uuid
from pathlib import Path

from dicomgenerator.export import export
from dicomgenerator.templates import CTDatasetFactory


def generate_dicom_structure(structure, output_dir):
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

