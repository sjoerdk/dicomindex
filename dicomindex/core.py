from collections import defaultdict
from pathlib import Path

import pydicom
from pydicom.errors import InvalidDicomError

from dicomindex.exceptions import DICOMIndexError


def read_dicom_file(path):
    """

    Parameters
    ----------
    path: Path
        Read this path as dicom file

    Raises
    ------
    DICOMIndexError
        When given path cannot be opened as DICOM
    FileNotFoundError
        When given path does not exist

    """
    try:
        return pydicom.filereader.dcmread(str(path), stop_before_pixels=True)
    except InvalidDicomError as e:
        raise DICOMIndexError(e)


def index_dicom_dir(dicom_dir: Path):
    """ Go through dicom dir and build a patient/study/series structure

    Parameters
    ----------
    dicom_dir: Path
        Index this folder

    Returns
    -------

    """
    files = []
    for file in (x for x in dicom_dir.rglob('*') if x.is_file()):
        files.append(file)
        content = read_dicom_file(file)
        instances = [file]
        series = content['SeriesInstanceUID'].value
        study = content['StudyInstanceUID'].value
        patient = content['PatientID'].value

        #TODO: continue





