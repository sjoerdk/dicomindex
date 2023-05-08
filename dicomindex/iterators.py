"""Methods and classes for iterating over dicom files"""
from pathlib import Path

from pydicom.misc import is_dicom
from dicomindex.core import (
    read_dicom_file,
)
from dicomindex.exceptions import DICOMIndexError


class AllFiles:
    def __init__(self, path):
        """Path for each file in path recursively"""
        self.path = Path(path)

    def __iter__(self):
        return iter(file for file in (x for x in self.path.rglob("*") if x.is_file()))


class AllDICOMFiles:
    def __init__(self, path):
        """Path for each DICOM file in path recursively"""
        self.path = Path(path)

    def __iter__(self):
        return iter(
            file
            for file in (x for x in self.path.rglob("*") if x.is_file() and is_dicom(x))
        )


class AllDICOMDatasets:
    def __init__(self, path):
        """Dataset for each DICOM file in path recursively

        On slower filesystems this is faster than first getting AllDICOMFiles and
        then opening each file, because AllDICOMFiles does a (partial) read already.
        The reads are the expensive part.
        """
        self.path = Path(path)

    def __iter__(self):
        for x in (y for y in self.path.rglob("*") if y.is_file()):
            try:
                yield read_dicom_file(x)
            except DICOMIndexError:
                continue  # ignore error
