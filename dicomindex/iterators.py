"""Methods and classes for iterating over dicom files"""
from pathlib import Path
from typing import Type

from pydicom.misc import is_dicom
from dicomindex.core import (
    DICOMFileIterator,
    RootPathLevel,
    RootPathLevels,
    read_dicom_file,
)
from dicomindex.exceptions import DICOMIndexError


class AllFiles(DICOMFileIterator):
    def __init__(self, path):
        """Path for each file in path recursively"""
        self.path = Path(path)

    def __iter__(self):
        return iter(file for file in (x for x in self.path.rglob("*") if x.is_file()))


class AllDICOMFiles(DICOMFileIterator):
    def __init__(self, path):
        """Path for each DICOM file in path recursively"""
        self.path = Path(path)

    def __iter__(self):
        return iter(
            file
            for file in (x for x in self.path.rglob("*") if x.is_file() and is_dicom(x))
        )


class AllDICOMDatasets(DICOMFileIterator):
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


class DICOMFilePerSeries(DICOMFileIterator):
    def __init__(
        self, root_path: str, root_level: Type[RootPathLevel] = RootPathLevels.archive
    ):
        """One DICOM file per series in a patient/study/series structured
        folder

        Can iterate over an archive much quicker

        Parameters
        ----------
        root_path:
            Iterate over this path
        root_level: RootPathLevel, optional
            Is root path an archive (recurse through patient folders), a single
            patient (recurse through study folders) or a series?
            Defaults to archive.

        Notes
        -----
        This method requires the archive to be in a strict patient/study/series/files
        folder structure. Any dicom files outside this structure will be ignored.
        """
        self.root_path = Path(root_path)
        self.root_level = root_level

    def __iter__(self):
        series_paths = iter(
            x for x in self.root_path.glob(self.root_level.series_glob) if x.is_dir()
        )
        for series_path in series_paths:
            try:
                yield next(
                    x for x in series_path.glob("*") if x.is_file() and is_dicom(x)
                )
            except StopIteration:
                continue
