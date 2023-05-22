from collections import namedtuple
from typing import List


class PathStatuses:
    PROCESSED = "PROCESSED"
    SKIPPED_ALREADY_VISITED = "SKIPPED_ALREADY_VISITED"
    SKIPPED_FAILED = "SKIPPED_FAILED"
    SKIPPED_NON_DICOM = "SKIPPED_NON_DICOM"
    VISITED = "VISITED"


PathStatus = namedtuple("PathStatus", ["path", "status"])


class Statistics:
    """Records statistics for a typical dicomindex run"""

    def __init__(self):
        self.status_list: List[PathStatus] = []

    def add(self, path, status):
        self.status_list.append(PathStatus(path=path, status=status))

    def skipped_failed(self):
        return [x for x in self.status_list if x.status == PathStatuses.SKIPPED_FAILED]

    def skipped_non_dicom(self):
        return [
            x for x in self.status_list if x.status == PathStatuses.SKIPPED_NON_DICOM
        ]

    def processed(self):
        return [x for x in self.status_list if x.status == PathStatuses.PROCESSED]

    def skipped(self):
        return [
            x
            for x in self.status_list
            if x.status
            in (
                PathStatuses.SKIPPED_ALREADY_VISITED,
                PathStatuses.SKIPPED_FAILED,
                PathStatuses.SKIPPED_NON_DICOM,
            )
        ]

    def skipped_visited(self):
        return [
            x
            for x in self.status_list
            if x.status == PathStatuses.SKIPPED_ALREADY_VISITED
        ]

    def visited(self):
        return self.status_list

    def summary(self):
        return (
            f"Visited {len(self.visited())}, processed {len(self.processed())},"
            f" skipped {len(self.skipped())} "
            f"(already-visited/non-dicom/failed: {len(self.skipped_visited())}/"
            f"{len(self.skipped_non_dicom())}/"
            f"{len(self.skipped_failed())})"
        )
