class DICOMIndexError(Exception):
    pass


class NotDICOMError(DICOMIndexError):
    pass


class NoObjectIDFoundError(DICOMIndexError):
    pass
