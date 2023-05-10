class DICOMIndexError(Exception):
    pass


class NotDICOMError(DICOMIndexError):
    def __init__(self, message, path=None):
        self.path = path
        super().__init__(message)
