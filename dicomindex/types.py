"""Custom SQLAlchemy data types

Translates between pydicom datatypes and sqlalchemy fields.
Mainly just 'flatten complex pydicom data type to string' currently
"""

from datetime import datetime, time

import sqlalchemy.types as types


class DICOMSequence(types.TypeDecorator):
    """Saves pydicom Sequence objects"""

    impl = types.String

    cache_ok = True

    def process_bind_param(self, value, dialect):
        # just flatten and truncate I just want this in db, nothing fancy for now
        if value is None:
            return ""
        else:
            return str(value)[:255]

    def process_result_value(self, value, dialect):
        return value


class DICOMDate(types.TypeDecorator):
    """Dicom date string. Like 20120425 (VR = DA)"""

    impl = types.Date

    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value:
            return datetime.strptime(value, "%Y%m%d")


class DICOMDateTime(types.TypeDecorator):
    """Dicom date string. Like 2012042514350204.123 (VR = DT)"""

    impl = types.DateTime

    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value:
            return datetime.strptime(value, "%Y%m%d%H%M%S")


class DICOMTime(types.TypeDecorator):
    """Dicom date string. Like 14350204.123 (VR = TM)"""

    impl = types.Time

    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value:
            if '.' not in value:  # make strptime below work even without ms
                value = value + '.0'
            return datetime.strptime(value, "%H%M%S.%f").time()


class DICOMFlattenedString(types.TypeDecorator):
    """Converts complex DICOM value to string"""

    impl = types.String

    cache_ok = True

    def process_bind_param(self, value, dialect):
        # just flatten. I just want this in db, nothing fancy for now
        if value is None:
            return ""
        else:
            return str(value)

    def process_result_value(self, value, dialect):
        return value


class DICOMName(DICOMFlattenedString):
    """For DICOM PersonName"""


class DICOMUID(DICOMFlattenedString):
    """For DICOM UniqueIdentifier"""


class DICOMIntegerString(DICOMFlattenedString):
    """For DICOM IntegerString"""


class DICOMMultipleString(DICOMFlattenedString):
    """For DICOM objects with VM (Value Multiplicity) > 1
    """
