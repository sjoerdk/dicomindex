"""Custom SQLAlchemy data types"""

import sqlalchemy.types as types


class DICOMSequence(types.TypeDecorator):
    """Saves pydicom Sequence objects.
    """

    impl = types.String

    cache_ok = True

    def process_bind_param(self, value, dialect):
        # just flatten. I just want this in db, nothing fancy for now
        if value is None:
            return ""
        else:
            return str(value)[:255]

    def process_result_value(self, value, dialect):
        return value


class DICOMName(types.TypeDecorator):
    """Saves pydicom PersonName objects.
    """

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
