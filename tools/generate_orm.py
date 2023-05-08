"""Generate orm.py. Requires templates/orm.py.j2

dicomindex saves the values of dicom elements to database. To do this is needs to
represent the sometimes complex datatypes that come out of pydicom in a simpler
database structure.

The conversion is controlled by custom field types in dicomindex/fields.py
orm.py maps each dicom element to a sqlachemy field. Standard if possible, custom if
needed.

The type of field that should be used is determined by the DICOM element VR
(Value Representation). Creating and modifying this mapping by hand would be
tedious and error-prone. So it's generated here.
"""

from dicomgenerator.dicom import VRs
from jinja2 import Template
from pydicom.datadict import dictionary_VM, dictionary_VR
from pydicom.tag import Tag

from dicomindex.fields import InstanceLevel, SeriesLevel, StudyLevel
from tools import TEMPLATE_PATH


def tag_to_vr(tag_name):
    """Go from tag name like 'PatientID' to its Value Representation (VR).
    VR is a DICOM term to indicate Data type (long, int, bytes, etc.)
    """
    vr_name = get_vr(tag_name)
    if " or " in vr_name:
        # two ore more vrs are registered for this tag. Just take the first.
        # why are there two anyway? That's silly.
        vr_name = vr_name.split(" or ")[0]
    try:
        vr = VRs.short_name_to_vr(vr_name)
    except ValueError as e:
        raise ValueError(f"Error getting vr for {tag_name}: {e}") from e
    return vr


def tag_to_sqlalchemy(tag_name: str):  # noqa: C901  (Too complex. But DICOM..)
    """Given a DICOM element tag like 'PatientID', Give python code that will
    create a SQLAlchemy model field to hold that element's value

    """
    vr = tag_to_vr(tag_name)
    vm = get_vm(tag_name)

    def get_string_field(length, vm):
        """Some dicom fields have multiplicity > 1 which means their pydicom type
        will be a list. These need special handling to get them into db

        Notes
        -----
        Regular string-type DICOM parameters are not cast to regular string, but
        rather to cast to DICOMFlattenedString. The later does an explicit str()
        conversion. This is because real-life DICOM cannot be trusted to conform
        to VM.
        """
        if vm == "1":
            return f"DICOMFlattenedString({length})"
        elif vm == "1-n":
            return f"DICOMMultipleString({length})"
        elif int(vm) > 1:
            return f"DICOMMultipleString({length})"
        else:
            raise ValueError(f'Unknown VM value "{vm}"')

    if vr == VRs.ApplicationEntity:
        return (
            f"{tag_name}: Mapped[Optional[str]] = "
            f"mapped_column({get_string_field(16,vm)})"
        )
    elif vr == VRs.AgeString:
        return (
            f"{tag_name}: Mapped[Optional[str]] = "
            f"mapped_column({get_string_field(4,vm)})"
        )
    elif vr == VRs.AttributeTag:
        return (
            f"{tag_name}: Mapped[Optional[str]] = "
            f"mapped_column({get_string_field(4,vm)})"
        )
    elif vr == VRs.CodeString:
        return (
            f"{tag_name}: Mapped[Optional[str]] = "
            f"mapped_column({get_string_field(16,vm)})"
        )
    elif vr == VRs.Date:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(DICOMDate())"
    elif vr == VRs.DecimalString:
        return (
            f"{tag_name}: Mapped[Optional[str]] = "
            f"mapped_column({get_string_field(32,vm)})"
        )
    elif vr == VRs.DateTime:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(DICOMDateTime())"
    elif vr == VRs.FloatingPointSingle:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(4))"
    elif vr == VRs.FloatingPointDouble:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(8))"
    elif vr == VRs.IntegerString:
        return (
            f"{tag_name}: Mapped[Optional[str]] = "
            f"mapped_column(DICOMIntegerString(12))"
        )
    elif vr == VRs.LongString:
        return (
            f"{tag_name}: Mapped[Optional[str]] = "
            f"mapped_column({get_string_field(64,vm)})"
        )
    elif vr == VRs.LongText:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(10240))"
    elif vr == VRs.OtherByteString:
        raise ValueError(f"I dont want to put bytestring {tag_name} into a db. Sorry")
    elif vr == VRs.OtherDoubleString:
        raise ValueError(f"I dont want to put this {tag_name} into a db. Sorry")
    elif vr == VRs.OtherFloatString:
        raise ValueError(f"I dont want to put this {tag_name} into a db. Sorry")
    elif vr == VRs.OtherWordString:
        raise ValueError(f"I dont want to put this {tag_name} into a db. Sorry")
    elif vr == VRs.PersonName:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(DICOMName(192))"
    elif vr == VRs.ShortString:
        return (
            f"{tag_name}: Mapped[Optional[str]] = "
            f"mapped_column({get_string_field(16,vm)})"
        )
    elif vr == VRs.SignedLong:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(8))"
    elif vr == VRs.Sequence:
        return (
            f"{tag_name}: Mapped[Optional[str]] = " f"mapped_column(DICOMSequence(265))"
        )
    elif vr == VRs.SignedShort:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(4))"
    elif vr == VRs.ShortText:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(1024))"
    elif vr == VRs.Time:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(DICOMTime())"
    elif vr == VRs.UniqueIdentifier:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(DICOMUID(128))"
    elif vr == VRs.UnsignedLong:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(4))"
    elif vr == VRs.Unknown:
        raise ValueError(f"I dont want to put this {tag_name} into a db. Sorry")
    elif vr == VRs.UnsignedShort:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(4))"
    elif vr == VRs.UnlimitedText:
        raise ValueError(f"I dont want to put this {tag_name} into a db. Sorry")
    else:
        raise ValueError(
            f"I dont know how to generate code for" f" {vr}, the VR of '{tag_name}'"
        )


def generate_study_fields():
    """Generate code for Patient level fields in orm.Patient()"""
    fields = StudyLevel()
    skip = ["StudyInstanceUID", "PatientID"]  # already in model
    return [tag_to_sqlalchemy(tag) for tag in fields.fields if tag not in skip]


def generate_series_fields():
    """Generate code for Patient level fields in orm.Patient()"""
    fields = SeriesLevel()
    skip = ["StudyInstanceUID", "SeriesInstanceUID"]  # already in model
    return [tag_to_sqlalchemy(tag) for tag in fields.fields if tag not in skip]


def generate_instance_fields():
    """Generate code for Patient level fields in orm.Patient()"""
    fields = InstanceLevel()
    skip = ["SeriesInstanceUID", "SOPInstanceUID"]  # already in model
    return [tag_to_sqlalchemy(tag) for tag in fields.fields if tag not in skip]


def get_vr(tag_name):
    """Find the correct Value Representation for this tag from pydicom"""
    return dictionary_VR(Tag(tag_name))


def get_vm(tag_name):
    """Find the correct Value Multiplicity for this tag from pydicom"""
    return dictionary_VM(Tag(tag_name))


if __name__ == "__main__":
    """Generate the content of dicomindex/orm.py"""
    with open(TEMPLATE_PATH / "orm.py.j2") as f:
        template = Template(f.read())
    content = template.render(
        study_fields=generate_study_fields(),
        series_fields=generate_series_fields(),
        instance_fields=generate_instance_fields(),
    )

    print(content)
