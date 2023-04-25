"""Generate pyton code to store DICOM fields for any valid DICOM field
you throw at it. Saves headache
"""
from dicomgenerator.dicom import VRs
from pydicom.datadict import dictionary_VR
from pydicom.tag import Tag

from dicomindex.fields import InstanceLevel, SeriesLevel, StudyLevel


def tag_to_sqlalchemy(tag_name: str):
    """Given a DICOM element tag like 'PatientID', Give python code that will
    create a SQLAlchemy model field to hold that element's value

    """
    vr_name = get_vr(tag_name)
    if " or " in vr_name:
        # two ore more vrs are registered for this tag. Just take the first.
        # why are there two anyway? That's silly.
        vr_name = vr_name.split(' or ')[0]
    try:
        vr = VRs.short_name_to_vr(vr_name)
    except ValueError as e:
        raise ValueError(f'Error getting vr for {tag_name}: {e}')

    if vr == VRs.ApplicationEntity:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(16))"
    elif vr == VRs.AgeString:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(4))"
    elif vr == VRs.AttributeTag:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(4))"
    elif vr == VRs.CodeString:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(16))"
    elif vr == VRs.Date:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(8))"
    elif vr == VRs.DecimalString:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(32))"
    elif vr == VRs.DateTime:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(36))"
    elif vr == VRs.FloatingPointSingle:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(4))"
    elif vr == VRs.FloatingPointDouble:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(8))"
    elif vr == VRs.IntegerString:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(12))"
    elif vr == VRs.LongString:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(64))"
    elif vr == VRs.LongText:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(10240))"
    elif vr == VRs.OtherByteString:
        raise ValueError(f'I dont want to put bytestring {tag_name} into a db. Sorry')
    elif vr == VRs.OtherDoubleString:
        raise ValueError(f'I dont want to put this {tag_name} into a db. Sorry')
    elif vr == VRs.OtherFloatString:
        raise ValueError(f'I dont want to put this {tag_name} into a db. Sorry')
    elif vr == VRs.OtherWordString:
        raise ValueError(f'I dont want to put this {tag_name} into a db. Sorry')
    elif vr == VRs.PersonName:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(192))"
    elif vr == VRs.ShortString:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(16))"
    elif vr == VRs.SignedLong:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(8))"
    elif vr == VRs.Sequence:
        # read and write sequences as flat text. Worry later
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(265))"
    elif vr == VRs.SignedShort:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(4))"
    elif vr == VRs.ShortText:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(1024))"
    elif vr == VRs.Time:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(16))"
    elif vr == VRs.UniqueIdentifier:
        return f"{tag_name}: Mapped[Optional[str]] = mapped_column(String(128))"
    elif vr == VRs.UnsignedLong:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(4))"
    elif vr == VRs.Unknown:
        raise ValueError(f'I dont want to put this {tag_name} into a db. Sorry')
    elif vr == VRs.UnsignedShort:
        return f"{tag_name}: Mapped[Optional[float]] = mapped_column(Float(4))"
    elif vr == VRs.UnlimitedText:
        raise ValueError(f'I dont want to put this {tag_name} into a db. Sorry')
    else:
        raise ValueError(f"I dont know how to generate code for"
                         f" {vr}, the VR of '{tag_name}'")


def generate_study_fields():
    """Generate code for Patient level fields in orm.Patient()"""
    fields = StudyLevel()
    skip = ['StudyInstanceUID', 'PatientID']  # already in model
    code = [tag_to_sqlalchemy(tag) for tag in fields.fields if tag not in skip]
    for line in code:
        print(line)


def generate_series_fields():
    """Generate code for Patient level fields in orm.Patient()"""
    fields = SeriesLevel()
    skip = ['StudyInstanceUID', 'SeriesInstanceUID']  # already in model
    code = [tag_to_sqlalchemy(tag) for tag in fields.fields if tag not in skip]
    for line in code:
        print(line)


def generate_instance_fields():
    """Generate code for Patient level fields in orm.Patient()"""
    fields = InstanceLevel()
    skip = ['SeriesInstanceUID', 'SOPInstanceUID']  # already in model
    code = [tag_to_sqlalchemy(tag) for tag in fields.fields if tag not in skip]
    for line in code:
        print(line)


def get_vr(tag_name):
    """Find the correct Value Representation for this tag from pydicom"""
    return dictionary_VR(Tag(tag_name))


if __name__ == '__main__':
    #generate_study_fields()
    #generate_series_fields()
    generate_instance_fields()
