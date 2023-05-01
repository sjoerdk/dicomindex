from typing import List, Optional

from pydicom import Dataset
from sqlalchemy import Float, ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from dicomindex.fields import InstanceLevel, SeriesLevel, StudyLevel
from dicomindex.types import DICOMDate, DICOMDateTime, DICOMIntegerString, \
    DICOMMultipleString, DICOMName, DICOMSequence, DICOMTime, DICOMUID


class Base(DeclarativeBase):
    pass


class Patient(Base):
    __tablename__ = "patient"
    PatientID: Mapped[str] = mapped_column(primary_key=True)
    studies: Mapped[List["Study"]] = relationship(
    back_populates = "patient", cascade = "all, delete-orphan")

    @classmethod
    def init_from_dataset(cls, dataset: Dataset):
        """Try to fill all fields of this model with info from dataset"""
        return cls(PatientID=dataset.PatientID)



class Study(Base):
    __tablename__ = "study"
    StudyInstanceUID: Mapped[str] = mapped_column(primary_key=True)
    PatientID: Mapped[str] = mapped_column(ForeignKey("patient.PatientID"))
    patient: Mapped["Patient"] = relationship(back_populates="studies")
    series: Mapped[List["Series"]] = relationship(back_populates="study",
        cascade="all, delete-orphan")

    # === DICOM fields ===
    StudyDescription: Mapped[Optional[str]] = mapped_column(String(64))
    IssuerOfPatientID: Mapped[Optional[str]] = mapped_column(String(64))
    ReasonForPerformedProcedureCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    PatientBirthTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    SourceApplicationEntityTitle: Mapped[Optional[str]] = mapped_column(String(16))
    StudyDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    ProcedureCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    PatientBirthDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    PatientName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    NumberOfStudyRelatedInstances: Mapped[Optional[str]] = mapped_column(DICOMIntegerString(12))
    InstanceAvailability: Mapped[Optional[str]] = mapped_column(String(16))
    CurrentPatientLocation: Mapped[Optional[str]] = mapped_column(String(64))
    ReferringPhysicianName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    IssuerOfAccessionNumberSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    StudyTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    StudyStatusID: Mapped[Optional[str]] = mapped_column(String(16))
    SOPClassesInStudy: Mapped[Optional[str]] = mapped_column(DICOMUID(128))
    RequestedProcedureID: Mapped[Optional[str]] = mapped_column(String(16))
    OtherPatientIDsSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    ConfidentialityCode: Mapped[Optional[str]] = mapped_column(String(64))
    PatientSex: Mapped[Optional[str]] = mapped_column(String(16))
    ModalitiesInStudy: Mapped[Optional[str]] = mapped_column(DICOMMultipleString(16))
    IssuerOfPatientIDQualifiersSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    StudyID: Mapped[Optional[str]] = mapped_column(String(16))
    NumberOfStudyRelatedSeries: Mapped[Optional[str]] = mapped_column(DICOMIntegerString(12))
    AccessionNumber: Mapped[Optional[str]] = mapped_column(String(16))
    InstitutionName: Mapped[Optional[str]] = mapped_column(String(64))
    InstitutionalDepartmentName: Mapped[Optional[str]] = mapped_column(String(64))

    @classmethod
    def init_from_dataset(cls, dataset: Dataset):
        """Try to fill all fields of this model with info from dataset"""
        fields_to_transfer = StudyLevel.fields.union({'PatientID'})
        return cls(**{tag: dataset.get(tag) for tag in fields_to_transfer})



class Series(Base):
    __tablename__ = "series"
    SeriesInstanceUID: Mapped[str] = mapped_column(primary_key=True)
    StudyInstanceUID: Mapped[str] = mapped_column(ForeignKey("study.StudyInstanceUID"))
    study: Mapped["Study"] = relationship(back_populates="series")
    instances: Mapped[List["Instance"]] = relationship(back_populates="series",
                                                  cascade="all, delete-orphan")

    # === DICOM fields ===
    PerformingPhysicianName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    SeriesDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    PerformedProcedureStepStatus: Mapped[Optional[str]] = mapped_column(String(16))
    SeriesTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    NumberOfSeriesRelatedInstances: Mapped[Optional[str]] = mapped_column(DICOMIntegerString(12))
    PerformedProcedureStepDescription: Mapped[Optional[str]] = mapped_column(String(64))
    SmallestPixelValueInSeries: Mapped[Optional[float]] = mapped_column(Float(4))
    SeriesNumber: Mapped[Optional[str]] = mapped_column(DICOMIntegerString(12))
    SeriesType: Mapped[Optional[str]] = mapped_column(DICOMMultipleString(16))
    Modality: Mapped[Optional[str]] = mapped_column(String(16))
    SeriesDescription: Mapped[Optional[str]] = mapped_column(String(64))
    OperatorsName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    SpatialResolution: Mapped[Optional[str]] = mapped_column(String(32))
    BodyPartExamined: Mapped[Optional[str]] = mapped_column(String(16))
    ProtocolName: Mapped[Optional[str]] = mapped_column(String(64))
    PerformedProcedureStepStartTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    PresentationIntentType: Mapped[Optional[str]] = mapped_column(String(16))
    FrameOfReferenceUID: Mapped[Optional[str]] = mapped_column(DICOMUID(128))
    AnatomicRegionSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    Laterality: Mapped[Optional[str]] = mapped_column(String(16))
    PerformedProcedureStepStartDate: Mapped[Optional[str]] = mapped_column(DICOMDate())

    @classmethod
    def init_from_dataset(cls, dataset: Dataset):
        """Try to fill all fields of this model with info from dataset"""
        fields_to_transfer = SeriesLevel.fields.union({'StudyInstanceUID'})
        return cls(**{tag: dataset.get(tag) for tag in fields_to_transfer})


class Instance(Base):
    __tablename__ = "instance"
    SOPInstanceUID: Mapped[str] = mapped_column(primary_key=True)
    SeriesInstanceUID: Mapped[str] = mapped_column(
        ForeignKey("series.SeriesInstanceUID"))
    series: Mapped["Series"] = relationship(back_populates="instances")
    path: Mapped[str] = mapped_column(String(512))

    # === DICOM fields ===
    BitsAllocated: Mapped[Optional[float]] = mapped_column(Float(4))
    ContentDescription: Mapped[Optional[str]] = mapped_column(String(64))
    PresentationCreationDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    InstitutionAddress: Mapped[Optional[str]] = mapped_column(String(1024))
    ContentTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    TransferSyntaxUID: Mapped[Optional[str]] = mapped_column(DICOMUID(128))
    ConceptNameCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    InstanceNumber: Mapped[Optional[str]] = mapped_column(DICOMIntegerString(12))
    ContentCreatorName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    ContentDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    Rows: Mapped[Optional[float]] = mapped_column(Float(4))
    TimeOfLastCalibration: Mapped[Optional[str]] = mapped_column(DICOMTime())
    NumberOfFrames: Mapped[Optional[str]] = mapped_column(DICOMIntegerString(12))
    SoftwareVersions: Mapped[Optional[str]] = mapped_column(DICOMMultipleString(64))
    Columns: Mapped[Optional[float]] = mapped_column(Float(4))
    DateOfLastCalibration: Mapped[Optional[str]] = mapped_column(DICOMDate())
    PresentationCreationTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    Manufacturer: Mapped[Optional[str]] = mapped_column(String(64))
    ContentLabel: Mapped[Optional[str]] = mapped_column(String(16))
    PixelPaddingValue: Mapped[Optional[float]] = mapped_column(Float(4))
    SOPClassUID: Mapped[Optional[str]] = mapped_column(DICOMUID(128))
    CorrectedImage: Mapped[Optional[str]] = mapped_column(DICOMMultipleString(16))
    ObservationDateTime: Mapped[Optional[str]] = mapped_column(DICOMDateTime())
    ManufacturerModelName: Mapped[Optional[str]] = mapped_column(String(64))
    DeviceSerialNumber: Mapped[Optional[str]] = mapped_column(String(64))
    StationName: Mapped[Optional[str]] = mapped_column(String(16))

    @classmethod
    def init_from_dataset(cls, dataset: Dataset, path: str):
        """Try to fill all fields of this model with info from dataset"""
        fields_to_transfer = InstanceLevel.fields.union({'SeriesInstanceUID'})
        param_dict = {tag: dataset.get(tag) for tag in fields_to_transfer}
        param_dict['path'] = path
        return cls(**param_dict)
