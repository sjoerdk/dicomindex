from typing import List, Optional

from pydicom import Dataset
from sqlalchemy import Float, ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from dicomindex.fields import InstanceLevel, SeriesLevel, StudyLevel
from dicomindex.types import (
    DICOMDate,
    DICOMDateTime,
    DICOMFlattenedString,
    DICOMIntegerString,
    DICOMMultipleString,
    DICOMName,
    DICOMSequence,
    DICOMTime,
    DICOMUID,
)


class Base(DeclarativeBase):
    pass


class Patient(Base):
    __tablename__ = "patient"
    PatientID: Mapped[str] = mapped_column(primary_key=True)
    studies: Mapped[List["Study"]] = relationship(
        back_populates="patient", cascade="all, delete-orphan"
    )

    @classmethod
    def init_from_dataset(cls, dataset: Dataset):
        """Try to fill all fields of this model with info from dataset"""
        return cls(PatientID=dataset.PatientID)


class Study(Base):
    __tablename__ = "study"
    StudyInstanceUID: Mapped[str] = mapped_column(primary_key=True)
    PatientID: Mapped[str] = mapped_column(ForeignKey("patient.PatientID"))
    patient: Mapped["Patient"] = relationship(back_populates="studies")
    series: Mapped[List["Series"]] = relationship(
        back_populates="study", cascade="all, delete-orphan"
    )

    # === DICOM fields ===
    ReferringPhysicianName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    PatientBirthTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    AccessionNumber: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(16))
    ReasonForPerformedProcedureCodeSequence: Mapped[Optional[str]] = mapped_column(
        DICOMSequence(265)
    )
    SourceApplicationEntityTitle: Mapped[Optional[str]] = mapped_column(
        DICOMFlattenedString(16)
    )
    IssuerOfPatientIDQualifiersSequence: Mapped[Optional[str]] = mapped_column(
        DICOMSequence(265)
    )
    InstanceAvailability: Mapped[Optional[str]] = mapped_column(
        DICOMFlattenedString(16)
    )
    PatientName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    InstitutionName: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(64))
    CurrentPatientLocation: Mapped[Optional[str]] = mapped_column(
        DICOMFlattenedString(64)
    )
    ProcedureCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    IssuerOfPatientID: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(64))
    PatientSex: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(16))
    NumberOfStudyRelatedInstances: Mapped[Optional[str]] = mapped_column(
        DICOMIntegerString(12)
    )
    NumberOfStudyRelatedSeries: Mapped[Optional[str]] = mapped_column(
        DICOMIntegerString(12)
    )
    StudyTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    PatientBirthDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    ConfidentialityCode: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(64))
    InstitutionalDepartmentName: Mapped[Optional[str]] = mapped_column(
        DICOMFlattenedString(64)
    )
    StudyStatusID: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(16))
    ModalitiesInStudy: Mapped[Optional[str]] = mapped_column(DICOMMultipleString(16))
    StudyDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    SOPClassesInStudy: Mapped[Optional[str]] = mapped_column(DICOMUID(128))
    RequestedProcedureID: Mapped[Optional[str]] = mapped_column(
        DICOMFlattenedString(16)
    )
    IssuerOfAccessionNumberSequence: Mapped[Optional[str]] = mapped_column(
        DICOMSequence(265)
    )
    StudyDescription: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(64))
    StudyID: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(16))
    OtherPatientIDsSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))

    @classmethod
    def init_from_dataset(cls, dataset: Dataset):
        """Try to fill all fields of this model with info from dataset"""
        fields_to_transfer = StudyLevel.fields.union({"PatientID"})
        return cls(**{tag: dataset.get(tag) for tag in fields_to_transfer})


class Series(Base):
    __tablename__ = "series"
    SeriesInstanceUID: Mapped[str] = mapped_column(primary_key=True)
    StudyInstanceUID: Mapped[str] = mapped_column(ForeignKey("study.StudyInstanceUID"))
    study: Mapped["Study"] = relationship(back_populates="series")
    instances: Mapped[List["Instance"]] = relationship(
        back_populates="series", cascade="all, delete-orphan"
    )

    # === DICOM fields ===
    Laterality: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(16))
    AnatomicRegionSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    PerformedProcedureStepStartDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    PerformedProcedureStepDescription: Mapped[Optional[str]] = mapped_column(
        DICOMFlattenedString(64)
    )
    SeriesNumber: Mapped[Optional[str]] = mapped_column(DICOMIntegerString(12))
    Modality: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(16))
    PerformedProcedureStepStatus: Mapped[Optional[str]] = mapped_column(
        DICOMFlattenedString(16)
    )
    NumberOfSeriesRelatedInstances: Mapped[Optional[str]] = mapped_column(
        DICOMIntegerString(12)
    )
    BodyPartExamined: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(16))
    SeriesDescription: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(64))
    PerformedProcedureStepStartTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    ProtocolName: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(64))
    PresentationIntentType: Mapped[Optional[str]] = mapped_column(
        DICOMFlattenedString(16)
    )
    SeriesType: Mapped[Optional[str]] = mapped_column(DICOMMultipleString(16))
    SeriesTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    SmallestPixelValueInSeries: Mapped[Optional[float]] = mapped_column(Float(4))
    SpatialResolution: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(32))
    SeriesDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    PerformingPhysicianName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    OperatorsName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    FrameOfReferenceUID: Mapped[Optional[str]] = mapped_column(DICOMUID(128))

    @classmethod
    def init_from_dataset(cls, dataset: Dataset):
        """Try to fill all fields of this model with info from dataset"""
        fields_to_transfer = SeriesLevel.fields.union({"StudyInstanceUID"})
        return cls(**{tag: dataset.get(tag) for tag in fields_to_transfer})


class Instance(Base):
    __tablename__ = "instance"
    SOPInstanceUID: Mapped[str] = mapped_column(primary_key=True)
    SeriesInstanceUID: Mapped[str] = mapped_column(
        ForeignKey("series.SeriesInstanceUID")
    )
    series: Mapped["Series"] = relationship(back_populates="instances")
    path: Mapped[str] = mapped_column(String(512))

    # === DICOM fields ===
    ContentTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    StationName: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(16))
    DateOfLastCalibration: Mapped[Optional[str]] = mapped_column(DICOMDate())
    TransferSyntaxUID: Mapped[Optional[str]] = mapped_column(DICOMUID(128))
    Rows: Mapped[Optional[float]] = mapped_column(Float(4))
    Manufacturer: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(64))
    ContentLabel: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(16))
    ObservationDateTime: Mapped[Optional[str]] = mapped_column(DICOMDateTime())
    ContentDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    SOPClassUID: Mapped[Optional[str]] = mapped_column(DICOMUID(128))
    PresentationCreationDate: Mapped[Optional[str]] = mapped_column(DICOMDate())
    CorrectedImage: Mapped[Optional[str]] = mapped_column(DICOMMultipleString(16))
    InstitutionAddress: Mapped[Optional[str]] = mapped_column(String(1024))
    NumberOfFrames: Mapped[Optional[str]] = mapped_column(DICOMIntegerString(12))
    DeviceSerialNumber: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(64))
    TimeOfLastCalibration: Mapped[Optional[str]] = mapped_column(DICOMTime())
    ContentDescription: Mapped[Optional[str]] = mapped_column(DICOMFlattenedString(64))
    Columns: Mapped[Optional[float]] = mapped_column(Float(4))
    ContentCreatorName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    SoftwareVersions: Mapped[Optional[str]] = mapped_column(DICOMMultipleString(64))
    BitsAllocated: Mapped[Optional[float]] = mapped_column(Float(4))
    ManufacturerModelName: Mapped[Optional[str]] = mapped_column(
        DICOMFlattenedString(64)
    )
    InstanceNumber: Mapped[Optional[str]] = mapped_column(DICOMIntegerString(12))
    PresentationCreationTime: Mapped[Optional[str]] = mapped_column(DICOMTime())
    PixelPaddingValue: Mapped[Optional[float]] = mapped_column(Float(4))
    ConceptNameCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))

    @classmethod
    def init_from_dataset(cls, dataset: Dataset, path: str):
        """Try to fill all fields of this model with info from dataset"""
        fields_to_transfer = InstanceLevel.fields.union({"SeriesInstanceUID"})
        param_dict = {tag: dataset.get(tag) for tag in fields_to_transfer}
        param_dict["path"] = path
        return cls(**param_dict)
