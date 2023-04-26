from typing import List, Optional

from pydicom import Dataset
from sqlalchemy import Float, ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from dicomindex.fields import InstanceLevel, SeriesLevel, StudyLevel
from dicomindex.types import DICOMName, DICOMSequence


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
    NumberOfStudyRelatedInstances: Mapped[Optional[str]] = mapped_column(String(12))
    PatientSex: Mapped[Optional[str]] = mapped_column(String(16))
    RequestedProcedureID: Mapped[Optional[str]] = mapped_column(String(16))
    ProcedureCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    StudyStatusID: Mapped[Optional[str]] = mapped_column(String(16))
    NumberOfStudyRelatedSeries: Mapped[Optional[str]] = mapped_column(String(12))
    IssuerOfAccessionNumberSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    ReasonForPerformedProcedureCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    IssuerOfPatientID: Mapped[Optional[str]] = mapped_column(String(64))
    PatientBirthDate: Mapped[Optional[str]] = mapped_column(String(16))
    StudyDescription: Mapped[Optional[str]] = mapped_column(String(64))
    InstanceAvailability: Mapped[Optional[str]] = mapped_column(String(16))
    SOPClassesInStudy: Mapped[Optional[str]] = mapped_column(String(128))
    IssuerOfPatientIDQualifiersSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    ConfidentialityCode: Mapped[Optional[str]] = mapped_column(String(64))
    StudyDate: Mapped[Optional[str]] = mapped_column(String(16))
    SourceApplicationEntityTitle: Mapped[Optional[str]] = mapped_column(String(16))
    PatientBirthTime: Mapped[Optional[str]] = mapped_column(String(16))
    InstitutionalDepartmentName: Mapped[Optional[str]] = mapped_column(String(64))
    InstitutionName: Mapped[Optional[str]] = mapped_column(String(64))
    AccessionNumber: Mapped[Optional[str]] = mapped_column(String(16))
    OtherPatientIDsSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    PatientName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    StudyID: Mapped[Optional[str]] = mapped_column(String(16))
    CurrentPatientLocation: Mapped[Optional[str]] = mapped_column(String(64))
    ModalitiesInStudy: Mapped[Optional[str]] = mapped_column(String(16))
    ReferringPhysicianName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    StudyTime: Mapped[Optional[str]] = mapped_column(String(16))

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
    FrameOfReferenceUID: Mapped[Optional[str]] = mapped_column(String(128))
    ProtocolName: Mapped[Optional[str]] = mapped_column(String(64))
    NumberOfSeriesRelatedInstances: Mapped[Optional[str]] = mapped_column(String(12))
    PerformedProcedureStepStatus: Mapped[Optional[str]] = mapped_column(String(16))
    SeriesType: Mapped[Optional[str]] = mapped_column(String(16))
    OperatorsName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    SeriesDescription: Mapped[Optional[str]] = mapped_column(String(64))
    SeriesTime: Mapped[Optional[str]] = mapped_column(String(16))
    SeriesDate: Mapped[Optional[str]] = mapped_column(String(16))
    PerformedProcedureStepStartDate: Mapped[Optional[str]] = mapped_column(String(16))
    SmallestPixelValueInSeries: Mapped[Optional[float]] = mapped_column(Float(4))
    BodyPartExamined: Mapped[Optional[str]] = mapped_column(String(16))
    PresentationIntentType: Mapped[Optional[str]] = mapped_column(String(16))
    Laterality: Mapped[Optional[str]] = mapped_column(String(16))
    SeriesNumber: Mapped[Optional[str]] = mapped_column(String(12))
    SpatialResolution: Mapped[Optional[str]] = mapped_column(String(32))
    PerformedProcedureStepStartTime: Mapped[Optional[str]] = mapped_column(String(16))
    PerformingPhysicianName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    AnatomicRegionSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    PerformedProcedureStepDescription: Mapped[Optional[str]] = mapped_column(String(64))
    Modality: Mapped[Optional[str]] = mapped_column(String(16))

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
    PresentationCreationTime: Mapped[Optional[str]] = mapped_column(String(16))
    TransferSyntaxUID: Mapped[Optional[str]] = mapped_column(String(128))
    InstanceNumber: Mapped[Optional[str]] = mapped_column(String(12))
    ManufacturerModelName: Mapped[Optional[str]] = mapped_column(String(64))
    NumberOfFrames: Mapped[Optional[str]] = mapped_column(String(12))
    ConceptNameCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    ContentLabel: Mapped[Optional[str]] = mapped_column(String(16))
    ObservationDateTime: Mapped[Optional[str]] = mapped_column(String(32))
    SOPClassUID: Mapped[Optional[str]] = mapped_column(String(128))
    Columns: Mapped[Optional[float]] = mapped_column(Float(4))
    DateOfLastCalibration: Mapped[Optional[str]] = mapped_column(String(16))
    CorrectedImage: Mapped[Optional[str]] = mapped_column(String(16))
    InstitutionAddress: Mapped[Optional[str]] = mapped_column(String(1024))
    StationName: Mapped[Optional[str]] = mapped_column(String(16))
    SoftwareVersions: Mapped[Optional[str]] = mapped_column(String(64))
    TimeOfLastCalibration: Mapped[Optional[str]] = mapped_column(String(16))
    DeviceSerialNumber: Mapped[Optional[str]] = mapped_column(String(64))
    ContentCreatorName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    PresentationCreationDate: Mapped[Optional[str]] = mapped_column(String(16))
    ContentTime: Mapped[Optional[str]] = mapped_column(String(16))
    ContentDate: Mapped[Optional[str]] = mapped_column(String(16))
    BitsAllocated: Mapped[Optional[float]] = mapped_column(Float(4))
    Rows: Mapped[Optional[float]] = mapped_column(Float(4))
    PixelPaddingValue: Mapped[Optional[float]] = mapped_column(Float(4))
    ContentDescription: Mapped[Optional[str]] = mapped_column(String(64))
    Manufacturer: Mapped[Optional[str]] = mapped_column(String(64))

    @classmethod
    def init_from_dataset(cls, dataset: Dataset, path: str):
        """Try to fill all fields of this model with info from dataset"""
        fields_to_transfer = InstanceLevel.fields.union({'SeriesInstanceUID'})
        param_dict = {tag: dataset.get(tag) for tag in fields_to_transfer}
        param_dict['path'] = path
        return cls(**param_dict)
