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
    ConfidentialityCode: Mapped[Optional[str]] = mapped_column(String(64))
    StudyTime: Mapped[Optional[str]] = mapped_column(String(16))
    StudyStatusID: Mapped[Optional[str]] = mapped_column(String(16))
    SourceApplicationEntityTitle: Mapped[Optional[str]] = mapped_column(String(16))
    SOPClassesInStudy: Mapped[Optional[str]] = mapped_column(String(128))
    IssuerOfPatientIDQualifiersSequence: Mapped[Optional[str]] = mapped_column(
        String(265))
    NumberOfStudyRelatedInstances: Mapped[Optional[str]] = mapped_column(String(12))
    OtherPatientIDsSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    PatientName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    PatientSex: Mapped[Optional[str]] = mapped_column(String(16))
    CurrentPatientLocation: Mapped[Optional[str]] = mapped_column(String(64))
    ProcedureCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    IssuerOfPatientID: Mapped[Optional[str]] = mapped_column(String(64))
    ReferringPhysicianName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    NumberOfStudyRelatedSeries: Mapped[Optional[str]] = mapped_column(String(12))
    StudyDate: Mapped[Optional[str]] = mapped_column(String(8))
    InstanceAvailability: Mapped[Optional[str]] = mapped_column(String(16))
    PatientBirthDate: Mapped[Optional[str]] = mapped_column(String(8))
    AccessionNumber: Mapped[Optional[str]] = mapped_column(String(16))
    PatientBirthTime: Mapped[Optional[str]] = mapped_column(String(16))
    InstitutionalDepartmentName: Mapped[Optional[str]] = mapped_column(String(64))
    IssuerOfAccessionNumberSequence: Mapped[Optional[str]] = mapped_column(
        String(265))
    StudyDescription: Mapped[Optional[str]] = mapped_column(String(64))
    RequestedProcedureID: Mapped[Optional[str]] = mapped_column(String(16))
    StudyID: Mapped[Optional[str]] = mapped_column(String(16))
    ReasonForPerformedProcedureCodeSequence: Mapped[Optional[str]] = mapped_column(
        String(265))
    ModalitiesInStudy: Mapped[Optional[str]] = mapped_column(String(16))
    InstitutionName: Mapped[Optional[str]] = mapped_column(String(64))

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
    PresentationIntentType: Mapped[Optional[str]] = mapped_column(String(16))
    SeriesTime: Mapped[Optional[str]] = mapped_column(String(16))
    FrameOfReferenceUID: Mapped[Optional[str]] = mapped_column(String(128))
    NumberOfSeriesRelatedInstances: Mapped[Optional[str]] = mapped_column(String(12))
    SmallestPixelValueInSeries: Mapped[Optional[float]] = mapped_column(Float(4))
    SpatialResolution: Mapped[Optional[str]] = mapped_column(String(32))
    PerformedProcedureStepStartDate: Mapped[Optional[str]] = mapped_column(String(8))
    PerformingPhysicianName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    ProtocolName: Mapped[Optional[str]] = mapped_column(String(64))
    SeriesNumber: Mapped[Optional[str]] = mapped_column(String(12))
    SeriesDate: Mapped[Optional[str]] = mapped_column(String(8))
    Modality: Mapped[Optional[str]] = mapped_column(String(16))
    BodyPartExamined: Mapped[Optional[str]] = mapped_column(String(16))
    PerformedProcedureStepStatus: Mapped[Optional[str]] = mapped_column(String(16))
    SeriesType: Mapped[Optional[str]] = mapped_column(String(16))
    OperatorsName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    PerformedProcedureStepStartTime: Mapped[Optional[str]] = mapped_column(String(16))
    SeriesDescription: Mapped[Optional[str]] = mapped_column(String(64))
    AnatomicRegionSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    PerformedProcedureStepDescription: Mapped[Optional[str]] = mapped_column(
        String(64))
    Laterality: Mapped[Optional[str]] = mapped_column(String(16))

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

    # ===== DICOM fields =======
    InstitutionAddress: Mapped[Optional[str]] = mapped_column(String(1024))
    ManufacturerModelName: Mapped[Optional[str]] = mapped_column(String(64))
    NumberOfFrames: Mapped[Optional[str]] = mapped_column(String(12))
    DateOfLastCalibration: Mapped[Optional[str]] = mapped_column(String(8))
    ContentTime: Mapped[Optional[str]] = mapped_column(String(16))
    ContentLabel: Mapped[Optional[str]] = mapped_column(String(16))
    PresentationCreationDate: Mapped[Optional[str]] = mapped_column(String(8))
    SOPClassUID: Mapped[Optional[str]] = mapped_column(String(128))
    SoftwareVersions: Mapped[Optional[str]] = mapped_column(String(64))
    ContentDate: Mapped[Optional[str]] = mapped_column(String(8))
    BitsAllocated: Mapped[Optional[float]] = mapped_column(Float(4))
    TransferSyntaxUID: Mapped[Optional[str]] = mapped_column(String(128))
    ConceptNameCodeSequence: Mapped[Optional[str]] = mapped_column(DICOMSequence(265))
    Columns: Mapped[Optional[float]] = mapped_column(Float(4))
    CorrectedImage: Mapped[Optional[str]] = mapped_column(String(16))
    InstanceNumber: Mapped[Optional[str]] = mapped_column(String(12))
    ObservationDateTime: Mapped[Optional[str]] = mapped_column(String(36))
    TimeOfLastCalibration: Mapped[Optional[str]] = mapped_column(String(16))
    ContentDescription: Mapped[Optional[str]] = mapped_column(String(64))
    Rows: Mapped[Optional[float]] = mapped_column(Float(4))
    ContentCreatorName: Mapped[Optional[str]] = mapped_column(DICOMName(192))
    DeviceSerialNumber: Mapped[Optional[str]] = mapped_column(String(64))
    Manufacturer: Mapped[Optional[str]] = mapped_column(String(64))
    PixelPaddingValue: Mapped[Optional[float]] = mapped_column(Float(4))
    PresentationCreationTime: Mapped[Optional[str]] = mapped_column(String(16))
    StationName: Mapped[Optional[str]] = mapped_column(String(16))

    @classmethod
    def init_from_dataset(cls, dataset: Dataset, path: str):
        """Try to fill all fields of this model with info from dataset"""
        fields_to_transfer = InstanceLevel.fields.union({'SeriesInstanceUID'})
        param_dict = {tag: dataset.get(tag) for tag in fields_to_transfer}
        param_dict['path'] = path
        return cls(**param_dict)


