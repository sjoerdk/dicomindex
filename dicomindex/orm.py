from typing import List

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Patient(Base):
    __tablename__ = "patient"
    PatientID: Mapped[str] = mapped_column(primary_key=True)
    studies: Mapped[List["Study"]] = relationship(
    back_populates = "patient", cascade = "all, delete-orphan")


class Study(Base):
    __tablename__ = "study"
    StudyInstanceUID: Mapped[str] = mapped_column(primary_key=True)
    PatientID: Mapped[str] = mapped_column(ForeignKey("patient.PatientID"))
    patient: Mapped["Patient"] = relationship(back_populates="studies")
    series: Mapped[List["Series"]] = relationship(back_populates="study",
        cascade="all, delete-orphan")


class Series(Base):
    __tablename__ = "series"
    SeriesInstanceUID: Mapped[str] = mapped_column(primary_key=True)
    StudyInstanceUID: Mapped[str] = mapped_column(ForeignKey("study.StudyInstanceUID"))
    study: Mapped["Study"] = relationship(back_populates="series")
    instances: Mapped[List["Instance"]] = relationship(back_populates="series",
                                                  cascade="all, delete-orphan")


class Instance(Base):
    __tablename__ = "instance"
    SOPInstanceUID: Mapped[str] = mapped_column(primary_key=True)
    SeriesInstanceUID: Mapped[str] = mapped_column(
        ForeignKey("series.SeriesInstanceUID"))
    series: Mapped["Series"] = relationship(back_populates="instances")

    path: Mapped[str] = mapped_column(String(512))
