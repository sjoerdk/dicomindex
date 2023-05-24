import pytest
from dicomgenerator.export import export
from dicomgenerator.templates import CTDatasetFactory

from dicomindex.dataset import RequiredDataset, RequiredTagNotFound


def test_required_dataset(tmp_path):
    """Required Dataset re-codes generic AttributeError to more specific tag
    not found errors. This changes some rather involved code on pydicom.Dataset.

    Make sure different ways of getting tags from Dataset are behaving as expected
    """

    # set up a normal Dataset and a RequiredDataset (both missing StudyInstanceUID)
    dicom_dir = tmp_path / "dicom"
    dicom_dir.mkdir(parents=True)
    ds = CTDatasetFactory(PatientID="a_patient")
    del ds["StudyInstanceUID"]
    export(ds, path=(dicom_dir / "a_file"))
    required = RequiredDataset(ds)

    # These calls should raise an exception
    with pytest.raises(RequiredTagNotFound):
        _ = required["StudyInstanceUID"]

    with pytest.raises(RequiredTagNotFound):
        _ = required.StudyInstanceUID

    # These calls should not
    assert required.get("StudyInstanceUID", None) is None
    assert required.get("StudyInstanceUID") is None
    assert required.get("StudyInstanceUID", "default") == "default"
    assert required.get("PatientID") == "a_patient"
    assert required["PatientID"].value == "a_patient"
    assert required.PatientID == "a_patient"
