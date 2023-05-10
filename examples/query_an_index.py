"""Inspect a created index"""

from dicomindex.orm import Study
from tests.test_processing import SQLiteSession

index_file = "/tmp/archive.sql"

with SQLiteSession(index_file) as session:
    studies = session.query(Study).all()

    print(f"found {len(studies)} studies in {index_file}")
    study = studies[0]
    print(f'study "{study.StudyInstanceUID}" had {len(study.series)} series')
