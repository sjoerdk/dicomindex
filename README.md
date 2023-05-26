# dicom index


[![CI](https://github.com/sjoerdk/dicomindex/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/sjoerdk/dicomindex/actions/workflows/build.yml?query=branch%3Amain)
[![PyPI](https://img.shields.io/pypi/v/dicomindex)](https://pypi.org/project/dicomindex/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dicomindex)](https://pypi.org/project/dicomindex/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

Extracts DICOM information from files into a single sqlite file

## Installation
```
pip install dicomindex
```

## Usage

### Index a DICOM in folder
To recursively read all DICOM files in `/path/to/folder` and add patient/study/series objects to index.
```
dicomindex index_full /tmp/myindex.sql /path/to/folder
```

### Index faster
For big file collections over slow connections, indexing all files can easily take weeks. Annoying.
You could use this to for a faster index:
```
dicomindex index_per_folder /tmp/myindex.sql /path/to/base_folder
```
This will find a single DICOM file to index in each folder recursively. This speeds up indexing significantly, but assumes
`base_folder` contains sub-folders that are structured in a patient/series/study way.


## Explore
I've indexed this folder. Now what?

### What's in my index file? (quick overview)
```
dicomindex stats /path/to/folder
```
### Detailed exploration - Datasette

* Install [datasette](https://datasette.io/)
* From commandline, run ```datsette /tmp/myindex.sql```

### Detailed exploration - Python 

Query the db file directly using dicomindex [sqlalchemy](https://www.sqlalchemy.org/) ORM:
```python
from dicomindex.orm import Study
from dicomindex.persistence import SQLiteSession

with SQLiteSession("/tmp/archive.sql") as session:
    studies = session.query(Study).all()
    print(f"found {len(studies)} studies")
```

## Non-goals
dicomindex will not do the following:
* Comprehensive storage. Use a PACS like [orthanc](https://www.orthanc-server.com)
* Exploration and browsing tools. Use [datasette](https://datasette.io/) 

## Useful for 
* DICOM data exploration and sanitizing legacy archives
* Messy, semi-structured data, 'archives' that are just collections of folders with DICOM files in them
