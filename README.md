# dicom index


[![CI](https://github.com/sjoerdk/dicomindex/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/sjoerdk/dicomindex/actions/workflows/build.yml?query=branch%3Amaster)
[![PyPI](https://img.shields.io/pypi/v/dicomindex)](https://pypi.org/project/dicomindex/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dicomindex)](https://pypi.org/project/dicomindex/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

Reads DICOM files and indexes those in a single sqlite file. Helps answer the question 'what is in this huge folder full of dicom files?'

## Usage

### Index a DICOM in folder
To recursively read DICOM files in `/path/to/folder` and add patient/study/series objects to index.

Note: To This reads only a single dicom file in each series folder
```
dicomindex index_full /tmp/myindex.sql /path/to/folder
```

### What's in my index file?
```
dicomindex stats /path/to/folder
```

### This is taking forever
For big file collections over slow connections, indexing all files can easily take weeks. Annoying.
You could use this to for a faster index:
```
dicomindex index_per_folder /tmp/myindex.sql /path/to/base_folder
```
This will find a single DICOM file to index in each folder. This speeds up indexing at the cost
of completeness. Most of all it assumes that `base_folder` is structured in a patient/series/study way.


### Explore
I want to look around at what's in my index file.
* Install [datasette](https://datasette.io/)
* From commandline, run ```datsette /tmp/myindex.sql```
 

## Goals
* Extract patients/studies/series from DICOM files, save to a single db file  

## Non-goals
dicomindex will not do the following:
* Comprehensive storage. Use a PACS like [orthanc](https://www.orthanc-server.com)
* Exploration and browsing tools. Use [datasette](https://datasette.io/) 

## Useful for
* Data scientists/ researchers that can write python scripts 
* DICOM data exploration and sanitizing legacy archives
* Messy, semi-structured data, 'archives' that are just collections of folders with DICOM files in them
