import logging
from pathlib import Path

import click
from click import Path as ClickPath
from tabulate import tabulate  # type: ignore
from tqdm import tqdm

from dicomindex.persistence import SQLiteSession
from dicomindex.processing import index_folder_full, index_one_file_per_folder

from dicomindex.logs import get_module_logger
from dicomindex.orm import (
    DICOMFileDuplicate,
    Instance,
    NonDICOMFile,
    Patient,
    Series,
    Study,
)

logger = get_module_logger("cli")


def configure_logging(verbose):
    if verbose == 0:
        logging.basicConfig(level=logging.INFO)
        logging.info("Set loglevel to INFO")
    if verbose >= 1:
        logging.basicConfig(level=logging.DEBUG)
        logging.info("Set loglevel to DEBUG")


@click.group()
@click.option("-v", "--verbose", count=True)
def main(verbose):
    r"""DICOM index - see what dicom is in your folders

    Use the commands below with -h for more info
    """
    configure_logging(verbose)


@click.command(name="index_full")
@click.argument("index_file", type=ClickPath())
@click.argument("base_folder", type=ClickPath(exists=True))
def index_full_func(index_file, base_folder):
    """Index all files in base folder. Slow but complete"""
    base_folder = Path(base_folder)

    logger.info(f"Starting index of '{base_folder}'. Writing to '{index_file}'")
    if Path(index_file).exists():
        click.confirm(
            f'file "{index_file}" already exists. Do you want to '
            f"append to this file?",
            abort=True,
        )

    with SQLiteSession(index_file) as session:
        with tqdm(total=1) as pbar:
            stats = index_folder_full(base_folder, session, progress_bar=pbar)

    logger.info("Finished")
    logger.info(stats.summary())


@click.command(name="index_per_folder")
@click.argument("index_file", type=ClickPath())
@click.argument("base_folder", type=ClickPath(exists=True))
def index_per_folder_func(index_file, base_folder):
    """Index one file in each folder. Fast indexing but assumes files for the same
    patient/study/series are in one folder each. Might miss a lot of data is not
    of that structure
    """
    base_folder = Path(base_folder)

    logger.info(f"Starting index of '{base_folder}'. Writing to '{index_file}'")
    if Path(index_file).exists():
        click.confirm(
            f'file "{index_file}" already exists. Do you want to '
            f"append to this file?",
            abort=True,
        )

    with SQLiteSession(index_file) as session:
        with tqdm(total=1) as pbar:
            stats = index_one_file_per_folder(base_folder, session, progress_bar=pbar)

    logger.info("Finished")
    logger.info(stats.summary())


@click.command(name="stats")
@click.argument("index_file", type=ClickPath(exists=True))
def stats_func(index_file):
    """What is in the given index file?"""

    logger.debug(f"Loading '{index_file}'")
    with SQLiteSession(index_file) as session:
        print(f"Statistics for {index_file}")
        table = {
            "Patients": [session.query(Patient).count()],
            "Studies": [session.query(Study).count()],
            "Series": [session.query(Series).count()],
            "Instances:": [session.query(Instance).count()],
            "Duplicates:": [session.query(DICOMFileDuplicate).count()],
            "Non_dicom:": [session.query(NonDICOMFile).count()],
        }

        print(tabulate(table, headers="keys", tablefmt="simple"))


main.add_command(index_full_func)
main.add_command(index_per_folder_func)
main.add_command(stats_func)
