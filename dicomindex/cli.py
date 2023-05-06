import logging
from pathlib import Path

import click
from click import Path as ClickPath
from tabulate import tabulate  # type: ignore

from dicomindex.core import (
    DICOMIndex,
    NewDicomFiles,
    read_dicom_file,
)
from dicomindex.iterators import DICOMFilePerSeries
from dicomindex.logs import get_module_logger
from dicomindex.orm import Instance, Patient, Series, Study
from dicomindex.persistence import SQLiteSession
from dicomindex.threading import DICOMDatasetsThreaded, var_len_tqdm

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


@click.command(name="index")
@click.argument("index_file", type=ClickPath())
@click.argument("base_folder", type=ClickPath(exists=True))
def index_func(index_file, base_folder):
    """Index all files in base folder"""
    base_folder = Path(base_folder)

    logger.info(f"Starting index of '{base_folder}'. Writing to '{index_file}'")
    if Path(index_file).exists():
        click.confirm(
            f'file "{index_file}" already exists. Do you want to '
            f"append to this file?",
            abort=True,
        )
    with SQLiteSession(index_file) as session:
        index = DICOMIndex.init_from_session(session)
        base_count = len(index.paths)
        logger.debug(f"Found {base_count} instances already in index")

        def path_iter():
            for path in base_folder.rglob("*"):
                if path in index.paths or not path.is_file():
                    continue  # skip this path
                else:
                    yield path

        # progress bar added with var_len_tqdm
        for ds in var_len_tqdm(DICOMDatasetsThreaded(path_iter())):
            to_add = index.create_new_db_objects(ds, ds.filename)
            session.add_all(to_add)
            session.commit()

    logger.info("Finished")


@click.command(name="index_structured")
@click.argument("index_file", type=ClickPath())
@click.argument("base_folder", type=ClickPath(exists=True))
def index_structured_func(index_file, base_folder):
    """Index one file per series in patient/study/series/ structured base folder"""

    logger.info(f"Starting index of '{base_folder}'. Writing to '{index_file}'")
    if Path(index_file).exists():
        click.confirm(
            f'file "{index_file}" already exists. Do you want to '
            f"append to this file?",
            abort=True,
        )
    with SQLiteSession(index_file) as session:
        index = DICOMIndex.init_from_session(session)
        base_count = len(index.paths)
        logger.debug(
            f"Found {base_count} instances already in index. " f"Counting from there"
        )
        for count, file in enumerate(
            NewDicomFiles(DICOMFilePerSeries(base_folder), index)
        ):
            to_add = index.create_new_db_objects(read_dicom_file(file), str(file))
            session.add_all(to_add)
            session.commit()
            logger.debug(f"{count+base_count} - {file}")
    logger.info("Finished")


@click.command()
@click.argument("index_file", type=ClickPath(exists=True))
def stats(index_file):
    """What is in the given index file?"""

    logger.debug(f"Loading '{index_file}'")
    with SQLiteSession(index_file) as session:
        print(f"Statistics for {index_file}")
        table = {
            "Patients": [session.query(Patient).count()],
            "Series": [session.query(Series).count()],
            "Studies": [session.query(Study).count()],
            "Instances:": [session.query(Instance).count()],
        }

        print(tabulate(table, headers="keys", tablefmt="simple"))


main.add_command(index_func)
main.add_command(index_structured_func)
main.add_command(stats)