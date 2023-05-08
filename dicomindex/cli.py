import logging
from pathlib import Path

import click
from click import Path as ClickPath
from tabulate import tabulate  # type: ignore

from dicomindex.processing import index_folder

from dicomindex.logs import get_module_logger
from dicomindex.orm import Instance, Patient, Series, Study
from dicomindex.persistence import SQLiteSession

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
        stats = index_folder(base_folder, session, use_progress_bar=True)

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
        }

        print(tabulate(table, headers="keys", tablefmt="simple"))


main.add_command(index_func)
main.add_command(stats_func)
