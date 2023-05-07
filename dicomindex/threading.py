"""Multithreaded stuff for optimized IO operations

Dicomindex needs to index DICOM after all. Best do it fast.
"""

from functools import wraps
from multiprocessing import Process, Queue
from multiprocessing.pool import ThreadPool
from pathlib import Path
from queue import Empty
from typing import Any, Callable, Iterable

from tqdm import tqdm

from dicomindex.core import read_dicom_file
from dicomindex.exceptions import DICOMIndexError
from dicomindex.logs import get_module_logger

logger = get_module_logger("threading")


class WorkerMessages:
    """Messages threads and processes can return instead of result values"""

    NOT_DICOM = "NOT_DICOM"
    FINISHED = "FINISHED"


def open_dataset(path):
    try:
        return read_dicom_file(path)
    except DICOMIndexError:
        return WorkerMessages.NOT_DICOM


class EagerIterator:
    """Iterator with a length which is updated by iterating in separate process
    length indicates the number of items in iterator

    Created of Path.rglob() which might take a long time to complete, does not
    take too much memory to deplete, and is quite useful to know the length of
    (how many files do I still need to process)
    """

    def __init__(self, iterator, timeout=10):
        """

        Parameters
        ----------
        iterator:
            Iterator to iterate over in separate thread
        timeout:
            Number of seconds to wait for next(iterator) before raising exception
        """
        self.iterator = iterator
        self.timeout = timeout
        self.generator = None
        self.process = None
        self.length = 0
        self.value_queue = Queue()
        self.message_queue = Queue()

    def _init_generator(self):
        self.process = Process(
            target=self.push_iter_to_queue,
            args=(self.iterator, self.value_queue, self.message_queue),
        )
        self.process.start()
        while True:
            val = self.value_queue.get(timeout=self.timeout)
            if val == WorkerMessages.FINISHED:
                self._update_length()  # get the total right
                break
            else:
                yield val

    def _update_length(self):
        while not self.message_queue.empty():
            try:
                self.length = self.message_queue.get(block=False)
            except Empty:
                continue  # no message, no problem.

    def __next__(self):
        if not self.generator:
            self.generator = self._init_generator()
        return next(self.generator)

    def __iter__(self):
        return self

    def __len__(self):
        self._update_length()
        return self.length

    def __enter__(self):
        if not self.generator:
            self.generator = self._init_generator()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.process.terminate()

    @staticmethod
    def push_iter_to_queue(iterator, value_queue, message_queue):
        """Push all items in iter into value_queue, send item count through
        message_queue

        If run in a separate process, can determine the size of a slow iterator as
        quickly as possible without having to wait for depletion.
        """
        count = 0
        for count, item in enumerate(iterator, start=1):
            value_queue.put(item)
            message_queue.put(count)
        value_queue.put(WorkerMessages.FINISHED)
        logger.debug(f"push_iter_to_queue - completed. Pushed {count} to value_queue")


class FileProcessor:
    """Parallel-processes file paths coming from an iterable as they are coming in.

    Created for doing IO operations on paths coming out of Path.rglob(*).
    Updates its __len__ attribute as file paths are coming in. This allows a
    fancy progress bar where the total as well as the processed counts are updated

    Notes
    -----
    For useful progress bar, assumes path_generator is (much) faster than
    process_function.
    """

    def __init__(
        self, path_generator: Iterable[str], process_function: Callable[[str], Any]
    ):
        """Map the paths coming from path_generator to process_function"""
        self.loaded = 0
        self.generator = None
        self.path_generator = path_generator
        self.process_function = process_function

    def _init_generator(self):

        # start eagerly loading path generator
        with EagerIterator(self.path_generator) as paths:

            # create and load workers
            with ThreadPool() as pool:

                def counting_iterator(iterator):
                    """For having a running updatable total"""
                    for x in iterator:
                        self.loaded = len(iterator)
                        yield x

                logger.debug("mapping input iter to thread pool")
                yield from pool.imap_unordered(
                    self.process_function, counting_iterator(paths)
                )

                logger.debug("finished. Exiting thread pool")

    def __next__(self):
        if not self.generator:
            self.generator = iter(self._init_generator())
        return next(self.generator)

    def __len__(self):
        return self.loaded

    def __iter__(self):
        return self


def var_len_tqdm(iterable, **kwargs):
    """Prints tqdm progress bar, but allows for iterable with changing length"""

    @wraps(iterable)
    def wrapped_iterable():
        with tqdm(total=1, **kwargs) as pbar:
            for x in iterable:
                pbar.total = len(iterable)
                pbar.update()
                yield x

    return iter(wrapped_iterable())


class DICOMDatasetsThreaded:
    def __init__(self, path_iter):
        """Read Dataset for each path in path_iter"""
        self.path_iter = path_iter
        self.not_ds_skipped = 0
        self.ds_returned = 0
        self.generator = None

    def _init_generator(self):
        return FileProcessor(
            path_generator=self.path_iter, process_function=open_dataset
        )

    def __len__(self):
        return len(self.generator) - self.not_ds_skipped

    def __iter__(self):
        if not self.generator:
            self.generator = self._init_generator()

        self.not_ds_skipped = 0
        self.ds_returned = 0
        for result in self.generator:
            if result == WorkerMessages.NOT_DICOM:
                self.not_ds_skipped += 1
                continue
            else:
                self.ds_returned += 1
                yield result


class AllDICOMDatasetsThreaded(DICOMDatasetsThreaded):
    def __init__(self, path):
        """Dataset for each DICOM file in path recursively

        On slower filesystems this is faster than first getting AllDICOMFiles and
        then opening each file, because AllDICOMFiles does a (partial) read already.
        The reads are the expensive part.
        """
        super().__init__(
            path_iter=iter(x for x in Path(path).rglob("*") if x.is_file())
        )
