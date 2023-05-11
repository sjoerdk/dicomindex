"""Multithreaded stuff for optimized IO operations

Dicomindex needs to index DICOM after all. Best do it fast.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from multiprocessing import Process, Queue
from queue import Empty
from typing import Any, Callable, Iterable

from tqdm import tqdm

from dicomindex.core import read_dicom_file
from dicomindex.exceptions import NotDICOMError
from dicomindex.iterators import AllFiles
from dicomindex.logs import get_module_logger

logger = get_module_logger("threading")


class WorkerMessages:
    """Messages threads and processes can return instead of result values"""

    FINISHED = "FINISHED"


def open_file(path):
    try:
        return read_dicom_file(path)
    except NotDICOMError as e:
        e.path = path  # for bookkeeping
        raise e


class EagerIterator:
    """Iterator with a length which is updated by iterating in separate process
    length indicates the number of items in iterator

    Created of Path.rglob() which might take a long time to complete, does not
    take too much memory to deplete, and is quite useful to know the length of
    (how many files do I still need to process)

    Usage
    -----
    Designed as closure:
    >>> for item in EagerIterator():
    >>>    print(item)
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
        logger.debug("Terminating path iterator process")
        if self.process.is_alive():
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
        """Map the paths coming from path_generator to process_function

        Parameters
        ----------
        path_generator
        process_function
        """
        self.pre_fetched = 0
        self.generator = None
        self.path_generator = path_generator
        self.process_function = process_function

    def _init_generator(self):
        # start eagerly loading path generator
        with EagerIterator(self.path_generator) as paths:
            # create and load workers
            with ThreadPoolExecutor() as executor:

                def future_creator():
                    """Creates futures from path as they are requested
                    Also updates number of pre-fetched paths for nice progress bar
                    """
                    for path in paths:
                        self.pre_fetched = len(paths)
                        yield executor.submit(self.process_function, path)

                yield from as_completed(future_creator())

    def __next__(self):
        if not self.generator:
            self.generator = iter(self._init_generator())
        return next(self.generator)

    def __len__(self):
        return self.pre_fetched

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


class DICOMDatasetOpener:
    def __init__(self, path_iter):
        """Returns completed Future for each path in path_iter"""
        self.path_iter = path_iter
        self.generator = None

    def _init_generator(self):
        return FileProcessor(path_generator=self.path_iter, process_function=open_file)

    def __len__(self):
        return len(self.generator)

    def __iter__(self):
        if not self.generator:
            self.generator = self._init_generator()

        yield from self.generator


class AllDICOMDatasetsOpener(DICOMDatasetOpener):
    def __init__(self, path):
        """Dataset for each DICOM file in path recursively

        On slower filesystems this is faster than first getting AllDICOMFiles and
        then opening each file, because AllDICOMFiles does a (partial) read already.
        The reads are the expensive part.
        """
        super().__init__(path_iter=AllFiles(path))
