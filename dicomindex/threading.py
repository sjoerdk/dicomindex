"""Multithreaded stuff for optimized IO operations

Dicomindex needs to index DICOM after all. Best do it fast.
"""
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from multiprocessing import Process, Queue
from queue import Empty
from typing import Any, Callable, Iterable, Optional, Set

from tqdm import tqdm

from dicomindex.core import read_dicom_file
from dicomindex.exceptions import NotDICOMError
from dicomindex.iterators import AllFiles
from dicomindex.logs import get_module_logger

logger = get_module_logger("threading")


EagerIteratorStatus = namedtuple("EagerIteratorStatus", ['visited',
                                                         'has_finished'])


class EagerIterator:

    def __init__(self, iterator):
        """Iterator with a length which is updated by iterating in separate process
           length indicates the number of items in iterator

           Created of Path.rglob() which might take a long time to complete, does not
           take too much memory to deplete, and is quite useful to know the length of
           (how many files do I still need to process)


        Parameters
        ----------
        iterator:
            Iterator to iterate over in separate thread

        Notes
        -----
        Designed as closure:
        >>> for item in EagerIterator():
        >>>    print(item)

        """
        self.iterator = iterator
        self.generator = None
        self.process = None
        self.process_status: EagerIteratorStatus = \
            EagerIteratorStatus(visited=0, has_finished=False)

        self.value_queue = Queue()
        self.message_queue = Queue()

    def _init_generator(self):
        self.process = Process(
            target=self.push_iter_to_queue,
            args=(self.iterator, self.value_queue, self.message_queue),
        )
        self.process.start()
        while self.value_queue.qsize() > 0 or not self.process_status.has_finished:
            val = self.value_queue.get(timeout=1000)  # timeout as fallback
            self.update_process_status()
            yield val

        self.process.join()  # clean up

    def update_process_status(self):
        while not self.message_queue.empty():
            try:
                self.process_status = self.message_queue.get(block=False)
            except Empty:
                continue  # no message, no problem.

    def __next__(self):
        if not self.generator:
            self.generator = self._init_generator()
        return next(self.generator)

    def __iter__(self):
        return self

    def __len__(self):
        self.update_process_status()
        return self.process_status.visited

    def __enter__(self):
        if not self.generator:
            self.generator = self._init_generator()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Terminating path iterator process")
        if self.process.is_alive():
            self.process.join()

    @staticmethod
    def push_iter_to_queue(iterator, value_queue, message_queue):
        """Push all items in iter into value_queue, send item count through
        message_queue

        If run in a separate process, can determine the size of a slow iterator as
        quickly as possible without having to wait for depletion.
        """
        visited = 0
        for count, item in enumerate(iterator, start=1):
            visited += 1
            value_queue.put(item)
            message_queue.put(EagerIteratorStatus(visited=visited,
                                                  has_finished=False))
        message_queue.put(
            EagerIteratorStatus(visited=visited, has_finished=True))



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
