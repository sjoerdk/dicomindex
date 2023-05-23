"""Multithreaded stuff for optimized IO operations

Dicomindex needs to index DICOM after all. Best do it fast.
"""
from collections import namedtuple
from multiprocessing import Process, Queue
from queue import Empty

from dicomindex.logs import get_module_logger

logger = get_module_logger("threading")


EagerIteratorStatus = namedtuple("EagerIteratorStatus", ["visited", "has_finished"])
ITERATOR_DEPLETED_SENTINEL = object()


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
        self.process_status: EagerIteratorStatus = EagerIteratorStatus(
            visited=0, has_finished=False
        )

        self.value_queue = Queue()
        self.message_queue = Queue()
        self.has_finished = False

    def _init_generator(self):
        self.process = Process(
            target=self.push_iter_to_queue,
            args=(self.iterator, self.value_queue, self.message_queue),
        )
        self.process.start()

        while self.more_values_to_expect():
            logger.debug(
                f"Queue size is {self.value_queue.qsize()} and "
                f"process finished is {self.process_status.has_finished}"
            )
            val = self.value_queue.get(timeout=60)  # timeout as fallback

            # Extra check in addition to more_values_to_expect() due to race
            # condition between value_queue and message_queue
            if val == ITERATOR_DEPLETED_SENTINEL:
                continue  # Check again. Still not quite deadlock-proof. processes..
            else:
                yield val
        logger.debug("Cleaning up. Joining iterator process")

        self.process.join(timeout=60)  # clean up

    def more_values_to_expect(self):
        """There will be more to get from value queue"""
        self.update_process_status()
        return self.value_queue.qsize() > 1 or not self.process_status.has_finished

    def update_process_status(self):
        if self.has_finished:
            return  # no updates needed, nothing will change
        if self.process_status.has_finished:
            self.has_finished = True

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
            logger.debug("Joining iterator process")
            self.process.join(timeout=60)

    @staticmethod
    def push_iter_to_queue(iterator, value_queue, message_queue):
        """Push all items in iter into value_queue, send item count through
        message_queue

        If run in a separate process, can determine the size of a slow iterator as
        quickly as possible without having to wait for depletion.
        """
        visited = 0
        for item in iterator:
            visited += 1
            value_queue.put(item)
            message_queue.put(
                EagerIteratorStatus(visited=visited, has_finished=False), timeout=60
            )
        value_queue.put(ITERATOR_DEPLETED_SENTINEL, timeout=60)
        message_queue.put(
            EagerIteratorStatus(visited=visited, has_finished=True), timeout=60
        )
        logger.debug("push_iter_to_queue finished")
