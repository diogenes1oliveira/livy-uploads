__all__ = ('SignalMonitor',)

import logging
import signal
import queue
from typing import Optional, List


LOGGER = logging.getLogger(__name__)


class SignalMonitor:
    def __init__(self, signals: Optional[List[int]] = None, pause: Optional[float] = None):
        self.pause = pause or 1.0
        self.signals = list(map(signal.Signals, signals)) if signals is not None else list(set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGINT})
        self.queue = queue.Queue()
        self._original_handlers = {}
        self._closed = None

    def setup(self) -> None:
        LOGGER.info('listening to signals %s', self.signals)
        for s in self.signals:
            self._original_handlers[s] = signal.getsignal(s)
            signal.signal(s, self._enqueue_signal)
        self._closed = False

    def close(self) -> None:
        if self._closed is True:
            return
        if self._closed is False:
            for s in self.signals:
                signal.signal(s, self._original_handlers[s])
        self._closed = True

    def _enqueue_signal(self, signum, frame) -> None:
        sig = signal.Signals(signum)
        LOGGER.debug('received signal %s', sig)
        self.queue.put_nowait(sig)

    def next_signal(self) -> signal.Signals:
        if self._closed:
            raise EOFError

        try:
            return self.queue.get(timeout=self.pause)
        except queue.Empty:
            raise TimeoutError
