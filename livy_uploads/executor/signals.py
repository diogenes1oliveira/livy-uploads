__all__ = ('SignalMonitor',)

import logging
from multiprocessing import Value
import signal
import queue
import threading
from typing import Optional, List, Union


LOGGER = logging.getLogger(__name__)


class SignalMonitor:
    def __init__(
        self,
        signals: Optional[List[int]] = None,
        pause: Optional[float] = 1.0,
        stop_signal: Optional[Union[int, signal.Signals]] = signal.SIGTERM,
        max_stop_count: Optional[int] = 2,
    ):
        self.pause = pause or 1.0
        self.max_stop_count = max_stop_count or 2

        if signals is not None:
            signals = set(map(signal.Signals, signals))
        else:
            signals = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGINT, signal.SIGTERM}

        if signal.SIGINT in signals or signal.SIGTERM in signals:
            raise ValueError('SIGINT and SIGTERM are not allowed')

        self.signals = {int(s) for s in (signals | {signal.SIGINT, signal.SIGTERM})}
        self.stop_signal = int(stop_signal if stop_signal is not None else signal.SIGTERM)
        self.queue = queue.Queue()
        self._original_handlers = {}
        self._stop_count = 0
        self._killed = False
        self._closed: Optional[threading.Event] = None

    def setup(self) -> None:
        LOGGER.info('listening to signals %s', set(map(signame, self.signals)))

        for s in self.signals:
            self._original_handlers[s] = signal.getsignal(s)
            if s in {int(signal.SIGINT), int(signal.SIGTERM)}:
                signal.signal(s, self._handle_stop_signal)
            else:
                signal.signal(s, self._enqueue_signal)

        self._closed = threading.Event()

    def close(self) -> None:
        if self._closed is None or self._closed.is_set():
            return

        for s in self.signals:
            signal.signal(s, self._original_handlers[s])

        self._closed.set()

    def _enqueue_signal(self, signum, frame) -> None:
        LOGGER.debug('received signal %r', signame(signum))
        self.queue.put_nowait(signum)

    def _handle_stop_signal(self, signum, frame) -> None:
        if self._killed:
            LOGGER.warning('already asked process to be killed, ignoring stop signal %r', signame(signum))
            return

        self._stop_count += 1

        LOGGER.info('received stop signal %r (count=%d)', signame(signum), self._stop_count)
        if self._stop_count < self.max_stop_count:
            self.queue.put_nowait(self.stop_signal)
        else:
            LOGGER.warning('too many stop signals received, asking to kill process')
            self.queue.put_nowait(signal.SIGKILL)
            self._killed = True

    def next_signal(self) -> int:
        if self._closed and self._closed.is_set():
            raise EOFError

        try:
            return self.queue.get(timeout=self.pause)
        except queue.Empty:
            raise TimeoutError


def signame(s: Union[int, signal.Signals]) -> Union[int, signal.Signals]:
    try:
        return signal.Signals(s)
    except ValueError:
        return s


def parse_signal(s: Union[int, str, signal.Signals]) -> Union[int, signal.Signals]:
    if isinstance(s, str):
        try:
            signum = int(s)
        except ValueError:
            if not s.isalnum():
                raise ValueError(f'invalid signal name: {s}')
            s = s.upper()
            if not s.startswith('SIG'):
                s = 'SIG' + s
            try:
                signum = int(getattr(signal.Signals, s))
            except AttributeError:
                raise ValueError(f'invalid signal name: {s}')
    else:
        signum = int(s)

    try:
        return signal.Signals(signum)
    except ValueError:
        return signum
