from abc import ABC, abstractmethod
from datetime import datetime, timezone
import os
import select
import signal
import struct
import fcntl
import queue
import time
import termios
import sys
import tty
from typing import BinaryIO, NamedTuple, Optional, Tuple, Union


class Signal(NamedTuple):
    signum: Union[int, signal.Signals]
    'The signal number'
    tty_size: Optional[Tuple[int, int]]
    'The size of the TTY if available'
    t0: float
    'The monotonic timestamp when the signal was received'
    timestamp: datetime
    'The UTC timestamp when the signal was received'


class Console(ABC):
    """
    A class to abstract the stdin and signals of a terminal process.
    """

    def setup(self) -> None:
        """
        Setup the console.
        """
        pass

    def close(self) -> None:
        """
        Close the console.
        """
        pass

    def __enter__(self) -> 'Console':
        self.setup()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    @abstractmethod
    def get_signal(self) -> Optional[Signal]:
        """
        Gets an enqueued signal from the console.

        Raises:
            - `EOFError` if the console is closed.
        """
        raise NotImplementedError

    @abstractmethod
    def read(self) -> bytes:
        """
        Read data from the console.

        Raises:
            - `EOFError` if the console is closed.
        """
        raise NotImplementedError

    @property
    def tty_size(self) -> Optional[Tuple[int, int]]:
        """
        The size of the TTY.
        """
        return None


class NullConsole(Console):
    """
    A console implementation that immediately raises `EOFError`.
    """

    def read(self) -> bytes:
        raise EOFError


class InterruptibleConsole(Console):
    """
    A console implementation that can be interrupted.
    """

    def __init__(self, stdin: Optional[BinaryIO] = None, pause: Optional[float] = None):
        self.stdin: BinaryIO = stdin or sys.stdin.buffer
        self.pause = pause or 1.0
        r, w = os.pipe()
        self._rpipe = os.fdopen(r, 'rb', 0)
        self._wpipe = os.fdopen(w, 'wb', 0)
        self.signals_queue = queue.Queue()

    def setup(self) -> None:
        def handle_signal(signum, frame=None):
            self.signals_queue.put_nowait(Signal(signum, None, time.monotonic(), datetime.now(timezone.utc)))

        for s in set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGINT}:
            try:
                signal.signal(s, handle_signal)
            except ValueError:
                pass

    def close(self) -> None:
        self.signals_queue.put(None)
        if self._wpipe is not None:
            self._wpipe.close()
            self._wpipe = None

    def get_signal(self) -> Optional[Signal]:
        try:
            s = self.signals_queue.get(timeout=self.pause)
        except queue.Empty:
            return None

        if s is None:
            raise EOFError

        return s

    def wait(self, pause: Optional[float] = None) -> bool:
        pause = pause if pause is not None else self.pause
        if self._wpipe is None:
            raise EOFError

        readables, _, _ = select.select([self.stdin, self._rpipe], [], [], pause)
        if not readables:
            if self._wpipe is None:
                raise EOFError
            return False

        if self._rpipe in readables:
            self._rpipe.read()
            raise EOFError

        return True

    def read(self) -> bytes:
        if not self.wait():
            return b''

        return self.read_now()

    @abstractmethod
    def read_now(self) -> bytes:
        raise NotImplementedError


class LineConsole(InterruptibleConsole):
    """
    A console implementation that reads line-by-line.
    """

    def read_now(self) -> bytes:
        line = self.stdin.readline()
        if not line:
            raise EOFError
        return line


class ChunkedConsole(InterruptibleConsole):
    """
    A console implementation that reads in chunks.
    """

    def __init__(self, stdin: Optional[BinaryIO] = None, bufsize: Optional[int] = None, pause: Optional[float] = None):
        super().__init__(stdin, pause)
        self._bufsize = bufsize or 4096

    def read_now(self) -> bytes:
        data = self.stdin.read(self._bufsize)
        if not data:
            raise EOFError
        return data


class TTYConsole(ChunkedConsole):
    """
    A console implementation that reads from a TTY.
    """

    def __init__(self, stdin: Optional[BinaryIO] = None, bufsize: Optional[int] = None, pause: Optional[float] = None):
        super().__init__(stdin, bufsize, pause or 0.2)
        self._old_attrs = None
        self._old_blocking = None
        self._eof = False
        self._interrupted = False

    def setup(self) -> None:
        super().setup()
        self._old_blocking = os.get_blocking(self.stdin.fileno())
        self._old_attrs = termios.tcgetattr(self.stdin.fileno())
        tty.setraw(self.stdin.fileno())
        os.set_blocking(self.stdin.fileno(), False)

    def close(self) -> None:
        try:
            super().close()
        finally:
            if self._old_blocking is not None:
                os.set_blocking(self.stdin.fileno(), self._old_blocking)
                self._old_blocking = None
            if self._old_attrs is not None:
                termios.tcsetattr(self.stdin.fileno(), termios.TCSADRAIN, self._old_attrs)
                self._old_attrs = None

    @property
    def tty_size(self) -> Tuple[int, int]:
        s = struct.pack("HHHH", 0, 0, 0, 0)
        rows, cols, _, _ = struct.unpack("HHHH", fcntl.ioctl(self.stdin.fileno(), termios.TIOCGWINSZ, s))
        return rows, cols

    def get_signal(self) -> Optional[Signal]:
        s = super().get_signal()
        if s is None:
            return s

        if int(s.signum) == signal.SIGWINCH:
            s = s._replace(tty_size=self.tty_size)

        return s

    def read(self) -> bytes:
        if self._interrupted:
            raise KeyboardInterrupt

        return super().read()

    def read_now(self) -> bytes:
        if self._eof:
            raise EOFError

        buffer = bytearray()
        t0 = time.monotonic()

        while len(buffer) < self._bufsize and time.monotonic() - t0 < self.pause:
            try:
                b = self.stdin.read(1)
            except BlockingIOError:
                b = None

            if b is None:
                continue
            elif b == b'\x03':
                self._interrupted = True
                self.signals_queue.put_nowait(Signal(signal.SIGINT, None, time.monotonic(), datetime.now(timezone.utc)))
                break
            elif not b:
                self._eof = True
                break

            buffer.extend(b)

        return bytes(buffer)


if __name__ == '__main__':
    import logging
    import time
    import threading

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if sys.argv[1] == 'line':
        console = LineConsole(pause=1)
    elif sys.argv[1] == 'chunked':
        console = ChunkedConsole(pause=1, bufsize=5)
    elif sys.argv[1] == 'tty':
        console = TTYConsole(pause=1, bufsize=5)
    else:
        console = NullConsole()

    def stop():
        time.sleep(5.0)
        logging.info('closing console')
        console.close()
        logging.info('closed console')

    threading.Thread(target=stop, daemon=True).start()

    with console:
        while True:
            logging.info('reading with %s', console)
            try:
                line = console.read()
            except EOFError:
                logging.info('EOF')
                break
            else:
                logging.info('read %s', line)
