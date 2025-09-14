from abc import ABC, abstractmethod
import os
import select
import signal
import struct
import fcntl
import termios
import sys
from typing import BinaryIO, Optional, Tuple



class Console(ABC):
    """
    A class to abstract interruptibly reading from the stdin.
    """

    def __init__(self, stdin: Optional[BinaryIO] = None, max_wait: Optional[float] = 1.0):
        self.stdin = stdin or sys.stdin.buffer
        self.max_wait = max_wait or 1.0
        r, w = os.pipe()
        self._rpipe = os.fdopen(r, 'rb')
        self._wpipe = os.fdopen(w, 'wb')

    def setup(self) -> None:
        """
        Performs any setup necessary for the console.
        """
        pass

    def close(self) -> None:
        """
        Performs any cleanup necessary for the console.
        """
        if self._wpipe is not None:
            self._wpipe.close()
            self._wpipe = None

    def __enter__(self) -> 'Console':
        self.setup()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def wait(self) -> None:
        """
        Wait for the console to have data available.

        Raises:
            - `TimeoutError` if the console is not ready within the timeout.
            - `EOFError` if the console is closed.
        """
        rlist, _, _ = select.select([self.stdin, self._rpipe], [], [], self.max_wait)
        if not rlist:
            raise TimeoutError

        if self._rpipe in rlist:
            raise EOFError

    @abstractmethod
    def read(self) -> bytes:
        """
        Read data from the console.

        Raises:
            - `EOFError` if the console is closed while reading.
        """
        raise NotImplementedError

    @property
    def tty_size(self) -> Optional[Tuple[int, int]]:
        """
        The size of the TTY, if any.

        Raises:
            - `NotImplementedError` if the TTY size cannot be determined.
        """
        raise NotImplementedError


class LineConsole(Console):
    """
    A console implementation that reads from a line-based input.
    """

    @property
    def tty_size(self) -> Tuple[int, int]:
        """
        TTY size determined by the file descriptor of the stdin.

        Raises:
            - `NotImplementedError` if the stdin is not a TTY.
        """
        if not self.stdin.isatty():
            raise NotImplementedError('stdin is not a TTY')

        s = struct.pack("HHHH", 0, 0, 0, 0)
        rows, cols, _, _ = struct.unpack("HHHH", fcntl.ioctl(self.stdin.fileno(), termios.TIOCGWINSZ, s))
        return rows, cols

    def read(self) -> bytes:
        """
        Read data from the console.

        Raises:
            - `EOFError` if the console is closed.
        """
        try:
            self.wait()
        except TimeoutError:
            return b""

        data = self.stdin.readline()
        if not data:
            raise EOFError

        return data


class RawConsole(Console):
    """
    A console implementation that reads from a raw input.
    """

    def __init__(self, stdin: Optional[BinaryIO] = None, max_wait: Optional[float] = 1.0, bufsize: Optional[int] = 1024):
        super().__init__(stdin, max_wait)
        self.bufsize = bufsize or 1024

    def read(self) -> bytes:
        """
        Read data from the console.
        """
        try:
            self.wait()
        except TimeoutError:
            return b""

        data = self.stdin.read(self.bufsize)
        if not data:
            raise EOFError

        return data


if __name__ == '__main__':
    import logging
    import time
    import threading

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if sys.argv[1] == 'line':
        console = LineConsole()
    elif sys.argv[1] == 'chunked':
        console = ChunkedConsole(pause=1, bufsize=5)
    elif sys.argv[1] == 'tty':
        console = TTYConsole(max_wait=1, bufsize=5)
    else:
        console = NullConsole()

    # def stop():
    #     time.sleep(5.0)
    #     logging.info('closing console')
    #     console.close()
    #     logging.info('closed console')

    def on_signal(signum, frame):
        logging.info('signal %s', signum)
        logging.info('closing console')
        console.close()
        logging.info('closed console')

    signal.signal(signal.SIGINT, on_signal)
    # threading.Thread(target=stop, daemon=True).start()

    with console:
        logging.info('reading with %s', console)
        while True:
            try:
                line = console.read()
            except EOFError:
                logging.info('EOF')
                break
            else:
                logging.info('read %s', line)
