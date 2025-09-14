#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Implementations to interruptibly read from the console input.
'''

__all__ = ('Console', 'LineConsole', 'RawConsole', 'TTYConsole')

from abc import ABC, abstractmethod
import os
import logging
import select
import signal
import struct
import fcntl
import termios
import time
import tty
import sys
from typing import BinaryIO, Optional, Tuple


LOGGER = logging.getLogger(__name__)


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

    def wait(self):
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


class TTYConsole(Console):
    """
    A console implementation that reads from a TTY.
    """

    def __init__(self, stdin: Optional[BinaryIO] = None, max_wait: Optional[float] = None, bufsize: Optional[int] = 1024):
        super().__init__(stdin, max_wait or 0.3)
        if not self.stdin.isatty():
            raise OSError('stdin is not a TTY')
        self.bufsize = bufsize or 1024
        self._old_blocking = None
        self._old_attrs = None

    def setup(self) -> None:
        LOGGER.debug('setup TTYConsole with poll pause %s', self.max_wait)
        self._old_blocking = os.get_blocking(self.stdin.fileno())
        self._old_attrs = termios.tcgetattr(self.stdin.fileno())
        tty.setraw(self.stdin.fileno())
        os.set_blocking(self.stdin.fileno(), False)
        super().setup()

    def close(self) -> None:
        super().close()
        if self._old_blocking is not None:
            os.set_blocking(self.stdin.fileno(), self._old_blocking)
            self._old_blocking = None
        if self._old_attrs is not None:
            termios.tcsetattr(self.stdin.fileno(), termios.TCSADRAIN, self._old_attrs)
            self._old_attrs = None

    def read(self) -> bytes:
        """
        Read data from the console.

        Raises:
            - `EOFError` if the console is closed.
        """
        buffer = bytearray()
        t0 = time.monotonic()
        eof = False

        while len(buffer) < self.bufsize:
            if time.monotonic() - t0 > self.max_wait:
                break

            try:
                self.wait()
            except TimeoutError:
                break

            try:
                b = self.stdin.read(1)
            except BlockingIOError:
                b = None

            if b == b'':
                eof = True
            if not b:
                break

            # got a Ctrl+C
            if b == b'\x03':
                os.kill(os.getpid(), signal.SIGINT)
                break

            buffer.extend(b)
            # newlines, tab key, arrow keys finish the buffer
            if b in {b'\r', b'\n', b'\t', b'\x1b'}:
                break

        result = bytes(buffer)
        if eof and not result:
            raise EOFError

        return result


if __name__ == '__main__':
    import logging
    import time
    import threading

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if sys.argv[1] == 'line':
        console = LineConsole()
    elif sys.argv[1] == 'raw':
        console = RawConsole(max_wait=1, bufsize=5)
    elif sys.argv[1] == 'tty':
        console = TTYConsole(max_wait=0.3, bufsize=5)
    else:
        raise ValueError('unknown console type: %s', sys.argv[1])

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
        logging.info('reading with %s (my PID is %s)', console, os.getpid())
        while True:
            try:
                line = console.read()
            except EOFError:
                logging.info('EOF')
                break
            else:
                logging.info('read %s', line)
