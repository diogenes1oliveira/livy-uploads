from pathlib import Path
import os
import threading
import time
from typing import BinaryIO, Callable

import pytest

from livy_uploads.executor.console import LineConsole, RawConsole



class TestLineConsole:
    def test_file(self, tmp_path: Path):
        file = tmp_path / 'test.txt'
        file.write_text('line 1\nline 2\nline 3\n')

        console = LineConsole(stdin=file.open('rb'))

        assert console.read() == b'line 1\n'
        assert console.read() == b'line 2\n'
        assert console.read() == b'line 3\n'
        with pytest.raises(EOFError):
            console.read()

    def test_pipe(self):
        r, w = os.pipe()
        rpipe = os.fdopen(r, 'rb')
        wpipe = os.fdopen(w, 'wb')

        console = LineConsole(stdin=rpipe, max_wait=1.0)

        t0 = time.monotonic()
        assert console.read() == b''
        assert time.monotonic() - t0 >= 1.0

        delayed_write(wpipe, 0.3, b'line 1\n')
        t0 = time.monotonic()
        assert console.read() == b'line 1\n'
        assert time.monotonic() - t0 >= 0.3

        delay(0.1, wpipe.close)
        t0 = time.monotonic()
        with pytest.raises(EOFError):
            console.read()
        assert time.monotonic() - t0 >= 0.1


class TestRawConsole:
    def test_file(self, tmp_path: Path):
        file = tmp_path / 'test.txt'
        file.write_text('1\n2\n3\n')

        console = RawConsole(stdin=file.open('rb'), bufsize=2)

        assert console.read() == b'1\n'
        assert console.read() == b'2\n'
        assert console.read() == b'3\n'
        with pytest.raises(EOFError):
            console.read()

    def test_pipe(self):
        r, w = os.pipe()
        rpipe = os.fdopen(r, 'rb')
        wpipe = os.fdopen(w, 'wb')

        console = RawConsole(stdin=rpipe, max_wait=1.0, bufsize=2)

        t0 = time.monotonic()
        assert console.read() == b''
        assert time.monotonic() - t0 >= 1.0

        delayed_write(wpipe, 0.3, b'1\n')

        t0 = time.monotonic()
        assert console.read() == b'1\n'
        assert time.monotonic() - t0 >= 0.3

        delay(0.1, wpipe.close)

        t0 = time.monotonic()
        with pytest.raises(EOFError):
            console.read()
        assert time.monotonic() - t0 >= 0.1


def delay(dt: float, func: Callable):
    def run():
        time.sleep(dt)
        func()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()

def delayed_write(fp: BinaryIO, dt: float, data: bytes):
    delay(dt, lambda: [fp.write(data), fp.flush()])
