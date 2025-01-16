from datetime import datetime
from pathlib import Path
from logging import getLogger
import os
import queue
import signal
import subprocess
import sys
from typing import Any, Dict, List, Tuple, Optional, Union
import traceback
import threading
import time
from typing import Callable, Optional


StrOrPath = Union[str, Path]

LOGGER = getLogger(__name__)


class DaemonProcess:
    def __init__(
        self,
        start_cmd: List[str],
        pid_dir: StrOrPath,
        logs_dir: StrOrPath,
        stop_cmd: Optional[List[str]] = None,
        cwd: Optional[StrOrPath] = None,
        env: Optional[Dict[str, Optional[str]]] = None,
        pid_glob: Optional[str] = None,
        start_timeout: float = 5.0,
    ):
        '''
        Parameters:
        - start_cmd: the command to start the daemon
        - pid_dir: the directory where the daemon writes its PID file
        - logs_dir: the directory where the daemon writes its logs
        - stop_cmd: the command to stop the daemon. If unset, will default
        to sending a SIGTERM to the daemon's PID
        - cwd: the working directory of the process. Defaults to the current directory.
        - env: overrides for the environment variables. None will unset the variable.
        - pid_glob: a glob pattern to find the PID file. Defaults to '*.pid'
        - start_timeout: the time to wait for the daemon to start
        '''
        self.start_cmd = start_cmd
        self.stop_cmd = stop_cmd
        self.pid_dir = Path(pid_dir).absolute()
        self.logs_dir = Path(logs_dir).absolute()
        self.cwd = (Path(cwd) if cwd else Path.cwd()).absolute()
        self.env = env or {}
        self.pid_glob = pid_glob or '*.pid'
        self.start_timeout = start_timeout
        self._logs_follower = LogsFollower(self.logs_dir)

    def start(self) -> Tuple[int, Callable[[Optional[float]], str]]:
        LOGGER.debug('creating dirs')

        self.pid_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.pid_dir.chmod(0o700)
        self.logs_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.logs_dir.chmod(0o700)
        if not self.cwd.exists():
            self.cwd.mkdir(mode=0o700, parents=True, exist_ok=True)
            self.cwd.chmod(0o700)

        env = dict(os.environ)
        for k, v in self.env.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v
        self.env = env

        LOGGER.debug('starting process %s', self.start_cmd)
        log_path = self.logs_dir / 'livy_uploads.log'
        with log_path.open('wb') as fp:
            now = datetime.now().astimezone().isoformat(timespec='seconds')
            fp.write(f'INFO {now}: starting process\n'.encode('ascii'))
            proc = subprocess.Popen(
                args=self.start_cmd,
                env=self.env,
                stdin=subprocess.DEVNULL,
                stdout=fp,
                stderr=subprocess.STDOUT,
                cwd=self.cwd,
            )

            try:
                returncode = proc.wait(timeout=self.start_timeout)
            except subprocess.TimeoutExpired:
                LOGGER.warning("process didn't start in time, killing it")
                proc.kill()
                returncode = proc.wait(2.0)

        try:
            if returncode != 0:
                raise RuntimeError(f'process failed to start: {returncode}')
        finally:
            print(log_path.read_text(), file=sys.stderr)

        LOGGER.debug('process started up')

        self._logs_follower.start()
        return self.get_pid(), self._logs_follower.readline

    def get_pid(self) -> int:
        files = list(self.pid_dir.glob(self.pid_glob))
        if not files:
            raise FileNotFoundError('No PID file found')
        elif len(files) > 1:
            raise ValueError(f'Multiple PID files found: {files}')
        with files[0].open() as fp:
            return int(fp.read().strip())

    def poll(self) -> Optional[int]:
        try:
            pid = self.get_pid()
        except FileNotFoundError:
            return 1
        try:
            os.kill(pid, 0)
        except Exception:
            return 1
        else:
            return None

    def stop(self, timeout: float):
        try:
            if self.stop_cmd:
                LOGGER.debug('stopping process %s', self.stop_cmd)
                log_path = self.logs_dir / 'livy_uploads.log'
                with log_path.open('wb') as fp:
                    now = datetime.now().astimezone().isoformat(timespec='seconds')
                    fp.write(f'INFO {now}: stopping process\n'.encode('ascii'))
                    proc = subprocess.Popen(
                        args=self.stop_cmd,
                        env=self.env,
                        stdin=subprocess.DEVNULL,
                        stdout=fp,
                        stderr=subprocess.STDOUT,
                        cwd=self.cwd,
                    )

                    try:
                        returncode = proc.wait(timeout=timeout)
                    except subprocess.TimeoutExpired:
                        LOGGER.warning("process didn't stop in time, killing the stop CMD")
                        proc.kill()
                        returncode = proc.wait(2.0)

                try:
                    if returncode != 0:
                        raise RuntimeError(f'process failed to stop: returncode={returncode}')
                finally:
                    print(log_path.read_text(), file=sys.stderr)

            else:
                try:
                    pid = self.get_pid()
                except IOError:
                    return

                pid = self.get_pid()
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    if not _is_alive(pid):
                        return
                    raise

                t0 = time.monotonic()
                while True:
                    if not _is_alive(pid):
                        return

                    if time.monotonic() - t0 > timeout:
                        raise TimeoutError('Daemon did not stop in time')

                    time.sleep(0.5)
        finally:
            self._logs_follower.stop_flag.set()


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


class LogsFollower:
    '''
    A class to follow all .log files in a given directory
    '''

    def __init__(
        self,
        logs_dir: StrOrPath,
        pause: float = 1.0,
        stop_flag: Optional[threading.Event] = None,
        max_size: int = 1000,
    ):
        '''
        Parameters:
        - logs_dir: the directory with the .log files to folow
        - pause: the time to wait if there are no new lines in the logs
        - stop_flag: an optional threading.Event to stop the follower
        '''
        self.logs_dir = Path(logs_dir)
        self.pause = pause
        self.stop_flag = stop_flag or threading.Event()
        self.queue = queue.Queue(maxsize=max_size)
        self._threads: Dict[Path, threading.Thread] = {}

    def start(self):
        '''
        Starts the follower
        '''
        thread = threading.Thread(target=self._monitor_files, daemon=True)
        thread.start()

    def readline(self, timeout: Optional[float] = None) -> str:
        '''
        Gets the next line from the logs.

        An empty line signals the stop flag was set and there's nothing more to read.
        '''
        if self.stop_flag.is_set():
            try:
                return self.queue.get(timeout=self.pause)
            except queue.Empty:
                return ''
        else:
            return self.queue.get(timeout=timeout)

    def _monitor_files(self):
        '''
        Monitors the logs directory for new files
        '''
        while not self.stop_flag.is_set():
            for path in self.logs_dir.glob('*.log'):
                if path in self._threads:
                    continue

                thread = threading.Thread(target=self._tail_file, args=(path,), daemon=True)
                thread.start()
                self._threads[path] = thread

            time.sleep(self.pause)

    def _tail_file(self, path: Path):
        '''
        Puts new lines from a file into the queue
        '''
        try:
            with path.open() as fp:
                while not self.stop_flag.is_set():
                    curr = fp.tell()
                    line = fp.readline()
                    if line:
                        self.queue.put(line.rstrip('\r\n'))
                        continue

                    # maybe EOF
                    fp.seek(curr)
                    time.sleep(self.pause)
                    line = fp.readline()
                    if line:
                        self.queue.put(line.rstrip('\r\n'))
                        continue

                    # has the file been deleted?
                    if not path.exists():
                        break

                    # yeah, no new lines
                    time.sleep(self.pause)
        except Exception:
            _print_exc()
        finally:
            self._threads.pop(path, None)


def _print_exc():
    text = traceback.format_exc()
    try:
        sc: Any
        println = sc._gateway.jvm.java.lang.System.err.println
        println(text)
    except Exception:
        print(text, file=sys.__stderr__)
