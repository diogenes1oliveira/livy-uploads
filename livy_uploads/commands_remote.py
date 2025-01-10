import queue
import subprocess
from threading import Thread
from typing import Dict, List, Optional, Tuple
from uuid import uuid4


class ProcessHandle:
    def __init__(self, args: List[str]):
        self.args = args
        self.proc: Optional[subprocess.Popen] = None
        self.logs = queue.Queue()
        self.returncode: Optional[int] = None
        self.error: Optional[Exception] = None

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'{self.__class__.__name__}({self.args!r})'

    def start(self):
        self.proc = subprocess.Popen(
            self.args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )

        thread = Thread(target=self._monitor)
        thread.daemon = True
        thread.start()

    def _monitor(self):
        try:
            while True:
                line: str = self.proc.stdout.readline()
                if not line:
                    break
                self.logs.put(line.rstrip('\r\n'))
            self.returncode = self.proc.wait()
        except Exception as e:
            self.error = e

    def poll(self, max_lines: int = 500) -> Tuple[List[str], Optional[int]]:
        '''
        Queries the status of the command.
        '''
        if self.error:
            raise self.error

        logs = []
        while True:
            if len(logs) >= max_lines:
                return logs, None

            try:
                line = self.logs.get_nowait()
            except queue.Empty:
                break

            logs.append(line)

        return logs, self.returncode

    def stop(self, timeout: float = 2.0):
        '''
        Synchronously stops the command.
        '''
        if not self.proc or self.proc.poll() is not None:
            return

        self.proc.terminate()
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


class ProcessManager:
    processes: Dict[str, ProcessHandle] = {}

    @classmethod
    def start(cls, args: List[str]) -> str:
        name = 'process-' + args[0] + '-' + str(uuid4())
        cls.processes[name] = handle = ProcessHandle(args)
        handle.start()
        return name

    @classmethod
    def poll(cls, name: str, max_lines: int = 500) -> Tuple[List[str], Optional[int]]:
        try:
            handle = cls.processes[name]
        except KeyError:
            raise KeyError(f'no such process {name!r}') from None

        returncode: Optional[int] = None

        try:
            lines, returncode = handle.poll(max_lines)
            return lines, returncode
        except Exception:
            returncode = -1
            raise
        finally:
            if returncode is not None:
                del cls.processes[name]

    @classmethod
    def stop(cls, name: str, timeout: float = 2.0):
        try:
            handle = cls.processes[name]
        except KeyError:
            raise KeyError(f'no such process {name!r}') from None
        handle.stop(timeout)
        del cls.processes[name]
