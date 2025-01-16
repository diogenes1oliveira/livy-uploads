from abc import ABC, abstractmethod
import io
import queue
import sys
import threading
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple, Type


class BaseProcess(ABC):
    @abstractmethod
    def start(self) -> Tuple[int, Callable[[], str]]:
        '''
        Arranges for the process to be started.

        Returns:
        - the PID of the process
        - a function to read a line from the process's output stream
        '''

    @abstractmethod
    def poll(self) -> Optional[int]:
        '''
        Polls the process for its return code.
        '''

    @abstractmethod
    def stop(self, timeout: float):
        '''
        Termminates the process, killing it if it doesn't die within the timeout.
        '''


class RemoteProcessManager:
    registry: Dict[str, Type[BaseProcess]] = {}

    @classmethod
    def register(cls, process_class: Type[BaseProcess]):
        '''
        Registers a new process class
        '''
        cls.registry[process_class.__name__] = process_class

    def __init__(self):
        '''
        Initializes the manager.
        '''
        self.instances: Dict[int, BaseProcess] = {}
        self.stop_flags: Dict[int, threading.Event] = {}
        self.log_queues: Dict[int, queue.Queue] = {}

    def start(self, class_name: str, *args, **kwargs) -> int:
        '''
        Starts a new process.

        Parameters:
        - class_name: a previously registered class name
        - args, kwargs: arguments to pass to the process constructor

        Returns:
        - the new process PID
        '''
        process = self.registry[class_name](*args, **kwargs)
        pid, readline = process.start()
        self.instances[pid] = process
        self.stop_flags[pid] = threading.Event()
        self.log_queues[pid] = queue.Queue(maxsize=500)

        thread = threading.Thread(target=self._print_stream, args=(pid, readline), daemon=True)
        thread.start()

        return pid

    def poll(self, pid: int, batch_size: int = 100) -> Tuple[Optional[int], List[str]]:
        '''
        Polls the returncode and log lines of the process.

        - None is returned for the returncode if the process is still running.
        '''
        process = self.instances[pid]
        log_queue = self.log_queues[pid]
        lines: List[str] = []

        while True:
            try:
                line = log_queue.get_nowait()
                lines.append(line)
                if len(lines) >= batch_size:
                    break
            except queue.Empty:
                break

        if lines:
            return None, lines

        returncode = process.poll()

        if returncode is not None:
            stop_flag = self.stop_flags.pop(pid)
            stop_flag.set()
            del self.instances[pid]
            del self.log_queues[pid]

        return returncode, []

    def stop(self, pid: int, timeout: float = 2.0):
        '''
        Stops the process with the given PID.

        The process is killed if it doesn't die within the timeout.
        '''
        process = self.instances[pid]
        process.stop(timeout=timeout)

    def _print_stream(self, pid: int, readline: Callable[[], str]):
        '''
        Prints the log stream of a process.
        '''
        try:
            stop_flag = self.stop_flags[pid]
            log_queue = self.log_queues[pid]

            while not stop_flag.is_set():
                line = readline()
                if not line:
                    break
                line = line.rstrip('\r\n')
                log_queue.put(line)
        except Exception:
            text = traceback.format_exc()
            try:
                sc: Any
                println = sc._gateway.jvm.java.lang.System.err.println
                println(text)
            except Exception:
                print(text, file=sys.__stderr__)
