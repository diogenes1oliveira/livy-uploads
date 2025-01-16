import io
import os
import subprocess
from typing import Callable, Dict, List, Optional, Tuple


class PopenProcess:
    '''
    A command subprocess
    '''

    def __init__(self, args: List[str], env: Optional[Dict[str, Optional[str]]] = None, cwd: Optional[str] = None):
        self.args = args
        self.cwd = cwd or None
        self.proc = None
        self.env = dict(os.environ)

        for k, v in (env or {}).items():
            if v is None:
                self.env.pop(k, None)
            else:
                self.env[k] = v

    def start(self) -> Tuple[int, Callable[[], str]]:
        self.proc = subprocess.Popen(
            self.args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.env,
            universal_newlines=True,
            cwd=self.cwd,
        )
        return self.proc.pid, self.proc.stdout.readline

    def poll(self) -> Optional[int]:
        try:
            self.proc.wait(timeout=0.2)
        except subprocess.TimeoutExpired:
            pass
        return self.proc.poll()

    def stop(self, timeout: float):
        self.proc.terminate()
        try:
            self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
