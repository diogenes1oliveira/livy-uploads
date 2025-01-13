import io
import os
import subprocess
from typing import Dict, List, Optional, Tuple


class PopenProcess:
    '''
    A command subprocess
    '''

    def __init__(self, args: List[str], env: Optional[Dict[str, str]] = None, cwd: Optional[str] = None):
        self.args = args
        self.env = env or dict(os.environ)
        self.cwd = cwd or None
        self.proc = None

    def start(self) -> Tuple[int, io.StringIO]:
        self.proc = subprocess.Popen(
            self.args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.env,
            universal_newlines=True,
            cwd=self.cwd,
        )
        return self.proc.pid, self.proc.stdout

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
