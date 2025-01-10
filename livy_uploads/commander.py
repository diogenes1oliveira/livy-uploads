import json
import time
from typing import List, Optional, Tuple


from livy_uploads.endpoint import LivyEndpoint
from livy_uploads import commands_remote


class LivyCommander:
    '''
    A class to execute commands in a remote Spark session using the Livy API.
    '''

    _initialized = False

    def __init__(self, endpoint: LivyEndpoint):
        self.endpoint = endpoint

    @classmethod
    def from_ipython(cls, name: Optional[str] = None) -> 'LivyCommander':
        '''
        Creates a commander instance from the current IPython shell
        '''
        endpoint = LivyEndpoint.from_ipython(name)
        return cls(endpoint)

    def _initialize(self):
        if not self.__class__._initialized:
            with open(commands_remote.__file__, 'r') as fp:
                code = fp.read()

            code += '\nglobals()["ProcessManager"] = ProcessManager\n'
            self.endpoint.run_code(code)
            self.__class__._initialized = True

    def run_command(self, args: List[str]) -> Tuple[int, List[str]]:
        '''
        Executes a subprocess command in the remote Livy session.

        Returns a tuple of the return code and the merged output lines of the command.
        '''
        _, (returncode, stdout) = self.endpoint.run_code(f'''
            import json
            import subprocess
            
            args = {json.dumps(args)}

            proc = subprocess.run(
                args,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            return proc.returncode, proc.stdout
        ''')
        returncode = int(returncode)
        stdout = str(stdout)

        return returncode, stdout.splitlines()

    def run_command_fg(self, args: List[str], pause: float = 1.5):
        '''
        Executes a subprocess command in the remote Livy session.
        '''
        name = self.start_command(args)
        while True:
            lines, returncode = self.poll_command(name)
            for line in lines:
                print(line)
            if returncode is not None:
                print(f'$ process finished with return code {returncode}')
                break
            time.sleep(pause)

    def start_command(self, args: List[str]) -> str:
        '''
        Starts a subprocess command in the remote Livy session.
        '''
        self._initialize()

        _, name = self.endpoint.run_code(f'''
            args = {json.dumps(args)}
            return ProcessManager.start(args)
        ''')
        return str(name)

    def poll_command(self, name: str, max_lines: int = 500) -> Tuple[List[str], Optional[int]]:
        _, (lines, returncode) = self.endpoint.run_code(f'''
            name = {repr(name)}
            max_lines = {max_lines}

            return ProcessManager.poll(name, max_lines)
        ''')
        return lines, returncode

    def stop_command(self, name: str, timeout: float = 2.0):
        '''
        Starts a subprocess command in the remote Livy session.
        '''
        self.endpoint.run_code(f'''
            name = {repr(name)}
            timeout = {repr(timeout)}

            return ProcessManager.stop(name, timeout)
        ''')
