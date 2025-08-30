from base64 import b64decode, b64encode
from io import BytesIO
from logging import getLogger
import os
import pickle
import shutil
import textwrap
from tempfile import TemporaryDirectory
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, Tuple
from uuid import uuid4

from livy_uploads.exceptions import LivyStatementError
from livy_uploads.session import LivySession, LivyCommand
from livy_uploads.commands import LivyRunCode, LivyUploadFile


LOGGER = getLogger(__name__)
T = TypeVar('T')


class LivyExecutorMaster(LivyCommand[str]):
    '''
    Starts the executor master.
    '''

    def __init__(self, wstunnel_bin: Optional[str] = None):
        '''
        Parameters:
        - wstunnel_bin: the path to the wstunnel binary
        '''
        self.wstunnel_bin: str = wstunnel_bin or shutil.which('wstunnel') or '/usr/bin/wstunnel'

    def run(self, session: 'LivySession') -> str:
        '''
        Executes the upload
        '''
        LOGGER.info('checking if wstunnel is uploaded already')
        try:
            LivyRunCode(
                code='''
                import subprocess
                subprocess.run(['./wstunnel', '--version'], capture_output=True, check=True)
                '''
            ).run(session)
        except LivyStatementError:
            LOGGER.info('Uploading wstunnel binary at %r to the Livy session', self.wstunnel_bin)
            LivyUploadFile(
                source_path=self.wstunnel_bin,
                dest_path='./wstunnel',
                mode=0o755,
            ).run(session)
        else:
            LOGGER.info('wstunnel is already uploaded')

        LOGGER.info('Initializing executor master')
        code = f'''
            import base64
            import os
            import socket
            import subprocess
            import shutil
            import time
            from typing import List

            def get_free_port():
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind(('localhost', 0))
                    return s.getsockname()[1]

            def run_command(ws_url: str, ws_port: int, args: List[str]):
                try:
                    os.makedirs('bin')
                except FileExistsError:
                    pass

                wstunnel_bin = os.path.abspath('bin/wstunnel')
                shutil.copy('./wstunnel', wstunnel_bin)
                shutil.chmod(wstunnel_bin, 0o755)

                ws_proc = subprocess.Popen(
                    [wstunnel_bin, 'client', ws_url, '-L', f'stdio://localhost:{{ws_port}}'],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                )
                cmd_proc = subprocess.Popen(
                    args,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )

                ws_proc.wait()
                cmd_proc.wait()
                with open('bin/run', 'w') as f:
                    f.write(f'''
                    '''
                )
                spark.sparkContext.parallelize(args).foreach(lambda x: subprocess.run(x, shell=True))

            port = get_free_port()
            hostname = socket.getfqdn()

            try:
                process
            except NameError:
                pass
            else:
                process.kill()
                process.wait(1.5)

            process = subprocess.Popen(
                ['./wstunnel', 'server', f'wss://0.0.0.0:{{port}}'],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )

            time.sleep(1)
            url = f'wss://{{hostname}}:{{port}}'
            return url, process.pid
        '''
        _, (url, pid) = LivyRunCode(code=code).run(session)

        LOGGER.info('Executor master initialized at %r (wstunnel pid: %r)', url, pid)
        return url