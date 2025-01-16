from base64 import b64decode, b64encode
from io import BytesIO
from logging import getLogger
import os
import pickle
import shutil
import textwrap
from tempfile import TemporaryDirectory
import time
from typing import Any, Callable, Dict, List, Optional, TypeVar, Tuple
from uuid import uuid4

from livy_uploads.exceptions import LivyStatementError
from livy_uploads.session import LivySessionEndpoint, LivyCommand


LOGGER = getLogger(__name__)
T = TypeVar('T')


class LivyRunCode(LivyCommand[Tuple[List[str], Any]]):
    FUNC_PREFIX = 'livy_uploads_LivyRunCode_'

    '''
    Executes the function code snippet in the remote Livy session.

    This will wrap the code in a function to avoid polluting the global namespace. If you do need
    to assign variables, use the `globals()` dict. Also, you can use the return statement to
    get values back from the remote session.
    '''

    def __init__(self, code: str, pause: float = 0.3, vars: Optional[Dict[str, Any]] = None):
        '''
        Parameters:
        - code: the Pyspark function code to execute. It will be dedented automatically.
        - vars: variables to assign before the code. The values must be pickleable.
        '''
        self.code = code
        self.vars = vars or {}
        self.pause = pause

    def run(self, session: 'LivySessionEndpoint') -> Tuple[List[str], Any]:
        '''
        Returns:
        - a tuple of the output lines and the return value of the code
        '''
        code = ''

        code_name = self.FUNC_PREFIX + 'code'
        run_name = self.FUNC_PREFIX + 'run'

        code += '\n' + f'def {code_name}():'

        if vars:
            # inject the pickled variables
            code += '\n' + textwrap.indent(textwrap.dedent('''
                from base64 import b64decode
                import pickle
            '''), '    ')
            for var_name, var_value in self.vars.items():
                pickled_b64 = b64encode(pickle.dumps(var_value)).decode('ascii')
                code += f'\n    {var_name} = pickle.loads(b64decode({repr(pickled_b64)}))'
            code += '\n    del b64decode, pickle'

        code += '\n' + textwrap.indent(textwrap.dedent(self.code), '    ')
        code += f'\n' + textwrap.dedent(f'''
            def {run_name}():
                from base64 import b64encode
                import pickle

                value = {code_name}()

                pickled_b64 = b64encode(pickle.dumps(value)).decode('ascii')
                print('\\nLivyUploads:pickled_b64', len(pickled_b64), pickled_b64, end='\\n')

            {run_name}()
            del {run_name}, {code_name}
        ''')

        compile(code, 'source', mode='exec')  # no syntax errors

        r = session.post(
            f"/sessions/{session.session_id}/statements",
            json={
                'kind': 'pyspark',
                'code': code,
            },
        )
        st_id = r.json()['id']

        st_path = f"/sessions/{session.session_id}/statements/{st_id}"
        headers = session.build_headers({'accept': 'application/json'})

        while True:
            r = session.get(st_path, headers=headers)
            st = r.json()
            if st['state'] in ('waiting', 'running'):
                time.sleep(self.pause)
                continue
            elif st['state'] == 'available':
                break
            else:
                raise Exception(f'statement failed to execute: {st}')

        output = st['output']
        if output['status'] != 'ok':
            exc = LivyStatementError(
                output['ename'],
                output['evalue'],
                output['traceback'],
            )
            exc_builtin = exc.as_builtin()
            if exc_builtin:
                raise exc_builtin from exc
            else:
                raise exc
        try:
            lines: List[str] = output['data']['text/plain'].strip().splitlines()
        except KeyError:
            raise Exception(f'non-textual output: {output}')

        try:
            final_lines: List[str] = []
            for line in lines:
                if not line.startswith('LivyUploads:pickled_b64'):
                    final_lines.append(line)
                    continue

                parts = line.strip().split()
                try:
                    prefix, size, data_b64 = parts
                    size = int(size)
                except Exception as e:
                    raise RuntimeError(f'bad status line in {line!r}') from e

                if size != len(data_b64):
                    raise ValueError(
                        f'bad output, len does not match (expected {len(data_b64)}, got {size}: {lines}'
                    )
                if prefix != 'LivyUploads:pickled_b64':
                    raise ValueError(f'bad output, unexpected prefix {prefix!r}')

                value = pickle.loads(b64decode(data_b64))
                return lines[:-1], value

        except Exception as e:
            raise Exception(f'bad output, unexpected format: {output}') from e


class LivyUploadBlob(LivyCommand):
    '''
    Uploads a file-like object to the remote Spark session.

    The path to file can then be obtained remotely with `pyspark.SparkFiles.get(name)`.
    '''

    def __init__(self, source: BytesIO, name: str):
        '''
        Parameters:
        - source: the file-like object to read from
        - name: the name of the uploaded file
        '''
        self.source = source
        self.name = name

    def run(self, session: 'LivySessionEndpoint'):
        '''
        Executes the upload
        '''
        headers = session.build_headers({'accept': 'application/json'})
        headers.pop('content-type', None)

        session.post(
            f'/sessions/{session.session_id}/upload-file',
            headers=headers,
            files={'file': (self.name, self.source)},
        )


class LivyUploadFile(LivyCommand[str]):
    '''
    Uploads a (potentially large) file in chunks to a path in the remote Spark session.
    '''

    def __init__(self, source_path: str, dest_path: Optional[str] = None, chunk_size: int = 50_000, mode: int = 0o600, progress_func: Optional[Callable[[float], None]] = None):
        '''
        Parameters:
        - source_path: the path to the file to upload
        - dest_path: the path where the file will be saved in the remote session. If not provided, the basename of the source path will be used.
        - chunk_size: the size of the chunks to split the file into
        - mode: the permissions to set on the file after reassembly
        - progress_func: an optional function that will be called with a float between 0 and 1 to indicate the progress of the upload
        '''
        self.source_path = source_path
        self.dest_path = dest_path or os.path.basename(os.path.abspath(source_path))
        self.chunk_size = chunk_size
        self.mode = mode
        self.progress_func = progress_func or (lambda _: None)

    def run(self, session: 'LivySessionEndpoint') -> str:
        '''
        Executes the uploading of the file to the remote Spark session.

        The file will be split into chunks of the specified size, uploaded and reassembled in the remote session.
        '''
        file_size = os.stat(self.source_path).st_size
        num_chunks = (file_size + self.chunk_size - 1) // self.chunk_size
        basename = f'upload-file-{uuid4()}'

        with open(self.source_path, 'rb') as source:
            i = 0
            while True:
                chunk = source.read(self.chunk_size)
                if not chunk:
                    break
                upload_cmd = LivyUploadBlob(BytesIO(chunk), f'{basename}.{i}')
                upload_cmd.run(session)
                i += 1
                self.progress_func(i / num_chunks)

        num_chunks = i

        merge_cmd = LivyRunCode(
            vars=dict(
                basename=basename,
                num_chunks=num_chunks,
                dest_path=self.dest_path,
                mode=self.mode,
            ),
            code=f'''
                import os
                import os.path
                import pyspark

                os.makedirs(os.path.dirname(dest_path) or '.', exist_ok=True)
                with open(dest_path, 'wb') as fp:
                    pass
                os.chmod(dest_path, mode)

                with open(dest_path, 'wb') as fp:
                    for i in range(num_chunks):
                        chunk_name = f'{{basename}}.{{i}}'
                        with open(pyspark.SparkFiles.get(chunk_name), 'rb') as chunk_fp:
                            fp.write(chunk_fp.read())

                return os.path.realpath(dest_path)
            ''',
        )

        return merge_cmd.run(session)


class LivyUploadDir(LivyCommand[str]):
    '''
    Uploads a (potentially large) directory in chunks to a path in the remote Spark session.
    '''

    def __init__(self, source_path: str, dest_path: Optional[str] = None, chunk_size: int = 50_000, mode: int = 0o700, progress_func: Optional[Callable[[float], None]] = None):
        '''
        Parameters:
        - source_path: the path to the directory to upload
        - dest_path: the path where the directory will be saved in the remote session. If not provided, the basename of the source path will be used.
        - chunk_size: the size of the chunks to split the archive into
        - mode: the permissions to set on the directory after extraction
        - progress_func: an optional function that will be called with a float between 0 and 1 to indicate the progress of the upload
        '''
        if not source_path:
            raise ValueError('source_path must be provided')
        self.source_path = source_path
        self.dest_path = dest_path or os.path.basename(os.path.abspath(source_path))
        self.chunk_size = chunk_size
        self.mode = mode or 0o700
        self.progress_func = progress_func or (lambda _: None)

    def run(self, session: 'LivySessionEndpoint') -> str:
        '''
        Executes the uploading of the directory to the remote Spark session.

        The directory will be archived, split into chunks of the specified size, uploaded, reassembled and extracted in the remote session.
        '''
        archive_name = f'archive-{uuid4()}'

        with TemporaryDirectory() as tempdir:
            archive_source = shutil.make_archive(
                base_name=os.path.join(tempdir, archive_name),
                format='gztar',
                root_dir=self.source_path,
            )
            archive_dest = f'tmp/{os.path.basename(archive_source)}'
            upload_cmd = LivyUploadFile(
                source_path=archive_source,
                dest_path=archive_dest,
                chunk_size=self.chunk_size,
                progress_func=self.progress_func,
            )
            upload_cmd.run(session)

        extract_cmd = LivyRunCode(
            vars=dict(
                archive_dest=archive_dest,
                dest_path=self.dest_path,
                mode=self.mode,
            ),
            code=f'''
                import os
                import shutil

                try:
                    try:
                        shutil.rmtree(dest_path)
                    except FileNotFoundError:
                        pass

                    shutil.unpack_archive(archive_dest, dest_path)
                    os.chmod(dest_path, mode)
                finally:
                    try:
                        os.remove(archive_dest)
                    except FileNotFoundError:
                        pass

                return os.path.realpath(dest_path)
            ''',
        )

        return extract_cmd.run(session)


class LivyRunShell(LivyCommand[Tuple[str, int]]):
    '''
    Executes a shell command in the remote Spark session.
    '''

    def __init__(self, command: str, run_timeout: float = 5.0, stop_timeout: float = 2.0):
        '''
        Parameters:
        - command: the shell command to execute
        - run_timeout: the maximum time to wait for the command to complete. SIGTERM will be
        sent to process if this time is exceeded
        - stop_timeout: time to wait after sending SIGTERM to wait for the process to properly die
        '''
        self.command = command
        self.run_timeout = run_timeout
        self.stop_timeout = stop_timeout

    def run(self, session: 'LivySessionEndpoint') -> Tuple[str, int]:
        '''
        Executes the command and returns the output and the return code.
        '''
        code_cmd = LivyRunCode(
            vars=dict(
                command=self.command,
                run_timeout=self.run_timeout,
                stop_timeout=self.stop_timeout,
            ),
            code='''
                import subprocess

                proc = subprocess.Popen(
                    ['bash', '-c', command],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                )
                try:
                    proc.wait(run_timeout)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    try:
                        proc.wait(stop_timeout)
                    except suprocess.TimeoutExpired:
                        proc.kill()

                return proc.stdout.read(), proc.poll()
            ''',
        )
        _, (output, returncode) = code_cmd.run(session)
        return output, returncode
