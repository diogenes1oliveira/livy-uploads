from base64 import b64encode, b64decode
from io import BytesIO
import json
import math
import os
import pickle
import textwrap
import time
from tempfile import TemporaryDirectory
import shutil
from typing import Callable, Dict, List, Optional, Tuple
from uuid import uuid4

import requests


class LivyUploader:
    '''
    A class to upload generic data to a remote Spark session using the Livy API.
    '''

    FUNC_PREFIX = 'livy_uploads_LivyUploader_'

    def __init__(
        self,
        url: str,
        session_id: int,
        default_headers: Optional[Dict[str, str]] = None,
        verify: bool = True,
        auth=None,
        requests_session: Optional[requests.Session] = None,
        pause: float = 0.3,
    ):
        '''
        Parameters:
        - url: the base URL of the Livy server
        - session_id: the ID of the Spark session to upload to
        - default_headers: a dictionary of headers to include in every request
        - verify: whether to verify the SSL certificate of the server
        - auth: an optional authentication object to pass to requests
        - requests_session: an optional requests.Session object to use for making requests
        - pause: the number of seconds to wait between polling for the status of a statement
        '''
        self.url = url.rstrip('/')
        self.session_id = session_id
        self.default_headers = {k.lower(): v for k, v in (
            default_headers or {}).items()}
        self.verify = verify
        self.auth = auth
        self.requests_session = requests_session or requests.Session()
        self.pause = pause

    @classmethod
    def from_ipython(cls, name: Optional[str] = None) -> 'LivyUploader':
        '''
        Creates an uploader instance from the current IPython shell
        '''
        from IPython.core.getipython import get_ipython

        kernel_magics = get_ipython(
        ).magics_manager.magics['cell']['send_to_spark'].__self__
        livy_session = kernel_magics.spark_controller.get_session_by_name_or_default(
            name)
        livy_client = livy_session.http_client._http_client

        session: requests.Session = livy_client._session

        return cls(
            url=livy_client._endpoint.url,
            session_id=livy_session.id,
            default_headers=livy_client._headers,
            verify=livy_client.verify_ssl,
            auth=livy_client._auth,
            requests_session=livy_client._session,
        )

    def upload_path(self, source_path: str, dest_path: Optional[str] = None, chunk_size: int = 50_000, mode: int = -1, progress_func: Optional[Callable[[float], None]] = None):
        '''
        Uploads a file or directory to the remote Spark session.

        Parameters:
        - source_path: the path to the file or directory to upload
        - dest_path: the path where the file or directory will be saved in the remote session. If not provided, the basename of the source path will be used.
        - chunk_size: the size of the chunks to split the file or archive into
        - mode: the permissions to set on the file or directory after reassembly or extraction. If -1, it will default to 0o600 for files and 0o700 for directories.
        - progress_func: an optional function that will be called with a float between 0 and 1 to indicate the progress of the upload
        '''
        if os.path.isdir(source_path):
            return self.upload_dir(
                source_path=source_path,
                dest_path=dest_path,
                chunk_size=chunk_size,
                mode=mode if mode != -1 else 0o700,
                progress_func=progress_func,
            )
        elif os.path.isfile(source_path):
            return self.upload_file(
                source_path=source_path,
                dest_path=dest_path,
                chunk_size=chunk_size,
                mode=mode if mode != -1 else 0o600,
                progress_func=progress_func,
            )
        elif not os.path.exists(source_path):
            raise FileNotFoundError(f'no such source path: {source_path!r}')
        else:
            raise Exception(f'unrecognized source path type: {source_path!r}')

    def upload_file(self, source_path: str, dest_path: Optional[str] = None, chunk_size: int = 50_000, mode: int = 0o600, progress_func: Optional[Callable[[float], None]] = None):
        '''
        Uploads a file to the remote Spark session.

        The file will be split into chunks of the specified size, uploaded and reassembled in the remote session.

        Parameters:
        - source_path: the path to the file to upload
        - dest_path: the path where the file will be saved in the remote session. If not provided, the basename of the source path will be used.
        - chunk_size: the size of the chunks to split the file into
        - mode: the permissions to set on the file after reassembly
        - progress_func: an optional function that will be called with a float between 0 and 1 to indicate the progress of the upload
        '''
        source_path = os.path.abspath(source_path)
        dest_path = dest_path or os.path.basename(source_path)
        with open(source_path, 'rb') as source:
            basename, num_chunks = self.upload_chunks(
                source=source,
                chunk_size=chunk_size,
                progress_func=progress_func,
            )
        self.run_code(f'''
            import os
            import os.path
            import pyspark

            basename = {repr(basename)}
            num_chunks = {num_chunks}
            dest_path = {repr(dest_path)}
            mode = {repr(mode)}

            os.makedirs(os.path.dirname(dest_path) or '.', exist_ok=True)
            with open(dest_path, 'wb') as fp:
                pass
            os.chmod(dest_path, mode)

            with open(dest_path, 'wb') as fp:
                for i in range(num_chunks):
                    chunk_name = f'{{basename}}.{{i}}'
                    with open(pyspark.SparkFiles.get(chunk_name), 'rb') as chunk_fp:
                        fp.write(chunk_fp.read())
        ''')

    def upload_dir(self, source_path: str, dest_path: Optional[str] = None, chunk_size: int = 50_000, mode: int = 0o700, progress_func: Optional[Callable[[float], None]] = None):
        '''
        Uploads a directory to the remote Spark session.

        The directory will be archived, uploaded and extracted in the remote session.

        Parameters:
        - source_path: the path to the directory to upload
        - dest_path: the path where the directory will be saved in the remote session. If not provided, the basename of the source path will be used.
        - chunk_size: the size of the chunks to split the archive into
        - mode: the permissions to set on the directory after extraction
        - progress_func: an optional function that will be called with a float between 0 and 1 to indicate the progress of the upload
        '''
        source_path = os.path.abspath(source_path)
        dest_path = dest_path or os.path.basename(source_path)
        archive_name = f'archive-{uuid4()}'

        with TemporaryDirectory() as tempdir:
            archive_source = shutil.make_archive(
                base_name=os.path.join(tempdir, archive_name),
                format='gztar',
                root_dir=source_path,
            )
            archive_dest = f'tmp/{os.path.basename(archive_source)}'

            self.upload_file(
                source_path=archive_source,
                dest_path=archive_dest,
                chunk_size=chunk_size,
                mode=0o700,
                progress_func=progress_func,
            )

        self.run_code(f'''
            import os
            import shutil

            archive_path = {repr(archive_dest)}
            dest_path = {repr(dest_path)}
            mode = {repr(mode)}
            
            try:
                if os.path.exists(dest_path):
                    shutil.rmtree(dest_path)

                os.makedirs(dest_path, exist_ok=True)
                os.chmod(dest_path, mode)

                shutil.unpack_archive(archive_path, dest_path)
            finally:
                try:
                    os.remove(archive_path)
                except FileNotFoundError:
                    pass
        ''')

    def upload_chunks(self, source: BytesIO, source_size: Optional[int] = None, chunk_size: int = 50_000, progress_func: Optional[Callable[[float], None]] = None) -> Tuple[str, int]:
        '''
        Uploads the chunks of a file-like object to the remote Spark session.

        Parameters:
        - source: the file-like object to read from
        - source_size: the size of the source file, if known. If not provided, the size will be determined by seeking to the end and back.
        - chunk_size: the size of the chunks to split the file into
        - progress_func: an optional function that will be called with a float between 0 and 1 to indicate the progress of the upload
        '''
        if source_size is None:
            source.seek(0, os.SEEK_END)
            source_size = source.tell()
            source.seek(0)

        num_chunks = math.ceil(source_size / chunk_size)
        basename = f'chunk-{uuid4()}'
        progress_func = progress_func or (lambda v: None)

        headers = self.build_headers()
        headers.pop('content-type', None)
        headers['accept'] = 'application/json'

        upload_url = f"{self.url}/sessions/{self.session_id}/upload-file"
        i = 0
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            chunk_name = f'{basename}.{i}'
            i += 1
            self.post(
                upload_url,
                headers=headers,
                files={'file': (chunk_name, BytesIO(chunk))},
            )
            progress_func(i / num_chunks)

        return basename, i

    def send_pickled(self, obj, var_name: str):
        '''
        Sends the object to the Spark session and assigns the result to a named global variable, using pickle to serialize it
        '''
        pickled_b64 = b64encode(pickle.dumps(obj)).decode('ascii')
        self.run_code(f'''
            from base64 import b64decode
            import pickle
            
            var_name = {repr(var_name)}
            pickled_b64 = {repr(pickled_b64)}

            globals()[var_name] = pickle.loads(b64decode(pickled_b64))
        ''')

    def get_pickled(self, var_name: str):
        '''
        Fetches the value of a global variable in the session, using pickle to serialize it
        '''
        out = self.run_code(f'''
            from base64 import b64encode
            import pickle
            
            var_name = {repr(var_name)}
            obj = globals()[var_name]

            pickled_b64 = b64encode(pickle.dumps(obj)).decode('ascii')
            print('pickled_b64', len(pickled_b64), pickled_b64)
        ''')
        if out['status'] != 'ok':
            raise Exception(f'bad output: {out}')
        prefix, size, data_b64 = out['data']['text/plain'].strip().split()
        if prefix != 'pickled_b64':
            raise Exception(f'bad output, unexpected prefix {prefix!r}: {out}')
        if int(size) != len(data_b64):
            raise Exception(
                f'bad output, len does not match (expected {len(data_b64)}, got {size}: {out}')

        return pickle.loads(b64decode(data_b64))

    def run_code(self, code: str):
        '''
        Executes the code snippet in the remote Livy session.

        The code should be a valid Python snippet that will be dedented automatically and wrapped in a function
        to avoid polluting the global namespace. If you do need to assign global variables, use the `globals()` dict.
        '''
        code = textwrap.indent(textwrap.dedent(code), '    ')

        func_name = self.FUNC_PREFIX + 'run_code'
        header = f'def {func_name}():\n'
        footer = f'\n\n{func_name}()'

        code = header + code + footer
        compile(code, 'source', mode='exec')  # no syntax errors

        execute_url = f"{self.url.rstrip('/')}/sessions/{self.session_id}/statements"
        r = self.post(
            execute_url,
            headers=self.build_headers(),
            json={
                'kind': 'pyspark',
                'code': code,
            },
        )
        r.raise_for_status()
        st_id = r.json()['id']

        st_url = f"{self.url.rstrip('/')}/sessions/{self.session_id}/statements/{st_id}"
        headers = self.build_headers()
        headers['accept'] = 'application/json'

        while True:
            r = self.get(st_url, headers=headers)
            st = r.json()
            if st['state'] in ('waiting', 'running'):
                time.sleep(self.pause)
                continue
            elif st['state'] == 'available':
                output = st['output']
                if output['status'] == 'error':
                    raise Exception(f'statement error: {output}')
                else:
                    return output

            raise Exception(f'statement failed: {st}')

    def run_command(self, args: List[str]) -> Tuple[int, List[str]]:
        '''
        Executes a subprocess command in the remote Livy session.

        Returns a tuple of the return code and the merged output lines of the command.
        '''
        out = self.run_code(f'''
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

            print(f'proc_returncode:', proc.returncode)
            for line in proc.stdout.splitlines():
                print('proc_output:', line)
        ''')

        if out['status'] != 'ok':
            raise Exception(f'bad output: {out}')

        text: str = out['data']['text/plain']
        lines = []
        returncode = None

        for line in text.splitlines():
            if line.startswith('proc_output: '):
                lines.append(line[len('proc_output: '):])
            elif line.startswith('proc_returncode: '):
                returncode = int(line[len('proc_returncode: '):])

        if returncode is None:
            raise Exception(f'bad output, no return code: {out}')

        return returncode, lines

    def build_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        '''
        Merges the list of default headers with the provided headers, and normalizes the keys to lowercase
        '''
        headers = {k.lower(): v for k, v in (headers or {}).items()}
        return {**self.default_headers, **headers}

    def post(self, url, **kwargs) -> requests.Response:
        r = self.requests_session.post(
            url,
            auth=self.auth,
            verify=self.verify,
            **kwargs,
        )
        r.raise_for_status()
        return r

    def get(self, url, **kwargs) -> requests.Response:
        r = self.requests_session.get(
            url,
            auth=self.auth,
            verify=self.verify,
            **kwargs,
        )
        r.raise_for_status()
        return r
