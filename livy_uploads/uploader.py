from base64 import b64encode
from io import BytesIO
import math
import os
import pickle
from tempfile import TemporaryDirectory
import shutil
from typing import Any, Callable, Optional, Tuple
from uuid import uuid4


from livy_uploads.endpoint import LivyEndpoint


class LivyUploader:
    '''
    A class to upload generic data to a remote Spark session using the Livy API.
    '''

    def __init__(self, endpoint: LivyEndpoint):
        self.endpoint = endpoint

    @classmethod
    def from_ipython(cls, name: Optional[str] = None) -> 'LivyUploader':
        '''
        Creates an uploader instance from the current IPython shell
        '''
        endpoint = LivyEndpoint.from_ipython(name)
        return cls(endpoint)

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
        self.endpoint.run_code(f'''
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

        self.endpoint.run_code(f'''
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

        headers = self.endpoint.build_headers()
        headers.pop('content-type', None)
        headers['accept'] = 'application/json'

        i = 0
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            chunk_name = f'{basename}.{i}'
            i += 1
            self.endpoint.post(
                '/upload-file',
                headers=headers,
                files={'file': (chunk_name, BytesIO(chunk))},
            )
            progress_func(i / num_chunks)

        return basename, i

    def send_pickled(self, obj: Any, var_name: str):
        '''
        Sends the object to the Spark session and assigns the result to a named global variable, using pickle to serialize it
        '''
        pickled_b64 = b64encode(pickle.dumps(obj)).decode('ascii')
        self.endpoint.run_code(f'''
            from base64 import b64decode
            import pickle
            
            var_name = {repr(var_name)}
            pickled_b64 = {repr(pickled_b64)}

            globals()[var_name] = pickle.loads(b64decode(pickled_b64))
        ''')

    def get_pickled(self, var_name: str) -> Any:
        '''
        Fetches the value of a global variable in the session, using pickle to serialize it
        '''
        _, value = self.endpoint.run_code(f'''
            var_name = {repr(var_name)}
            return globals()[var_name]
        ''')
        return value
