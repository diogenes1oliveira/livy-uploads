import os
import pickle
import shutil
import textwrap
from base64 import b64decode, b64encode
from io import BytesIO
from logging import getLogger
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union
from uuid import uuid4

from livy_uploads.exceptions import LivyStatementError
from livy_uploads.retry_policy import TimeoutRetryPolicy
from livy_uploads.session import LivyCommand, LivySession
from livy_uploads.utils import assert_type

LOGGER = getLogger(__name__)
T = TypeVar("T")


class CodeRetriableError(Exception):
    pass


class CodeRetryPolicy(TimeoutRetryPolicy):
    def __init__(self, timeout: float, pause: float):
        super().__init__(timeout, pause)
        self.timeout = timeout
        self.pause = pause

    def should_retry(self, e: Exception) -> bool:
        return super().should_retry(e) and isinstance(e, CodeRetriableError)


StrOrPath = Union[str, Path]


class LivyRunCode(LivyCommand[Tuple[List[str], Any]]):
    """
    Executes the code in the global namespace of the remote Livy session.

    Assigns the return value from the `_` variable.
    """

    def __init__(
        self,
        code: str,
        pause: float = 0.3,
        run_timeout: float = 30.0,
        request_timeout: float = 5.0,
        vars: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            code: the code block to execute. Will be dedented automatically.
            pause: the pause between retries
            run_timeout: the timeout for the entire command
            request_timeout: the timeout for each request to the Livy server
            vars: the variables to be injected before the code block
        """
        self.code = code
        self.vars = vars or {}
        self.retry_policy = CodeRetryPolicy(run_timeout, pause)
        self.request_timeout = request_timeout

    def run(self, session: "LivySession") -> Any:
        var_lines = [
            "import pickle",
            "import base64",
            "_ = None",
        ]

        for var_name, var_value in self.vars.items():
            pickled_b64 = b64encode(pickle.dumps(var_value)).decode("ascii")
            var_lines.append(f"{var_name} = pickle.loads(base64.b64decode({repr(pickled_b64)}))")

        return_lines = [
            "pickled_b64 = base64.b64encode(pickle.dumps(_)).decode('ascii')",
            "print('\\nLivyUploads:pickled_b64', len(pickled_b64), pickled_b64, end='\\n')",
        ]

        code = "\n".join(var_lines) + "\n" + textwrap.dedent(self.code) + "\n" + "\n".join(return_lines)

        r = session.request(
            "POST",
            f"/sessions/{session.session_id}/statements",
            json={
                "kind": "pyspark",
                "code": code,
            },
            timeout=self.request_timeout,
        )
        st_id = r.json()["id"]

        st_path = f"/sessions/{session.session_id}/statements/{st_id}"
        headers = session.build_headers({"accept": "application/json"})

        def _check() -> dict:
            r = session.request("GET", st_path, headers=headers, timeout=self.request_timeout)
            r.raise_for_status()
            st = assert_type(r.json(), dict)
            state = st["state"]
            if state in ("waiting", "running"):
                raise CodeRetriableError(f"statement {st_id} is still in state={state!r}")
            elif state == "available":
                return st
            else:
                raise Exception(f"statement failed to execute: {st}")

        st = self.retry_policy.run(_check)

        output = st["output"]
        if output["status"] != "ok":
            raise LivyStatementError(
                output["ename"],
                output["evalue"],
                output["traceback"],
            )
        try:
            lines: List[str] = output["data"]["text/plain"].strip().splitlines()
        except KeyError:
            raise Exception(f"non-textual output: {output}")

        try:
            final_lines: List[str] = []
            for line in lines:
                if not line.startswith("LivyUploads:pickled_b64"):
                    final_lines.append(line)
                    continue

                parts = line.strip().split()
                try:
                    prefix, size_s, data_b64 = parts
                    size = int(size_s)
                except Exception as e:
                    raise RuntimeError(f"bad status line in {line!r}") from e

                if size != len(data_b64):
                    raise ValueError(f"bad output, len does not match (expected {len(data_b64)}, got {size}: {lines}")
                if prefix != "LivyUploads:pickled_b64":
                    raise ValueError(f"bad output, unexpected prefix {prefix!r}")

                value = pickle.loads(b64decode(data_b64))
                return lines[:-1], value

        except Exception as e:
            raise Exception(f"bad output, unexpected format: {output}") from e


class LivyUploadBlob(LivyCommand[None]):
    """
    Uploads a file-like object to the remote Spark session.

    The path to file can then be obtained remotely with `pyspark.SparkFiles.get(name)`.
    """

    def __init__(self, source: BytesIO, name: str, request_timeout: float = 5.0):
        """
        Args:
            source: the file-like object to read from
            name: the name of the uploaded file
            request_timeout: the timeout for the POST request to the Livy server
        """
        self.source = source
        self.name = name
        self.request_timeout = request_timeout

    def run(self, session: "LivySession") -> None:
        """
        Executes the upload
        """
        headers = session.build_headers({"accept": "application/json"})
        headers.pop("content-type", None)

        session.request(
            "POST",
            f"/sessions/{session.session_id}/upload-file",
            headers=headers,
            files={"file": (self.name, self.source)},
            timeout=self.request_timeout,
        )


class LivyUploadFile(LivyCommand[str]):
    """
    Uploads a (potentially large) file in chunks to a path in the remote Spark session.
    """

    def __init__(
        self,
        source_path: StrOrPath,
        dest_path: Optional[str] = None,
        chunk_size: int = 50_000,
        mode: int = 0o600,
        chunk_timeout: float = 5.0,
        request_timeout: float = 5.0,
        run_timeout: float = 10.0,
        progress_func: Optional[Callable[[float], None]] = None,
    ):
        """
        Parameters:
        - source_path: the local path to the file to upload
        - dest_path: the path where the file will be saved in the remote session. If not provided, the basename of the source path will be used.
        - chunk_size: the size of the chunks to split the file into
        - mode: the permissions to set on the file after reassembly
        - chunk_timeout: the timeout for each chunk upload
        - request_timeout: the timeout for each request to the Livy server
        - run_timeout: the timeout for the merge command
        - progress_func: an optional function that will be called with a float between 0 and 1 to indicate the progress of the upload
        """
        self.source_path = str(source_path)
        self.dest_path = dest_path or os.path.basename(os.path.abspath(source_path))
        self.chunk_size = chunk_size
        self.mode = mode
        self.chunk_timeout = chunk_timeout
        self.request_timeout = request_timeout
        self.run_timeout = run_timeout
        self.progress_func = progress_func or (lambda _: None)

    def run(self, session: "LivySession") -> str:
        """
        Executes the uploading of the file to the remote Spark session.

        The file will be split into chunks of the specified size, uploaded and reassembled in the remote session.
        """
        file_size = os.stat(self.source_path).st_size
        num_chunks = (file_size + self.chunk_size - 1) // self.chunk_size
        basename = f"upload-file-{uuid4()}"

        with open(self.source_path, "rb") as source:
            i = 0
            while True:
                chunk = source.read(self.chunk_size)
                if not chunk:
                    break
                upload_cmd = LivyUploadBlob(BytesIO(chunk), f"{basename}.{i}", request_timeout=self.chunk_timeout)
                upload_cmd.run(session)
                i += 1
                self.progress_func(i / num_chunks)

        num_chunks = i

        merge_cmd = LivyRunCode(
            request_timeout=self.request_timeout,
            run_timeout=self.run_timeout,
            vars=dict(
                basename=basename,
                num_chunks=num_chunks,
                dest_path=self.dest_path,
                mode=self.mode,
            ),
            code=f"""
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

                _ = os.path.realpath(dest_path)
            """,
        )

        _, path = merge_cmd.run(session)
        return assert_type(path, str)


class LivyUploadDir(LivyCommand[str]):
    """
    Uploads a (potentially large) directory in chunks to a path in the remote Spark session.
    """

    def __init__(
        self,
        source_path: StrOrPath,
        dest_path: Optional[str] = None,
        chunk_size: int = 50_000,
        mode: int = 0o700,
        chunk_timeout: float = 5.0,
        request_timeout: float = 5.0,
        run_timeout: float = 10.0,
        unpack_timeout: float = 30.0,
        progress_func: Optional[Callable[[float], None]] = None,
    ):
        """
        Parameters:
        - source_path: the path to the directory to upload
        - dest_path: the path where the directory will be saved in the remote session. If not provided, the basename of the source path will be used.
        - chunk_size: the size of the chunks to split the archive into
        - mode: the permissions to set on the directory after extraction
        - chunk_timeout: the timeout for each chunk upload
        - request_timeout: the timeout for each request to the Livy server
        - run_timeout: the timeout for the merge command
        - unpack_timeout: the timeout for the unpack command
        - progress_func: an optional function that will be called with a float between 0 and 1 to indicate the progress of the upload
        """
        if not source_path:
            raise ValueError("source_path must be provided")
        self.source_path = str(source_path)
        self.dest_path = dest_path or os.path.basename(os.path.abspath(source_path))
        self.chunk_size = chunk_size
        self.mode = mode or 0o700
        self.chunk_timeout = chunk_timeout
        self.request_timeout = request_timeout
        self.run_timeout = run_timeout
        self.unpack_timeout = unpack_timeout
        self.progress_func = progress_func or (lambda _: None)

    def run(self, session: "LivySession") -> str:
        """
        Executes the uploading of the directory to the remote Spark session.

        The directory will be archived, split into chunks of the specified size, uploaded, reassembled and extracted in the remote session.
        """
        archive_name = f"archive-{uuid4()}"

        with TemporaryDirectory() as tempdir:
            archive_source = shutil.make_archive(
                base_name=os.path.join(tempdir, archive_name),
                format="gztar",
                root_dir=self.source_path,
            )
            archive_dest = f"tmp/{os.path.basename(archive_source)}"
            upload_cmd = LivyUploadFile(
                source_path=archive_source,
                dest_path=archive_dest,
                chunk_size=self.chunk_size,
                chunk_timeout=self.chunk_timeout,
                request_timeout=self.request_timeout,
                run_timeout=self.run_timeout,
                progress_func=self.progress_func,
            )
            upload_cmd.run(session)

        extract_cmd = LivyRunCode(
            vars=dict(
                archive_dest=archive_dest,
                dest_path=self.dest_path,
                mode=self.mode,
            ),
            request_timeout=self.request_timeout,
            run_timeout=self.unpack_timeout,
            code=f"""
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

                _ = os.path.realpath(dest_path)
            """,
        )

        _, path = extract_cmd.run(session)
        return assert_type(path, str)


class LivyRunShell(LivyCommand[Tuple[int, str, Optional[int]]]):
    """
    Executes a shell command in the remote Spark session.
    """

    def __init__(self, command: str, run_timeout: float = 5.0, stop_timeout: float = 2.0):
        """
        Args:
            command: the shell command to execute
            run_timeout: the maximum time to wait for the command to complete.
                SIGTERM will be sent to process if this time is exceeded
            stop_timeout: time to wait after sending SIGTERM to wait for the process to properly die
        """
        self.command = command
        self.run_timeout = run_timeout
        self.stop_timeout = stop_timeout

    def run(self, session: "LivySession") -> Tuple[int, str, Optional[int]]:
        """
        Executes the command and returns the PID, the output and the return code.
        """
        code_cmd = LivyRunCode(
            vars=dict(
                command=self.command,
                run_timeout=self.run_timeout,
                stop_timeout=self.stop_timeout,
            ),
            code="""
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

                _ = proc.pid, proc.stdout.read(), proc.poll()
            """,
        )
        _, (pid, output, returncode) = code_cmd.run(session)
        return (
            assert_type(pid, int),
            assert_type(output, str),
            (assert_type(returncode, int) if returncode is not None else None),
        )
