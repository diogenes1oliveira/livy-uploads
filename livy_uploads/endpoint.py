from base64 import b64decode
import pickle
import textwrap
import time
from typing import Any, Dict, List, Optional, Tuple

import requests


class LivyEndpoint:
    '''
    A class to upload generic data to a remote Spark session using the Livy API.
    '''

    FUNC_PREFIX = 'livy_uploads_LivyEndpoint_'

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
        - session_id: the ID of the Spark session to use
        - default_headers: a dictionary of headers to include in every request
        - verify: whether to verify the SSL certificate of the server
        - auth: an optional authentication object to pass to requests
        - requests_session: an optional requests.Session object to use for making requests
        - pause: the number of seconds to wait between polling for the status of a statement
        '''
        self.url = url.rstrip('/') + f'/sessions/{session_id}'
        self.session_id = session_id
        self.default_headers = {k.lower(): v for k, v in (
            default_headers or {}).items()}
        self.verify = verify
        self.auth = auth
        self.requests_session = requests_session or requests.Session()
        self.pause = pause

    @classmethod
    def from_ipython(cls, name: Optional[str] = None) -> 'LivyEndpoint':
        '''
        Creates an endpoint instance from the current IPython shell
        '''
        from IPython.core.getipython import get_ipython

        kernel_magics = get_ipython(
        ).magics_manager.magics['cell']['send_to_spark'].__self__
        livy_session = kernel_magics.spark_controller.get_session_by_name_or_default(
            name)
        livy_client = livy_session.http_client._http_client

        return cls(
            url=livy_client._endpoint.url,
            session_id=livy_session.id,
            default_headers=livy_client._headers,
            verify=livy_client.verify_ssl,
            auth=livy_client._auth,
            requests_session=livy_client._session,
        )

    def run_code(self, code: str) -> Tuple[List[str], Any]:
        '''
        Executes the code snippet in the remote Livy session.

        Because the code will be wrapped in a function, you can use the return statement to send data back.

        The code should be a valid Python snippet that will be dedented automatically and wrapped in a function
        to avoid polluting the global namespace. If you do need to assign global variables, use the `globals()` dict.
        '''
        user_code = code
        code = ''

        code_name = self.FUNC_PREFIX + 'code'
        run_name = self.FUNC_PREFIX + 'run'

        code += f'def {code_name}():\n'
        code += textwrap.indent(textwrap.dedent(user_code), '    ')
        code += f'\n' + textwrap.dedent(f'''
            def {run_name}():
                from base64 import b64encode
                import pickle

                value = {code_name}()

                pickled_b64 = b64encode(pickle.dumps(value)).decode('ascii')
                print('\\npickled_b64', len(pickled_b64), pickled_b64, end='')

            {run_name}()
        ''')

        compile(code, 'source', mode='exec')  # no syntax errors

        r = self.post(
            "/statements",
            headers=self.build_headers(),
            json={
                'kind': 'pyspark',
                'code': code,
            },
        )
        r.raise_for_status()
        st_id = r.json()['id']

        st_path = f"/statements/{st_id}"
        headers = self.build_headers()
        headers['accept'] = 'application/json'

        while True:
            r = self.get(st_path, headers=headers)
            st = r.json()
            if st['state'] in ('waiting', 'running'):
                time.sleep(self.pause)
                continue
            elif st['state'] == 'available':
                break
            else:
                raise Exception(f'statement failed: {st}')

        output = st['output']
        if output['status'] != 'ok':
            raise Exception(f'non-ok status in statement: {output}')

        try:
            lines: List[str] = output['data']['text/plain'].strip().splitlines()
        except KeyError:
            raise Exception(f'non-textual output: {output}')

        try:
            prefix, size, data_b64 = lines[-1].strip().split()
            size = int(size)
            if size != len(data_b64):
                raise ValueError(
                    f'bad output, len does not match (expected {len(data_b64)}, got {size}: {lines}'
                )
            if prefix != 'pickled_b64':
                raise ValueError(f'bad output, unexpected prefix {prefix!r}')

            value =  pickle.loads(b64decode(data_b64))
            return lines[:-1], value

        except Exception as e:
            raise Exception(f'bad output, unexpected format: {output}') from e

    def build_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        '''
        Merges the list of default headers with the provided headers, and normalizes the keys to lowercase
        '''
        headers = {k.lower(): v for k, v in (headers or {}).items()}
        return {**self.default_headers, **headers}

    def post(self, path: str, **kwargs) -> requests.Response:
        r = self.requests_session.post(
            self.url + path,
            auth=self.auth,
            verify=self.verify,
            **kwargs,
        )
        r.raise_for_status()
        return r

    def get(self, path: str, **kwargs) -> requests.Response:
        r = self.requests_session.get(
            self.url + path,
            auth=self.auth,
            verify=self.verify,
            **kwargs,
        )
        r.raise_for_status()
        return r
