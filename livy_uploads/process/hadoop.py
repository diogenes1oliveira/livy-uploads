from contextlib import ExitStack
from logging import getLogger
from pathlib import Path
import shlex
import shutil
import socket
import stat
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse
from tempfile import TemporaryDirectory


StrOrPath = Union[str, Path]

LOGGER = getLogger(__name__)


class LivyUploadsHadoopProcessLocal:
    @classmethod
    def install_local(
        cls,
        archive_url: str,
        archive_home: StrOrPath,
        init_py: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        '''
        Locally install the archive within the remote session.

        Parameters:
        - archive_url: the URL of the archive of the program to run
        - archive_home: a relative path where the archive is to be extracted
        - script_path: the path to the start/stop script within the archive
        - init_py: Python code to adjust the environment before running the script.
            It should change the environment dictionary env: in place.

        Returns:
        - a dictionary with the environment variables to run the script
        '''
        from pyspark import SparkContext, SparkFiles
        sc = SparkContext.getOrCreate()

        try:
            u = urlparse(archive_url)
            archive_name = Path(u.path).name
        except ValueError:
            archive_name = Path(archive_url).name

        sc.addFile(archive_url)
        archive_path = SparkFiles.get(archive_name)
        archive_home = (Path(archive_home or Path.cwd()) / archive_home).absolute()

        with TemporaryDirectory() as tmpdir:
            output_path = tmpdir + '/out'
            shutil.unpack_archive(archive_path, output_path)
            items = list(Path(output_path).glob('*'))
            if not items or len(items) > 1 or not items[0].is_dir():
                raise Exception(f'expected only one directory in the archive: {items}')

            try:
                shutil.rmtree(archive_home)
            except FileNotFoundError:
                pass
            shutil.move(str(items[0]), archive_home)

        log_dir = (archive_home / 'logs')
        run_dir = (archive_home / 'run')
        conf_dir = (archive_home / 'conf')

        for d in [log_dir, run_dir, conf_dir]:
            try:
                shutil.rmtree(d)
            except FileNotFoundError:
                pass
            d.mkdir(mode=0o700, parents=True, exist_ok=True)

        env = {
            'ARCHIVE_HOME': str(archive_home),
            'LOG_DIR': str(log_dir),
            'RUN_DIR': str(run_dir),
            'CONF_DIR': str(conf_dir),
            'REMOTE_HOSTNAME': socket.getfqdn(),
        }

        if init_py:
            exec(init_py, dict(env=env))

        return env

    @classmethod
    def get_free_ports(cls, names: List[str]) -> Dict[str, str]:
        '''
        Get free TCP ports for the given names.
        '''
        result: Dict[str, str] = {}

        with ExitStack() as stack:
            for name in names:
                sock = socket.socket()
                stack.callback(sock.close)
                sock.bind(('localhost', 0))
                result[name] = str(sock.getsockname()[1])

        return result


class HadoopProcess:
    def __init__(
        self,
        archive_url: str,
        confdir: StrOrPath,
        archive_home: StrOrPath,
        init_py: Optional[str] = None,
        random_ports_envs: Optional[List[str]] = None,
    ):
        '''
        Parameters:
        - archive_url: the URL of the archive of the program to run
        - confdir: local configuration path
        - archive_home: a relative path where the archive is to be extracted
        - init_py: Python code to adjust the environment before running the script.
            It should change the environment dictionary env: in place.
        - random_ports_envs: a list of environment variables that will be set to
        random free TCP ports before the startup
        '''
        self.archive_url = archive_url
        self.confdir = Path(confdir).absolute()
        self.archive_home = Path(archive_home)
        self.init_py = init_py
        self.random_ports_envs = random_ports_envs or []

    def install(self, session) -> Dict[str, Optional[str]]:
        '''
        Install and configure the archive within the remote session.

        Returns:
        - a dictionary with the environment variables to run the script
        '''
        if not self.confdir.exists():
            raise ValueError(f'configuration directory does not exist: {self.confdir}')

        from livy_uploads.session import LivySessionEndpoint
        from livy_uploads.commands import LivyRunCode
        session: LivySessionEndpoint = session

        LOGGER.info('sending the Hadoop initialization code')
        code = Path(__file__).read_text()
        code += f'\nglobals()["{LivyUploadsHadoopProcessLocal.__name__}"] = {LivyUploadsHadoopProcessLocal.__name__}\n'
        LivyRunCode(code).run(session)

        LOGGER.info('installing the Hadoop archive')
        cmd = LivyRunCode(
            vars=dict(
                archive_url=self.archive_url,
                archive_home=self.archive_home,
                init_py=self.init_py,
            ),
            code='''
                return LivyUploadsHadoopProcessLocal.install_local(
                    archive_url=archive_url,
                    archive_home=archive_home,
                    init_py=init_py,
                )
            ''',
        )
        _, env = session.run(cmd)
        return env

    def configure(self, session, env: Dict[str, Optional[str]]):
        '''
        Templates and sends the config files
        '''
        from livy_uploads.session import LivySessionEndpoint
        from livy_uploads.commands import LivyRunCode, LivyUploadDir

        session: LivySessionEndpoint = session

        LOGGER.info('getting random ports')
        _, ports_env = session.run(LivyRunCode(
            vars=dict(random_ports_envs=self.random_ports_envs),
            code='''
                return LivyUploadsHadoopProcessLocal.get_free_ports(random_ports_envs)
            ''',
        ))
        LOGGER.info('got remote ports: %s', ports_env)
        env.update(ports_env)

        LOGGER.info('templating the config files')
        for conf in self.confdir.glob('*'):
            if '.tmpl' not in conf.name:
                continue
            mode = stat.S_IMODE(conf.stat().st_mode)
            content = conf.read_text() % env
            dst = self.confdir / conf.name.replace('.tmpl', '')
            dst.touch()
            dst.chmod(mode)
            dst.write_text(content)

        LOGGER.info('uploading the config files')
        session.run(LivyUploadDir(
            source_path=str(self.confdir),
            dest_path=env['CONF_DIR'],
        ))


    def start(self, manager, env: Dict[str, Optional[str]], start_cmd: List[str], stop_cmd: Optional[List[str]] = None) -> int:
        '''
        Starts the hadoop process remotely.

        Returns:
        - the PID of the remote process
        '''
        from livy_uploads.process.manager import ProcessManager
        from livy_uploads.process.daemon import DaemonProcess
        from livy_uploads.process import daemon

        manager: ProcessManager = manager

        process_class = manager.register(daemon.__name__ + ':' + DaemonProcess.__name__)

        kwargs = dict(
            start_cmd=start_cmd,
            stop_cmd=stop_cmd,
            pid_dir=env['RUN_DIR'],
            logs_dir=env['LOG_DIR'],
            cwd=env['ARCHIVE_HOME'],
            env=env,
        )

        return manager.start(process_class, **kwargs)
