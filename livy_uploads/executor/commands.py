#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Commands to schedule a process in the remote cluster.
'''

__all__ = ('LivyPrepareMaster', 'LivyStartProcess', 'LivySignCertificate')


import logging
from pathlib import Path
from struct import pack
from tempfile import TemporaryDirectory
from typing import List, Optional, Tuple, TypeVar, Mapping, Union
from uuid import uuid4

from livy_uploads.commands import LivyRunCode, LivyUploadFile
from livy_uploads.executor import cluster
from livy_uploads.executor.cluster import WorkerInfo, CertManager
from livy_uploads.executor.cluster.model import ClientCert
from livy_uploads.executor.cluster.utils import assert_type
from livy_uploads.session import LivySession, LivyCommand
from livy_uploads.pack import pack_package

LOGGER = logging.getLogger(__name__)
T = TypeVar('T')
PACKAGE = __name__.split('.')[0]


class LivyPrepareMaster(LivyCommand[str]):
    '''
    Prepares the executor master.
    '''

    def run(self, session: 'LivySession') -> str:
        '''
        Executes the upload
        '''
        LOGGER.info('packing the package code in %r', PACKAGE)
        with TemporaryDirectory() as tmpdir:
            package_zip = pack_package(PACKAGE, tmpdir)
            pyfile = package_zip.name

            LOGGER.info('sending the package code')
            LivyUploadFile(
                source_path=package_zip,
                dest_path=pyfile,
                chunk_size=1024,
                mode=0o644,
            ).run(session)

        LOGGER.info('starting the callback server')
        command = LivyRunCode(
            vars=dict(
                pyfile=pyfile,
            ),
            code='''
                spark.sparkContext.addPyFile(pyfile)
                import socket
                from livy_uploads.executor.cluster import CallbackServer, CallbackClient, CertManager

                try:
                    cert_manager
                except NameError:
                    cert_manager = CertManager(basedir='var')
                    cert_manager.setup()

                ca_data = cert_manager.ca_data

                try:
                    callback_url
                    started_now = False
                except NameError:
                    hostname = socket.getfqdn()
                    callback_server = CallbackServer(hostname=hostname, cert_manager=cert_manager)
                    callback_server.start()
                    callback_url = callback_server.url
                    started_now = True

                _ = callback_url, started_now
            ''',
        )
        _, (url, started_now) = command.run(session)
        url: str
        started_now: bool
        LOGGER.info('callback server %s at %s', 'started' if started_now else 'already running', url)
        return url


class LivySignCertificate(LivyCommand[Tuple[CertManager, ClientCert]]):
    '''
    Signs a certificate.
    '''

    def __init__(
        self,
        name: str = 'test@localhost',
        basedir: Optional[Union[str, Path]] = None,
    ):
        self.name = name
        self.basedir = Path(basedir or 'var')
        self.cert_manager = CertManager(basedir=self.basedir)

    def run(self, session: 'LivySession') -> Tuple[CertManager, ClientCert]:
        '''
        Executes the command and returns the signed certificate.
        '''
        LOGGER.info('getting the remote CA data')
        command = LivyRunCode(
            code='''
                _ = cert_manager.ca_data
            ''',
        )
        _, ca_data = command.run(session)
        ca_data = assert_type(ca_data, str)

        LOGGER.info('creating a certificate request')
        cert_manager = CertManager(basedir=self.basedir, ca_data=ca_data)
        cert_manager.setup()
        csr_path, _, conf_path, _ = cert_manager.make_request(name=self.name)

        LOGGER.info('signing the certificate')
        command = LivyRunCode(
            vars=dict(
                name=self.name,
                csr=csr_path.read_text(),
                conf=conf_path.read_text(),
            ),
            code='''
                cert_data = cert_manager.sign_request(csr=csr, conf=conf)
                _ = cert_data
            ''',
        )
        _, cert_data = command.run(session)
        cert_data = assert_type(cert_data, str)
        cert = cert_manager.get_mtls_cert(ClientCert, name=self.name, cert=cert_data)

        return self.cert_manager, cert


class LivyStartProcess(LivyCommand[WorkerInfo]):
    '''
    Runs a process in the executor.
    '''

    def __init__(
        self,
        command: str,
        args: Optional[List[str]] = None,
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
        stdin: Optional[bool] = True,
        tty_size: Optional[Tuple[int, int]] = None,
        port: Optional[int] = 0,
        bind_address: Optional[str] = '0.0.0.0',
        hostname: Optional[str] = None,
        pause: Optional[float] = None,
        log_dir: Optional[str] = 'var/log',
    ):
        self.kwargs = dict(
            command=command,
            args=args or [],
            env=env or {},
            cwd=cwd,
            stdin=stdin,
            tty_size=tty_size,
            port=port,
            bind_address=bind_address,
            hostname=hostname,
            pause=pause,
            log_dir=log_dir,
        )

    def run(self, session: 'LivySession') -> WorkerInfo:
        '''
        Executes the command and returns the received worker info.
        '''
        name = str(uuid4())
        fname = 'run_' + name.replace('-', '_')
        command = LivyRunCode(
            code=f'''
                import logging
                from pyspark import InheritableThread
                from livy_uploads.executor.cluster import WorkerServer, CallbackClient, HttpBuiltinClient, CertManager

                kwargs['name'] = name
                kwargs['worker_secret'] = callback_server.get_worker_secret(name)
                kwargs['ca_data'] = cert_manager.ca_data
                kwargs['callback_url'] = callback_url

                def {fname}_worker(kwargs):
                    logging.basicConfig(
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                    )
                    worker_secret = kwargs.pop('worker_secret')
                    ca_data = kwargs.pop('ca_data')
                    callback_url = kwargs.pop('callback_url')
                    name = kwargs['name']

                    cert_manager = CertManager(basedir='var', ca_data=ca_data)
                    callback_client = CallbackClient(
                        server_url=callback_url,
                        worker_name=name,
                        worker_secret=worker_secret,
                        cert_manager=cert_manager,
                    )
                    kwargs['callback'] = callback_client
                    worker = WorkerServer(**kwargs)
                    return worker.run()

                def {fname}_master(kwargs):
                    rdd = spark.sparkContext.parallelize([kwargs]).map({fname}_worker)
                    rdd.collect()

                thread = InheritableThread(daemon=True, target={fname}_master, args=(kwargs,))
                thread.start()

                info = callback_server.get_info(name)
                if info:
                    _ = info.asdict()
            ''',
            vars=dict(
                kwargs=self.kwargs,
                name=name,
            ),
        )
        
        LOGGER.info('starting the command and waiting for the worker info')
        _, kwargs = command.run(session)
        if not kwargs:
            raise TimeoutError('no info received from the worker')
        return WorkerInfo.fromdict(kwargs)
