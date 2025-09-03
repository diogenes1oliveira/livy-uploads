#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Commands to schedule a process in the remote cluster.
'''

__all__ = ('LivyPrepareMaster', 'LivyStartProcess')


import logging
from typing import List, Optional, TypeVar, Mapping
from uuid import uuid4

from livy_uploads.commands import LivyRunCode, LivyUploadFile
from livy_uploads.executor import cluster
from livy_uploads.executor.cluster import WorkerInfo
from livy_uploads.session import LivySession, LivyCommand


LOGGER = logging.getLogger(__name__)
T = TypeVar('T')


class LivyPrepareMaster(LivyCommand[str]):
    '''
    Prepares the executor master.
    '''

    def run(self, session: 'LivySession') -> str:
        '''
        Executes the upload
        '''
        LOGGER.info('sending the cluster code')
        LivyUploadFile(
            source_path=cluster.__file__,
            dest_path='livy_uploads_executor_cluster.py',
            chunk_size=1024,
            mode=0o644,
        ).run(session)

        LOGGER.info('starting the callback server')
        command = LivyRunCode(
            code='''
                spark.sparkContext.addPyFile('livy_uploads_executor_cluster.py')
                from livy_uploads_executor_cluster import CallbackServer

                try:
                    callback_server
                    started = False
                except NameError:
                    callback_server = CallbackServer()
                    callback_server.start()
                    started = True

                _ = callback_server.url, started
            ''',
        )
        _, (url, started) = command.run(session)
        url: str
        started: bool
        LOGGER.info('callback server %s at %s', 'started' if started else 'already running', url)
        return url


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
                from livy_uploads_executor_cluster import WorkerServer

                kwargs['name'] = name
                kwargs['callback'] = callback_server.url.rstrip('/') + '/info'

                def {fname}_worker(kwargs):
                    logging.basicConfig(
                        level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                    )

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
        _, kwargs = command.run(session)
        if not kwargs:
            import pdb; pdb.set_trace()
            raise TimeoutError('no info received from the worker')
        return WorkerInfo.fromdict(kwargs)
