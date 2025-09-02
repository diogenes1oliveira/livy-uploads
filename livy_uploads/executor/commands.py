#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Commands to schedule a process in the remote cluster.
'''

__all__ = ('LivyPrepareMaster', 'LivyStartProcess')


import logging
from pathlib import Path
from typing import List, Optional, TypeVar, Mapping
from uuid import uuid4

from livy_uploads.commands import LivyRunCode
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

        code = Path(cluster.__file__).read_text()
        disable_main = f'import os; os.environ["{cluster.ENV_DISABLE_MAIN}"] = "1"'
        command = LivyRunCode(
            code=disable_main + '\n' + code,
            globals=cluster.__all__,
        )
        command.run(session)

        LOGGER.info('starting the callback server')
        command = LivyRunCode(
            code='''
                callback_server = CallbackServer()
                callback_server.start()
                return callback_server.url
            ''',
            globals=['callback_server'],
        )
        _, url = command.run(session)
        url: str
        LOGGER.info('callback server started at %s', url)
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
                from pyspark import InheritableThread

                kwargs['name'] = name
                kwargs['callback'] = callback_server.url.rstrip('/') + '/info'
                def {fname}(kwargs):
                    worker = WorkerServer(**kwargs)
                    return worker.run()

                rdd = spark.sparkContext.parallelize([kwargs]).map({fname})
                thread = InheritableThread(daemon=True, target=rdd.collect)
                thread.start()

                return callback_server.get_info(name).asdict()
            ''',
            vars=dict(
                kwargs=self.kwargs,
                name=name,
            ),
            globals=[fname],
        )
        _, kwargs = command.run(session)
        return WorkerInfo.fromdict(kwargs)
