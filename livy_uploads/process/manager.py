import importlib
import time
from typing import Callable, Dict, List, Optional, Tuple

from livy_uploads.session import LivySessionEndpoint
from livy_uploads.process import manager_remote
from livy_uploads.commands import LivyRunCode


class ProcessManager:
    INSTANCE = 'livy_uploads_RemoteProcessManager_instance'

    def __init__(self, session: LivySessionEndpoint):
        self.session = session
        self._initialized = False
        self._registry: Dict[str, str] = {}
        self._last_seen_offset = 0

    def initialize(self, force: bool = False):
        '''
        Sends and initializes the process manager code in the remote session.

        This method is idempotent.
        '''
        if self._initialized and not force:
            return

        test_cmd = LivyRunCode(
            vars=dict(var_name=self.INSTANCE),
            code='''
                return var_name in globals()
            ''',
        )
        _, initialized = self.session.run(test_cmd)
        if initialized and not force:
            self._initialized = True
            return

        with open(manager_remote.__file__, 'r') as fp:
            code = fp.read()

        code += f'\nglobals()[{self.INSTANCE!r}] = RemoteProcessManager()\n'
        install_cmd = LivyRunCode(code)
        self.session.run(install_cmd)
        self._initialized = True

    def register(self, module_spec: str, force: bool = False) -> str:
        '''
        Registers a new process class in the remote session.

        This works by getting the module code and injecting it in the remote session;
        therefore, your module should only refer to modules that are already available
        in the remote session.
        '''
        module, _, class_name = module_spec.rpartition(':')
        if class_name in self._registry and not force:
            return class_name

        module = importlib.import_module(module)
        with open(module.__file__) as fp:
            code = fp.read()

        code += f'\n{self.INSTANCE}.register({class_name})\n'
        self.initialize(force=force)

        register_cmd = LivyRunCode(code)
        self.session.run(register_cmd)

        self._registry[class_name] = module_spec
        return class_name

    def start(self, class_name: str, *args, **kwargs) -> int:
        '''
        Starts a new process.

        Parameters:
        - class_name: a previously registered class name

        Returns:
        - the new process PID
        '''
        if class_name not in self._registry:
            raise ValueError(f'Unregistered class name: {class_name}')

        cmd = LivyRunCode(
            vars=dict(
                class_name=class_name,
                args=args,
                kwargs=kwargs,
                handle=self.INSTANCE,
            ),
            code='''
                return globals()[handle].start(class_name, *args, **kwargs)
            '''
        )
        _, pid = self.session.run(cmd)
        return pid

    def poll(self, pid: int, batch_size: int = 100) -> Tuple[Optional[int], List[str]]:
        '''
        Polls the returncode and output lines of the process.
        '''
        cmd = LivyRunCode(
            vars=dict(
                pid=pid,
                handle=self.INSTANCE,
                batch_size=batch_size,
            ),
            code='''
                return globals()[handle].poll(pid, batch_size)
            '''
        )
        _, value = self.session.run(cmd)
        return value

    def stop(self, pid: int, timeout: float = 2.0):
        '''
        Stops a process, killing it if it doesn't die within the timeout
        '''
        cmd = LivyRunCode(
            vars=dict(
                pid=pid,
                timeout=timeout,
                handle=self.INSTANCE,
            ),
            code='''
                globals()[handle].stop(pid, timeout)
            '''
        )
        self.session.run(cmd)

    def follow(self, pid: int, pause: float = 1.0, batch_size: int = 500, println: Optional[Callable[[str], None]] = None) -> int:
        '''
        Prints the output of the process in realtime until it finishes.
        '''
        println = println or print

        while True:
            returncode, lines = self.poll(pid, batch_size=batch_size)
            for line in lines:
                println(line)

            if returncode is not None:
                return returncode

            time.sleep(pause)
