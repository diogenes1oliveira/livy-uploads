from pathlib import Path
import time

import pytest

from livy_uploads.process.manager_remote import RemoteProcessManager
from livy_uploads.process.popen import PopenProcess


class_name = PopenProcess.__name__


class TestRemoteProcessManager:
    manager: RemoteProcessManager = None

    @classmethod
    def setup_class(cls):
        RemoteProcessManager.register(PopenProcess)
        cls.manager = RemoteProcessManager()

    def test_one_off(self):
        pid = self.manager.start(class_name, args=['bash', '-c', 'sleep 2 && echo hello && echo >&2 world'])
        assert pid > 0
        time.sleep(1.0)
        assert self.manager.poll(pid) == (None, [])

        time.sleep(2.0)
        assert self.manager.poll(pid) == (None, ['hello', 'world'])
        assert self.manager.poll(pid) == (0, [])

        with pytest.raises(KeyError):
            self.manager.poll(pid)

    def test_stream(self, tmp_path: Path):
        cmd = '''
            echo line0
            while ! [ -f continue ]; do sleep 0.5; done
            echo >&2 line1
            exit 42
        '''
        pid = self.manager.start(class_name, args=['bash', '-c', cmd], cwd=str(tmp_path))
        assert pid > 0

        time.sleep(1.0)
        assert self.manager.poll(pid) == (None, ['line0'])

        (tmp_path / 'continue').touch()
        time.sleep(2.0)
        assert self.manager.poll(pid) == (None, ['line1'])
        assert self.manager.poll(pid) == (42, [])

        with pytest.raises(KeyError):
            self.manager.poll(pid)
