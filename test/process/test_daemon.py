from pathlib import Path
import time
import queue

import pytest

from livy_uploads.process.daemon import DaemonProcess, LogsFollower


class TestLogsFollower:
    def test_follow_logs(self, tmp_path: Path):
        follower = LogsFollower(tmp_path, pause=0.1)
        follower.start()

        f1 = (tmp_path / 'file1.log').open('w')

        with pytest.raises(queue.Empty):
            follower.readline(timeout=1)

        f1.write('line1\n')
        f1.flush()
        assert follower.readline(timeout=1) == 'line1'

        with pytest.raises(queue.Empty):
            follower.readline(timeout=1)

        f1.write('line2\n')
        f1.flush()
        assert follower.readline(timeout=1) == 'line2'

        (tmp_path / 'file2.log').write_text('line3\n')
        assert follower.readline(timeout=1) == 'line3'


class TestDaemonProcess:
    def test_daemon(self, tmp_path: Path):
        shell = '''
            nohup bash -c '
                echo line0
                sleep 2

                STOP=
                trap 'STOP=1' SIGTERM
                while [ -z "$STOP" ]; do
                    sleep 0.1
                done
                echo 'stopping'
                sleep 3
            ' 2>&1 > logs/daemon.log &
            printf '%s' "$!" > run/daemon.pid
        '''
        process = DaemonProcess(
            start_cmd=['bash', '-c', shell],
            pid_dir=tmp_path / 'run',
            logs_dir=tmp_path / 'logs',
            cwd=tmp_path,
        )
        pid, readline = process.start()
        assert pid > 0
        time.sleep(1.0)
        assert process.poll() is None
        assert readline(1.0) == 'line0'

        time.sleep(2.0)
        assert process.poll() is None

        t0 = time.monotonic()
        process.stop(timeout=10.0)
        dt = time.monotonic() - t0
        assert 3.0 <= dt < 6

        assert readline(1.0) == 'stopping'
