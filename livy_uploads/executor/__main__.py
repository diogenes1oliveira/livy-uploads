import argparse
import logging
import json
import os
import sys

from livy_uploads.executor.client import LivyExecutorClient
from livy_uploads.executor.cluster import get_winsize
from livy_uploads.utils import assert_type
from livy_uploads.executor.signals import parse_signal


LOGGER = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default='executor.json', type=argparse.FileType('r'), help='Configuration file')
    parser.add_argument('-l', '--log-level', default='INFO', help='Log level', choices=['DEBUG', 'INFO', 'WARNING'])

    # Arguments for LivyExecutorClient.start
    parser.add_argument('-e', '--env', nargs='*', help='Environment variables in KEY=VALUE format')
    parser.add_argument('-w', '--cwd', help='Working directory to run the command in')
    parser.add_argument('--no-stdin', action='store_false', dest='stdin', default=True, help='Disable stdin in the process')
    parser.add_argument('--tty', action='store_true', default=None, help='Allocate a TTY')
    parser.add_argument('--no-tty', action='store_false', dest='tty', help='Do not allocate a TTY')
    parser.add_argument('--worker-port', type=int, default=0, help='Port the worker server will listen on (0 for auto)')
    parser.add_argument('--worker-hostname', help='Advertised worker hostname, defaults to the FQDN')
    parser.add_argument('--bind-address', default='0.0.0.0', help='Override the address to bind the worker to')
    parser.add_argument('--stop-signal', type=parse_signal, help='Signal to send to the process to stop it')
    parser.add_argument('--max-stop-count', type=int, default=2, help='Maximum number of stop signals to send to the process before sending SIGKILL')

    # Command and its arguments as positional arguments (must be last)
    parser.add_argument('command', help='The command to run')
    parser.add_argument('command_args', nargs='*', help='Arguments to pass to the command')

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level)
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    if log_level == logging.DEBUG:
        logging.getLogger('requests').setLevel(logging.INFO)
        logging.getLogger('urllib3').setLevel(logging.INFO)
        logging.getLogger('requests_gssapi').setLevel(logging.INFO)
    else:
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests_gssapi').setLevel(logging.WARNING)

    LOGGER.info('args: %s', args)
    config = assert_type(json.load(args.config), dict)

    client = LivyExecutorClient.from_config(config)
    client.setup()

    # Parse environment variables from --env arguments
    env = None
    if args.env:
        env = {}
        for env_var in args.env:
            key, sep, value = env_var.partition('=')
            if not sep:
                env[key] = os.environ[key]
            else:
                env[key] = value

    tty_size = None

    if args.stdin:
        if sys.stdin.buffer.isatty():
            if args.tty is not False:
                tty_size = get_winsize(sys.stdin.fileno())
        else:
            if args.tty is True:
                tty_size = (24, 80)
    else:
        tty_size = None

    # Call start method with parsed arguments
    monitor = client.start(
        command=args.command,
        args=args.command_args,
        env=env,
        cwd=args.cwd,
        stdin=args.stdin,
        tty_size=tty_size,
        worker_port=args.worker_port,
        worker_hostname=args.worker_hostname,
        bind_address=args.bind_address,
        stop_signal=args.stop_signal,
        max_stop_count=args.max_stop_count,
    )
    returncode = monitor.run(
        stdin=sys.stdin.buffer if args.stdin else None,
        stdout=sys.stdout.buffer,
        tty=args.tty,
    )

    sys.exit(returncode)


if __name__ == '__main__':
    main()
