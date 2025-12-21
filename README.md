[![PyPI](https://img.shields.io/pypi/v/livy-uploads.svg)](https://pypi.org/project/livy-uploads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Build](https://github.com/diogenes1oliveira/livy-uploads/actions/workflows/publish.yml/badge.svg)](https://github.com/diogenes1oliveira/livy-uploads/actions/workflows/publish.yml)

# livy-uploads

Upload files and arbitrary objects to Livy

## Install

```shell
$ pip install livy-uploads[magics]
```

## Using

You can find the full interactive notebook at [examples/magics.ipynb](examples/magics.ipynb).

### Load the extension

```python
%load_ext autoreload
%aimport -sparkmagic # it loses the references to the sessions if it reloads
%autoreload 2
```

```python
%reload_ext livy_uploads.magics
```

### Configuring the local session

#### Configuring logging

```python
%%local

import logging


LOGGER = logging.getLogger(__name__)
LOGGER.info('gone to the black hole')

%configure_logging

LOGGER.info('now we can see at least this one')

# 2025-12-21 15:35:47 [INFO] __main__: now we can see at least this one
```

#### Loading env files

```python
%dotenv

# 2025-12-21 15:35:48 [INFO] livy_uploads.paths: loading envfile from /app/.env
# 2025-12-21 15:35:48 [INFO] livy_uploads.paths: setting 2 environment variables: $USER_UID, $USER_GID
```

```python
%dotenv env.empty

# 2025-12-21 15:35:49 [INFO] livy_uploads.paths: loading envfile from /app/env.empty
# 2025-12-21 15:35:49 [INFO] livy_uploads.paths: no environment variables to set from /app/env.empty
```

#### Resolving notebooks

```python
%nblib hostinfo.ipynb

# 2025-12-21 15:35:49 [INFO] livy_uploads.paths: setting NBLIB_PATH='/app/livy_uploads'
# 2025-12-21 15:35:49 [WARNING] livy_uploads.magics: resolved %run hostinfo.ipynb to /app/livy_uploads/hostinfo.ipynb
#
# {'now': '2025-12-21T15:35:50.810404+00:00',
#  'host': 'eb034981a6cc',
#  'pyspark_version': '3.3.2',
#  'scala_version': '<not available>'}
```

### Configuring the remote session

#### Resolving notebooks with `%%spark`

```python
%nblib spark/hostinfo.ipynb

# 2025-12-21 15:35:53 [WARNING] livy_uploads.magics: resolved %run spark/hostinfo.ipynb to /app/livy_uploads/spark/hostinfo.ipynb
#
# Starting Spark application
# [Shows Spark session table with application ID, state, and links]
# SparkSession available as 'spark'.
# {'now': '2025-12-21T15:36:27.287003+00:00',
#  'host': 'nodemanager1.localhost',
#  'pyspark_version': '3.3.2',
#  'scala_version': 'version 2.12.15'}
```

#### Fetching remote variable

```python
%%local

try:
    del remote_info
except Exception:
    pass
```

```python
import socket

remote_info = {'now': datetime.now().astimezone(), 'hostname': socket.getfqdn()}
```

```python
%get_obj_from_spark -n remote_info

print('remote_info:', remote_info)
assert 'nodemanager' in remote_info['hostname']

# remote_info: {'now': datetime.datetime(2025, 12, 21, 15, 36, 27, 546409, tzinfo=datetime.timezone(datetime.timedelta(0), 'UTC')), 'hostname': 'nodemanager1.localhost'}
```

#### Sending local variable

```python
try:
    del local_info
except Exception:
    pass
```

```python
%%local

import socket

local_info = {'now': datetime.now().astimezone(), 'hostname': socket.getfqdn()}

%send_obj_to_spark -n local_info
```

```python
print('local_info:', local_info)
assert 'nodemanager' not in local_info['hostname']

# local_info: {'now': datetime.datetime(2025, 12, 21, 15, 36, 27, 663805, tzinfo=datetime.timezone(datetime.timedelta(0), 'UTC')), 'hostname': 'eb034981a6cc'}
```

#### Running shell commands

```python
%%shell_command

ls -lahF . | head -n 15

# total 208K
# drwx--x--- 3 root root 4.0K Dec 21 15:36 ./
# drwx--x--- 8 root root 4.0K Dec 21 15:36 ../
# -rw-r--r-- 1 root root   12 Dec 21 15:36 .container_tokens.crc
# -rw-r--r-- 1 root root   16 Dec 21 15:36 .default_container_executor.sh.crc
# -rw-r--r-- 1 root root   16 Dec 21 15:36 .default_container_executor_session.sh.crc
# -rw-r--r-- 1 root root  104 Dec 21 15:36 .launch_container.sh.crc
# lrwxrwxrwx 1 root root   52 Dec 21 15:36 __spark_conf__ -> /tmp/usercache/root/filecache/131/__spark_conf__.zip/
# lrwxrwxrwx 1 root root   71 Dec 21 15:36 __spark_libs__ -> /tmp/usercache/root/filecache/117/__spark_libs__1464618954637771830.zip/
# lrwxrwxrwx 1 root root   55 Dec 21 15:36 commons-codec-1.9.jar -> /tmp/usercache/root/filecache/141/commons-codec-1.9.jar*
# -rw-r--r-- 1 root root   69 Dec 21 15:36 container_tokens
# -rwx------ 1 root root  623 Dec 21 15:36 default_container_executor.sh*
# -rwx------ 1 root root  568 Dec 21 15:36 default_container_executor_session.sh*
# lrwxrwxrwx 1 root root   55 Dec 21 15:36 kryo-shaded-4.0.2.jar -> /tmp/usercache/root/filecache/140/kryo-shaded-4.0.2.jar*
# -rwx------ 1 root root  12K Dec 21 15:36 launch_container.sh*
# $ command exited with code 0 (pid=2696)
```

```python
%%shell_command

hostname -I

# 172.18.0.7
# $ command exited with code 0 (pid=2699)
```

```python
%%shell_command

bash -c 'echo foo && exit 42'

# foo
# $ command exited with code 42 (pid=2700)
```

```python
%%local

assert shell_output == 'foo\n'
assert shell_returncode == 42
```

#### Sending local file

```python
%local !ls -lahF
%send_path_to_spark -p magics.ipynb

# total 68K
# drwxrwxr-x  5 app app 4.0K Dec 21 15:36 ./
# drwxrwxr-x 21 app app 4.0K Dec 21 15:13 ../
# drwxr-xr-x  2 app app 4.0K Jan 13  2025 .ipynb_checkpoints/
# -rw-rw-r--  1 app app  11K Dec 21 15:36 magics.ipynb
# drwxrwxr-x  3 app app 4.0K Jan  9  2025 sample-dir/
# drwxrwxr-x  2 app app 4.0K Dec 20 19:45 spark/
# -rw-rw-r--  1 app app  33K Dec 20 19:45 test-spark-another-version.ipynb
# Uploaded magics.ipynb to /tmp/usercache/root/appcache/application_1766330690133_0003/container_1766330690133_0003_01_000001/magics.ipynb
```

```python
%%shell_command

ls -lahF | grep magics

# -rw------- 1 root root  11K Dec 21 15:36 magics.ipynb
# $ command exited with code 0 (pid=2707)
```

```python
%%local

assert 'magics.ipynb' in shell_output
```

#### Sending local directory

```python
%local !find sample-dir/

# sample-dir/
# sample-dir/inner
# sample-dir/inner/bar.txt
# sample-dir/foo.txt
```

```python
%send_path_to_spark -p sample-dir/

# Uploaded sample-dir to /tmp/usercache/root/appcache/application_1766330690133_0003/container_1766330690133_0003_01_000001/sample-dir
```

```python
%%shell_command
pwd

# /tmp/usercache/root/appcache/application_1766330690133_0003/container_1766330690133_0003_01_000001
# $ command exited with code 0 (pid=2712)
```

```python
%%shell_command

find "$PWD/sample-dir"

# /tmp/usercache/root/appcache/application_1766330690133_0003/container_1766330690133_0003_01_000001/sample-dir
# /tmp/usercache/root/appcache/application_1766330690133_0003/container_1766330690133_0003_01_000001/sample-dir/inner
# /tmp/usercache/root/appcache/application_1766330690133_0003/container_1766330690133_0003_01_000001/sample-dir/inner/bar.txt
# /tmp/usercache/root/appcache/application_1766330690133_0003/container_1766330690133_0003_01_000001/sample-dir/foo.txt
# $ command exited with code 0 (pid=2713)
```

```python
%%local

assert 'sample-dir/' in shell_output
```

#### Following session logs

```python
%logs_follow -p 50

# [Truncated - shows last 50 lines of YARN/Spark logs including:]
# 25/12/21 15:36:03 INFO Client: Uploading resource file:...
# 25/12/21 15:36:11 INFO Client: Submitting application application_1766330690133_0003 to ResourceManager
# 25/12/21 15:36:11 INFO YarnClientImpl: Submitted application application_1766330690133_0003
# ...
```

```python
%logs_follow -p 50

# Output (stdout):
# No new logs
```

```python
sc._gateway.jvm.java.lang.System.err.println('Hello World')
```
