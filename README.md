# livy-uploads

Upload files and arbitrary objects to Livy


## Install

```shell
$ pip install livy-uploads[magics]
```

## Using

You can find sample notebooks in [examples/](examples/).

### Load the extension


```python
%load_ext livy_uploads.magics
```

### Sending local variable


```python
# remote context

list(sorted(globals()))

>> ['HiveContext', 'StreamingContext', '__builtins__', 'cloudpickle', 'sc', 'spark', 'sqlContext']
```

```python
%local

foo = {2, 3, 4}
```


```python
%send_obj_to_spark -n foo
```


```python
# remote context

foo

>> {2, 3, 4}
```

### Fetching remote variable


```python
%local

list(sorted(globals()))

>> ['In',
    'Out',
    '_',
    '__',
    '___',
    '__builtin__',
    '__builtins__',
    '__doc__',
    '__loader__',
    '__name__',
    '__package__',
    '__spec__',
    '_dh',
    '_i',
    '_i1',
    '_i2',
    '_i3',
    '_i4',
    '_i5',
    '_i6',
    '_ih',
    '_ii',
    '_iii',
    '_oh',
    'display_dataframe',
    'exit',
    'foo',
    'get_ipython',
    'ip',
    'quit']
```



```python
# remote context

from datetime import datetime

now = datetime.now().astimezone()
now

>> datetime.datetime(2025, 1, 9, 14, 45, 41, 349353, tzinfo=datetime.timezone(datetime.timedelta(0), 'UTC'))
```

```python
%get_obj_from_spark -n now
```


```python
%local

now

>> datetime.datetime(2025, 1, 9, 14, 45, 41, 349353, tzinfo=datetime.timezone(datetime.timedelta(0), 'UTC'))
```

### Running commands


```bash
%%remote_command

ls -lahF .
```

```
>> total 168K
>> drwxrwxrwt  1 root root 4.0K Jan  9 14:45 ./
>> drwxr-xr-x  1 root root 4.0K Jan  9 13:49 ../
>> -rw-------  1 app  app   22K Jan  9 14:45 6488636581872497527
>> drwxr-xr-x  2 app  app  4.0K Jan  9 14:45 blockmgr-f79d84a4-9370-481f-b3a2-97cced461f5a/
>> drwxr-xr-x  2 app  app  4.0K Jan  9 14:45 hsperfdata_app/
>> drwxr-xr-x  2 root root 4.0K Jul 25 17:21 hsperfdata_root/
>> -rw-------  1 app  app  3.9K Jan  9 14:45 livyConf227983779386325274.properties
>> -rw-------  1 app  app  8.5K Jan  9 14:39 magics.ipynb
>> drwx------  2 app  app  4.0K Jan  9 14:45 rsc-tmp2405036589438631214/
>> drwxrwxr-x  3 app  app  4.0K Jan  9 14:41 sample-dir/
>> drwxr-xr-x  2 app  app  4.0K Jan  9 13:12 spark/
>> drwx------ 13 app  app  4.0K Jan  9 14:06 spark1395112189021859575/
>> drwx------ 13 app  app  4.0K Jan  9 13:56 spark1465473366972115685/
>> drwx------ 13 app  app  4.0K Jan  9 14:28 spark1595841604280139447/
>> drwx------ 13 app  app  4.0K Jan  9 14:05 spark2023205353082522704/
>> drwx------ 13 app  app  4.0K Jan  9 13:50 spark2114182954385381169/
>> drwx------ 13 app  app  4.0K Jan  9 13:59 spark3073887871148269416/
>> drwx------  4 app  app  4.0K Jan  9 14:45 spark-3a444ea2-f363-44e6-bfa2-cf3f6b79c746/
>> drwx------ 13 app  app  4.0K Jan  9 14:45 spark4397270058654842612/
>> drwx------ 13 app  app  4.0K Jan  9 14:32 spark4522255066864081523/
>> drwx------ 13 app  app  4.0K Jan  9 14:30 spark4986958530123244860/
>> drwx------ 13 app  app  4.0K Jan  9 14:37 spark5296938888428083291/
>> drwx------ 13 app  app  4.0K Jan  9 13:55 spark618008921008140159/
>> drwxr-xr-x  2 app  app  4.0K Jan  9 14:45 spark-63306977-229e-433e-b179-aafbfab1d643/
>> drwx------ 13 app  app  4.0K Jan  9 14:23 spark6663194247870455494/
>> drwx------ 13 app  app  4.0K Jan  9 14:19 spark7235635426939002033/
>> drwx------ 13 app  app  4.0K Jan  9 14:38 spark7442046500716671899/
>> drwx------ 13 app  app  4.0K Jan  9 14:21 spark8039448064864145943/
>> drwx------ 13 app  app  4.0K Jan  9 14:35 spark8465221815050356711/
>> drwx------ 13 app  app  4.0K Jan  9 13:58 spark8478313138891766791/
>> drwx------ 13 app  app  4.0K Jan  9 14:24 spark8645289667499063460/
>> drwx------ 13 app  app  4.0K Jan  9 14:03 spark8868848822936880619/
>> drwx------ 13 app  app  4.0K Jan  9 14:00 spark950338814731202414/
>> drwxr-xr-x  2 app  app  4.0K Jan  9 14:41 tmp/
>> drwx------  2 app  app  4.0K Jan  9 14:45 tmpgtf2hg21/
>> $ process finished with return code 0
```

### Sending local file


```python
%local !ls -lahF
```

```
>> total 24K
>> drwxrwxr-x 4 app app 4.0K Jan  9 14:45 ./
>> drwxrwxr-x 9 app app 4.0K Jan  9 14:43 ../
>> drwxr-xr-x 2 app app 4.0K Jan  9 13:49 .ipynb_checkpoints/
>> -rw-r--r-- 1 app app 5.1K Jan  9 14:45 magics.ipynb
>> drwxrwxr-x 3 app app 4.0K Jan  9 14:41 sample-dir/
```


```python
%send_path_to_spark -p magics.ipynb
```


```bash
%%remote_command

ls -lahF | grep magics
```

```
>> -rw-------  1 app  app  5.1K Jan  9 14:45 magics.ipynb
>> $ process finished with return code 0
```


### Sending local directory


```python
%local !find sample-dir/
```

```
>> sample-dir/
>> sample-dir/inner
>> sample-dir/inner/bar.txt
>> sample-dir/foo.txt
```


```python
%send_path_to_spark -p sample-dir/
```


```bash
%%remote_command
pwd
```

```
>> /tmp
>> $ process finished with return code 0
```


```bash
%%remote_command

find "$PWD/sample-dir"
```

```
>> /tmp/sample-dir
>> /tmp/sample-dir/inner
>> /tmp/sample-dir/inner/bar.txt
>> /tmp/sample-dir/foo.txt
>> $ process finished with return code 0
```
