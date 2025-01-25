from livy_uploads.process.hadoop import LivyUploadsHadoopProcessLocal



def test_get_free_ports_range():
    get_ports = LivyUploadsHadoopProcessLocal.get_free_ports_range

    ports_2 = get_ports(['PORT1', 'PORT2'], 32769, 40000)
    assert len(ports_2) == 2
    assert 32769 <= int(ports_2['PORT1']) <= 40000
    assert 32769 <= int(ports_2['PORT2']) <= 40000
