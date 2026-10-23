"""Runtime markers and per-test local storage isolation."""
import copy
import os
import pytest


@pytest.fixture(autouse=True)
def isolated_local_runtime(request, tmp_path):
    from config.settings import settings
    from storage.storage_manager import storage
    previous = copy.deepcopy(settings)
    old_storage = dict(storage.__dict__)
    old_env = dict(os.environ)
    mode='integration' if request.node.get_closest_marker('integration') else 'test'
    settings.set_runtime_mode(mode)
    settings.storage.local_data_dir = tmp_path / 'lake'
    settings.database.sqlite_path = tmp_path / 'warehouse.sqlite'
    storage.set_mode(mode, base_dir=settings.storage.local_data_dir)
    try:
        yield
    finally:
        settings.__dict__.clear()
        settings.__dict__.update(previous.__dict__)
        storage.__dict__.clear()
        storage.__dict__.update(old_storage)
        os.environ.clear()
        os.environ.update(old_env)


@pytest.fixture(scope='session')
def spark_session(tmp_path_factory):
    from config.settings import settings
    from spark.spark_session import SparkSessionFactory
    settings.set_runtime_mode('test')
    spark=SparkSessionFactory.get_spark_session(master='local[2]',extra_configs={
        'spark.driver.bindAddress':'127.0.0.1','spark.driver.host':'127.0.0.1',
        'spark.ui.enabled':'false','spark.sql.warehouse.dir':str(tmp_path_factory.mktemp('spark-warehouse')),
    })
    if spark is None:
        pytest.fail('Required Spark JVM is unavailable; this gate cannot be skipped')
    yield spark
    spark.stop()
