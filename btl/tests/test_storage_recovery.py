from pathlib import Path
import pytest
from spark.storage_safe import SafeParquetIO

pytestmark = pytest.mark.unit


class Frame:
    @property
    def write(self): return self
    def mode(self, mode): return self
    def parquet(self, path):
        Path(path).mkdir()
        (Path(path) / 'data').write_text('new')
    def count(self): return 1


class Reader:
    def parquet(self, path): return Frame()


class Spark:
    read = Reader()


@pytest.mark.parametrize('failure', ['exception', 'false'])
def test_failed_promotion_restores_original(tmp_path, monkeypatch, failure):
    target = tmp_path / 'table'
    target.mkdir()
    (target / 'data').write_text('original')
    rename = SafeParquetIO._rename_storage_path
    def fail(spark, src, dst):
        if '__staging_' in src:
            if failure == 'exception': raise OSError('promotion failed')
            return False
        return rename(spark, src, dst)
    monkeypatch.setattr(SafeParquetIO, '_rename_storage_path', fail)
    with pytest.raises(RuntimeError):
        SafeParquetIO.publish_parquet_atomic(Spark(), Frame(), str(target), expected_count=1)
    assert (target / 'data').read_text() == 'original'


def test_rollback_failure_preserves_backup(tmp_path, monkeypatch):
    target = tmp_path / 'table'
    target.mkdir()
    (target / 'data').write_text('original')
    rename = SafeParquetIO._rename_storage_path
    def fail(spark, src, dst):
        if '__staging_' in src or '__backup_' in src: raise OSError('unavailable')
        return rename(spark, src, dst)
    monkeypatch.setattr(SafeParquetIO, '_rename_storage_path', fail)
    with pytest.raises(RuntimeError) as exc:
        SafeParquetIO.publish_parquet_atomic(Spark(), Frame(), str(target))
    assert exc.value.recovery_required is True
    assert (Path(exc.value.backup_path) / 'data').read_text() == 'original'


def test_count_mismatch_does_not_replace_target(tmp_path):
    target = tmp_path / 'table'
    target.mkdir()
    (target / 'data').write_text('original')
    with pytest.raises(RuntimeError):
        SafeParquetIO.publish_parquet_atomic(Spark(), Frame(), str(target), expected_count=2)
    assert (target / 'data').read_text() == 'original'


def test_partial_backup_failure_does_not_replace_original(tmp_path,monkeypatch):
    target=tmp_path/'table';target.mkdir()
    (target/'row1').write_text('one');(target/'row2').write_text('two')
    def partial(spark,src,dst):
        Path(dst).mkdir();(Path(dst)/'row1').write_text('one')
        raise OSError('backup copy interrupted')
    monkeypatch.setattr(SafeParquetIO,'_copy_storage_backup',partial,raising=False)
    with pytest.raises(RuntimeError):
        SafeParquetIO.publish_parquet_atomic(Spark(),Frame(),str(target))
    assert (target/'row1').read_text()=='one'
    assert (target/'row2').read_text()=='two'
