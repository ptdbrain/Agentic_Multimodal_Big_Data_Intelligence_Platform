import threading,time
import pytest

pytestmark = pytest.mark.unit


def test_two_writers_serialize_and_nested_publication_is_reentrant(tmp_path):
    from storage.write_lock import write_lock
    path=str(tmp_path/'table')
    values=[]
    def writer():
        with write_lock(path):
            old=len(values)
            time.sleep(.01)
            with write_lock(path):
                assert len(values)==old
                values.append(1)
    threads=[threading.Thread(target=writer) for _ in range(2)]
    for t in threads:t.start()
    for t in threads:t.join(timeout=5)
    assert all(not t.is_alive() for t in threads)
    assert len(values)==2
