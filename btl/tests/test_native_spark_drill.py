"""Safety contracts for the real native Spark worker-outage harness."""

import pytest

pytestmark = pytest.mark.unit


def test_cluster_verifier_requires_distinct_live_loopback_workers():
    from scripts.verify_native_spark import verified_workers

    state = {
        "workers": [
            {"id": "w1", "host": "127.0.0.1", "port": 37078, "state": "ALIVE"},
            {"id": "w2", "host": "127.0.0.1", "port": 37079, "state": "ALIVE"},
        ]
    }
    assert len(verified_workers(state, {37078, 37079})) == 2
    state["workers"][1]["state"] = "DEAD"
    with pytest.raises(ValueError):
        verified_workers(state, {37078, 37079})
    state["workers"][1].update(state="ALIVE", host="other-host")
    with pytest.raises(ValueError):
        verified_workers(state, {37078, 37079})
    state["workers"][1].update(host="127.0.0.1", id="w1")
    with pytest.raises(ValueError):
        verified_workers(state, {37078, 37079})


def test_retry_verifier_recognizes_stage_reexecution_with_reset_attempt_number():
    from scripts.verify_native_spark import verified_task_retries

    first = {"stage_id": 2, "partition_id": 0, "task_attempt_id": 32, "attempt_number": 0}
    repeated = dict(first, task_attempt_id=70)
    assert verified_task_retries([first, repeated]) == [(2, 0)]
    with pytest.raises(ValueError):
        verified_task_retries([first, dict(first)])


def test_native_cluster_never_adopts_an_existing_listener(tmp_path):
    import socket
    from scripts.verify_native_spark import NativeCluster
    vendor=tmp_path/'vendor';(vendor/'bin').mkdir(parents=True)
    (vendor/'bin/spark-class').touch()
    with socket.socket() as listener:
        listener.bind(('127.0.0.1',0));listener.listen()
        cluster=NativeCluster(vendor,tmp_path/'work',rpc_port=listener.getsockname()[1])
        with pytest.raises(OSError):cluster.start()
        assert not (tmp_path/'work').exists()
