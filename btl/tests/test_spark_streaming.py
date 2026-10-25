import pytest
from scripts.verify_streaming_dq_acceptance import verify_streaming_checkpoint_and_restart

pytestmark = pytest.mark.spark


def test_file_stream_late_data_quarantine_and_restart(spark_session):
    verify_streaming_checkpoint_and_restart(spark_session,mode='test')
