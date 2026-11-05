"""Simulator CLI options must control the actual workload and window metrics."""
import ast
from pathlib import Path
import subprocess
import sys
import pytest

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[1]


def run(*arguments):
    return subprocess.run([sys.executable,str(ROOT/'scripts/run_streaming.py'),'--mode','simulator',*arguments],
                          cwd=ROOT,text=True,capture_output=True,timeout=20)


def result(output):
    marker='[Simulator Result] Micro-batch processed: '
    return ast.literal_eval(next(line[len(marker):] for line in output.splitlines() if line.startswith(marker)))


def test_simulator_window_flag_changes_the_calculated_window_duration():
    completed=run('--window','5 minutes','--watermark','20 seconds')
    assert completed.returncode == 0, completed.stderr
    windows=result(completed.stdout)['window_metrics']
    assert windows
    assert all(row['window_end_epoch']-row['window_start_epoch']==300 for row in windows)


def test_invalid_simulator_duration_is_rejected_instead_of_ignored():
    completed=run('--window','invalid duration')
    assert completed.returncode != 0
    assert '[Simulator Result]' not in completed.stdout


def test_simulator_seed_produces_reproducible_results_and_controls_the_workload():
    first=run('--seed','13');second=run('--seed','13');different=run('--seed','14')
    assert first.returncode == second.returncode == different.returncode == 0
    assert result(first.stdout) == result(second.stdout)
    assert result(first.stdout) != result(different.stdout)


def test_simulator_does_not_accept_a_topic_as_if_it_were_reading_kafka():
    completed=run('--topic','raw.reviews')
    assert completed.returncode == 2
    assert '[Simulator Result]' not in completed.stdout


def test_simulator_watermark_flag_controls_the_observed_watermark():
    zero=run('--seed','13','--watermark','0 seconds')
    ten_minutes=run('--seed','13','--watermark','10 minutes')
    assert zero.returncode == ten_minutes.returncode == 0
    assert result(zero.stdout)['current_watermark']-result(ten_minutes.stdout)['current_watermark'] == 600
