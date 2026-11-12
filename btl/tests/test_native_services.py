"""Native service launch configuration must keep data and listeners explicit."""

import pytest

pytestmark = pytest.mark.unit


def test_native_service_command_requires_persistent_paths_and_loopback(tmp_path):
    from operations.services import native_command

    config = {
        "persistent_root": str(tmp_path),
        "postgres": {
            "binary": "/vendor/postgres",
            "data_dir": str(tmp_path),
            "socket_dir": str(tmp_path),
            "port": 25432,
        },
    }
    command, environment = native_command(config, "postgres")
    assert command == ["/vendor/postgres", "-D", str(tmp_path), "-p", "25432", "-h", "127.0.0.1", "-k", str(tmp_path)]
    assert environment == {}
    config["postgres"]["data_dir"] = "/tmp/ephemeral-data"
    with pytest.raises(ValueError, match="persistent"):
        native_command(config, "postgres")
    with pytest.raises(ValueError):
        native_command(config, "unknown")
