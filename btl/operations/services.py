"""Build foreground native service commands with explicit persistent data roots."""

from pathlib import Path
import shlex


def native_command(config, name):
    if name not in ("postgres", "minio", "kafka"):
        raise ValueError("Unknown native service")
    value = config[name]
    root = Path(config["persistent_root"]).resolve()

    def persistent(path):
        target = Path(path)
        if not target.is_absolute() or not target.resolve().is_relative_to(root):
            raise ValueError("Service data must belong to the configured persistent root")
        return str(target)

    binary = str(Path(value["binary"]))
    if not Path(binary).is_absolute():
        raise ValueError("Absolute vendor executable required")
    if name == "postgres":
        return [
            binary,
            "-D",
            persistent(value["data_dir"]),
            "-p",
            str(int(value["port"])),
            "-h",
            "127.0.0.1",
            "-k",
            persistent(value["socket_dir"]),
        ], {}
    if name == "minio":
        env_path = Path(value["environment_file"])
        if env_path.stat().st_mode & 0o077:
            raise ValueError("Private service environment required")
        env = {}
        for line in env_path.read_text().splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            key, text = line.split("=", 1)
            if key in ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"):
                pieces = shlex.split(text)
                env[key] = pieces[0] if pieces else ""
        if not all(env.get(key) for key in ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY")):
            raise ValueError("Object-store credentials unavailable")
        return [
            binary,
            "server",
            persistent(value["data_dir"]),
            "--address",
            "127.0.0.1:" + str(int(value["port"])),
            "--console-address",
            "127.0.0.1:" + str(int(value["console_port"])),
        ], {"MINIO_ROOT_USER": env["MINIO_ACCESS_KEY"], "MINIO_ROOT_PASSWORD": env["MINIO_SECRET_KEY"]}
    properties = Path(persistent(value["properties"]))
    lines = dict(
        line.split("=", 1)
        for line in properties.read_text().splitlines()
        if line.strip() and not line.startswith("#") and "=" in line
    )
    for directory in lines.get("log.dirs", "").split(","):
        persistent(directory)
    if "127.0.0.1" not in lines.get("advertised.listeners", ""):
        raise ValueError("Explicit loopback Kafka listener required")
    return [binary, str(properties)], {
        "JAVA_HOME": value["java_home"],
        "KAFKA_HEAP_OPTS": value.get("heap_opts", "-Xms256m -Xmx768m"),
    }
