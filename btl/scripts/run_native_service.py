"""Run one configured native service in the foreground for systemd ownership."""

import argparse, json, os, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from operations.services import native_command


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--service", choices=["postgres", "minio", "kafka"], required=True)
    args = parser.parse_args()
    if args.config.stat().st_mode & 0o077:
        raise ValueError("Private native-service configuration required")
    command, environment = native_command(json.loads(args.config.read_text()), args.service)
    os.execvpe(command[0], command, dict(os.environ, **environment))


if __name__ == "__main__":
    main()
