#!/usr/bin/bash
set -euo pipefail

if (( $# < 3 )); then
    echo "Usage: run-docker-workspace-job.sh PROJECT_DIR PYTHON SCRIPT [ARGS...]" >&2
    exit 64
fi

project_dir=$1
python=$2
script=$3
shift 3

for ((attempt = 0; attempt < 30; attempt++)); do
    if [[ -d "$project_dir" && -x "$python" && -r "$script" ]]; then
        if docker version --format '{{.Server.Version}}' >/dev/null 2>&1; then
            cd "$project_dir"
            exec "$python" "$script" "$@"
        fi
    fi
    sleep 4
done

if [[ ! -d "$project_dir" || ! -x "$python" || ! -r "$script" ]]; then
    echo "Sentinel workspace or job executable is unavailable after 120 seconds: $project_dir" >&2
else
    echo "Sentinel could not reach Docker after waiting 120 seconds." >&2
    docker version --format '{{.Server.Version}}' >&2 || true
fi
exit 1
