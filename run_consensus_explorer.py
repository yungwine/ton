import os
import subprocess
import time
from pathlib import Path

cur_dir = None
process = None

while True:
    time.sleep(10)
    base_path = Path("/validator-logs-dir/")

    new_dirs = [base_path / d for d in sorted(
        d for d in os.listdir(base_path)
        if os.path.isdir(base_path / d)
    )]

    if new_dirs[-1] != cur_dir:
        cur_dir = new_dirs[-1]
        print(f'running explorer for new dir {cur_dir}')
        if process:
            process.kill()

        logs = [str(cur_dir / f) for f in os.listdir(cur_dir)]

        process = subprocess.Popen(['uv', 'run', 'consensus_explorer', '--host', '0.0.0.0', '--logs', *logs])
