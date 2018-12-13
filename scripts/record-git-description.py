#!/usr/bin/env python
import sys
import os
import subprocess

# Check the contents of the record_path.
# If it doesn't match the contents of the current "git describe", overwrite it.
repo_dir = sys.argv[1]
record_path = sys.argv[2]

current_description = subprocess.check_output(b'git describe', shell=True).decode('utf-8').strip()

different = False
if not os.path.exists(record_path):
    different = True
else:
    with open(record_path, 'r') as f:
        prev_description = f.read().strip()
    different = (prev_description != current_description)

if different:
    with open(record_path, 'w') as f:
        f.write(current_description)

# Echo the name of the record file
sys.stdout.write(record_path)
