#!/usr/bin/env python
import sys
import os

# Check the contents of the record_path.
# If it doesn't match the contents of the current git SHA, overwrite it.
repo_dir = sys.argv[1]
record_path = sys.argv[2]

with open(repo_dir + '/.git/HEAD') as f:
    head = f.read().strip()

head_parts = head.split()
if len(head_parts) == 1:
    # this is 'detacthed head mode'
    current_sha = head_parts[0]
else:
    with open(repo_dir + '/.git/' + head_parts[-1], 'r') as f:
        current_sha = f.read().strip()

different = False
if not os.path.exists(record_path):
    different = True
else:
    with open(record_path, 'r') as f:
        prev_sha = f.read().strip()
    different = (prev_sha != current_sha)

if different:
    with open(record_path, 'w') as f:
        f.write(current_sha)
