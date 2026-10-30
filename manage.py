"""Consistent native-state SQLite backup."""
import argparse
from pathlib import Path
import sqlite3
p=argparse.ArgumentParser();p.add_argument('database');p.add_argument('destination');args=p.parse_args()
destination=Path(args.destination)
if destination.exists():raise SystemExit('Refusing to overwrite an existing backup')
with sqlite3.connect(args.database) as source,sqlite3.connect(destination) as target:
    source.backup(target)
    if target.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise SystemExit('Backup failed verification')
print('Backup verified')
