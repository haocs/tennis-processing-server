"""Supervise API + singleton scheduler in one container; exit if either fails."""
import signal
import subprocess
import sys
import time

children=[]
def stop(*args):
    for child in children:
        if child.poll() is None: child.terminate()
    deadline=time.monotonic()+20
    for child in children:
        try: child.wait(timeout=max(.1,deadline-time.monotonic()))
        except subprocess.TimeoutExpired: child.kill();child.wait()
signal.signal(signal.SIGTERM,lambda *args:(stop(),sys.exit(0)))
signal.signal(signal.SIGINT,lambda *args:(stop(),sys.exit(0)))
try:
    children.append(subprocess.Popen([sys.executable,'scheduler.py']))
    children.append(subprocess.Popen([sys.executable,'-m','gunicorn','--bind','0.0.0.0:8000','--workers','2','--threads','4','--timeout','120','--worker-tmp-dir','/tmp','--access-logfile','-','app:create_app()']))
    while all(child.poll() is None for child in children): time.sleep(1)
finally: stop()
raise SystemExit(1)
