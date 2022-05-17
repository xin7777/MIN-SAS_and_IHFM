import subprocess

proc = subprocess.Popen('./ueba_producer')
try:
    outs, errs = proc.communicate(timeout=10)
    print(outs)
except subprocess.TimeoutExpired:
    proc.kill()
    outs, errs = proc.communicate()
    print(outs)