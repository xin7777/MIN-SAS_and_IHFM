import subprocess

proc = subprocess.Popen('./ueba_consumer', stdin=subprocess.PIPE)
try:
    proc.communicate(input=bytearray('123', encoding='utf-8'), timeout=15)
except TimeoutError:
    proc.kill()
