import datetime
import time
from enum import Enum
import json
import subprocess
import numpy as np


class Behavior(Enum):
    LOGIN = 0
    LOGOUT = 1
    DOWNLOAD = 2
    UPLOAD = 3
    NORMAL = 4
    ABNORMAL = 5
    TIME = 6


users = ['wzq1', 'wzq2', 'wzq3', 'wzq4', 'wzq5', 'wzq6']
prefix = '/MIN/VPN'
login_url = '/login HTTP'
logout_url = '/logout HTTP'
download_file_url = ['/wzq1/aaa.zip HTTP', '/wzq1/aaa.tar.gz HTTP']
upload_file_url = '/wzq1/aaa.txt HTTP'
normal_request = '/repo HTTP'
abnormal_request = '/hahaha/123/this_is_an_abnormal_request HTTP'
urls = ['GET ' + login_url, 'POST ' + logout_url, 'GET ' + download_file_url[int(np.random.random_sample() * 2)], 'POST ' + upload_file_url, 'GET ' + normal_request, 'GET ' + abnormal_request, 'GET ' + normal_request]
count = 0
while True:
    test_data = {'Command': 'Log', 'Type': 'Network', 'Level': 0, 'Prefix': prefix, 'Sig': 'd3pxMTk5'}
    behavior = int(np.random.random_sample() * 7)
    test_data['Username'] = users[int(np.random.random_sample() * 6)]
    test_data['Timestamp'] = int(time.time())
    test_data['Action'] = urls[behavior]
    print(f'behavior:{behavior}')
    # if behavior == 6:
    #     a = f"2021-01-15 03:26:27.531"
    #     print(a)
    #     d = datetime.datetime.strptime(a, "%Y-%m-%d %H:%M:%S.%f")
    #     t = d.timetuple()
    #     test_data['Timestamp'] = int(time.mktime(t))
    count += 1
    test_data['Count'] = count
    test_data2 = json.dumps(test_data)
    print(f'{count}th packet send')
    print(test_data)
    proc = subprocess.Popen('./ueba_consumer', stdin=subprocess.PIPE)
    try:
        proc.communicate(input=bytearray(test_data2, encoding='utf-8'), timeout=15)
    except TimeoutError:
        proc.kill()
    time.sleep(2)
