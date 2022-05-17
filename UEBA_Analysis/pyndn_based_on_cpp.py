import ctypes
import os
import time
from multiprocessing import Queue
from threading import Thread
import time
import log_parser
import ueba_analyze
import producer


def produce(q):
    # so = ctypes.cdll.LoadLibrary
    # lib = so('./libpycallclass.so')
    # lib.collect.restype = ctypes.py_object
    print('start ndn producer test')

    while True:
        logs = producer.Collect()
        print(logs)
        q.put(logs)
    # time.sleep(1)


def consume(q):
    print('start waiting vpn server logs')
    while 1:
        logs = q.get()
        # TODO 分析日志
        parser = log_parser.LogParser(logs)
        df = parser.dict2dataframe()
        print(df)
        ueba_analyze.run(df)


if __name__ == '__main__':
    queue = Queue()
    pro = Thread(target=produce, args=(queue,))
    con = Thread(target=consume, args=(queue,))
    pro.start()
    con.start()
