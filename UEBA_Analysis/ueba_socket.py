# encoding:utf-8
import threading
import logging
from time import sleep
from multiprocessing import Queue
from kafka import KafkaConsumer, KafkaProducer


import log_parser
import ueba_analyze


def read_log(client_socket):
    bufsize = 4096
    res = client_socket.recv(bufsize)
    future = producer.send('ueba_log', res)
    result = future.get(10)
    ueba_logger.debug(result)


'''
socket 方案被 kafka 替代
'''


def ueba_producer():
    ueba_logger.debug('start waiting vpn server logs...')

    global logs
    create_timer()

    for msg in consumer:
        # ueba_logger.debug(msg.value)
        try:
            lock.acquire()
            logs.append(str(msg.value, encoding='utf-8'))
            lock.release()
        except UnicodeDecodeError:
            lock.release()
            continue


def create_timer():	
		t = threading.Timer(20, ueba_inspect)
		t.start()


def ueba_inspect():
    # global inspecting
		global logs
		create_timer()
		ueba_logger.debug('start inspecting vpn server logs...')
    # inspecting = True
		if len(logs) == 0:
				return
		ueba_logger.debug(f'{len(logs)} logs received')
	
		lock.acquire() 
		parser = log_parser.LogParser(logs)
		logs = []
		lock.release()

		df = parser.dict2dataframe()
		print(df)
		if not df.empty:
				ueba_analyze.run(df)




if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    consumer = KafkaConsumer('ueba_log', bootstrap_servers=['localhost:9092'])
    ueba_logger = logging.getLogger('ueba_logger')
    ueba_logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('./ueba_log/ueba.log', mode='w', encoding='UTF-8')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ueba_logger.addHandler(fh)
    ueba_logger.addHandler(ch)

    logs = []
    lock = threading.Lock()
    q = Queue()
    pro = threading.Thread(target=ueba_producer, name='ueba_producer', args=())
    pro.start()