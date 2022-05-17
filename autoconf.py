#!/usr/bin/python3
#coding=utf-8
#修改时搜索目录即可
import os
import sys
import argparse
import time
import threading
import configparser
import multiprocessing

conf = configparser.ConfigParser()
conf.read("config/config.cfg")

def pmacctd_start():
    cmd_check = "ps -aux | grep pmacct | wc -l"
    nums = os.system(cmd_check)
    if nums > 2:
        print("pmacctd has already been running")
        pmacctd_shutdown()
    cmd_start = " pmacctd -f " + conf.get("Path","pmacct_conf") + " >/dev/null"
    try:
        os.system(cmd_start)
        print("pmacctd started")
    except Exception as e:
        print(e)

def pmacctd_shutdown():
    file_ = os.popen("ps -aux | grep pmacct")
    text = file_.read()
    file_.close()

    pid_list = []
    for each in text.split("\n"):
        if ("grep" not in each) and each:
            pid_list.append(each.split()[1])
    print(pid_list)

    #kill the pids
    for pid in pid_list:
        cmd = "kill -s 9 " + pid
        try:
            os.system(cmd)
            print(pid + " killed")
        except:
            print("kill failed")


def kafka_start_zookeeper():
    cmd_check_zookeeper = "ps -aux | grep zookeeper.properties | wc -l"
    nums = os.system(cmd_check_zookeeper)
    if nums > 2:
        print("zookeeper.properties has been running")
        kafka_shutdown()

    cmd_start_zookeeper = conf.get("Path","zookeeper_start") + " " + conf.get("Path","zookeeper_config")+" >/dev/null"
    try:
        print("kafka zookeeper started")
        os.system(cmd_start_zookeeper)
    except Exception as e:
        print(e)


def kafka_start_server():
    cmd_check_server = "ps -aux | grep server.properties | wc -l"
    nums2 = os.system(cmd_check_server)
    if nums2 > 2:
        print("server.properties has been running")
        kafka_shutdown()
        #kafka_shutdown()

    cmd_start_server = conf.get("Path","kafka_start") + ' ' + conf.get("Path","kafka_config") + ' >/dev/null'
    try:
        print("kafka server started")
        os.system(cmd_start_server)
    except Exception as e:
        print("afka_server failed\n " , e)
    print("kafka started")


def kafka_shutdown():
    file_ = os.popen("ps -aux | grep zookeeper.properties")
    text = file_.read()
    file_.close()

    pid_list = []
    for each in text.split("\n"):
        if ("grep" not in each) and each:
            pid_list.append(each.split()[1])
    print(pid_list)

    #kill the pids
    for pid in pid_list:
        cmd = "kill -s 9 " + pid
        try:
            os.system(cmd)
            print(pid + " killed (zookeeper)")
        except:
            print("kill zookeeper failed")

    file_ = os.popen("ps -aux | grep server.properties")
    text = file_.read()
    file_.close()

    pid_list = []
    for each in text.split("\n"):
        if ("grep" not in each) and each:
            pid_list.append(each.split()[1])
    print(pid_list)

    #kill the pids
    for pid in pid_list:
        cmd = "kill -s 9 " + pid
        try:
            os.system(cmd)
            print(pid + " killed(server.properties)")
        except:
            print("kill server.properties failed")


def hadoop_start():
    cmd_start = "su hadoop -c " + conf.get("Path","hadoop")+"start-dfs.sh" + " >/dev/null"
    try:
        os.system(cmd_start)
        print("hadoop started")
    except Exception as e:
        print(e)

def hadoop_shutdown():
    cmd_stop = "su hadoop -c "+conf.get("Path","hadoop")+"stop-dfs.sh"
    try:
        os.system(cmd_stop)
        print("hadoop stopping")
    except Exception as e:
        print(e)


def all_shutdown():
    t_kafka = threading.Thread(name="kafka",target=kafka_shutdown, args=())
    t_pmacctd = threading.Thread(name="pmacct", target=pmacctd_shutdown, args=())
    t_hadoop = threading.Thread(name="hadoop", target=hadoop_shutdown, args=())
    t_kafka.start()
    t_pmacctd.start()
    t_hadoop.start()

def test_kafka():
    import json
    from kafka import KafkaProducer

    try:
        producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
        producer.send('test_rhj', "test".encode('utf-8'))
        print("kafka is running")
        producer.close()
        return True
    except Exception as e:
        print("\n\n\n***********************************\nkafka server start failed ")
        print("ERROR: ",e)
        print("=======================================\nplease run the command : \nsudo "+conf.get("Path","kafka_start") + ' ' + conf.get("Path","kafka_config") + ' >/dev/null' + "\n====================================\n\n\n")
        return False



if __name__  == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--restart', action="store_true", help="restart the pmacct, kafka, hadoop")
    parser.add_argument("-s", "--shutdown", action = "store_true", help="shutdown the kafka, pmacct, hadoop")
    parser.add_argument("-t", "--test", action = "store_true", help="test the kafka is running")
    args = parser.parse_args()
    if len(sys.argv) < 2:
        print("usage: -h to learn more")
        print("please run the -s first")



    elif args.restart:
        all_shutdown()
        t_kafka_zookeeper = multiprocessing.Process(name="kafka_zookeeper",target=kafka_start_zookeeper, args=())
        t_pmacctd = multiprocessing.Process(name="pmacct", target=pmacctd_start, args=())
        t_hadoop = multiprocessing.Process(name="hadoop", target=hadoop_start, args=())
        #t_kafka_zookeeper.setDaemon(True)

        t_kafka_zookeeper.start()
        time.sleep(8)
        ans = False
        while not ans:
            t_kafka_server = multiprocessing.Process(name="kafka_server",target=kafka_start_server,args=())
            t_kafka_server.start()
            time.sleep(8)
            #t_pmacctd.setDaemon(True)
            #t_hadoop.setDaemon(True)
            ans = test_kafka()
        t_pmacctd.start()
        t_hadoop.start()



    elif args.shutdown:
        t_kafka = threading.Thread(name="kafka",target=kafka_shutdown, args=())
        t_pmacctd = threading.Thread(name="pmacct", target=pmacctd_shutdown, args=())
        t_hadoop = threading.Thread(name="hadoop", target=hadoop_shutdown, args=())
        t_kafka.start()
        t_pmacctd.start()
        t_hadoop.start()

    elif args.test:
        test_kafka()
