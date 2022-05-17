#!/usr/bin/python3
#coding=utf-8
#********need sudo********
import multiprocessing
import threading
import os
import spark_analyze
#import net_message
import S_Assessment
import argparse
import TIP_module
from AssetInfo import AssetInfo

def exec_spark_ip():
    try:
        #os.system("python spark_analyze.py 'ip'")
        spark_analyze.start("ip")
    except Exception as e:
        print(e)
        exit()

def exec_spark_min():
    try:
        #os.system("python spark_analyze.py 'min'")
        spark_analyze.start("min")
    except Exception as e:
        print(e)
        exit()

def exec_spark_all():
    try:
        spark_analyze.start("all")
    except Exception as e:
        print(e)
        exit()


def exec_net():
    try:
        #os.system("python net_message.py -dv")
        params = {'detail':True, 'view':True}
        net_message.start(params)
    except Exception as e:
        print(e)
        exit()

def exec_sa():#态势评估
    try:
        #os.system("python S_Assessment.py")

        test = S_Assessment.S_Assessment()
        test.run(3)#每隔3分钟更新一次主机态势值
    except Exception as e:
        print(e)
        exit()

def exec_redis_operate():
    try:
        import redis_py
        redis_py.run()
    except Exception as e:
        print(e)
        exit()

def exec_TIP():
    try:
        TIP_module.start()
    except Exception as e:
        print(e)
        exit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--distribute', action="store_true", help='distribute mode, open the redis to publish & subscribe command')
    args = parser.parse_args()
    t_1 = multiprocessing.Process(target=exec_spark_all, args = ())
    t_2 = multiprocessing.Process(target=exec_net, args = ())
    t_3 = multiprocessing.Process(target=exec_sa, args = ())
    t_4 = multiprocessing.Process(target=exec_redis_operate, args = ())

    t_5 = multiprocessing.Process(target=exec_TIP, args = ())

    # 执行更新资产中心信息
    asset = AssetInfo()
    asset.server_info_update()
    asset.database_info_update()

    t_1.start()
    t_2.start()
    t_3.start()
    t_5.start()

    if(args.distribute):
        t_4.start()
    exec_spark_all()
