# -*- coding: utf-8 -*-
#!/usr/bin/env python
from geolite2 import geolite2
import time
import threading
import os
import re
import datetime
import pyinotify
import logging
import configparser

import urllib.request, urllib.error, urllib.parse
import json
import dpkt
import hashlib
from OTXv2 import OTXv2
import IndicatorTypes
import requests
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


from mongodb3 import *


def parse(res):
    res = res.result()
    pass
    #线程池回调函数
    #print("res has over.\n",res)

class MyEventHandler(pyinotify.ProcessEvent):

    def __init__(self):
        self.conf = configparser.ConfigParser()
        self.conf.read("config/config.cfg")

        f = open(self.conf.get("Path","iptables_log"), "a+")
        f.seek(0,2)
        self.pos = f.tell() #利用pos记录末尾位置，只检测刷新的IP
        f.close()
        #print (pos)

        '''
        self.window_size = 10 #设置窗口大小
        self.query = TIP_query()

        self.TIP_messages = mongoOperate("Situation_Awareness", "TIP_messages")
        self.TIP_messages.db_connect()
        with open("config/white_list.txt") as f:
            content = f.read()
            f.close()
        content = content.replace('\n','').split(' ')  #分割
        self.white_list = list(filter(None, content))  #去空
        self.ip_window = [] #维护一个黑名单列表，当同一个IP频繁出现时，不进行反复检索
        '''


        #创建线程池，线程数量为3
        self.executor = ThreadPoolExecutor(max_workers=5)

    #当文件被修改时调用函数
    def process_IN_MODIFY(self, event):
        try:
            self.capture_ip()
        except Exception as e:
            print (str(e))


    def capture_ip(self):
        try:
            fd = open(self.conf.get("Path", "iptables_log"),"a+")
            if fd.tell() == 0:
                return
            if fd.tell() > 37000:
                fd.seek(0)
                fd.truncate()
                self.pos = 0
                return
            if self.pos != 0:
                fd.seek(self.pos,0)
                #print("位置是:",self.pos)
            while True:
                line = fd.readline()
                #print("data:",line)
                #if line.strip():
                #    print line.strip()
                #delete the newest line
                self.pos = self.pos + len(line)

                rule_src = r'SRC(\S)*'
                ans_s = re.search(rule_src, line)
                if ans_s:
                    src_ip = ans_s.group(0)
                else:
                    break

                rule_dst = r'DST(\S)*'
                ans_d = re.search(rule_dst, line)
                if ans_d:
                    dst_ip = ans_d.group(0)
                else:
                    break
                ip = [src_ip[4:], dst_ip[4:]]
                print("ip: \n", ip)
                #print(self.ip_window)
                self.executor.submit(task, ip).add_done_callback(parse)
                #self.executor.shutdown(False) #分离线程
                #self.deal_ip(ip)

                if not line.strip():
                    break
            #fd.seek(self.pos, 0)
            #fd.truncate()
            print("删除之后的位置是",fd.tell())
            fd.close()
        except Exception as e:
            fd.close()
            print (str(e))

    def deal_ip(self,ip_list):#弃用
        #print("Accept:",ip_list)
        #print("%s is runing" %os.getpid())
        while len(ip_list) > 0:

            if ip_list[0] in self.white_list:     #若在白名单中，则不进行检索
                ip_list.pop(0)
                continue
            #ip not in the window 查询ip
            if ip_list[0] in self.ip_window:
                ip_list.pop(0)
                continue  #若在黑名单中，不进行检索

            result = self.query.IsMalicious(ip_list[0])

            if result:
                result['status'] = self.judge_conn(ip_list[0])
                self.TIP_messages.db_insert(result)
                self.ip_window.append(ip_list[0])
                if len(self.ip_window) > self.window_size:
                    self.ip_window.pop(0)
            else:
                self.white_list.append(ip_list[0])
                with open("config/white_list.txt", "a+") as f:
                    f.write(ip_list[0] + ' ')
                    f.close()
            #文件IP列表写入该IP
            #self.ip_window.append(ip_list[0])
            #if len(self.ip_window) > 5:
            #    self.ip_window.pop(0)
            ip_list.pop(0)


class TIP_query():
    def __init__(self):
        #self.url = 'http://api.ipstack.com/{}?access_key=1bdea4d0bf1c3bf35c4ba9456a357ce3'
        self.API_KEY = "8ae47a61b85d5ac78c30cf8f28398017b86f629dc76697c08a0ab4ddd4474037"
        self.OTX_SERVER = 'https://otx.alienvault.com/'
        self.reader = geolite2.reader()

    #获取json的层级化对象value值，keys为指定的层级化键值列表
    def getValue(self, results, keys):
        if type(keys) is list and len(keys) > 0:

            if type(results) is dict:
                key = keys.pop(0)
                if key in results:
                    return self.getValue(results[key], keys)
                else:
                    return None
            else:
                if type(results) is list and len(results) > 0:
                    return self.getValue(results[0], keys)
                else:
                    return results
        else:
            return results

    def get_location(self, ip):
        message = self.reader.get(ip)
        if message != None:
            try:
                city = message['city']['names']['zh-CN']
                return city
            except Exception as e:
                city = "Unknow"
                return city
        else:
            return "Unknow"

    def CheckIp(self, otx, ip):
        #print("check: ",ip)
        alerts = []
        result = otx.get_indicator_details_by_section(IndicatorTypes.IPv4, ip, 'general')

        # Return nothing if it's in the whitelist
        validation = self.getValue(result, ['validation'])
        if not validation:
            pulses = self.getValue(result, ['pulse_info', 'pulses'])
            if pulses:
                for pulse in pulses:
                    if 'name' in pulse:
                        alerts.append('In pulse: ' + pulse['name'])
        return alerts

    def IsMalicious(self, ip):
        #print("IsMalicious start:\n")
        t = time.strftime("%Y-%m-%d %H:%M:%S")
        otx = OTXv2(self.API_KEY, server=self.OTX_SERVER)
        location = self.get_location(ip)
        alerts = self.CheckIp(otx,ip)
        if len(alerts) > 0:
            res = {"ip":ip, "location":location, "time": t, "alerts":alerts}
            #self.TIP_messages.db_insert(res)
            print("alert: ",res)
            return res
        else:
            return False

window_size = 10 #设置窗口大小
query = TIP_query()

TIP_messages = mongoOperate("Situation_Awareness", "TIP_messages")
TIP_messages.db_connect()
with open("config/white_list.txt") as f:
    content = f.read()
    f.close()
content = content.replace('\n','').split(' ')  #分割
white_list = list(filter(None, content))  #去空
ip_window = [] #维护一个黑名单列表，当同一个IP频繁出现时，不进行反复检索
lock = threading.Lock()


class Process_deal:

    def __init__(self):
        self.window_size = window_size #设置窗口大小
        self.query = query

        self.TIP_messages = TIP_messages
        self.conf = configparser.ConfigParser()
        self.conf.read("config/config.cfg")

    def deal_ip(self,ip_list):
        #print("Accept:",ip_list)
        #print(ip_window)
        while len(ip_list) > 0:
            if ip_list[0] in white_list:     #若在白名单中，则不进行检索
                ip_list.pop(0)
                continue
            #ip not in the window 查询ip
            if ip_list[0] in ip_window:
                ip_list.pop(0)
                continue  #若在黑名单中，不进行检索

            #url = "http://" + self.conf.get("Address","TIP") + ":8089/api/TIP/ip_msg?ip=" + ip_list[0] + "&key=8ae47a61b85d5ac78c30cf8f28398017b86f629dc76697c08a0ab4ddd4474037"
            url = "http://" + self.conf.get("Address","TIP") + ":8089/api/TIP/"
            d = {'ip':ip_list[0], 'key':"8ae47a61b85d5ac78c30cf8f28398017b86f629dc76697c08a0ab4ddd4474037"}
            print("发送：",url)
            result = {}
            try:
                #result = requests.get(url)
                result = requests.post(url, data=d)
                result = result.json()
            except Exception as e:
                result = {}
                print(e)
            #print(result)
            #result = self.query.IsMalicious(ip_list[0])

            if result.__contains__("ip"):
            #if result:
                print("包含")
                result['status'] = self.judge_conn(ip_list[0])
                self.TIP_messages.db_insert(result)
                ip_window.append(ip_list[0])
                if len(ip_window) > self.window_size:
                    ip_window.pop(0)
            else:
                print("不包含")
                white_list.append(ip_list[0])
                with open("config/white_list.txt", "a+") as f:
                    f.write(ip_list[0] + ' ')
                    f.close()
            #文件IP列表写入该IP
            #self.ip_window.append(ip_list[0])
            #if len(self.ip_window) > 5:
            #    self.ip_window.pop(0)
            ip_list.pop(0)


    def judge_conn(self,ip):
        cmd = "netstat -antup | grep ESTABLISH | grep " + ip
        #cmd = "netstat -antup | grep " + ip
        out = os.popen(cmd)
        if out.read() == '':
            #未建立连接的IP
            return "not established"
            pass
        else:
            #已经建立连接的IP,进一步判断,若ssh进程，判断IP是否连接登录
            cmd_sshd = cmd + "| grep sshd"
            out_sshd = os.popen(cmd_sshd)
            if out_sshd.read() != '':
                #是sshd进程
                cmd_login = "last | grep " + ip
                out_login = os.popen(cmd_login)
                if out_login.read() != '': #判断是否有登录历史
                    return "established"
                else:
                    return "not established"
            else:
                #不是sshd进程
                return "established"
            pass



def task(ip):

    lock.acquire() #锁
    temp = Process_deal()
    temp.deal_ip(ip)
    lock.release()




def start():
# watch manager
    wm = pyinotify.WatchManager()
    conf = configparser.ConfigParser()
    conf.read("config/config.cfg")

    wm.add_watch(conf.get("Path","iptables_log"), pyinotify.ALL_EVENTS, rec=True)
    eh = MyEventHandler()

    # notifier
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()


if __name__ == '__main__':
    start()
    #test()
