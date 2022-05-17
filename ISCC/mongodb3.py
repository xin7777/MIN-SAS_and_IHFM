#!/usr/bin/python
# encoding: utf-8
# -*- coding: utf8 -*-

#tip:查询时排序要用列表，不可{_id:-1}


import configparser
import os
import sys
import threading
import queue
import time
import datetime
import argparse
#from Filter_module import *
#为了防止mongo查询的结果都是Unicode编码的
#import yaml
#from bson import json_util


import json


try:
    import pymongo
    from pymongo import MongoClient
except ImportError:
    try:
        command_to_execute = "pip3 install pymongo"
        os.system(command_to_execute)
    except OSError:
        print("Can NOT install pymongo, Aborted!")
        sys.exit(1)
    import pymongo
    from pymongo import MongoClient

try:
    from pymongo.objectid import ObjectId
except Exception:
    from bson.objectid import ObjectId


class mongoOperate():

        #initialization
    def __init__(self, db_database, db_collection):
        #time.sleep(1)
        self.db_database = db_database
        self.db_collection = db_collection
        self.db = False
        self.collection = False
        self.db_connect()


    def db_connect(self):
        try:
            self.db_conn = MongoClient()
            exec("self.db = " + str(self.db_conn) + "." + str(self.db_database))
            print(self.db)
            conf = configparser.ConfigParser()
            conf.read("config/config.cfg")
            self.db.authenticate(conf.get("Password","Mongo"), conf.get("Password","Mongo"))
            exec("self.collection = " + "self.db" + "." + str(self.db_collection))
            print(self.collection)
            self.db_connect_status = str(self.db_conn).split(" ")[-1][:-1]
            print("\n=====Connected to MONGODB =====\n" + self.db_connect_status + "\n")
            self.mon = self.db[str(self.db_collection)]
        except Exception as e:
            print("MongoDB connect failed", e)


    def db_read(self, comand):
        try:
            if comand == "all":
                result = self.collection.find().sort([("_id",-1)])
            else:
                result = self.collection.find().sort([("_id",-1)]).limit(comand)

            ans = []
            size = 0
            for each in result:
                #each['_id'] = str(each['_id'])
                size = size + each['bytes']
                #ans.append(each)
            print("======================")
            print("Sum bytes: ",size,"bytes ",)
            size = size/1024.
            if(size >=1):
                print(size, "KB ",)
            size = size/1024
            if(size>=1):
                print(size, "MB ",)
            size = size/1024
            if(size>=1):
                print(size,"GB",)

            return ans
        except Exception as e:
            print(e, "\n search error\n")

    def db_read_id(self, ID):
        try:
            result = self.collection.find_one({"_id":ObjectId(ID)})
            #result = yaml.safe_load(json_util.dumps(result))
            return result
        except Exception as e:
            print(e)

    def db_read_time(self, time_s, time_e):
        try:
            result = self.collection.find({"Time":{'$gte':time_s,'$lt':time_e}}).sort([("_id",-1)]).limit(1000)
            ans = []
            for each in result:
                #each = yaml.safe_load(json_util.dumps(each))
                each['_id'] = str(each['_id'])
                ans.append(each)
            return ans
        except Exception as e:
            print(e)


    def db_del(self, _id):
        try:
            self.collection.delete_one({"_id":ObjectId(_id)})
        except Exception as e:
            print(e)

    def db_del_command(self, command):
        try:
            self.collection.delete_one(command)
        except Exception as e:
            print(e)

    def db_close(self):
        self.db_conn.close()

    def get_count(self):
        if self.collection:
            return self.collection.count()
        else:
            return -1


    def get_user_frequency(self, ip_src, mins, days_set, limit_set):
        if self.collection:
            now_time = datetime.datetime.now()
            if days_set == 0 :
                previous_time = (now_time+datetime.timedelta(minutes=-mins)).strftime("%Y-%m-%d %H:%M:%S")
            elif days_set > 0 :
                previous_time = (now_time+datetime.timedelta(days=-days_set)).strftime("%Y-%m-%d %H:%M:%S")
            else:
                return -1

            result = self.collection.find({"ip_src":ip_src, "Time":{'$gte':previous_time}}).sort([("_id",args.order)]).limit(limit_set)

            if limit_set < 20 and limit_set > 0:
                for each in result:
                    print(each,"\n")
            return result

    def get_users(self):
        users = self.collection.distinct("ip_src")
        for each in users:
            print(each)
        return users


    def get_special_key_values(self, key):
        self.collection.find({key:{"$exists":true}},{ key:1, "_id":0})
        return 1

#usage:
#mongodb = mongoOperate("packet_flow", "packet_flow")
#mongodb.run(data_queue)

if __name__ == "__main__":
#def main():
    print("only display at most 20 messages if limit param is set")
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--find', action='store_true', help="find under demands, need -e,-l")
    parser.add_argument('-g', '--get_frequency', action='store_true', help="view the user's frequency, need -u, -m, -d, -l")
    parser.add_argument('-a', '--all', action='store_true', help="get all messages, need -o, -l")
    parser.add_argument('-us', '--users', action='store_true', help="get all users")

    parser.add_argument('-c', '--count', action='store_true', help="the count of the whole db")


    parser.add_argument('-o', '--order', default=1, help="the order of results",type=int)
    parser.add_argument('-m', '--minutes', default=1, help="minutes ago -> now set",type=int)
    parser.add_argument('-l', '--limit', default=0, help="limit the displayed count",type=int)
    parser.add_argument('-u', '--username', default='', help="set username", type=str)
    parser.add_argument('-d', '--days', default=0,help="days ago -> now ",type=int)
    parser.add_argument('-e', '--demand',default="", help="search conditions", type=str)
    args = parser.parse_args()

    mongDB = mongoOperate("flow", "flow")
    #mongDB.db_connect()
    #mongDB.db_read(8)
    #mongDB.db_close()
#       mongDB.kafka_insert("first test")

    if args.count:
        print("the count :\n " ,mongDB.get_count())

    if args.all:
        #print("data : \n" , mongDB.db_read(args.limit))
        mongDB.db_read(args.limit)

    if args.get_frequency:
        print("result : \n" , mongDB.get_user_frequency(args.username, args.minutes, args.days, args.limit))

    if args.find:
        print("find :\n" , mongDB.get_message(args.demand,args.limit))

    if args.users:
        print("users:\n", mongDB.get_users())
        mongDB.db_del('5f695b964281304dac1c7423')
