import multiprocessing
import datetime
from concurrent.futures import ThreadPoolExecutor
import argparse
import TIP_module
import S_Assessment
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import *
from block_tcp import *
import kafka
import json
import pymongo
from pymongo import MongoClient
import threading
from urlDetect.urlDetect import *
import json
import csv
import sys
from sys import *
import os
import configparser
from AssetInfo import AssetInfo
from geolite2 import geolite2
import SMTP_email

conf = configparser.ConfigParser()
conf.read('config/config.cfg')
os.environ["PYSPARK_PYTHON"] = conf.get("Path","Python3.6")
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options -XX:+UseConcMarkSweepGC --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar pyspark-shell'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['SPARK_HOME'] = conf.get("Path", "SPARK_HOME")
email_obj = SMTP_email.Email()


from flowDetect.flowDetect import *
from utils import *
from time import gmtime,strftime
import time
import psutil

import SA_predict

counter=0
offsets = []
def out_put(m):
    print(m)
def store_offset(rdd):
    global offsets
    offsets = rdd.offsetRanges()
    return rdd

def print_offset(rdd):
    for o in offsets:
        print(("%s %s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset,o.untilOffset-o.fromOffset)))

'''
usage:
config = SparkConf()
#scontext = SparkContext(appName='kafka_pyspark_test',)
scontext = SparkContext("local[2]", "kafka_pyspark_test")
stream_context = StreamingContext(scontext,5)
msg_stream = KafkaUtils.createDirectStream(stream_context,['test',],kafkaParams={"metadata.broker.list": "127.0.0.1:9092,"})
#result = msg_stream.map(lambda x :json.loads(x).keys()).reduce(out_put)
#msg_stream.transform(store_offset,).foreachRDD(print_offset)
#result.pprint()

targets = msg_stream.map(lambda msg_stream: msg_stream[1])
'''

json_values = []


freq_dict = {}
def mapper(record):
    record = json.loads(record)
    res = {}
    res["Source IP"] = record.get("ip_src")
    '''
    if record.get("ip_src") == "121.15.171.82":
        with open("test_lgx.txt","a") as f:
                f.write("success")
                f.close()
    '''
    res['Dest IP'] = record.get('ip_dst')
    res['Transport Layer'] = record.get('ip_proto')
    res['Source Port'] = record.get('port_src')
    res['Dest Port']= record.get('port_dst')
    #res['Attack Length'] = record.get('bytes')
    res['Attack Length'] = 0
    res['Packet Length'] = record.get('bytes')
    res['timestamp'] = record.get('timestamp_start')
    if res["Source IP"] not in freq_dict:
        freq_dict[res["Source IP"]] = 1
        res["frequency"] = 1
    else:
        freq_dict[res["Source IP"]] += 1
        res["frequency"] = freq_dict[res["Source IP"]] + 1
    res = json.dumps(res)
    #result = ddos_detect(json.dumps(res))
    '''if result["Hostile_Packets_Detected"] != 0:
        #write to the log file
    #if hostile != 0:
        #evil event log
        event_lg.debug(data)
    else:
#       Logger("flow_normal_log").get_log().critical(data)
        lg.debug(data)
'''

    #write(res)
    return res




from kafka import KafkaProducer
#持久化，输出到新的kafka topic

count = 1
executor = ThreadPoolExecutor(max_workers=10)  #spark deal threadpool
executor_sender = ThreadPoolExecutor(max_workers=1) #email threadpool
executor_last_time = datetime.datetime.now() + datetime.timedelta(minutes=5) #get 5minutes ago object


def nums_count(data):
    global count
    count += 1
    print(count)



def sendTest1(message):
    records = message.collect()
    #message.count().pprint()
    with open("test_data.json", "a") as json_file:
        for json_str in records:
            json_file.write(json_str + '\n')
        json_file.close()


def sendTest(message):
    try:
        records = message.collect()
        print(("=========================",type(records)))
        if(isinstance(records, list)):
            executor.submit(sync_deal, records)
    except Exception as e:
        print("stream error")
        pass

def sync_deal(records):
    #ddos_result = ddos_detect(records)
    try:
        ddos_result = ddos_detect(records)
        #ddos_result["Hostile_Packets_Detected"]=0
        print(ddos_result)
        if ddos_result["Hostile_Packets_Detected"] != '0':
            event_data = json.loads(ddos_result["Hostile_Packets_Info"])
            event_data[0]["event"] = "DDoS"
            print("+++++++++++++++++++++++++++++++++",event_data)
            sendMongoDB(event_data)
            print("++++++++++++++++++++++++++++++++++")
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            message = {"Type":"network", "Command":"Log", "Prefix":"/mis/update/danger_log", "Level":1, "Action":"DDos attack", "Sig":"xxx", "Timestamp":timestamp}
            #block_message(message)
        pass
    except Exception as e:
        print("ddos:\n",e)
        pass
    try:
        #'''
        nmap_result = nmap_detect(records)
        #nmap_result["Hostile_Packets_Detected"]=0
        print("nmap_result: ",nmap_result)
        if nmap_result["Hostile_Packets_Detected"] != '0':
            #print '默认接收为json格式，转换'
            event_data = json.loads(nmap_result["Hostile_Packets_Info"])
            for each in event_data:
                each["event"] = "Scan"
            print(event_data)
            sendMongoDB(event_data)
            message = {"Type":"network", "Command":"Log", "Prefix":"/mis/update/danger_log", "Level":1, "Action":"Scan warning", "Sig":"xxx", "Timestamp":time.strftime("%Y-%m-%d %H:%M:%S")}
            #block_message(message)
        #'''
        pass
    except Exception as e:
        print("nmap_result error:",e)
        #pass
    try:
        S_Assess(nmap_result["Hostile_Packets_Detected"], ddos_result["Hostile_Packets_Detected"])
        pass
        #S_Assess(0,0)
    except Exception as e:
        print("S_Assess:",e)
        #print ddos_result
        pass

#查询对应城市信息后插入MongoDB
def sendMongoDB(records):
    record = records[0]
    ip = record["Source IP"]
    local_ip = record["Dest IP"]

    reader = geolite2.reader()
    remote_message = reader.get(ip)
    local_message = reader.get(local_ip)
    print("remote_message:",remote_message)

    if local_message == None :
        local_message = reader.get(conf.get("Address", "internet"))
        local_message['city']['names']['zh-CN'] = conf.get("Address", "city_name")
        local_message['location']['latitude'] = float(conf.get("Address", "latitude"))
        local_message['location']['longitude'] = float(conf.get("Address", "longitude"))

    if remote_message != None:
        new_record = {}
        new_record["srcIp"] = ip
        new_record["srcPort"] = record["Source Port"]
        new_record["type"] = record["event"]
        new_record["destIp"] = record["Dest IP"]

        try:
            new_record["destName"] = "Local: "+local_message['city']['names']['zh-CN'] #这里需要更改
        except:
            #new_record["destName"] = "Local: "+local_message['country']['names']['zh-CN']
            new_record["destName"] = "Local: "+local_message['location']['time_zone']

        new_record["destLocY"] = local_message['location']['latitude']
        new_record["destLocX"] = local_message['location']['longitude']
        new_record["time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        #if 'city' in remote_message.keys():
        try:
            new_record["srcName"] = remote_message['city']['names']['zh-CN']
            #elif 'country' in remote_message.keys():
        except:
            #new_record["srcName"] = remote_message['country']['names']['zh-CN']
            new_record["srcName"] = remote_message['location']['time_zone']
        #else:
        #    new_record["srcName"] = remote_message['subdivisions'][0]['names']['zh-CN']
        new_record["srcLocY"] = remote_message['location']['latitude']
        new_record["srcLocX"] = remote_message['location']['longitude']

        #插入SA_event表
        global db
        global executor_last_time
        db_col = db["SA_event"]
        db_col.insert_one(new_record)
        print("send Mongo success")
        executor_now_time = datetime.datetime.now()
        if (executor_now_time - executor_last_time).seconds>=300:
            executor_sender.submit(email_obj.send, new_record)
            executor_last_time = executor_now_time





#最大带宽
MAX_SPEED = 65
#态势评估
def S_Assess(nmap, ddos):
    if nmap!='0' or ddos!='0':
            #定权值
        nmap = 1 if nmap>"0" else 0
        ddos = 2 if ddos>"0" else 0
        #时间向量
        t_now = strftime("%H:%M:%S")
        if t_now >= "0" and t_now < "9": #24:00 - 9:00
            t_vector = 1
        elif t_now < "18": #9:00 - 18:00
            t_vector = 3
        else:
            t_vector = 2
        #带宽占用比
        speed_o = list(psutil.net_io_counters())
        time.sleep(1)
        speed_n = list(psutil.net_io_counters())
        speed = float(speed_n[1] - speed_o[1])/(1024*1024)
        speed_percent = speed/MAX_SPEED
        #cpu占用比
        cpu = psutil.cpu_percent(None)/100
        #cpu和bindwidth中和
        quality = (speed_percent + cpu)
        #求态势值
        result = t_vector*(pow(10,nmap) + pow(10,ddos)*quality)
        #with open("test_value.txt", "a") as file_object:
        #   file_object.write("1\n")
    #       file_object.write("result: "+str(result) + "time_vector: "+str(t_vector) + "nmap: "+ str(nmap) + " ddos: "+ str(ddos)+" quality: "+ str(quality)+"\n")
    #       file_object.write("pow(10,ddos)*quality"+str(pow(10,ddos)*quality)+"\n")
            #file_object.close()
    else:
        result = 0
    #sendMongoDB
    global collection
    #ans = {"value":result, "time":strftime("%Y-%m-%d %H:%M:%S")}
    ans = {"value":result, "time":int(time.time() * 1000), "htime":strftime("%Y-%m-%d %H:%M:%S")}
    collection.insert_one(ans)
    SA_predict.start()



def min_detect(records):
    message = records.collect()
    # print(message)
    if len(message) > 0:
        result = detectBatch(message)
    else:
        result = []
    #result = message
    print("result: ",result)

    for each in result:
        header =eval(message[each[0]])
        each = eval(message[each[1]])
        print(type(header),header,'\n',each)
        server_id = "1"
        rule_id = "001"
        level = 8#6
        timestamp = time.time()
        location_id = "2"
        src_ip = each["src_ip"]
        dst_ip = each["dst_ip"]
        src_port = each["sport"]
        dst_port = each["dport"]
        alertid = "0"
        user = each["username"]
        full_log = "request abnormal, maybe malicious request\n" + each["data"]
        sql = '''insert into alert(server_id, rule_id, level, timestamp, location_id, src_ip, dst_ip, src_port, dst_port, alertid, user, full_log) value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        cursor.execute(sql, (server_id, rule_id, level, timestamp, location_id, src_ip, dst_ip, src_port, dst_port, alertid, user, full_log))
        sql_db.commit()
        #send to mongodb
        global db_min
        #each['danger'] = full_log
        db_min['event_log'].insert_one(each)
        print("mongo insert success,(event_log)min-packet")
        message = {"Type":"network", "Command":"Log", "Prefix":"/mis/update/danger_log", "Level":1, "Action":full_log, "Sig":"xxx", "Timestamp":time.strftime("%Y-%m-%d %H:%M:%S")}
        # block_tcp(message)
        #key_locator = test_decode_func(each["data"])
        #os.system("python3 pyndn_decode.py "+header['data'])
        #db_min['event_log'].insert_one(key_locator)
        with open("evil_data.txt", 'w') as f:
            f.write((header['data']))
            f.close()
        os.system("python3 pyndn_decode.py")

# event_lg = event_Logger("flow_log").get_event_log()
# lg = Logger("flow_normal_log").get_log()
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['Situation_Awareness']
    db.authenticate(conf.get("Password","Mongo"), conf.get("Password","Mongo"))
    db_min = client['packet_flow']
    db_min.authenticate(conf.get("Password","Mongo"), conf.get("Password","Mongo"))
    collection_name = 'SA_value'
    collection = db[collection_name]
except Exception as e:
    print(e, "\n mongodb init failed")

try:
    import pymysql
    sql_db = pymysql.connect(host="localhost", user="root", password=conf.get("Password","Mysql"), port=int(conf.get("Port","mysql")))
    cursor = sql_db.cursor()
    cursor.execute("use ossec;")
except Exception as e:
    print(e,"\n mysql init failed")

def run_ip_detect(stream_context):
    '''
    config = SparkConf()
    #scontext = SparkContext(appName='kafka_pyspark_test',)
    scontext = SparkContext("local[2]", "kafka_pyspark_test")
    stream_context = StreamingContext(scontext,5)
    '''
    num_partition = 1
    msg_stream = [0] * num_partition
    targets = [0] * num_partition
    process = [0] * num_partition
    for i in range(num_partition):
        try:
            partition = i
            start_p = 0
            topicPartition = TopicAndPartition('test', partition)
            fromOffset = {topicPartition: start_p}
            msg_stream[i] = KafkaUtils.createDirectStream(stream_context,['test',],kafkaParams={"metadata.broker.list": "127.0.0.1:9092"})
            #,"auto.offset.reset":"largest"})
            #, fromOffsets=fromOffset)
            #msg_stream[i].transform(store_offset).foreachRDD(print_offset)
            targets[i] = msg_stream[i].map(lambda stream: stream[1])
            process[i] = targets[i].map(mapper)

            process[i].foreachRDD(sendTest)
            process[i].count().pprint()
            stream_context.start()
            stream_context.awaitTermination()
        except Exception as e:
            print("offset error")
            pass

    #process.foreachRDD(lambda rdd: rdd.foreachPartition(sendMongoDB))
    #msg_stream.saveAsTextFile('out.txt')
    #msg_stream.pprint()
    #stream_context.start()
    #stream_context.awaitTermination()

    '''
    config = SparkConf()
    #scontext = SparkContext(appName='kafka_pyspark_test',)
    scontext = SparkContext("local[2]", "kafka_pyspark_test")
    stream_context = StreamingContext(scontext,5)
    '''
def justin_test():
    partition2 = 1
    start_p2 = 0
    topicPartition2 = TopicAndPartition('test', partition2)
    fromOffset2 = {topicPartition2: start_p2}
    msg_stream2 = KafkaUtils.createDirectStream(stream_context,['test',],kafkaParams={"metadata.broker.list": "127.0.0.1:9092,", "auto.offset.reset":"largest"}, fromOffsets=fromOffset2)
    '''result = msg_stream.map(lambda x :json.loads(x).keys()).reduce(out_put)'''
    msg_stream2.transform(store_offset).foreachRDD(print_offset)
    '''
    result.pprint()
    '''

    targets2 = msg_stream2.map(lambda msg_stream2: msg_stream2[1])
    process2 = targets2.map(mapper)

    #process.pprint()

    #process.map(lambda key:key["ip_src"]).countByValue()
    #process.pprint()
    #process.foreachRDD(sendKafka)
    process2.foreachRDD(sendTest)
    process2.count().pprint()

    #process.foreachRDD(lambda rdd: rdd.foreachPartition(sendMongoDB))
    #msg_stream.saveAsTextFile('out.txt')
    #msg_stream.pprint()
    stream_context.start()
    stream_context.awaitTermination()


def run_min_detect(stream_context):
    #config = SparkConf()
    #scontext = SparkContext(appName='kafka_pyspark_test',)
    #scontext = SparkContext("local[1]", "kafka_pyspark_min-packet")
    #stream_context = StreamingContext(scontext,5)
    msg_stream = KafkaUtils.createDirectStream(stream_context,['MIN-packet',],kafkaParams={"metadata.broker.list": "127.0.0.1:9092,", "auto.offset.reset":"largest"})
    Targets = msg_stream.map(lambda msg_stream: msg_stream[1])
    Targets.pprint()
    Targets.foreachRDD(min_detect)

    stream_context.start()
    stream_context.awaitTermination()

def run_all_detect(stream_context):
    min_stream = KafkaUtils.createDirectStream(stream_context,['MIN-packet',],kafkaParams={"metadata.broker.list": "127.0.0.1:9092,", "auto.offset.reset":"largest"})
    ip_stream = KafkaUtils.createDirectStream(stream_context,['test',],kafkaParams={"metadata.broker.list": "127.0.0.1:9092,", "auto.offset.reset":"largest"})
    #min_stream
    Targets = min_stream.map(lambda min_stream: min_stream[1])
    Targets.pprint()
    Targets.count().pprint()
    Targets.foreachRDD(min_detect)

    #ip_stream
    targets = ip_stream.map(lambda ip_stream: ip_stream[1])
    process = targets.map(mapper)
    process.foreachRDD(sendTest)
    process.count().pprint()
    stream_context.start()
    stream_context.awaitTermination()


def start(mode):
    config = SparkConf().setMaster("local[40]").set("spark.cores.max",20).set("spark.default.parallelism", 30).set("spark.streaming.kafka.maxRatePerPartition",25000).set("spark.ui.enabled",False).set("spark.streaming.backpressure.enabled", True).set("spark.executor.cores",1).set("spark.executor.memory","50g")
    #.set("num-executor",100)
    #.set("spark.streaming.backpressure.enabled",True).set("spark.streaming.kafka.maxRatePerPartition", 20000)
    #.set("spark.executor.cores",1)
    #.set("spark.executor.cores", 1).set("spark.cores.max",10).set("spark.default.parallelism", 200).set("spark.executor.memory","50g").setMaster("local[*]")
    #.set("spark.streaming.kafka.maxRatePerPartition", 10000)
    #.set("spark.executor.cores", 2).set("spark.cores.max",18)
    #scontext = SparkContext("local[10]", "kafka_pyspark_min-packet")
    scontext = SparkContext(conf=config)
    stream_context = StreamingContext(scontext,3)
    if mode == "ip":
        run_ip_detect(stream_context)
        #run_ip_detect2(stream_context)
        thread1 = multiprocessing.Process(target=run_ip_detect, args = (stream_context,))
        #thread2 = multiprocessing.Process(target=run_ip_detect2, args = (stream_context2,))
        #thread1.start()
        #thread2.start()
    elif mode  == "min":
        run_min_detect(stream_context)
    elif mode  == "all":
        run_all_detect(stream_context)
        #不可使用多线程或者多进程方式同时运行streamcontext，切记
    else:
        print("params error")
    #ddos_detect(['sdf'])


def exec_TIP():#TIP_module
    try:
        TIP_module.start()
    except Exception as e:
        print(e)
        exit()

def exec_sa():#态势评估
    try:

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

def exec_assetinfo():
    while True:
        print("更新资产信息！！！！")
        try :
            asset = AssetInfo()
            asset.server_info_update()
            asset.database_info_update()
        except Exception as e:
            print("***************************update asset error: \n", e)
        print("Data: OK")
        time.sleep(3600 * 24)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--distribute', action="store_true", help="distribute mode, open the redis to publish & subscribe command")
    parser.add_argument('-i', '--ip', action="store_true", help="detect the ip flow")
    parser.add_argument('-m', '--min', action="store_true", help="detect the min flow")
    parser.add_argument('-a', '--all', action="store_true", help="detect all flow")
    sys_args = parser.parse_args()

#    if len(sys.argv[1:]) != 1 :
#        print("must have a param, 'ip' or 'min' or 'all'")
#        exit()
    # 执行更新资产中心信息
    t=threading.Thread(target=exec_assetinfo)
    t.start()

    t_1 = multiprocessing.Process(target=exec_TIP, args = ())
    t_2 = multiprocessing.Process(target=exec_sa, args = ())
    t_3 = multiprocessing.Process(target=exec_redis_operate, args = ())
    t_1.start()
    t_2.start()

    if (sys_args.distribute):
        t_3.start()

    if (sys_args.ip):
        start("ip")
    elif (sys_args.min):
        start("min")
    elif (sys_args.all):
        start("all")
