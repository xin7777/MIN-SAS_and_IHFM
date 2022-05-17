from pyspark import SparkContext
from pyspark import SparkConf
import argparse
import time
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import *
import kafka
import json
import pymongo
from pymongo import MongoClient
import sys
import os
import configparser

conf = configparser.ConfigParser()
conf.read('config/config.cfg')
os.environ["PYSPARK_PYTHON"] = conf.get("Path","Python3.6")
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options -XX:+UseConcMarkSweepGC --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar pyspark-shell'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from kafka import KafkaProducer
#持久化，输出到新的kafka topic

#插入MongoDB
def sendMongoDB(message):
    global db
    db_col = db["flow"]
    records = message.collect()
    for record in records:
        record = json.loads(record)
        db_col.insert_one(record)
        print("insert:",record," ",type(record))


try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['flow']
    db.authenticate(conf.get("Password","Mongo"), conf.get("Password","Mongo"))
    db_min = client['packet_flow']
    db_min.authenticate(conf.get("Password","Mongo"), conf.get("Password","Mongo"))
    collection_name = 'SA_value'
    collection = db[collection_name]
except Exception as e:
    print(e, "\n mongodb init failed")

def run_ip_detect(stream_context):
    num_partition = 1
    msg_stream = [0] * num_partition
    targets = [0] * num_partition
    process = [0] * num_partition
    for i in range(num_partition):
        try:
            partition = i
            start_p = 0
            topicPartition = TopicAndPartition('flow', partition)
            fromOffset = {topicPartition: start_p}
            msg_stream[i] = KafkaUtils.createDirectStream(stream_context,['flow',],kafkaParams={"metadata.broker.list": "127.0.0.1:9092"})
            targets[i] = msg_stream[i].map(lambda stream: stream[1])

            targets[i].foreachRDD(sendMongoDB)
            targets[i].count().pprint()
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

def start(mode):
    config = SparkConf().setMaster("local[40]").set("spark.cores.max",20).set("spark.default.parallelism", 30).set("spark.streaming.kafka.maxRatePerPartition",25000).set("spark.ui.enabled",False).set("spark.streaming.backpressure.enabled", True).set("spark.executor.cores",1).set("spark.executor.memory","50g")
    scontext = SparkContext(conf=config)
    stream_context = StreamingContext(scontext,3)
    if mode == "ip":
        run_ip_detect(stream_context)
    else:
        print("params error")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ip', action="store_true", help="detect the ip flow")
    sys_args = parser.parse_args()

    if (sys_args.ip):
        start("ip")
    else:
        print("usage: -i")
