#encoding=utf-8
import redis
from mongodb3 import *
import json
import os
import configparser
import pymysql


conf = configparser.ConfigParser()
conf.read("config/config.cfg")

class RedisHelper(object):

    def __init__(self, channel_sub):
        self.__conn = redis.Redis(host=conf.get("Address", "Intranet"),port=conf.get("Port","redis"),password=conf.get("Password","Redis"))
        #subscribe
        self.chan_sub = channel_sub



    def public(self, chan, msg):
        #publish
        self.__conn.publish(chan, msg)
        return True

    def subscribe(self):
        #return object, you can subscribe channel or listen the messages in channels by the object
        pub = self.__conn.pubsub()
        #subscribe channel
        pub.subscribe(self.chan_sub)
        ret = pub.parse_response()
        print(("ret:%s" % ret))
        return pub

def Mysql_init():
    sql_conn = pymysql.connect("localhost","root",conf.get("Password","Mysql"))
    sql_cursor = sql_conn.cursor()
    return sql_conn, sql_cursor


def run():
    obj = RedisHelper("survice")
    #for i in range(5):
    #       obj.public("test_ %s" % i)
    redis_sub = obj.subscribe()

    packet_packet = mongoOperate("packet_flow", "packet_flow")
    packet_event = mongoOperate("packet_flow", "event_log")
    SA_value = mongoOperate("Situation_Awareness", "SA_value")
    SA_host_value = mongoOperate("Situation_Awareness", "SA_host_value")
    SA_event = mongoOperate("Situation_Awareness", "SA_event")
    SA_predict_value = mongoOperate("Situation_Awareness", "SA_predict_value")
    SA_predict_host_value = mongoOperate("Situation_Awareness", "SA_host_predict_value")
    Assets = mongoOperate("Situation_Awareness", "Assets")
    fw_rules = mongoOperate("Situation_Awareness", "fw_rules")

    packet_packet.db_connect()
    packet_event.db_connect()
    SA_value.db_connect()
    SA_host_value.db_connect()
    SA_event.db_connect()
    SA_predict_value.db_connect()
    SA_predict_host_value.db_connect()
    Assets.db_connect()
    fw_rules.db_connect()

    sql_conn = pymysql.connect("localhost","root",conf.get("Password","Mysql"))
    sql_cursor = sql_conn.cursor()

    while True:
        #[message, channel, content]
        msg = eval(redis_sub.parse_response()[2])
        if msg['command'] == 'live_test':
            print("command: heart_beat")
            obj.public("heart_beat", conf.get("Address","Intranet"));
        elif msg['host'] == conf.get("Address", "Intranet") or msg['host'] == 'all':
            if msg['command'] == 'packet_flow':
                print("command: packet_flow")
                res_list = packet_packet.db_read(500)
                for each in res_list:
                    obj.public("packet_flow", json.dumps(each))
                obj.public("packet_flow", "finish")

            elif msg['command'] == 'packet_flow_del':
                print("command: packet_flow_del")
                try:
                    _id = msg['id']
                    packet_packet.db_del(_id)
                    obj.public("packet_flow_del", "ok")
                except Exception as e:
                    print(e)

            elif msg['command'] == 'packet_flow_time':
                print("command: packet_flow_time")
                try:
                    time_s = msg['time_s']
                    time_e = msg['time_e']
                    res_list = packet_packet.db_read_time(time_s, time_e)
                    for each in res_list:
                        obj.public("packet_flow_time", json.dumps(each))
                    obj.public("packet_flow_time", "finish")
                except Exception as e:
                    print(e)

            elif msg['command'] == 'packet_detail':
                print("command: packet_detail")
                ID = msg['id']
                result = packet_packet.db_read_id(ID)
                obj.public("packet_detail", json.dumps(result))
                obj.public("packet_detail", "finish")

            elif msg['command'] == 'host_log':
                print("command: host_log")
                try:
                    sql_cursor.execute("use ossec;")
                    sql_cursor.execute("SELECT a.id,a.location_id,a.level,a.full_log,a.timestamp,b.hostname,c.name,(SELECT GROUP_CONCAT(d.cat_name) from category d WHERE d.cat_id in (select e.cat_id from signature_category_mapping e where e.rule_id =a.rule_id))cat_name FROM alert a,server b,location c  WHERE a.id<=(select max(id) from alert) and a.id>((select max(id) from alert) -500) and a.server_id = b.id and a.location_id = c.id order by a.id desc;")
                    res_list = sql_cursor.fetchall()
                    for each in res_list:
                        obj.public("host_log", json.dumps(each))
                        #print(json.dumps(each))
                    obj.public("host_log", "finish")
                except Exception as e:
                    print(e)
                    if "MySQL server has gone away" not in str(e):
                        raise Exception("another exception")#后面的语句不予执行
                    sql_conn = Mysql_init()[0]
                    sql_cursor = Mysql_init()[1]
                    sql_cursor.execute("use ossec;")
                    sql_cursor.execute("SELECT a.id,a.location_id,a.level,a.full_log,a.timestamp,b.hostname,c.name,(SELECT GROUP_CONCAT(d.cat_name) from category d WHERE d.cat_id in (select e.cat_id from signature_category_mapping e where e.rule_id =a.rule_id))cat_name FROM alert a,server b,location c  WHERE a.id<=(select max(id) from alert) and a.id>((select max(id) from alert) -500) and a.server_id = b.id and a.location_id = c.id order by a.id desc;")
                    res_list = sql_cursor.fetchall()
                    for each in res_list:
                        obj.public("host_log", json.dumps(each))
                        #print(json.dumps(each))
                    obj.public("host_log", "finish")

            elif msg['command'] == 'host_log_detail':
                print("command: host_log_detail")
                ID = str(msg['id'])
                try:
                    sql_cursor.execute("use ossec;")
                    sql_cursor.execute("SELECT FROM alert where id= '" + ID + "';")
                    result = sql_cursor.fetchone()
                    obj.public("host_log_detail", json.dumps(result))
                    obj.public("host_log_detail", "finish")
                except Exception as e:
                    print(e)
                    if "MySQL server has gone away" not in str(e):
                        raise Exception("another exception")
                    sql_conn = Mysql_init()[0]
                    sql_cursor = Mysql_init()[1]
                    sql_cursor.execute("use ossec;")
                    sql_cursor.execute("SELECT FROM alert where id= '" + ID + "';")
                    result = sql_cursor.fetchone()
                    obj.public("host_log_detail", json.dumps(result))
                    obj.public("host_log_detail", "finish")


            elif msg['command'] == 'host_log_del':
                print("command: host_log_del")
                ID = str(msg['id'])
                try:
                    sql_cursor.execute("use ossec;")
                    res = sql_cursor.execute("DELETE FROM alert where id= '" + ID + "';")
                    if(res == 0):
                        obj.public("host_log_del", "failed")
                    else:
                        obj.public("host_log_del", "ok")
                except Exception as e:
                    print(e)
                    if "MySQL server has gone away" not in str(e):
                        raise Exception("another exception")
                    sql_conn = Mysql_init()[0]
                    sql_cursor = Mysql_init()[1]
                    sql_cursor.execute("use ossec;")
                    res = sql_cursor.execute("DELETE FROM alert where id= '" + ID + "';")
                    if(res == 0):
                        obj.public("host_log_del", "failed")
                    else:
                        obj.public("host_log_del", "ok")


            elif msg['command'] == 'host_log_time':
                print("command: host_log_time")
                time_s = str(msg['time_s'])
                time_e = str(msg['time_e'])
                try:
                    sql_cursor.execute("use ossec;")
                    sql_cursor.execute('SELECT a.id,a.location_id,a.level,a.full_log,a.timestamp,b.hostname,c.name,(SELECT GROUP_CONCAT(d.cat_name) from category d WHERE d.cat_id in (select e.cat_id from signature_category_mapping e where e.rule_id =a.rule_id))cat_name FROM alert a,server b,location c  WHERE a.id<=(select max(id) from alert) and a.id>((select max(id) from alert) -1000) and a.server_id = b.id and a.location_id = c.id and timestamp between ' + time_s + ' and ' + time_e + ' order by a.id desc ')
                    res_list = sql_cursor.fetchall()
                    for each in res_list:
                        obj.public("host_log_time", json.dumps(each))
                        print(json.dumps(each))
                    obj.public("host_log_time", "finish")
                except Exception as e:
                    print(e)
                    if "MySQL server has gone away" not in str(e):
                        raise Exception("another exception")
                    sql_conn = Mysql_init()[0]
                    sql_cursor = Mysql_init()[1]
                    sql_cursor.execute("use ossec;")
                    sql_cursor.execute('SELECT a.id,a.location_id,a.level,a.full_log,a.timestamp,b.hostname,c.name,(SELECT GROUP_CONCAT(d.cat_name) from category d WHERE d.cat_id in (select e.cat_id from signature_category_mapping e where e.rule_id =a.rule_id))cat_name FROM alert a,server b,location c  WHERE a.id<=(select max(id) from alert) and a.id>((select max(id) from alert) -1000) and a.server_id = b.id and a.location_id = c.id and timestamp between ' + time_s + ' and ' + time_e + ' order by a.id desc ')
                    res_list = sql_cursor.fetchall()
                    for each in res_list:
                        obj.public("host_log_time", json.dumps(each))
                        print(json.dumps(each))
                    obj.public("host_log_time", "finish")

            elif msg['command'] == 'event_log':
                print("command: event_log")
                res_list = packet_event.db_read(500)
                for each in res_list:
                    obj.public("event_log", json.dumps(each))
                    #print(json.dumps(each))
                obj.public("event_log", "finish")

            elif msg['command'] == 'event_del':
                print("command: event_del")
                try:
                    _id = msg['id']
                    packet_event.db_del(_id)
                    obj.public("event_del", "ok")
                except Exception as e:
                    print(e)

            elif msg['command'] == 'event_time':
                print("command: event_time")
                try:
                    time_s = msg['time_s']
                    time_e = msg['time_e']
                    res_list = packet_event.db_read_time(time_s, time_e)
                    for each in res_list:
                        obj.public("event_time", json.dumps(each))
                        #print(json.dumps(each))
                    obj.public("event_time", "finish")
                except Exception as e:
                    print(e)

            elif msg['command'] == 'event_detail':
                print("command: event_detail")
                ID = msg['id']
                result = packet_event.db_read_id(ID)
                obj.public("event_detail", json.dumps(result))
                obj.public("event_detail", "finish")

            elif msg['command'] == 'SA_value':
                print("command: SA_value")
                res_list = SA_value.db_read(1)
                for each in res_list:
                    obj.public("SA_value", json.dumps(each))
                obj.public("SA_value", "finish")

            elif msg['command'] == 'SA_host_value':
                print("command: SA_host_value")
                res_list = SA_host_value.db_read(1)
                for each in res_list:
                    obj.public("SA_host_value", json.dumps(each))
                obj.public("SA_host_value", "finish")

            elif msg['command'] == 'SA_event':
                print("command: SA_event")
                res_list = SA_event.db_read(500)
                ip_list = os.popen("iptables -S | sed -n '4,$p' | awk '{print $4}'").read()
                ip_list = ip_list.split('\n') #分割转化为数组
                ip_list = [_f for _f in list(set(ip_list)) if _f] #去重去空
                print("ip_list", ip_list)
                #去掉掩码
                for i in range(len(ip_list)):
                    ip_list[i] = ip_list[i][:-3]
                for each in res_list:
                    if each['srcIp'] in ip_list:
                        each['ip_block'] = "取消黑名单"
                    else:
                        each['ip_block'] = "加入黑名单"
                    obj.public("SA_event", json.dumps(each))
                    #print(json.dumps(each))
                obj.public("SA_event", "finish")

            elif msg['command'] == 'SA_event_time':
                print('command: SA_event_time')
                try:
                    time_s = msg['time_s']
                    time_e = msg['time_e']
                    res_list = SA_event.db_SAread_time(time_s, time_e)
                    ip_list = os.popen("iptables -S | sed -n '4,$p' | awk '{print $4}'").read()
                    ip_list = ip_list.split('\n') #分割转化为数组
                    ip_list = [_f for _f in list(set(ip_list)) if _f] #去重去空
                    for i in range(len(ip_list)):
                        ip_list[i] = ip_list[i][:-3]
                    for each in res_list:
                        if each['srcIp'] in ip_list:
                            each['ip_block'] = "取消黑名单"
                        else:
                            each['ip_block'] = "加入黑名单"
                        obj.public("SA_event_time", json.dumps(each))
                    obj.public("SA_event_time", "finish")
                except Exception as e:
                    print(e)



            elif msg['command'] == 'SA_event_del':
                print("command: SA_event_del")
                try:
                    _id = msg['id']
                    SA_event.db_del(_id)
                    obj.public("SA_event_del", "ok")
                except Exception as e:
                    print(e)

            elif msg['command'] == 'SA_ip_block':
                print('command: SA_ip_block')
                try:
                    mal_ip = msg['mal_ip']
                    os.system('iptables -I INPUT -s ' + mal_ip + ' -j DROP')
                    print('iptables -I INPUT -s '+mal_ip+' -j DROP')
                    obj.public("SA_ip_block", "ok")
                except Exception as e:
                    print(e)

            elif msg['command'] == 'SA_ip_cancel':
                print('command: SA_ip_cancel')
                try:
                    mal_ip = msg['mal_ip']
                    os.system('iptables -D INPUT -s ' + mal_ip + ' -j DROP')
                    print('iptables -D INPUT -s '+mal_ip+' -j DROP')
                    obj.public("SA_ip_cancel", "ok")
                except Exception as e:
                    print(e)

            #这里需要更改完善
            elif msg['command'] == 'event_log_time':
                mongDB = mongoOperate("packet_flow", "event_log")
                mongDB.db_connect()
                res_list = mongDB.db_read_time(time_s, time_e)


            elif msg['command'] == 'ids_log_del':
                ID = msg['params']
                mongDB = mongoOperate("packet_flow", "event_log")
                mongDB.db_connect()
                mongDB.db_delete(ID)

            elif msg['command'] == 'SA_line_value':
                print("command: SA_line_value")
                res_list = SA_value.db_read(540)
                for each in res_list:
                    obj.public("SA_line_value", json.dumps(each))
                    #print(json.dumps(each))
                obj.public("SA_line_value", "finish")

            elif msg['command'] == 'SA_host_line_value':
                print("command: SA_host_line_value")
                res_list = SA_host_value.db_read(120)
                for each in res_list:
                    obj.public("SA_host_line_value", json.dumps(each))
                    print(json.dumps(each))
                obj.public("SA_host_line_value", "finish")

            elif msg['command'] == 'SA_predict_line_value':
                print("command: SA_predict_line_value")
                res_list = SA_predict_value.db_read(540)
                for each in res_list:
                    obj.public("SA_predict_line_value", json.dumps(each))
                    #print(json.dumps(each))
                obj.public("SA_predict_line_value", "finish")

            elif msg['command'] == 'SA_host_predict_line_value':
                print("command: SA_host_predict_line_value")
                res_list = SA_predict_host_value.db_read(120)
                for each in res_list:
                    obj.public("SA_line_value", json.dumps(each))
                    #print(json.dumps(each))
                obj.public("SA_host_predict_line_value", "finish")

            elif msg['command'] == 'asset_info':
                print("command: asset_info")
                res_list = Assets.db_read(100)
                for each in res_list:
                    obj.public("asset_info", json.dumps(each))
                    #print(json.dumps(each))
                obj.public("asset_info", "finish")

            elif msg['command'] == 'asset_count':
                print("command: asset_count")
                res_list = Assets.db_read(1000)#limit 1000
                #compute the server count and dbs count
                ans = {"count_databases":0, "count_servers":0}
                for each in res_list:
                    if each['asset_type'] == "database":
                        ans['count_databases'] += 1
                    elif each['asset_type'] == "server":
                        ans['count_servers'] += 1
                obj.public("asset_count", json.dumps(ans))
                obj.public("asset_count", "finish")


            elif msg['command'] == 'security_count':
                print("command: security_count")
                res_list = Assets.db_read(1000)#limit 1000
                #compute the server count and dbs count
                ans = {"count_risked":0, "count_protected":0, "count_unprotected":0}
                for each in res_list:
                    if each['asset_security'] == "存在风险":
                        ans['count_risked'] += 1
                    elif each['asset_security'] == "保护中":
                        ans['count_protected'] += 1
                    elif each['asset_security'] == "未受保护":
                        ans['count_unprotected'] += 1
                obj.public("security_count", json.dumps(ans))
                obj.public("security_count", "finish")

            elif msg['command'] == 'update_msg':
                print("command: update_msg")
                cmd = 'python3 ' + conf.get("Path","SA_system") + "AssetInfo.py -u server"
                try:
                    os.system(cmd)
                    obj.public("update_msg", "ok")
                except Exception as e:
                    obj.public("update_msg", "err")
                    print(e)

            elif msg['command'] == 'update_dbs':
                print("command: update_dbs")
                cmd = 'python3 ' + conf.get("Path","SA_system") + "AssetInfo.py -u databases"
                try:
                    os.system(cmd)
                    obj.public("update_msg", "ok")
                except Exception as e:
                    obj.public("update_msg", "err")
                    print(e)

            elif msg['command'] == 'all_SA_event':
                print("command: all_SA_event")
                res_list = SA_event.db_read(500)
                for each in res_list:
                    obj.public("all_SA_event", json.dumps(each))
                obj.public("all_SA_event", "finish")

            elif msg['command'] == 'all_SA_line_value':
                print("command: all_SA_line_value")
                res_list = SA_value.db_read(540)
                name = conf.get("Other", "name")
                res = {name:[]}
                for each in res_list:
                    #obj.public("SA_line_value", json.dumps(each))
                    res[name].append(each)
                    print(each)
                obj.public("all_SA_line_value", json.dumps(res))
                obj.public("all_SA_line_value", "finish")

            elif msg['command'] == 'all_SA_host_line_value':
                print("command: all_SA_host_line_value")
                res_list = SA_host_value.db_read(120)
                name = conf.get("Other", "name")
                res = {name: []}
                for each in res_list:
                    #obj.public("SA_host_line_value", json.dumps(each))
                    res[name].append(json.dumps(each))
                    #print(json.dumps(each))
                obj.public("all_SA_host_line_value", json.dumps(res))
                obj.public("all_SA_host_line_value", "finish")

            elif msg['command'] == 'SA_ACL_rules':
                print("command: SA_ACL_rules")
                print(msg['fw'])
                if msg['fw']['option'] == "insert":
                    cmd = "iptables -A " + msg['fw']['direction']
                    temp = msg['fw']
                    del temp['option']
                    fw_rules.db_insert(temp)
                elif msg['fw']['option'] == "delete":
                    cmd = "iptables -D " + msg['fw']['direction']
                    temp = msg['fw']
                    del temp['option']
                    fw_rules.db_del_command(temp)  #mongodb delete the rule
                else:
                    cmd = ""
                    obj.public("SA_ACL_rules", "fail")

                if msg['fw']['proto'] != 'null':
                    cmd = cmd + " -p " + msg['fw']['proto']
                #入方向不需要sport，出方向不需要dport
                if msg['fw']['direction'] == 'OUTPUT':
                    if msg['fw']['sport'] != 'null':
                        cmd = cmd + ' --sport ' + msg['fw']['sport']
                if msg['fw']['direction'] == 'INPUT':
                    if msg['fw']['dport'] != 'null':
                        cmd = cmd + ' --dport ' + msg['fw']['dport']
                if msg['fw']['source_ip'] != 'null':
                    cmd = cmd + ' -s ' + msg['fw']['source_ip']
                if msg['fw']['des_ip'] != 'null':
                    cmd = cmd + ' -d ' + msg['fw']['des_ip']
                cmd = cmd + ' -j ' + msg['fw']['operate']
                os.system(cmd)
                print("cmd: ",cmd)
                obj.public("SA_ACL_rules", "ok")

            elif msg['command'] == 'search_ACL_rules':
                print("command: search_ACL_rules")
                res_list = fw_rules.db_read("all")
                for each in res_list:
                    obj.public("search_ACL_rules", json.dumps(each))
                obj.public("search_ACL_rules", "finish")





if __name__ == "__main__":
    run()
