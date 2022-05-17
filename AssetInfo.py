#!/usr/bin/python3
# -*- encoding: utf-8 -*- 
"""
Modified by linlh

reference from: https://github.com/Alwayswithme/sysinfo
Author     :  Ye Jinchang
"""

import os
import re
import sys
import json
import socket
import pymysql
import argparse
import netifaces  # need to install this package
import subprocess # use to create a new process
import urllib.request
import pymongo
import configparser

from ipaddr    import IPAddress
from pymongo   import MongoClient
from itertools import chain
#from aes_test import prpcrypt

#from net_message import captureModule

conf = configparser.ConfigParser()
conf.read('config/config.cfg')

def check_permission():
    """
    Getting system info need to read some system file
    So need to have superior authority
    """
    euid = os.geteuid()
    if euid != 0:
        print('Script not started as root. Running sudo..')
        args = ['sudo', sys.executable] + sys.argv + [os.environ]
        # the next line replaces the currently-running process with the sudo
        os.execlpe('sudo', *args)

def sh(cmd, in_shell=False, get_str=True):
    """
    Using subprocess module to run sh command
    """
    output = subprocess.check_output(cmd, shell=in_shell)
    if get_str:
        return str(output, 'utf-8')
    return output

class Hwinfo:
    """
    Get server's hardware information
    Using command: dmidecode
    """
    def __init__(self):
        """
        execute shell command and get information about hardware
        """
        #mem = Memory()
        disk = Disk()
        net = Net()
        loc = conf.get("Address","city_name")
        self.db = DBS()
        infos = [
            self.product(), self.distro(), self.kernel(),
            self.processor(), self.baseboard(), self.serverName(), Rom(),
            disk,  net, loc,   self.db
            ]
        self.info_list = infos
        # self.total_memory = mem.getTotalMemory()
        self.total_memory = sh("bash ./getTotalMemory.sh", True).strip()
        self.disk_info = disk.getDiskInfo()
        self.net_info = net.getNetInfo()
        #self.loc_info = loc.getLocInfo()
        self.loc_info = conf.get("Address","city_name")
        
    def get_server_info(self):
        return {
            "asset_id":"SERVER-01",
            "asset_type":"server",
            "asset_tag":"",
            "asset_name":self.name,
            "asset_location": self.loc_info,
            "asset_security":self.getSecurity(),
            "asset_server":{
                "system":self.system_version,
                "cpu":self.processor_name,
                "kernel":self.kernel_version,
                "memory":str(self.total_memory),
                "disk":self.disk_info,
                "net":self.net_info
            }
        }
    
    def get_mongo_info(self):
        # this have a problem that one server can have multi dbs
        mongo_dict = self.db.getMongo()

        user_dict = []
        for user_name in mongo_dict['users'].keys():
            user_dict.append({'name':user_name, "dbs":mongo_dict['users'][user_name]['db']})

        db_dict = []
        for db_name in mongo_dict['dbs'].keys():
            db_dict.append({"name":db_name, "size":str(round(mongo_dict['dbs'][db_name], 2))+"MB"})

        return {
            "asset_id":"DATABASE-S01-01",
            "asset_type":"database",
            "asset_tag":"",
            "asset_name":"Mongo",
            "asset_location": self.loc_info,
            "asset_security": self.getSecurity(),
            "asset_databases":{
                "databases": db_dict,
                "users": user_dict,
                "port":"27017"
            }
        }
    
    def get_mysql_info(self):

        mysql_info = self.db.getMysql()

        return {
            "asset_id":"DATABASE-S01-02",
            "asset_type":"database",
            "asset_tag":"",
            "asset_name":"MySQL",
            "asset_location": self.loc_info,
            "asset_security": self.getSecurity(),
            "asset_databases":{
                "databases": mysql_info['dbs'],
                "users": mysql_info['users'],
                "port":"3306"
            }
        }

    def getSecurity(self):
        out = sh("ps -ajx | grep spark_analyze | wc -l", True)
        under_protect = "未受保护"
        # 存在风险
        if int(out) >= 3:
            under_protect = "保护中"
        return under_protect

    def getInfo(self):
        """
        Return all system information
        """
        return self.info_list

    def __str__(self):
        return ''.join([i.msg() for i in self.info_list])

    def product(self):
        """
        detect information about product
        """
        # cmd = 'dmidecode -s system-product-name | head -1' # dmidecode need root privilege
        cmd = 'cat /sys/class/dmi/id/product_name'
        output = sh(cmd, True)
        self.product_name = output.strip()
        return Info('Product', output.strip())

    def distro(self):
        """
        detect information about distribution
        """
        cmd = 'lsb_release -sirc'
        output = sh(cmd, True)
        self.system_version = output.strip().replace('\n', ' ')
        return Info('Distro', output.strip().replace('\n', ' '))

    def kernel(self):
        """
        detect information about kernel
        """
        cmd = ['uname', '-or']
        output = sh(cmd)
        self.kernel_version = output.strip()
        return Info('Kernel', output.strip())

    def serverName(self):
        """
        get server's name
        """
        cmd = ['uname', '-n']
        output = sh(cmd) 
        self.name = output.strip()
        return Info("Name", output.strip())

    def processor(self):
        """
        detect information about CPU
        """
        # cmd = 'dmidecode -s processor-version | head -1' # dmidecode need root privilege
        cmd = "cat /proc/cpuinfo  | grep 'model name'| uniq"
        output = sh(cmd, True)
        self.processor_name = output.split('\t') [1][2:].strip()
        return Info('Processor', output.split('\t') [1][2:].strip())

    def baseboard(self):
        """
        detect information about baseboard
        """
        vendor = sh('cat /sys/devices/virtual/dmi/id/board_vendor', True)
        name = sh('cat /sys/devices/virtual/dmi/id/board_name', True)
        chipset = sh('lspci | grep ISA | sed -e "s/.*: //" -e "s/LPC.*//" -e "s/Controller.*//"', True)
        desc = vendor + name + chipset
        return Info('BaseBoard', desc.replace('\n', ' ', 2).strip())
  

class Info:
    """
    represent any hardware information
    """
    WIDTH = 10 # title's length
    INDENT = '│──'

    def __init__(self, name, desc):
        self.name = name # attribute, such as rom, kernel, memory etc
        self.desc = desc # attribute's value
        self.subInfo = []

    def msg(self):
        """
        generate the message to print
        """
        if self.desc == 'noop':
            return ''
        msg = []
        margin = ' ' * (Info.WIDTH - len(self.name)) 
        main_msg = '{0}{1}: {2}\n'.format(self.name, margin, self.desc)
        msg.append(main_msg)
        sub_msg = [ self.indent_subInfo(i) for i in self.subInfo if i]
        if sub_msg:
            sub_msg[-1] = sub_msg[-1].replace('│', '└')
        return ''.join(msg + sub_msg)

    def addSubInfo(self, subInfo):
        self.subInfo.append(subInfo)

    def indent_subInfo(self, line):
        return Info.INDENT + line

    def __str__(self):
        return  '"name": {0}, "description": {1}'.format(self.name, self.desc)

class Rom(Info):
    """

    """
    def __init__(self):
        self.rom_list = self.roms()
        Info.__init__(self, 'Rom', self.getDesc() if self.rom_list else 'noop')

    def getDesc(self):
        roms = [self.transform(i) for i in self.rom_list]
        roms_msg = ['{0} {1}'.format(i['VENDOR'], i['MODEL']) for i in roms]
        return ' '.join(roms_msg)

    def transform(self, line):
        rom = {}
        for line in re.split(r'(?<=") ', line):
            if '=' in line:
                key, value = line.split('=')
                if key in 'VENDOR' or key in 'MODEL':
                    rom[key] = value.replace('"', '').strip()
        return rom

    def roms(self):
        cmd = """lsblk -dP -o VENDOR,TYPE,MODEL | grep 'TYPE="rom"'"""
        try:
            output = sh(cmd, True)
            rom_list = [x for x in output.split('\n') if x]
            return rom_list
        except Exception:
            # no rom
            return []

class OnboardDevice(Info):
    """
    Check server's onboard deveices
    """
    def __init__(self):
        self.ob_devices = self.onboardDevices()
        Info.__init__(self, 'Onboard', '' if self.ob_devices else 'noop')
        info = [self.obToStr(i) for i in self.ob_devices]
        for i in info:
            self.addSubInfo(i)

    def onboardDevices(self):
        cmd = ['dmidecode', '-t', '41']
        parsing = False
        ob_list = []
        splitter = ': '
        attrs = ['Reference Designation', 'Type']
        with subprocess.Popen(cmd, stdout=subprocess.PIPE,
                              bufsize = 1, universal_newlines = True) as p:
            for i in p.stdout:
                line = i.strip()
                if not parsing and line == 'Onboard Device':
                    parsing = True
                    ob = {}
                if parsing and splitter in line:
                    (key, value) = line.split(splitter, 1)
                    if key in attrs:
                        ob[key] = value
                elif parsing and not line:
                    parsing = False
                    ob_list.append(ob)
        return ob_list

    def obToStr(self, ob):
        tvalue = ob.get("Type")
        desvalue = ob.get("Reference Designation")
        ret = '{0}: {1}\n'.format(tvalue, desvalue)
        return ret

class Disk(Info):
    def __init__(self):
        self.disks = self.diskList()
        Info.__init__(self, 'Disks', '{0} {1} GB Total'.format(' '.join(self.disks), self.countSize()))
        self.details = self.disksDetail(self.disks)
        detail_strs = [ self.extractDiskDetail(i) for i in self.details]
        for i in detail_strs:
            self.addSubInfo(i)

    def countSize(self):
        sum = 0
        for i in self.disks:
            #cmd = 'blockdev --getsize64 ' + i
            cmd = 'cat /sys/block/{0}/size'.format(i[-3:])
            output = sh(cmd, True)
            sum += int(output) // (2 * 1024 * 1024)
        return sum

    def diskList(self):
        """
        find out how many disk in this machine
        """
        sds = sh('ls -1d /dev/sd[a-z]', in_shell=True)
        sd_list = [x for x in sds.split('\n') if x]
        return sd_list

    def disksDetail(self, sd_list):
        cmd = ['smartctl', '-i']
        parsing = False
        splitter = ':'
        disk_list = []
        try:
            for i in sd_list:
                new_cmd = cmd[:]
                new_cmd.append(i)
                with subprocess.Popen(new_cmd, stdout=subprocess.PIPE,
                                      bufsize = 1, universal_newlines=True) as p:
                    for j in p.stdout:
                        line = j.strip()
                        if not parsing and 'START OF INFORMATION' in line:
                            parsing = True
                            disk = {}
                        if parsing and splitter in line:
                            key, value = line.split(splitter, 1)
                            value = value.strip()
                            if key in 'Model Family':
                                disk['model'] = value
                            elif key in 'Device Model':
                                disk['device'] = value
                            elif key in 'User Capacity':
                                p = re.compile('\[.*\]')
                                m = p.search(value)
                                disk['capacity'] = m.group()
                        elif parsing and not line:
                            parsing = False
                            disk['node'] = i
                            disk_list.append(disk)
        except Exception:
            pass
        return disk_list

    def extractDiskDetail(self, disk):
        disk_node = disk.get("node", "Unknow disk node")
        disk_device = disk.get("device", "Unknow disk device")
        disk_capacity = disk.get("capacity", "Unknow disk capacity")
        line = '{node}: {device} {capacity}\n'.format(
            node=disk_node, device=disk_device, capacity=disk_capacity)
        return line

    def getDiskInfo(self):
        # this will only get mount on '/' root directory's device infomation
        # if a device is only insert on server, not mounted in server, cannot be detected
        disk_stat = sh("df -h | grep '^/dev'",True).strip().split()
        return {"total": disk_stat[1], "used":disk_stat[2]}

class Memory(Info):
    def __init__(self):
        self.mem = self.memory()
        Info.__init__(self, 'Memory', self.getDesc(self.mem))
        detail_strs = [ self.extractMemDetail(i) for i in self.mem]
        for i in detail_strs:
            self.addSubInfo(i)

    def memory(self):
        cmd = ['dmidecode', '-t', 'memory']
        parsing = False
        splitter = ': '
        attrs = ['Size', 'Type', 'Speed', 'Manufacturer', 'Locator']
        mem_list = []
        with subprocess.Popen(cmd, stdout=subprocess.PIPE,
                              bufsize = 1, universal_newlines = True) as p:
            for i in p.stdout:
                line = i.strip()
                if not parsing and line == 'Memory Device':
                    parsing = True
                    mem = {}
                if parsing and splitter in line:
                    (key, value) = line.split(splitter, 1)
                    if key in attrs:
                        mem[key] = value

                # read a empty, end the parsing
                elif parsing and not line:
                    parsing = False
                    mem_list.append(mem)
        return mem_list

    def extractMemDetail(self, mem):
        mem_type = mem.get("Type", "Unknown Type")
        mem_size = mem.get("Size", "Unknown Size")
        # maybe no memory in this slot
        if 'Unknown' in mem_type and 'No Module Installed' in mem_size:
            return ''
        mem_locator = mem.get("Locator", "Unknown Locator")
        mem_manufa = mem.get("Manufacturer", "Unknown Manufacturer")

        mem_speed = mem.get("Speed", "Unknown Speed")
        line = '{slot}: {manufa} {type} {speed}\n'.format(
            slot=mem_locator, manufa=mem_manufa,
            type=mem_type, speed=mem_type)
        return line

    def freeMemory(self):
        with open('/proc/meminfo') as file:
            for line in file:
                if 'MemFree' in line:
                    free_memKB = line.split()[1]
                    return (float(free_memKB)/(1024*1024))    # returns GBytes float

    def getDesc(self, mem_list):
        mem_size = [self.convertMemSize(i['Size']) for i in mem_list]
        self.total = sum(mem_size) / 1024
        msg = 'Total {:.0f}GB '.format(self.total) 
        free = self.freeMemory()
        msg += ' Free Memory {:.0f}GB '.format(free)
        msg += ' Usage {:.0f}%'.format((self.total - free)/self.total*100)
        return msg

    def getTotalMemory(self):
        return self.total

    def convertMemSize(self, size_str):
        (size, unit) = size_str.split(' ', 1)
        try:
            return int(size)
        except ValueError:
            return 0

class Net(Info):
    """
    Get server's netowrk information
    Include public IP address, private IP address, Mac address
    """
    def __init__(self):
        # self.cap = captureModule()
        # self.netinfo = self.cap.getSysMessage() 
        Info.__init__(self, 'Memory', self.getDesc())
        net_json = self.getNetDetail()
        display_format = '%-26s : %-20s\n'
        self.addSubInfo(display_format % ("Routing Gateway:", net_json['routingGateway']))
        self.addSubInfo(display_format % ("Routing NIC Name:", net_json['routingNicName']))
        self.addSubInfo(display_format % ("Routing NIC MAC Address:", net_json['routingNicMacAddr']))
        self.addSubInfo(display_format % ("Routing IP Address:", net_json['routingIPAddr']))
        self.addSubInfo(display_format % ("Routing IP Netmask:", net_json['routingIPNetmask']))
    
    def getNetDetail(self):

        routingGateway = netifaces.gateways()['default'][netifaces.AF_INET][0]
        routingNicName = netifaces.gateways()['default'][netifaces.AF_INET][1]
 
        for interface in netifaces.interfaces():
            if interface == routingNicName:
                # print netifaces.ifaddresses(interface)
                routingNicMacAddr = netifaces.ifaddresses(interface)[netifaces.AF_LINK][0]['addr']
                try:
                    routingIPAddr = netifaces.ifaddresses(interface)[netifaces.AF_INET][0]['addr']
                    # windows don' support netifaces
                    routingIPNetmask = netifaces.ifaddresses(interface)[netifaces.AF_INET][0]['netmask']
                except KeyError:
                    pass
        
        # display_format = '%-30s %-20s'
        # print(display_format % ("Routing Gateway:", routingGateway))
        # print(display_format % ("Routing NIC Name:", routingNicName))
        # print(display_format % ("Routing NIC MAC Address:", routingNicMacAddr))
        # print(display_format % ("Routing IP Address:", routingIPAddr))
        # print(display_format % ("Routing IP Netmask:", routingIPNetmask))
        self.public_ip = routingIPAddr
        self.mac = routingNicMacAddr
        return {"routingGateway":routingGateway, "routingNicName":routingNicName, "routingNicMacAddr":routingNicMacAddr, "routingIPAddr":routingIPAddr, "routingIPNetmask":routingIPNetmask}
        # return self.netinfo

    def getNetInfo(self):
        all_addr = chain.from_iterable(netifaces.ifaddresses(i).get(socket.AF_INET, []) for i in netifaces.interfaces())
        private_ip = ""
        for i in all_addr:
            if IPAddress(i['addr']).is_private:
                if len(private_ip) == 0:
                    private_ip = i['addr']
                else:
                    private_ip += (', ' + i['addr'])
        return {"public_ip":self.public_ip, "private_ip":private_ip, "mac":self.mac}

    def getDesc(self):
        return "server's netwrok information"

class Location(Info):
    """
    Get server's location
    """
    def __init__(self):
        self.loc_url = "https://geolocation-db.com/json"
        self.loc_req = urllib.request.urlopen(self.loc_url)
        self.loc_data = json.loads(self.loc_req.read())
        #print(self.loc_data)
        Info.__init__(self, 'Location', self.getDesc())
        self.loc_json = self.getLoc()
        display_format = '%-25s : %-20s\n'
        self.addSubInfo("Location: " + self.loc_json['country_name'] + ", " + self.loc_json['city'] +'\n')

    def getLoc(self):
        return self.loc_data

    def getLocInfo(self):
        return self.loc_json['country_name'] + ", " + self.loc_json['city']
    
    def getDesc(self):
        return "server location info"


class DBS(Info):
    def __init__(self):
        Info.__init__(self, 'Databases', self.getDesc())
        mongo_info = self.getMongo()
        dbs_format = 'MongoDB DBS   : %-20s Size: %-d MB\n'
        for key in mongo_info['dbs'].keys():
            self.addSubInfo(dbs_format % (key, int(mongo_info['dbs'][key])))
        users_format = 'MongoDB Users : Name: %-5s DB: %-5s Role: %-5s\n'
        for user_name in mongo_info['users'].keys():
            self.addSubInfo(users_format % (user_name, mongo_info['users'][user_name]['db'], mongo_info['users'][user_name]['roles']))
        port_format = 'MongoDB Port  : %d\n'
        self.addSubInfo(port_format % (27017))

    def getDesc(self):
        return "databases information"

    def getMysql(self):
        self.mysql_conn = pymysql.connect(host='localhost', user='root', password=conf.get("Password","Mysql"), database='information_schema')
        cursor = self.mysql_conn.cursor()
        # find all databases
        cursor.execute("SHOW DATABASES;")
        dbs = cursor.fetchall()
        dbs_list = [ i[0] for i in dbs ]
        # get each database's size
        get_dbsize_sql = "select concat(round(sum(data_length/1024/1024),2),'MB') as data from tables where table_schema='{0}';"
        dbs_dict = []
        #dbs_json = {}
        for db in dbs_list:
            cursor.execute(get_dbsize_sql.format(db))
            dbs_size = cursor.fetchall()
            #dbs_json[db] = dbs_size[0][0]
            dbs_dict.append({"name":db, "size": dbs_size[0][0]})
    
        # get all database users infomation
        get_users_sql = 'select User from mysql.user;'
        exclude_list = ["mysql.sys", "mysql.session", "debian-sys-maint"]
        cursor.execute(get_users_sql)
        users_data = cursor.fetchall()
        user_list = []
        add_user_list = {}
        for db_name in users_data:
            if db_name[0] not in exclude_list:
                user_list.append(db_name[0])
                add_user_list[db_name[0]] = 0

        # get user's authority
        get_user_auth_sql = "show grants for {0}@localhost;"
        auth_list = []
        for user in user_list:
            try:
                cursor.execute(get_user_auth_sql.format(user))
                user_auth_data = cursor.fetchall()
            
                # user auth example: 
                for user_auth in user_auth_data:
                    if (user_auth[0].find("GRANT ALL PRIVILEGES")):
                        if add_user_list[user] == 0:
                            auth_list.append({"name":user,"dbs":"ALL"})
                            add_user_list[user] = 1
                    if (user_auth[0].find('`') != -1):
                        start = user_auth[0].find('`')
                        end = user_auth[0].find('`', start + 1)
                        if add_user_list[user] == 0:
                            auth_list.append({"name":user, "dbs":user_auth[start+1:end]})
                            add_user_list[user] = 1     
            except Exception as e:
                print(e)
        
        return {"dbs":dbs_dict, "users": auth_list}


    def getMongo(self):
        #self.mongo_conn = MongoClient() 
        self.mongo_conn = pymongo.MongoClient("mongodb://localhost:27017/")
        auth = self.mongo_conn.admin.authenticate(conf.get("Password", "Mongo"), conf.get("Password","Mongo"))
        if auth == False: 
            print("MongoDB auth error")
            return -1
        # get dbs size info
        mongo_dbs = self.mongo_conn.list_database_names()
        dbs_json = {}
        for db_name in mongo_dbs:
            stats_json = self.mongo_conn[db_name].command('dbstats')
            dbs_json[db_name] = stats_json['storageSize']/(1024*1024)
        # get dbs users info
        mongo_users = self.mongo_conn.admin.command('usersInfo')
        users_json = {}
        for user in mongo_users['users']:
            users_json[user['user']] = {'db': user['db'], 'roles': user['roles'][0]['role']}
        mongo_info = {'dbs': dbs_json, 'users': users_json}
        self.mongo_conn.close()
        return mongo_info


class AssetInfo:
    def __init__(self):
        #self.mongo_conn = MongoClient()
        self.mongo_conn = pymongo.MongoClient("mongodb://localhost:27017/")
        auth = self.mongo_conn.admin.authenticate(conf.get("Password","Mongo"), conf.get("Password","Mongo"))
        if auth == False:
            print("MongoDB auth error")
        self.asset_db = self.mongo_conn["Situation_Awareness"]
        self.asset_collection = self.asset_db['Assets']

    def server_info_update(self):
        server_info = Hwinfo().get_server_info()
        #server_info_json = json.dumps(server_info)
        # check this info have write already, if yes, then only update
        print(server_info)
        if self.asset_collection.find({'asset_id':server_info['asset_id']}).count() == 0:
            self.asset_collection.insert_one(server_info)
        else:
            self.asset_collection.update_one({'asset_id':server_info['asset_id']}, {'$set': server_info})

    def database_info_update(self):
        mongo_info = Hwinfo().get_mongo_info()
        mysql_info = Hwinfo().get_mysql_info()
        print(mongo_info)
        print(mysql_info)

        if self.asset_collection.find({'asset_id':mongo_info['asset_id']}).count() == 0:
            self.asset_collection.insert_one(mongo_info)
        else:
            self.asset_collection.update_one({'asset_id':mongo_info['asset_id']}, {'$set': mongo_info})

        if self.asset_collection.find({'asset_id':mysql_info['asset_id']}).count() == 0:
            self.asset_collection.insert_one(mysql_info)
        else:
            self.asset_collection.update_one({'asset_id':mysql_info['asset_id']}, {'$set': mysql_info})

if __name__ == "__main__":
    #check_permission()
    #print(Hwinfo().get_server_info())
    asset = AssetInfo()
    # 初始化aes密钥和iv
    #aes = prpcrypt('keyskeyskeyskeys', '1234567812345678')
    #asset.server_info_update()

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--update', choices=['server', 'databases'], dest='action', help='update asset information')
    args = parser.parse_args()
    if args.action == 'server':
        try:
            asset.server_info_update()
        except Exception as e:
            print("server info error\n")
            print(e)
    elif args.action == 'databases': 
        try:
            asset.database_info_update()
        except Exception as e:
            print("database info error\n")
            print(e)

