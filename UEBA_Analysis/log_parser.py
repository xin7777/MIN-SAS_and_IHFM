import json
import time
import pandas as pd
import base64


class LogParser(object):
    def __init__(self, logs):
        self.__logs = logs
        self.__dict = {'username': [], 'date': [], 'Time': [], 'request': [], 'command': [], 'reqtype': []}
        self.__log2dict()

    def __log2dict(self):
        try:
            for log in self.__logs:
                log_dict = json.loads(log)
                # 过滤异常数据包
                if log_dict['Data'].find('unknow') != -1:
                    continue

                # 模拟数据
                if len(log_dict['Username']) == 4:
                    self.__dict['username'].append(log_dict['Username'])
                else:
                    # 完善编码结果
                    missing_padding = 4 - len(log_dict['Username']) % 4
                    if missing_padding:
                        log_dict['Username'] += '=' * missing_padding
                    self.__dict['username'].append(str(base64.b64decode(log_dict['Username']), 'utf-8'))
                date, day_time = self.__parseTimestamp(log_dict['Timestamp'])
                self.__dict['date'].append(date)
                self.__dict['Time'].append(day_time)
                request, reqtype = self.__parseCommand(log_dict['Data'])
                self.__dict['request'].append(request)
                self.__dict['command'].append(request)
                self.__dict['reqtype'].append(reqtype)
        except ValueError: 
            print('parse error')

    def __parseCommand(self, Data):
        # 2021/3/22 格式改动 [0, httpPos]为请求
        # isGet = (Data.find('GET') != -1)
        # isPost = (Data.find('POST') != -1)
        httpPos = Data.find('HTTP') - 1
        request = Data[0: httpPos]
        return request, 'GET'
        # if not isGet and not isPost and httpPos == -2:
        #     raise ValueError("invalid value")
        # if isGet:
        #     request = Data[(isGet + 3): httpPos]
        #     return request, 'GET'
        # elif isPost:
        #     request = Data[(isPost + 4): httpPos]
        #     return request, 'POST'

    def __parseTimestamp(self, timestamp):
        time_struct = time.gmtime(float(timestamp))
        year, mon, day, hour, mins, sec, wday, yday, isdst = time_struct
        date = "%d/%d/%d" % (year, mon, day)
        hour_str = str(hour+8)
        min_str = str(mins)
        sec_str = str(sec)
        if (hour+8) < 10:
            hour_str = '0' + str(hour+8)
        elif (hour+8) == 24:
            hour_str = '00'
        if mins < 10:
            min_str = '0' + str(mins)
        if sec < 10:
            sec_str = '0' + str(sec)
        day_time = "%s:%s:%s" % (hour_str, min_str, sec_str)
        return date, day_time

    def dict2dataframe(self):
        try:
            return pd.DataFrame.from_dict(self.__dict)
        except:
            return pd.DataFrame.from_dict({})
