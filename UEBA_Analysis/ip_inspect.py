# coding:utf-8
import pandas as pd
import numpy as np
from sklearn.neighbors import KernelDensity
from sklearn.model_selection import GridSearchCV
from scipy import stats
import requests
import time
from enum import Enum

data_path = 'ueba.xlsx'
# 记录用户历史常用IP
ip_mode = {}
# 记录用户使用ip列表
ip_list = {}
# 腾讯位置服务相关信息
tencent_url = 'https://apis.map.qq.com/ws/location/v1/ip?ip='
tencent_key = 'HKVBZ-AYC6J-JBEFL-F4MN6-ABZPJ-47FIJ'


class IP_TYPE(Enum):
    NORMAL = 0
    ATTENTION = 1
    WARNING = 2
    DANGER = 3


def load_data(path):
    data = pd.read_excel(path)
    return data


'''
统计所有用户的ip使用情况
'''


def count_ip(data):
    # 数据去重
    data.drop_duplicates('src_ip', inplace=True)
    counts = data['username'].value_counts()
    users = data['username'].value_counts().index
    return counts, users


def set_count(data, users, counts):
    data['ip_count'] = 0
    data['new_ip_count'] = 0
    data['mac_count'] = 0
    for i in range(len(users)):
        data.loc[data['username'] == users[i], ['ip_count', 'mac_count']] = counts[i]
    return data


'''
统计用户使用过的ip
'''


def list_user_ips(data):
    data.drop_duplicates('src_ip', inplace=True)
    users = data.loc[:, ['username']].values.tolist()
    ips = data.loc[:, ['src_ip']].values.tolist()
    ip_dict = {}
    for i in range(len(users)):
        if users[i][0] in ip_dict:
            ip_dict[users[i][0]].append(ips[i][0])
        else:
            ip_dict[users[i][0]] = [ips[i][0]]

    return ip_dict


'''
统计用户最常用IP，默认121.15.171.90
'''


def query_ip_mode(data, users):
    ip_mode_dict = {}
    for user in users:
        user_ips = data.loc[data['username'] == user, ['src_ip']]
        if user_ips.shape[0] == 0:
            ip_mode_dict[user] = '121.15.171.90'
        else:
            ip_mode_dict[user] = user_ips.value_counts().index[0][0]

    return ip_mode_dict


'''
统计用户ip所属地
'''


def query_ip_destination(ip_addr):
    try:
        url = tencent_url + ip_addr + '&key=' + tencent_key
        res = requests.get(url).json()
        add_info = res['result']['ad_info']
        return add_info['nation'], add_info['province'], add_info['city']
    except:
        return '', '', ''


'''
检查用户是否存在异地登录行为
'''


def inspect_suspicious_ip_behaviors(user, ip_list):
    user_mode_ip = ip_mode[user]
    if user_mode_ip == '':
        user_mode_ip = '121.15.171.90'
    mode_nation, mode_province, mode_city = query_ip_destination(user_mode_ip)
    for ip in ip_list:
        temp_nation, temp_province, temp_city = query_ip_destination(ip)
        if temp_nation != mode_nation:
            return IP_TYPE.DANGER.value, ip
        if temp_province != mode_province:
            return IP_TYPE.WARNING.value, ip
        if temp_city != mode_city:
            return IP_TYPE.ATTENTION.value, ip
    return IP_TYPE.NORMAL.value, ''


def set_suspicious_ip_behaviors(df, users):
    global ip_mode
    ip_mode = query_ip_mode(df, users)
    global ip_list
    ip_list = list_user_ips(df)
    for user in users:
        if user not in ip_list:
            print(user)
            df.loc[df['username'] == user, ['suspicious_ip_level']] = IP_TYPE.NORMAL
            df.loc[df['username'] == user, ['suspicious_ip']] = ''
            time.sleep(1)
            continue
        res, suspicious_ip = inspect_suspicious_ip_behaviors(user, ip_list[user])
        df.loc[df['username'] == user, ['suspicious_ip_level']] = res
        df.loc[df['username'] == user, ['suspicious_ip']] = suspicious_ip
        time.sleep(1)


# if __name__ == '__main__':
#     df = load_data(data_path)
#     tmp = df.copy(deep=True)
#     # ------------------统计当日用户使用ip-------------------
#     counts, users = count_ip(tmp)
    # plt.bar(users, counts, align='center')
    # plt.title('User IP usage')
    # plt.ylabel('ip count')
    # plt.xlabel('users')
    # plt.show()
    # df = set_count(df, users, counts)
    # print df
    # ------------------统计过去一月用户常使用ip-------------------
    # dict = list_user_ips(tmp)
    # # print dict
    # data = series_to_array(df['online'])
    # kde_analyze(data)

