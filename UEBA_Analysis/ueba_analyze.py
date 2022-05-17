# coding:utf-8
# import ip_inspect
import logging
import time
# import ensemble_analysis
import kde_time
import resource_inspect
import url_inspect
# import flow_inspect
import event_handler
import threading
import math

import numpy as np
import pandas as pd
from sklearn.preprocessing import normalize

import pymongo
import json
import os

data_path = '/home/gdcni18/xin777/MIN-security-system/SA_system3/UEBA_Analysis/ueba.csv'

'''
 建立mongodb连接
'''


def connect():
    client = pymongo.MongoClient('localhost', 27017)
    return client


'''
 将预测数据写入 mongodb
'''


def store_data(data, client, name):
    db = client['packet_flow']
    db.authenticate("pkusz", "pkusz")
    try:
        if db[name].insert(data):
            print('success')
    except Exception as e:
        print('failure')
        logging.exception(e)


'''
加载基线数据
'''


def load_data(path):
    reader = pd.read_csv(path, sep=',', iterator=True)
    loop = True
    chunkSize = 2000
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    data = pd.concat(chunks)
    # data = data.drop('Unnamed: 11', axis=1)

    kde_time.time_process(data)
    # 数据去重
    data.drop_duplicates('online', inplace=True)
    return data


'''
获得基线用户列表
'''


def get_users(data):
    data.drop_duplicates('username', inplace=True)
    return np.array(data['username'])


def get_dates(data):
    data.drop_duplicates('date', inplace=True)
    return np.array(data['date'])


def mid_to_max(data, best):
    M = np.max(data - best)
    data = 1 - (data - best) * 1.0 / (M + 0.01)
    return data


def interval_to_max(data, low, high):
    M = max(low - np.min(data), np.max(data) - high)
    values = []
    for i in data:
        if i < low:
            values.append(1 - (low - i) * 1.0 / M)
        elif i > high:
            values.append(1 - (i - high) * 1.0 / M)
        else:
            values.append(1)
    return np.array(values)


def store_logs(data, name):
    db = client['packet_flow']
    db.authenticate("pkusz", "pkusz")
    try:
        if db[name].insert_many(data):
            print('success')
    except Exception as e:
        print('failure')
        logging.exception(e)


def topsis(mt):
    max_vec = np.max(mt, axis=0).reshape(1, -1)
    min_vec = np.min(mt, axis=0).reshape(1, -1)
    max_vec = np.tile(max_vec, (mt.shape[0], 1))
    min_vec = np.tile(min_vec, (mt.shape[0], 1))
    max_mt = mt - max_vec
    min_mt = mt - min_vec
    max_mt = np.power(max_mt, 2)
    min_mt = np.power(min_mt, 2)
    max_sum = np.sum(max_mt, axis=1)
    min_sum = np.sum(min_mt, axis=1)
    max_sum = np.sqrt(max_sum)
    min_sum = np.sqrt(min_sum)
    res = min_sum / (max_sum + min_sum)
    return res


'''
根据用户行为向量生成一系列UEBA安全事件
'''


def create_ueba_events(data):
    # users = data['username'].values.tolist()
    # times = data['Time'].values.tolist()
    events = []
    # 目前先只保留url检测 已恢复
    columns = ['time_count', 'post_times', 'login_times', 'download_times', 'url_count', 'resource_times']
    # columns = ['url_count']
    for index, row in data.iterrows():
        for column in columns:
            if row[column] != 0:
                event = event_handler.Event()
                event.setEventBasic(row['username'], row['Time'])
                event.setEventType(column, "A(An) {column} event".format(column=column))
                if column == 'time_count' or column == 'url_count':
                    event.setEventValue(event_handler.EventValue(1, row[column], 'times'))
                elif column == 'resource_times':
                    event.setEventValue(event_handler.EventValue(row['resource_use'], row[column], 'times'))
                else:
                    event.setEventValue(event_handler.EventValue(row[column.split('_')[0] + '_check'], row[column], 'times'))
                events.append(event)

    dict_arr = []
    for event in events:
        dict_arr.append(event.to_dict())
    # for column in columns:
    #     if column == 'username' or column == 'Time':
    #         continue
    #     handler = event_handler.EventHandler(data)
    #     handler.create_events(column, "A(An) {column} event".format(column=column))
    #     # TODO
    #     column_data = data[column].values.tolist()
    #     handler.create_events2(create_event_values
    #                            (column_data))
    #     events_dict_arr = handler.events_to_dict_arr()
    if len(dict_arr) > 0:
        store_logs(dict_arr, 'ueba_event')
    print('finish creating events')


def create_event_values(column_data):
    event_value_list = []
    for data in column_data:
        if data > 0:
            event_value_list.append(event_handler.EventValue(1, data, 'times'))
        else:
            event_value_list.append(event_handler.EventValue(0, data, 'times'))
    return event_value_list


def logs_to_dict_arr(logs):
    logs_dict_arr = []
    for log in logs:
        logs_dict_arr.append(json.loads(log))
    return logs_dict_arr


def run(df):
    # week_dict = flow_inspect.count_weekly_flow(data, dates, users)
    # print week_dict
    # 存储日志
    logs = df.to_dict(orient='records')
    t1 = threading.Thread(target=store_logs, args=(logs, 'ueba_logs',))
    t1.start()
    tmp = df.copy(deep=True)
    users = get_users(tmp)
    # ---------------------
    # 操作时间检测 暂时取消
    start_time = time.time()
    kde_time.time_process(df)
    df.drop_duplicates('online', inplace=True)
    kde_time.set_time_count(df, kde_time.count_abnormal_operations(df, users))
    end_time = time.time()
    print('Time inspect finished. %.3s s' % (end_time - start_time))
    # ---------------------
    # 访问行为检测
    start_time = time.time()
    url_inspect.set_url_count(df, url_inspect.count_abnormal_operations(df, users))
    end_time = time.time()
    print('URL inspect finished. %.3s s' % (end_time - start_time))
    # --------------------
    # 资源使用检测
    start_time = time.time()
    resource_inspect.set_resource_count(df, users)
    end_time = time.time()
    print('Resource inspect finished. %.3s s' % (end_time - start_time))
    # --------------------
    # 下载文件检测
    start_time = time.time()
    resource_inspect.set_download_count(df, users)
    end_time = time.time()
    print('Download inspect finished. %.3s s' % (end_time - start_time))
    # --------------------
    # 账号登录检测
    start_time = time.time()
    resource_inspect.set_login_count(df, users)
    end_time = time.time()
    print('Login inspect finished. %.3s s' % (end_time - start_time))
    # --------------------
    # 敏感操作检测
    start_time = time.time()
    resource_inspect.set_post_count(df, users)
    end_time = time.time()
    print('Post inspect finished. %.3s s' % (end_time - start_time))

    # --------------------
    # IP与MAC地址检测 保留功能
    # start_time = time.time()
    # tmp = data.copy(deep=True)
    # counts, users2 = ip_inspect.count_ip(tmp)
    # ip_inspect.set_count(data, users2, counts)
    # ip_inspect.set_suspicious_ip_behaviors(data, users)
    # end_time = time.time()
    # print('IP inspect finished. %.3s s' % (end_time - start_time))
    # --------------------
    # 流量使用检测 保留功能
    # start_time = time.time()
    # flow_inspect.set_flow_count(data, dates, users)
    # data.drop_duplicates('username', inplace=True)
    # end_time = time.time()
    # print('Flow inspect finished. %.3s s' % (end_time - start_time))

    #
    df.drop_duplicates('username', inplace=True)
    # 暂时取消评估机制
    # res = df.loc[:,
    #       ['username', 'time_count', 'resource_use', 'resource_times',
    #        'date', 'post_check', 'post_times', 'login_check', 'login_times',
    #        'download_check', 'download_times', 'url_count']]

    # 安全事件生成
    event_data = df.loc[:,
                 ['username', 'time_count', 'resource_use', 'post_check', 'login_check',
                  'download_check', 'url_count', 'Time', 'resource_times', 'post_times', 'login_times', 'download_times']]
    # event_data = df.loc[:,
    #              ['username',
    #               'url_count', 'Time', 'date']]
    print(event_data)
    t = threading.Thread(target=create_ueba_events, args=(event_data,))
    t.start()

    # 行为得分评估
    # df['resource_use'] = mid_to_max(np.array(df.loc[:, ['resource_use']]).reshape(1, -1)[0], 0)
    # df['post_check'] = mid_to_max(np.array(df.loc[:, ['post_check']]).reshape(1, -1)[0], 0)
    # df['login_check'] = mid_to_max(np.array(df.loc[:, ['login_check']]).reshape(1, -1)[0], 0)
    # df['time_count'] = mid_to_max(np.array(df.loc[:, ['time_count']]).reshape(1, -1)[0], 0)
    # df['url_count'] = mid_to_max(np.array(df.loc[:, ['url_count']]).reshape(1, -1)[0], 0)
    # df['download_check'] = mid_to_max(np.array(df.loc[:, ['download_check']]).reshape(1, -1)[0], 0)
    # # df['flow_use'] = mid_to_max(np.array(df.loc[:, ['flow_use']]).reshape(1, -1)[0], 0)
    # # df['suspicious_ip_level'] = mid_to_max(np.array(df.loc[:, ['suspicious_ip_level']]).reshape(1, -1)[0], 0)
    # train_data = np.array(df.loc[:, ['resource_use', 'post_check', 'login_check', 'url_count', 'time_count',
    #                                  'download_check']])
    # train_data = normalize(train_data, axis=0, norm='max')
    # result = np.sqrt(topsis(train_data) * 100) * 10
    # for (index, score) in enumerate(result):
    #     if math.isnan(score):
    #         # 单用户情况 不评分
    #         result[index] = np.random.rand(1) * 40 + 60
    #
    # res['score'] = result
    #
    # print(res)
    # store_data(res.to_dict(orient='records'), client, 'ueba_data')
    t.join()
    t1.join()
    print('finish creating events and inspecting behaviors')


base_data = load_data(data_path)
tmp = base_data.copy(deep=True)
dates = get_dates(tmp)
tmp = base_data.copy(deep=True)
base_users = get_users(tmp)
resource_inspect.base_data = base_data
resource_inspect.dates = dates
resource_inspect.base_users = base_users
client = connect()

# if __name__ == '__main__':
#     client = connect()
#     dates = ['2020/8/7', '2020/8/8', '2020/8/9', '2020/8/10', '2020/8/11', '2020/8/12', '2020/8/13']
#     for date in dates:
#         df = train(date)
#         ueba_data = json.loads(df.to_json(orient='records'))
#
#         for data in ueba_data:
#             data['date'] = date
