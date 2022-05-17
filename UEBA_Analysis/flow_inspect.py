import pandas as pd
import numpy as np
from scipy.stats import ttest_1samp

import main as ma

data_path = './ueba.xlsx'

user_history_flow_dict = {}


def load_json(path):
    df = pd.read_excel(path)
    return df


def process_count(df):
    df['Time'] = pd.to_datetime(df['Time'])
    df['online'] = df['Time'].dt.hour + df['Time'].dt.minute / 100
    return df


'''
统计用户一日每小时平均使用流量
'''


def count_hourly_flow(df):
    day_count = {}
    day_arr = []
    for i in range(24):
        hour_data = np.array(df.loc[(df['online'] > i) & (df['online'] < i + 1), ['bytes']])
        if hour_data.shape[0] != 0:
            day_count[i] = np.sum(hour_data) * 1.0 / 1024
            day_arr.append(day_count[i])
        else:
            day_count[i] = 0
            day_arr.append(0)

    return day_count, day_arr


def count_weekly_flow(df, dates):
    week_arr = []
    week_dict = {}
    for date in dates:
        day_data = df.loc[df['date'] == date, ['bytes', 'online']]
        day_count, day_arr = count_hourly_flow(day_data)
        week_arr.append(day_arr)
    week_arr = np.array(week_arr) / users.shape[0]
    week_arr = np.sum(week_arr, axis=0) / dates.shape[0]
    for i in range(24):
        week_dict[i] = week_arr[i]
    # week_dict[str(i)] = week_arr[i]
    return week_dict


'''
统计用户历史流量使用数据
'''


def count_user_history_flow(df, dates, user):
    user_flow_dict = {}

    for date in dates:
        day_data = df.loc[(df['date'] == date) & (df['username'] == user), ['bytes', 'online']]
        day_count, day_arr = count_hourly_flow(day_data)
        for i in range(24):
            if i not in user_flow_dict:
                user_flow_dict[i] = []
            user_flow_dict[i].append(day_count[i])

    return user_flow_dict


def count_abnormal_flow(data, users, week):
    result = {}
    for user in users:
        if user not in result:
            result[user] = 0
        test_data = data.loc[data['username'] == user, ['bytes', 'online']]
        user_flow_dict, user_flow_arr = count_hourly_flow(test_data)
        # print user_flow_dict
        result[user] = inspect(week, user_flow_dict)
    return result


def inspect(dict1, dict2):
    res = 0
    for i in range(24):
        if dict2[i] > dict1[i]:
            res += dict2[i] - dict1[i]
    return res


'''
检测用户流量使用情况
'''


def inspect_flow_use(df, users):
    hour = int(df['online'].values.tolist()[0])
    for user in users:
        user_flow_data = np.array(df.loc[df['username'] == user, ['bytes']])
        user_flow_sum = 0
        if user_flow_data.shape[0] != 0:
            user_flow_sum = np.sum(user_flow_data) * 1.0 / 1024
        if user_flow_sum == 0:
            continue

        try:
            user_history_flow = user_history_flow_dict[user]
            user_history_hourly_flow = np.array(user_history_flow[hour])
            user_history_minute_flow = user_history_hourly_flow / 60
            # 单边t假设检验
            (t, p) = ttest_1samp(user_history_minute_flow, user_flow_sum)
            # print((t, p))
            if t > 0:
                one_side_p = 1 - p / 2
            else:
                one_side_p = p / 2

            if one_side_p > 0.005:
                df.loc[df['username'] == user, ['flow_use']] = 0
            else:
                df.loc[df['username'] == user, ['flow_use']] = 1

        except ValueError:
            df.loc[df['username'] == user, ['flow_use']] = 0
        finally:
            df.loc[df['username'] == user, ['flow_sum']] = user_flow_sum


def set_flow_count(df, dates, users):
    for user in users:
        user_history_flow_dict[user] = count_user_history_flow(df, dates, user)
    inspect_flow_use(df, users)

# df = load_json(data_path)
# df = process_count(df)
# tmp_df = df.copy(deep=True)
# users = ma.get_users(tmp_df)
# tmp_df = df.copy(deep=True)
# dates = ma.get_dates(tmp_df)
# # week_dict = count_weekly_flow(df, dates, users)
# # print week_dict
# #client = ma.connect()
# #ma.store_data(week_dict, client, 'flow_usage')
#
#
# print(df['flow_use'])
