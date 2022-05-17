# coding:utf-8
import pandas as pd
import numpy as np
import math
import string
from sklearn.neighbors import KernelDensity
from sklearn.model_selection import GridSearchCV
from scipy.stats import ttest_1samp

data_path = 'ueba.csv'

user_history_resource_dict = {}
user_history_download_dict = {}
user_history_login_dict = {}
user_history_post_dict = {}
base_data = ''
dates = []
base_users = []


NORMAL = 0
ABNORMAL = 1


def load_data(path):
    data = pd.read_excel(path)
    return data


def count_ip(data):
    # 数据去重
    data.drop_duplicates('src_ip', inplace=True)
    counts = data['username'].value_counts()
    users = data['username'].value_counts().index
    return counts, users


'''
统计用户每小时访问资源次数
'''


def count_hourly_resource(df):
    day_count = {}
    day_arr = []
    for i in range(24):
        hour_data = df.loc[(df['online'] > i) & (df['online'] < i + 1), ['command']]
        hour_commands = hour_data.values.tolist()
        if i not in day_count:
            day_count[i] = 0
        for command in hour_commands:
            command = command[0].lower()
            if command.find('file'):
                day_count[i] += 1
        day_arr.append(day_count[i])

    return day_count, day_arr


'''
统计每小时文件下载次数
'''


def count_hourly_download(df):
    day_count = {}
    day_arr = []
    for i in range(24):
        hour_data = df.loc[(df['online'] > i) & (df['online'] < i + 1), ['command']]
        hour_commands = hour_data.values.tolist()
        if i not in day_count:
            day_count[i] = 0
        for command in hour_commands:
            command = command[0].lower()
            if command.find('.zip') != -1 or command.find('.tar.gz') != -1:
                day_count[i] += 1
        day_arr.append(day_count[i])

    return day_count, day_arr


'''
统计每小时用户登录次数
'''


def count_hourly_login(df):
    day_count = {}
    day_arr = []
    for i in range(24):
        hour_data = df.loc[(df['online'] > i) & (df['online'] < i + 1), ['command']]
        hour_commands = hour_data.values.tolist()
        if i not in day_count:
            day_count[i] = 0
        for command in hour_commands:
            command = command[0].lower()
            if command.find('login') != -1:
                day_count[i] += 1
        day_arr.append(day_count[i])

    return day_count, day_arr


'''
统计每小时用户敏感操作次数
'''


def count_hourly_post(df):
    day_count = {}
    day_arr = []
    for i in range(24):
        hour_data = df.loc[(df['online'] > i) & (df['online'] < i + 1), ['reqtype']]
        hour_commands = hour_data.values.tolist()
        if i not in day_count:
            day_count[i] = 0
        for command in hour_commands:
            command = command[0].lower()
            if command == 'post':
                day_count[i] += 1
        day_arr.append(day_count[i])

    return day_count, day_arr


'''
统计用户历史资源访问次数
'''


def count_user_history_files(df, dates, user):
    user_resource_dict = {}

    for date in dates:
        day_data = df.loc[(df['date'] == date) & (df['username'] == user), ['command', 'online']]
        day_count, day_arr = count_hourly_resource(day_data)
        for i in range(24):
            if i not in user_resource_dict:
                user_resource_dict[i] = []
            user_resource_dict[i].append(day_count[i])

    return user_resource_dict


def count_user_history_downloads(df, dates, user):
    user_download_dict = {}

    for date in dates:
        day_data = df.loc[(df['date'] == date) & (df['username'] == user), ['command', 'online']]
        day_count, day_arr = count_hourly_download(day_data)
        for i in range(24):
            if i not in user_download_dict:
                user_download_dict[i] = []
            user_download_dict[i].append(day_count[i])

    return user_download_dict


def count_user_history_logins(df, dates, user):
    user_login_dict = {}

    for date in dates:
        day_data = df.loc[(df['date'] == date) & (df['username'] == user), ['command', 'online']]
        day_count, day_arr = count_hourly_login(day_data)
        for i in range(24):
            if i not in user_login_dict:
                user_login_dict[i] = []
            user_login_dict[i].append(day_count[i])

    return user_login_dict


def count_user_history_posts(df, dates, user):
    user_post_dict = {}

    for date in dates:
        day_data = df.loc[(df['date'] == date) & (df['username'] == user), ['reqtype', 'online']]
        day_count, day_arr = count_hourly_post(day_data)
        for i in range(24):
            if i not in user_post_dict:
                user_post_dict[i] = []
            user_post_dict[i].append(day_count[i])

    return user_post_dict


def set_file_count(data, dict):
    data['file_count'] = data['command']
    for i in dict:
        data.loc[data['username'] == i, ['file_count']] = dict[i]
    return data


def count_abnormal_operations(data, users):
    result = {}
    for user in users:
        if user not in result:
            result[user] = 0
        test_data = data.loc[data['username'] == user, ['command']]
        test_data = np.array(test_data).reshape(1, -1)
        # result[user] = inspect(test_data[0])
        result[user] = inspect(test_data[0])
    return result


def inspect(data):
    tmp = 0
    for i in data:
        if i.find('file') or i.find('File'):
            tmp += 1
    return tmp


'''
检测用户资源访问情况
'''


def resource_inspect(df, users):
    hour = int(df['online'].values.tolist()[0])
    for user in users:
        user_resource_data = df.loc[df['username'] == user, ['command']]
        user_resource_data = user_resource_data.values.tolist()

        user_resource_times = 0
        for command in user_resource_data:
            command = command[0].lower()
            if command.find('delete') != -1:
                user_resource_times += 1

        try:
            if user not in user_history_resource_dict:
                user_history_minute_resource = [0, 0, 0, 0, 0, 0, 0]
            else:
                user_history_resource = user_history_resource_dict[user]
                user_history_hourly_resource = np.array(user_history_resource[hour])
                user_history_minute_resource = user_history_hourly_resource / 60
            # 单边t假设检验
            (t, p) = ttest_1samp(user_history_minute_resource, user_resource_times)
            # print((t, p))
            if t > 0:
                one_side_p = 1 - p / 2
            else:
                one_side_p = p / 2

            if (not math.isnan(one_side_p) and one_side_p > 0.005) or user_resource_times < 3:
                df.loc[df['username'] == user, ['resource_use']] = NORMAL
            else:
                df.loc[df['username'] == user, ['resource_use']] = ABNORMAL

        except ValueError:
            df.loc[df['username'] == user, ['resource_use']] = NORMAL
        finally:
            df.loc[df['username'] == user, ['resource_times']] = user_resource_times


'''
检测用户文件下载情况
'''


def download_inspect(df, users):
    hour = int(df['online'].values.tolist()[0])
    for user in users:
        user_download_data = df.loc[df['username'] == user, ['command']]
        user_download_data = user_download_data.values.tolist()

        user_download_times = 0
        for command in user_download_data:
            command = command[0].lower()
            if command.find('.tar.gz') != -1 or command.find('.zip') != -1 or command.find('ownload') != -1:
                user_download_times += 1

        try:
            if user not in user_history_download_dict:
                user_history_minute_download = [0, 0, 0, 0, 0, 0, 0]
            else:
                user_history_download = user_history_download_dict[user]
                user_history_hourly_download = np.array(user_history_download[hour])
                user_history_minute_download = user_history_hourly_download / 60
            # 单边t假设检验
            (t, p) = ttest_1samp(user_history_minute_download, user_download_times)
            # print((t, p))
            if t > 0:
                one_side_p = 1 - p / 2
            else:
                one_side_p = p / 2

            if (not math.isnan(one_side_p) and one_side_p > 0.005) or user_download_times <= 3:
                df.loc[df['username'] == user, ['download_check']] = NORMAL
            else:
                df.loc[df['username'] == user, ['download_check']] = ABNORMAL

        except ValueError:
            df.loc[df['username'] == user, ['download_check']] = NORMAL
        finally:
            df.loc[df['username'] == user, ['download_times']] = user_download_times


'''
检测用户登录情况
'''


def login_inspect(df, users):
    hour = int(df['online'].values.tolist()[0])
    for user in users:
        user_login_data = df.loc[df['username'] == user, ['command']]
        user_login_data = user_login_data.values.tolist()

        user_login_times = 0
        for command in user_login_data:
            command = command[0].lower()
            if command.find('login') != -1 or command.find('logout') != -1 or command.find('登录') != -1 or command.find('退出登录') != -1:
                user_login_times += 1

        try:
            if user not in user_history_login_dict:
                user_history_minute_login = [0, 0, 0, 0, 0, 0, 0]
            else:
                user_history_login = user_history_login_dict[user]
                user_history_hourly_login = np.array(user_history_login[hour])
                user_history_minute_login = user_history_hourly_login / 60
            # 单边t假设检验
            (t, p) = ttest_1samp(user_history_minute_login, user_login_times)
            # print((t, p))
            if t > 0:
                one_side_p = 1 - p / 2
            else:
                one_side_p = p / 2

            if (not math.isnan(one_side_p) and one_side_p > 0.005) or user_login_times < 3:
                df.loc[df['username'] == user, ['login_check']] = NORMAL
            else:
                df.loc[df['username'] == user, ['login_check']] = ABNORMAL

        except ValueError:
            df.loc[df['username'] == user, ['login_check']] = NORMAL
        finally:
            df.loc[df['username'] == user, ['login_times']] = user_login_times


'''
检测用户敏感情况
'''


def post_inspect(df, users):
    hour = int(df['online'].values.tolist()[0])
    for user in users:
        user_post_data = df.loc[df['username'] == user, ['command']]
        user_post_data = user_post_data.values.tolist()
        user_post_times = 0
        for command in user_post_data:
            command = command[0].lower()
            if command.find('pload') != -1:
                user_post_times += 1

        try:
            if user not in user_history_post_dict:
                user_history_minute_post = [0, 0, 0, 0, 0, 0, 0]
            else:
                user_history_post = user_history_post_dict[user]
                user_history_hourly_post = np.array(user_history_post[hour])
                user_history_minute_post = user_history_hourly_post / 60
            # 单边t假设检验
            (t, p) = ttest_1samp(user_history_minute_post, user_post_times)
            # print((t, p))
            if t > 0:
                one_side_p = 1 - p / 2
            else:
                one_side_p = p / 2

            if (not math.isnan(one_side_p) and one_side_p > 0.005) or user_post_times <= 3:
                df.loc[df['username'] == user, ['post_check']] = NORMAL
            else:
                df.loc[df['username'] == user, ['post_check']] = ABNORMAL

        except ValueError:
            df.loc[df['username'] == user, ['post_check']] = NORMAL
        finally:
            df.loc[df['username'] == user, ['post_times']] = user_post_times


def set_resource_count(df, users):
    resource_inspect(df, users)


def set_download_count(df, users):
    download_inspect(df, users)


def set_login_count(df, users):
    login_inspect(df, users)


def set_post_count(df, users):
    post_inspect(df, users)


def initDicts():
    for user in base_users:
        user_history_post_dict[user] = count_user_history_posts(base_data, dates, base_users)
        user_history_login_dict[user] = count_user_history_logins(base_data, dates, base_users)
        user_history_download_dict[user] = count_user_history_downloads(base_data, dates, base_users)
        user_history_resource_dict[user] = count_user_history_files(base_data, dates, base_users)


initDicts()
# if __name__ == '__main__':
#     df = load_data(data_path)
#     tmp = df.copy(deep=True)
#     # ------------------统计当日用户使用ip-------------------
#     counts, users = count_ip(tmp)
