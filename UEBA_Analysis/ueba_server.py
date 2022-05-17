import json

from flask import Flask, request
from flask_cors import CORS
import pymongo
from ueba_redis import *

app = Flask(__name__)
CORS(app, resources=r'/*')
client = pymongo.MongoClient('localhost', 27017)
db = client['Situation_Awareness']
db.authenticate("pkusz", "pkusz")


def handle_objectId(d):
    d['_id'] = str(d['_id'])
    return d


def handle_date(date):
    arr = date.split('/')
    y = arr[0]
    m = arr[1]
    d = arr[2]
    if int(arr[1]) < 10:
        m = str(int(arr[1]))
    if int(arr[2]) < 10:
        d = str(int(arr[2]))
    return f'{y}/{m}/{d}'


@app.route('/events/stats/<int:level>')
def get_events_stats(level):
    time_count = int(db['ueba_event'].find({'name': 'time_count', 'level': level}).count())
    url_count = int(db['ueba_event'].find({'name': 'url_count', 'level': level}).count())
    download_count = int(db['ueba_event'].find({'name': 'download_check', 'level': level}).count())
    login_count = int(db['ueba_event'].find({'name': 'login_check', 'level': level}).count())
    post_count = int(db['ueba_event'].find({'name': 'post_check', 'level': level}).count())
    resource_use = int(db['ueba_event'].find({'name': 'resource_use', 'level': level}).count())

    return {'time': time_count, 'url': url_count, 'download': download_count,
            'login': login_count, 'post': post_count, 'resource': resource_use}


@app.route('/events/stats/keyword/<string:keyword>/<int:level>')
def get_events_stats_by_keyword(keyword, level):
    start_date = request.args.get('startDate') + " 00:00:00"
    end_date = request.args.get('endDate') + " 24:00:00"
    time_count = int(db['ueba_event'].find(
        {'username': {'$regex': keyword},
         'time': {'$lte': end_date, '$gte': start_date},
         'name': 'time_count', 'level': level}).count())
    url_count = int(db['ueba_event'].find({'username': {'$regex': keyword},
                                           'time': {'$lte': end_date, '$gte': start_date},
                                           'name': 'url_count', 'level': level}).count())
    download_count = int(db['ueba_event'].find({'username': {'$regex': keyword},
                                                'time': {'$lte': end_date, '$gte': start_date},
                                                'name': 'download_check', 'level': level}).count())
    login_count = int(db['ueba_event'].find({'username': {'$regex':  keyword},
                                             'time': {'$lte': end_date, '$gte': start_date},
                                             'name': 'login_check', 'level': level}).count())
    post_count = int(db['ueba_event'].find({'username': {'$regex': keyword},
                                            'time': {'$lte': end_date, '$gte': start_date},
                                            'name': 'post_check', 'level': level}).count())
    resource_use = int(db['ueba_event'].find({'username': {'$regex': keyword},
                                              'time': {'$lte': end_date, '$gte': start_date},
                                              'name': 'resource_use', 'level': level}).count())

    return {'time': time_count, 'url': url_count, 'download': download_count,
            'login': login_count, 'post': post_count, 'resource': resource_use}


@app.route('/events/<int:page_num>')
def get_events(page_num):
    try:
        page_size = eval(request.args.get('pageSize'))
        # 跳过的条数
        skip_count = (page_num - 1) * page_size
        events = list(db['ueba_event'].find().sort([('_id', -1)]).skip(skip_count).limit(page_size))
        events = list(map(handle_objectId, events))
        # 安全事件总条数
        count = int(db['ueba_event'].find().count())
        return {'events': events, 'total': count}
    except Exception:
        return {'events': [], 'total': 0}


@app.route('/events/keyword/<string:keyword>')
def get_events_by_keyword(keyword):
    try:
        page_size = eval(request.args.get('pageSize'))
        page_num = eval(request.args.get('pageNum'))
        start_date = request.args.get('startDate') + " 00:00:00"
        end_date = request.args.get('endDate') + " 24:00:00"
        # 跳过的条数
        skip_count = (page_num - 1) * page_size
        events = list(db['ueba_event']
                      .find({'username': {'$regex': keyword}, 'time': {'$lte': end_date, '$gte': start_date}})
                      .sort([('_id', -1)]).skip(skip_count).limit(page_size))
        events = list(map(handle_objectId, events))

        # 日志信息总条数
        count = int(db['ueba_event']
                    .find({'username': {'$regex': keyword}, 'time': {'$lte': end_date, '$gte': start_date}}).count())
        return {'events': events, 'total': count}
    except TypeError:
        print("查询错误")
        return {'events': [], 'total': 0}


@app.route('/logs/<int:page_num>')
def get_logs(page_num):
    try:
        page_size = eval(request.args.get('pageSize'))
        # 跳过的条数
        skip_count = (page_num - 1) * page_size
        logs = list(db['ueba_logs'].find().sort([('_id', -1)]).skip(skip_count).limit(page_size))
        logs = list(map(handle_objectId, logs))

        # 日志信息总条数
        count = int(db['ueba_logs'].find().count())
        return {'logs': logs, 'total': count}
    except TypeError:
        print("查询错误")
        return {'logs': [], 'total': 0}


@app.route('/logs/keyword/<string:keyword>')
def get_logs_by_keyword(keyword):
    try:
        page_size = eval(request.args.get('pageSize'))
        page_num = eval(request.args.get('pageNum'))
        start_date = handle_date(request.args.get('startDate'))
        end_date = handle_date(request.args.get('endDate'))
        # 跳过的条数
        skip_count = (page_num - 1) * page_size
        logs = list(db['ueba_logs']
                    .find({'username': {'$regex': keyword}, 'date': {'$lte': end_date, '$gte': start_date}})
                    .sort([('_id', -1)]).skip(skip_count).limit(page_size))
        logs = list(map(handle_objectId, logs))

        # 日志信息总条数
        count = int(db['ueba_logs']
                    .find({'username': {'$regex': keyword}, 'date': {'$lte': end_date, '$gte': start_date}}).count())
        return {'logs': logs, 'total': count}
    except TypeError:
        print("查询错误")
        return {'logs': [], 'total': 0}


@app.route('/logs/keyword/<string:username>/')
def get_logs_by_datetime(username):
    try:
        datetime = request.args.get('datetime')
        # datetime = str(datetime, encoding='utf-8')
        # print(datetime)
        date_and_time = datetime.split(' ')
        start_time = date_and_time[1][: 5] + ':00'
        end_time_int = int(date_and_time[1][3:5]) + 1
        if end_time_int >= 10:
            end_time = date_and_time[1][:3] + str(end_time_int) + ':00'
        else:
            end_time = date_and_time[1][:3] + '0' + str(end_time_int) + ':00'
        date = date_and_time[0]
        date = handle_date(date)
        logs = list(db['ueba_logs']
                    .find(
            {'username': {'$regex': username}, 'date': date, 'Time': {'$lte': end_time, '$gte': start_time}})
                    .sort([('_id', -1)]))
        logs = list(map(handle_objectId, logs))
        final_logs = []

        # 日志信息总条数
        count = int(db['ueba_logs']
                    .find(
            {'username': {'$regex': username}, 'date': date, 'Time': {'$lte': end_time, '$gte': start_time}})
                    .sort([('_id', -1)]).count())

        # 删除异常时间数据
        for log in logs:
            if len(log['Time']) != len(start_time):
                count -= 1
                continue
            final_logs.append(log)

        return {'logs': final_logs, 'total': count}
    except TypeError:
        print("查询错误")
        return {'logs': [], 'total': 0}


@app.route('/analysis/<int:page_num>')
def get_analysis(page_num):
    try:
        # TODO 修改筛选条件
        analysis = list(
            db['ueba_data'].find({}, {'_id': 0, 'username': 1, 'score': 1}).sort([('_id', -1)]).limit(page_num))
        users = [(x['username']) for x in analysis]
        scores = get_user_scores(users=users)
        return {'analysis': analysis, 'scores': scores}
    except Exception:
        return {'analysis': [], 'scores': []}


# 查询用户历史得分数据
def get_user_scores(users):
    scores = {}
    for user in users:
        scores[user] = list(
            db['ueba_data'].find({'username': user}, {'_id': 0, 'score': 1}).sort([('_id', -1)]).limit(5))
    return scores


# 查询各项统计top5
@app.route('/analysis/stats/')
def top_stats():
    try:
        # start = time.time()
        # res = get_hash(r_ins, 'analysis_stats')
        # if res is not None:
        #     end = time.time()
        #     print(f'{end - start} s used.')
        #     return res
        types = request.args.getlist('types')
        res = {}
        for e_type in types:
            res[e_type] = list(db['ueba_event'].aggregate([
                {'$match': {'name': e_type, 'level': {'$ne': 0}}},
                {'$group': {'_id': '$username', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 5}
            ]))
        res_in_str = json.dumps(res)
        end = time.time()
        # print(f'{end - start} s used.')
        print('expired.')
        # set_hash(r_ins, 'analysis_stats', res_in_str)
        return res
    except Exception:
        return {}


# 查询各类异常行为日志数目
@app.route('/analysis/stats/events')
def events_stats():
    try:
        # res = get_hash(r_ins, 'analysis_stats_events')
        # if res is not None:
        #     return res
        res = {}
        stats = list(db['ueba_event'].aggregate([
            {'$match': {'level': {'$ne': 0}}},
            {'$group': {'_id': '$name', 'count': {'$sum': 1}}},
        ]))
        for stat in stats:
            res[stat['_id']] = stat['count']
        res_in_str = json.dumps(res)
        print('expired.')
        # set_hash(r_ins, 'analysis_stats_events', res_in_str)
        return res
    except Exception:
        return {}


# 查询各类异常行为日志
@app.route('/analysis/details/event/')
def top_events():
    try:
        # res = get_hash(r_ins, 'analysis_events_details')
        # if res is not None:
        #     return res
        res = {}
        types = request.args.getlist('types')
        for e_type in types:
            temp = list(db['ueba_event'].aggregate([
                {'$match': {'name': e_type, 'level': {'$ne': 0}}},
                {'$sort': {'_id': -1}},
                {'$limit': 5}
            ]))
            res[e_type] = list(map(handle_objectId, temp))
        res_in_str = json.dumps(res)
        print('expired.')
        # set_hash(r_ins, 'analysis_events_details', res_in_str)
        return res
    except Exception:
        return {}


# 查询各类异常行为日志
@app.route('/analysis/details/event/<string:type>/<int:pageNum>')
def top_type_events(type, pageNum):
    try:
        res = {}
        skip_count = (pageNum - 1) * 5
        print(skip_count)
        temp = list(db['ueba_event'].aggregate([
            {'$match': {'name': type, 'level': {'$ne': 0}}},
            {'$sort': {'_id': -1}},
            {'$skip': skip_count},
            {'$limit': 5}
        ]))
        res[type] = list(map(handle_objectId, temp))
        return res
    except Exception:
        return {}


# 查询各用户使用流量数目
@app.route('/analysis/stats/users')
def userfreq_stats():
    try:
        # res = get_hash(r_ins, 'analysis_stats_users')
        # if res is not None:
        #     return res
        res = {}
        stats = list(db['ueba_logs'].aggregate([
            {'$group': {'_id': '$username', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}
        ]))
        for stat in stats:
            res[stat['_id']] = stat['count']
        res_in_str = json.dumps(res)
        print('expired.')
        # set_hash(r_ins, 'analysis_stats_users', res_in_str)
        return res
    except Exception:
        return {}


# r_ins = get_redis_ins()
app.run(host='0.0.0.0', port='5055')
