import time
import json


class EventValue:
    def __init__(self, level, value, unit):
        self.level = level
        self.value = value
        self.unit = unit


class Event:
    def setEventType(self, name, description):
        self.name = name
        self.description = description

    def setEventBasic(self, username, times):
        self.username = username
        print(times)
        self.time = str(times).replace('-', '/')
        # timestamp = int(str(times)[:10])
        # self.time = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(timestamp - 28800))

    def setEventValue(self, eventValue):
        self.level = eventValue.level
        self.value = eventValue.value
        self.unit = eventValue.unit

    def to_dict(self):
        event_data = {'name': self.name, 'description': self.description, 'username': self.username, 'time': self.time,
                      'level': self.level, 'value': self.value, 'unit': self.unit}
        return event_data


class EventHandler:
    """
    初始化
    """

    def __init__(self, behavior):
        self.__behavior = behavior
        self.__eventList = []
        # print(behavior)
        users = behavior['username'].values.tolist()
        times = behavior['Time'].values.tolist()
        for index, user in enumerate(users):
            self.__eventList.append(Event())
            self.__eventList[index].setEventBasic(user, times[index])

    '''
    将所有事件转换为dict
    '''

    def events_to_dict_arr(self):
        dict_arr = []
        for event in self.__eventList:
            dict_arr.append(event.to_dict())
        return dict_arr

    '''
    根据用户行为向量与指定名称创建事件
    '''

    def create_events(self, name, description):
        for event in self.__eventList:
            event.setEventType(name, description)

    def create_events2(self, eventValueList):
        for index, eventValue in enumerate(eventValueList):
            self.__eventList[index].setEventValue(eventValue)

# print(time.time())
