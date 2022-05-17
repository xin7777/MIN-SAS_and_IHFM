import math
import time

#main func
#data: [{"id":_,"sourceIP":_,"desIP":_,"port":_,"service":_,"name":_,"time":_,"level":_,"description":_},{...}...}
#index_value:{"cpu":[min,max],"mem":[min,max],"loss":[min,max],"connect":[min,max],"i_o":[min,max]}
# hping3 -q -n  --id 0 --icmp  --flood 121.15.171.91

CVSS = {"test":8}

def mainDeal(data, index_value):
    SPECIFIED_DATA = []
    NORMAL_DATA = []
    THREAT_DATA = []
    nums = len(data)
    types = []
    types_alert = {}
    for each in data:
        #第1层过滤,条件是condition_M
        #if each == condition_M:
        #    SPECIFIED_DATA.append(each)
        if each["name"] not in types:
            types.append(each["name"])
            types_alert[each["name"]] = []
        types_alert[each["name"]].append(each)

    #0.5和3是提前设置的阈值，第2层过滤
    other_data = []
    #print("types_alert",len(types_alert),types_alert["test"][0])
    for each_type in types:
        if len(types_alert[each_type])/float(nums) < 0.5 and types_alert[each_type][0]["level"] < 3:
            NORMAL_DATA.append(types_alert[each_type])
        else:
            other_data += types_alert[each_type]
    data = other_data #删除了第2层过滤的数据


    flag = 0
    performance = {}#the difference of performance entropy
    for each in index_value:
        index = each
        min_value = index_value[each][0]
        max_value = index_value[each][1]
        performance[index] = math.log(max_value/min_value, 2)
        #设置性能熵之差的阈值，然后进行比较
        if performance[index] > 0.44:
            flag = 1
    if flag == 0:
        #所有警报全为NORMAL DATA
        NORMAL_DATA = data
    else:
        #计算联合性能熵之差并判断
        #通过FAHP计算各个指标对应的权重
        joint_performance = 0.25*performance["cpu"] + 0.189*performance["mem"] + 0.176*performance["loss"] + 0.199*performance["connect"] + 0.186*performance["i_o"]
        #将联合性能熵之差与设定好的阈值进行比较
        if joint_performance > 1.04:
            THREAT_DATA = data 
        else:
            #进入第四层过滤
            print("joint_performance:",joint_performance)
            print("performance:",performance["cpu"],performance["mem"],performance["loss"],performance["connect"],performance["i_o"])
            for each_type in types:
                #****************************
                threat_degree = pow(10,9*len(types_alert[each_type]))
                #将威胁程度与提前设置好的阈值进行比较
                if threat_degree > 2000000:
                    THREAT_DATA.append(types_alert[each_type])
                else:
                    NORMAL_DATA.append(types_alert[each_type])
            
    return [SPECIFIED_DATA, NORMAL_DATA, THREAT_DATA]


#if __name__ == "__main__":
    
def run(nums):
    start_time = time.time()
    #print("start:",start_time)
    data = []
    data2 = []
    item = {"id":1,"sourceIP":1,"desIP":1,"port":1,"service":2,"name":"test","time":120,"level":7,"description":0}
    for i in range(nums * 1000):
        item["id"] = i
        data.append(item)
        data2.append(item)
        data2.append(item)
    mid_time = time.time()
    #print("data get:",mid_time)
    mainDeal(data2, {"cpu":[1,200],"mem":[1,2],"loss":[1,2],"connect":[1,2],"i_o":[1,2]})
    mainDeal(data, {"cpu":[1,2],"mem":[1,2],"loss":[1,2],"connect":[1,2],"i_o":[1,2]})


    end_time = time.time()
    #print("end:",end_time)
    print(nums*1000," 的总耗时:",1000*(end_time - start_time))
    return 1000*(end_time - start_time)

run(3)

'''
ans = []
for i in range(1,30,2):
    ans.append(run(i))
    print(ans)
'''
