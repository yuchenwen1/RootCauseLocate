import glob
import numpy as np
import pandas as pd

moniter_id = "72843754"
create_time = "2022-2-1 2:47"
item_name = "9b5208c0d71c7a0599dda32e3e2356c0"
where_info = "k8:6f38b51ac2bb885f8add3e0a66d69f6d"
root_cause = "753e3e6ff6ab4f7ac61fa1b56c04aeaf"


time_min = 0   #告警分钟数
moniter_dict = {"k0":1, "k1":2, "k2":3, "k5":4, "k6":5, "k7":6, "k8":7, "k11":8}
# 监控项内容与实际含义的对应词典

train_len = 400
test_len = 200
# 建立server数据汇总
server_total_and_success_dict = dict()
server_calling_relation_dict = dict()
candidates = set()

def scoring(root, node):
    if root not in server_total_and_success_dict:
        print("Not in!")
    a_dict = server_total_and_success_dict[root][0]
    b_dict = server_total_and_success_dict[node][0]

    a = []
    b = []
    for i in range(time_min - test_len, time_min):   # 使用200个数据点
        a.append(a_dict[str(i)])
        b.append(b_dict[str(i)])
    a_s = pd.Series(a)
    b_s = pd.Series(b)
    cor = a_s.corr(b_s)

    return cor

def find_upper_neighbors(node):
    upper_neighbors = set()
    for callerserver in server_calling_relation_dict:
        if node in server_calling_relation_dict[callerserver]:
            upper_neighbors.add(callerserver)
    return upper_neighbors

def judge_anomly(node):       # 有异常返回True
    # if node not in server_total_and_success_dict:
    #     return False
    datas = server_total_and_success_dict[node][0]
    data_list = []
    for i in range(time_min - (train_len + test_len), time_min - test_len):
        data_list.append(datas[str(i)])

    avg = np.mean(data_list)
    delta = np.std(data_list)

    min = avg - 3 * delta
    max = avg + 3 * delta

    for i in range(time_min - test_len, time_min):
        data = datas[str(i)]
        if data < min or data > max:
            return True
    return False


def select_data():
    global time_min,train_len,test_len, server_total_and_success_dict, server_calling_relation_dict, candidates

    t = create_time.split(" ")[1].split(":")
    h = int(t[0])
    m = int(t[1])
    time_min = int(60 * h + m)

    if time_min < 600:
        test_len = int(time_min / 3)
        train_len = 2 * test_len

    time = create_time.split(" ")[0].split("-")
    if len(time[1]) == 1:
        time[1] = "0" + time[1]
    if len(time[2]) == 1:
        time[2] = "0" + time[2]
    date = time[0] + time[1] + time[2]
    calling_files = glob.glob("../data20220325/app_opsdatagovern_aiops_export_caller_min_monitor_di/" + date + "/*.c000")
    callings = []
    for calling_file in calling_files:
        f = open(calling_file)
        part_data = f.readlines()
        f.close()
        callings.extend(part_data)

    if where_info != "":
    # 处理where_info
        where_info_items = where_info.split(",")
        where_info_dict = {}
        for where_info_item in where_info_items:
            key = where_info_item.split(":")[0]
            value = where_info_item.split(":")[1]
            if key not in moniter_dict:
                continue
            where_info_dict[moniter_dict[key]] = value

    waiting_callee_set = set()      # 向后调用的备选
    waiting_caller_set = set()      # 向前调用的备选
    all_method_set = set()

    # 先用moniter_id和where_info确定
    for calling in callings:
        if moniter_id not in calling:
            continue
        items = calling.split("|")
        if moniter_id != "" and moniter_id != items[0]:
            continue
        exit_flag = False
        if where_info != "":
            for key in where_info_dict:
                if items[key] != where_info_dict[key]:
                    exit_flag = True
                    break
                else:
                    print("find!")
                    print(calling)
            if exit_flag:
                continue

        caller_method = "|".join(items[1:5])
        callee_method = "|".join(items[5:9])

        all_method_set.add(caller_method)
        all_method_set.add(callee_method)
        waiting_callee_set.add(callee_method)
        waiting_caller_set.add(caller_method)
        # if item_name in caller_method:
        #     waiting_callee_set.add(callee_method)
        # if item_name in callee_method:
        #     waiting_caller_set.add(caller_method)

    # 判断第一步是否筛选到method了，如果没有筛选到任何记录，则使用item_name和where_info再进行筛查
    if len(waiting_caller_set) == 0 and len(waiting_callee_set) == 0:
        for calling in callings:
            if item_name not in calling:
                continue
            items = calling.split("|")
            exit_flag = False
            for key in where_info_dict:
                if items[key] != where_info_dict[key]:
                    exit_flag = True
                    break
            if exit_flag:
                continue

            caller_method = "|".join(items[1:5])
            callee_method = "|".join(items[5:9])

            all_method_set.add(caller_method)
            all_method_set.add(callee_method)
            waiting_callee_set.add(callee_method)
            waiting_caller_set.add(caller_method)
            # if item_name in caller_method:
            #     waiting_callee_set.add(callee_method)
            # if item_name in callee_method:
            #     waiting_caller_set.add(caller_method)
    print("all_method_number:", str(len(all_method_set)))
    # 先向下搜索
    method_stack = waiting_callee_set
    while len(method_stack) != 0:
        method = method_stack.pop()
        for calling in callings:
            if method not in calling:
                continue
            items = calling.split("|")
            callee_method = "|".join(items[5:9])
            # if item_name not in callee_method:
            #     continue
            if callee_method not in all_method_set:
                all_method_set.add(callee_method)
                method_stack.add(callee_method)
    print("finish down search")
    # 再向上搜索
    method_stack = waiting_caller_set
    while len(method_stack) != 0:
        method = method_stack.pop()
        for calling in callings:
            if method not in calling:
                continue
            items = calling.split("|")
            caller_method = "|".join(items[1:5])
            # if item_name not in caller_method:
            #     continue
            if caller_method not in all_method_set:
                all_method_set.add(caller_method)
                method_stack.add(caller_method)
    print("finish up search")
    # 深搜完毕，开始统计所有server
    all_server_set = set()
    for method in all_method_set:
        server = method.split("|")[0]
        all_server_set.add(server)
    all_server_set.add(item_name)

    if root_cause in all_server_set:
        print("root_cause in all_server_set")

    template = dict()
    for i in range(0, 1440):
        template[str(i)] = 0

    for server in all_server_set:
        server_total_and_success_dict[server] = [template.copy(), template.copy()]


    for server in all_server_set:
        server_calling_relation_dict[server] = set()

    for calling in callings:
        items = calling.split("|")
        caller_server = items[1]
        callee_server = items[5]
        if caller_server in all_server_set:
            total = items[9]
            success = items[11]

            total_items = total.split(",")
            for total_item in total_items:
                try:
                    key = total_item.split(":")[0]
                    value = int(total_item.split(":")[1])
                    server_total_and_success_dict[caller_server][0][key] += value
                except:
                    pass
            success_items = success.split(",")
            for success_item in success_items:
                try:
                    key = success_item.split(":")[0]
                    value = int(success_item.split(":")[1])
                    server_total_and_success_dict[caller_server][1][key] += value
                except:
                    pass
        if callee_server in all_server_set:
            total = items[9]
            success = items[11]

            total_items = total.split(",")
            for total_item in total_items:
                try:
                    key = total_item.split(":")[0]
                    value = int(total_item.split(":")[1])
                    server_total_and_success_dict[callee_server][0][key] += value
                except:
                    pass
            success_items = success.split(",")
            for success_item in success_items:
                try:
                    key = success_item.split(":")[0]
                    value = int(success_item.split(":")[1])
                    server_total_and_success_dict[callee_server][1][key] += value
                except:
                    pass
        if caller_server in all_server_set and callee_server in all_server_set:
            server_calling_relation_dict[caller_server].add(callee_server)
        # caller_method = "|".join(items[1:5])
        # callee_method = "|".join(items[5:9])
        # if caller_method in all_method_set and callee_method in all_method_set:
        #     server = items[1]
        #     total = items[9]
        #     success = items[11]
        #     callee_server = items[5]
        #
        #     total_items = total.split(",")
        #     for total_item in total_items:
        #         key = total_item.split(":")[0]
        #         value = int(total_item.split(":")[1])
        #         server_total_and_success_dict[server][0][key] += value
        #         # server_total_and_success_dict[callee_server][0][key] += value
        #
        #     success_items = success.split(",")
        #     for success_item in success_items:
        #         key = success_item.split(":")[0]
        #         value = int(success_item.split(":")[1])
        #         server_total_and_success_dict[server][1][key] += value
        #         # server_total_and_success_dict[callee_server][1][key] += value
        #
        #     server_calling_relation_dict[server].add(callee_server)
        #
        # elif callee_method in all_method_set:
        #     server = items[5]
        #     total = items[9]
        #     success = items[11]
        #     caller_server = items[1]
        #
        #     total_items = total.split(",")
        #     for total_item in total_items:
        #         key = total_item.split(":")[0]
        #         value = int(total_item.split(":")[1])
        #         # server_total_and_success_dict[server][0][key] += value
        #         server_total_and_success_dict[caller_server][0][key] += value
        #
        #     success_items = success.split(",")
        #     for success_item in success_items:
        #         key = success_item.split(":")[0]
        #         value = int(success_item.split(":")[1])
        #         # server_total_and_success_dict[server][1][key] += value
        #         server_total_and_success_dict[caller_server][1][key] += value
        #
        #     server_calling_relation_dict[caller_server].add(server)

    # 计算失败率
    for server in server_total_and_success_dict:
        total_dict = server_total_and_success_dict[server][0]
        success_dict = server_total_and_success_dict[server][1]
        for i in range(0, 1440):
            try:
                total_dict[str(i)] = 1 - success_dict[str(i)] / total_dict[str(i)]
            except:
                total_dict[str(i)] = 0
            # total_dict[str(i)] = total_dict[str(i)] - success_dict[str(i)]

    # 开始深搜
    root_node = item_name
    # candidates.add(root_node)
    # 先向下进行深搜
    stack = {root_node}
    # searched_server_set = {root_node}
    searched_server_set = set()
    while len(stack) != 0:
        node = stack.pop()
        neighbors = server_calling_relation_dict[node]
        if len(neighbors) == 0:
            candidates.add(node)
            continue
        for neighbor in neighbors:
            if neighbor not in searched_server_set and judge_anomly(neighbor):
                searched_server_set.add(neighbor)
                stack.add(neighbor)
                candidates.add(neighbor)

    # # 再向上深搜
    # stack = {root_node}
    # # searched_server_set = {root_node}
    # searched_server_set = set()
    # while len(stack) != 0:
    #     node = stack.pop()
    #     neighbors = find_upper_neighbors(node)
    #     if len(neighbors) == 0:
    #         candidates.add(node)
    #         continue
    #     for neighbor in neighbors:
    #         if neighbor not in searched_server_set and judge_anomly(neighbor):
    #             searched_server_set.add(neighbor)
    #             stack.add(neighbor)
    #             candidates.add(neighbor)

def get_results():
    candidate_score = dict()
    if root_cause not in candidates:
        print("ROOT CAUSE NOT DETECTED!")
        print(len(candidates))
        return
    for candidate in candidates:
        candidate_score[candidate] = scoring(item_name, candidate)

    sorted_list = sorted(candidate_score.items(), key=lambda kv: (kv[1], kv[0]))
    print(sorted_list)
    rank = len(sorted_list)
    total = len(sorted_list)
    while rank > 0:
        if sorted_list[rank - 1][0] == root_cause:
            print(root_cause, total - rank + 1, total)
            return
        else:
            rank -= 1
    return

select_data()
get_results()