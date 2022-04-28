import pandas as pd
import numpy as np
import datetime
import tqdm
import os
import glob
import re
from tqdm import tqdm
import datetime

sub_cols = ['slo_id', 'period_type', 'period_value', 'compare_type', 'threshold', 'rate', 'create_time', 'update_time', 'alarm_start_time', 'alarm_end_time', 'short_period_type', 'short_period_value', 'where_info']
alarm_files = glob.glob('../data20220325/app_opsdatagovern_aiops_export_mysql_t_slo_alarm_log_df/20220323/*.c000')
alarm_files = sorted(alarm_files)

sub_datum = []
slo_infos = []
for fi in alarm_files:
    f = open(fi, encoding='utf-8-sig')
    alarms = f.readlines()
    f.close()
    for line in alarms:
        slo_info = eval('{' + line.split('{')[1][:-1])
        where_info = '{' + line.split('{')[2][:-1]
        sub_data = line.split('{')[0].split('|')[:-1]
        sub_data.append(where_info)
        sub_datum.append(sub_data)
        slo_infos.append(slo_info)
df1 = pd.DataFrame(sub_datum, columns = sub_cols)
df2 = pd.DataFrame(slo_infos)

alarm_data = pd.concat([df1, df2], axis=1)

alarm_data = alarm_data.sort_values('alarm_start_time')
alarm_data = alarm_data.drop_duplicates(set(alarm_data.columns) - set(['create_time', 'update_time']))
alarm_data['alarm_hour'] = alarm_data.apply(lambda x: datetime.datetime.strptime(x['alarm_start_time'], "%Y-%m-%d %H:%M:%S").hour, axis = 1)

alarm_data.to_csv('../data20220325/output_data/alarm_data.csv', index = False)