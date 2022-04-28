import pandas as pd
import numpy as np
import datetime
import tqdm
import os
import glob
import re
from tqdm import tqdm 

def get_values(x):
    return dict(sorted(eval('{' + x +'}').items(), key=lambda d: d[0]))

def main():
    calling_files = glob.glob('../data3/app_opsdatagovern_aiops_export_caller_min_monitor_di/*/')
    output = '../data3/output_data'
    #os.makedirs(output, exist_ok = True)
    for calling_date in calling_files:
        if "20211222" not in calling_date:
            continue
        # print(calling_date)
        calling_files_date = glob.glob(f'{calling_date}/*.c000')
        current_date = re.search(r'\\2021\d{4}\\', calling_date).group(0)[1:-1]
#         if os.path.exists(f'{output}/{current_date}.npy'): continue
        print(calling_date)

        calling_data = {}
        for fi in tqdm(calling_files_date):
            f = open(fi)
            callings = f.readlines()
            f.close()
            for line in callings:
                splited_line = line.split('|')
                # print(splited_line)
                monitor_id = splited_line[0]
                date = current_date
                name = '|'.join(splited_line[1:9])
                
                if ':\\N' in '|'.join(splited_line[9:14]):
                    continue

                request_min = get_values(splited_line[9])
                duration_min = get_values(splited_line[10])
                success_min = get_values(splited_line[11])
                exception_min = get_values(splited_line[12])
                timeout_min = get_values(splited_line[13])

                if name in calling_data.keys(): continue
#                     print(f'Data duplication:{name}')
                try:
                    assert list(request_min.keys()) == list(duration_min.keys()) == list(success_min.keys()) == list(exception_min.keys()) == list(timeout_min.keys()), f'duration not equal'
                except:
                    print(fi)
                    print(name)
                    
                calling_data[name] = {'date': date, 'monitor_id': monitor_id, 'name': name, 'request_min': request_min,
                                     'duration_min': duration_min, 'success_min': success_min, 'exception_min': exception_min,'timeout_min': timeout_min}

        np.save(f'{output}/{current_date}.npy', calling_data)
        
    
main()
