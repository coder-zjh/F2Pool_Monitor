#!/root/anaconda3/bin/python
# -*- coding: UTF-8 -*-
# 程序主入口
import yaml
import pandas as pd
import time
from Tools import *
from Functions import *
from datetime import datetime

if __name__ == '__main__':
    # 读取yaml文件内容->获得客户-账户-机器数信息
    with open('customer_info.yml', 'r') as f:
        yaml_cont = yaml.load(f, Loader=yaml.FullLoader)['account-amount']
    df = pd.DataFrame()
    for account in yaml_cont.keys():
        df_res = get_response(account)
        if df_res == 'null':
            df = pd.DataFrame(df_res)
        else:
            df = df.append(df_res)
        time.sleep(4.5)
    # 定时任务:
    #       低算力报告:1点-7点不执行
    #       每小时离线:1点-7点不执行
    #       每日离线记录每小时执行
    if datetime.now().hour not in range(1, 8):
        # 低算力报告
        low_hr_report(df)
        # 每小时离线报告
        hourly_offline_report(df)
    # 离线记录=>每日邮件
    day_offline_record(df)
