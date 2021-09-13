#!/root/anaconda3/bin/python3
# -*- coding: UTF-8 -*-

# 程序主入口
import pandas as pd
import time
from Tools import *
from Functions import *

if __name__ == '__main__':
    # 读取customers.yml内容 ==> 获得账户相关信息
    account_record = parse_yaml('customers.yml', 'account-record')
    # 获取所有的账户名
    accounts = account_record.keys()

    df = pd.DataFrame()
    for account in accounts:
        df_res = get_response(account)
        if df_res.empty:
            continue
        else:
            df = df.append(df_res)
        time.sleep(4.5)

    # 某段时间不执行的函数集
    stop_period_func(df, [hourly_offline_report])

    # 24小时都在执行的函数
    # day_offline_record(df)
