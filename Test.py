#!/root/anaconda3/bin/python3
# -*- coding: UTF-8 -*-
import numpy as np
import pymysql
import pandas as pd
from Functions import *
from sqlalchemy import create_engine
import yaml
import time


# 解析yaml文件
def parse_yaml(file_name, obj):
    with open(file_name, 'r') as f:
        return yaml.load(f, Loader=yaml.FullLoader)[obj]


def area_filter(val):
    if val.startswith('tq'):
        return 'M'
    if len(val) == 5:
        return 'D'
    elif len(val) == 6 or val.startswith('rig'):
        return 'H'


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
        # 鱼池访问api限制，15次/分钟，设置每5秒一次请求，每分钟12次请求，即获得12个账户信息。
        time.sleep(5)
    register_count = register_area_account()
    # 实际访问api后获得的每个账户在每个场地的机器数
    realtime_count = realtime_area_count(df)

    # print(register_count)
    print('=====================================')
    # print(realtime_count)

    df = pd.merge(register_count, realtime_count, how='inner', on=['账号', '场地'])
    df['登记机器数'] = df['登记机器数'].map(lambda x: int(float(x)))
    df['diff'] = df['登记机器数']-df['count']
    print(df)
