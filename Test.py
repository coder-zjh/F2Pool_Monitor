#!/root/anaconda3/bin/python3
# -*- coding: UTF-8 -*-
import pymysql
import pandas as pd
from Functions import *
from sqlalchemy import create_engine
import yaml


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
    a = parse_yaml('customers.yml', 'account-record')
    # print(a.keys())

    df = pd.DataFrame([[1, 2, 3, 'M'], [1, 3, 7, 'M'], [2, 2, 6, 'D'], [1, 2, 5, 'H'], [1, 2, 3, 'D']],
                      columns=['a', 'b', 'c', 'd'])

    groups = df.groupby(['d', 'a'])
    area_name = ''
    msg = ''
    # for group_name, group_df in groups:
    #     if area_name != group_name[0]:
    #         msg += group_name[0]+'\n'
    #         area_name = group_name[0]
    #     msg += str(group_df['a'].values[0]) + '\t' + str(list(group_df['c'])) + '\n'
    # print(msg)
    # df = pd.DataFrame(
    #     [[1, 2, 3, 'tq1102'], [1, 3, 7, 'tq2234'], [2, 2, 6, '002981'], [1, 2, 5, '129981'], [1, 2, 3, '09872']],
    #     columns=['a', 'b', 'c', 'd'])
    # d = df['d'].map(lambda x: area_filter(x))
    # print(d)
