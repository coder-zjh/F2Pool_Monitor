#!/root/anaconda3/bin/python3
# -*- coding: UTF-8 -*-
# 功能类
import yaml
import pandas as pd
from dingtalkchatbot.chatbot import DingtalkChatbot
from Tools import *
from datetime import datetime
from sqlalchemy import create_engine


# 通过各场地编号规则，识别机器编号属于哪个场地，属于需长期维护的函数
def area_identify(val):
    if val.startswith('tq'):
        return 'M'
    if len(val) == 5:
        return 'D'
    elif len(val) == 6 or val.startswith('rig'):
        return 'H'


# 根据访问完的df，拆分各账户在各场地的机器数，返回这个结果df
def realtime_area_count(df):
    # 获得的df是['账号', '机器编号', '过去15min平均算力', '过去1h平均算力', '实时算力']
    df['场地'] = df['机器编号'].map(lambda x: area_identify(x))
    d = pd.DataFrame({'count': df.groupby(['账号', '场地']).size()}).reset_index()
    return d


# 函数集在某时间范围内豁免执行
def stop_period_func(df, functions=[]):
    stop_running_period = parse_yaml('conf.yml', 'stop_running_period')
    begin, end = stop_running_period['begin'], stop_running_period['end']
    if datetime.now().hour not in range(begin, end):
        for function in functions:
            function(df)


# 低算力报告：取实时算力<200M且过去15min平均算力<200M
def low_hr_report(df):
    # 机器人设置
    webhook, secret = parse_yaml('conf.yml', 'low_hr_report').values()
    low_hr_dingding = DingtalkChatbot(webhook=webhook, secret=secret)

    # 低算力评定规则 : 实时算力>0 且 实时算力<200 且 过去15min平均算力<200
    df = df[(df['实时算力'].between(0, 200)) & (df['过去15min平均算力'] < 200)][['账号', '机器编号', '实时算力']]

    # 发送内容
    msg = ''

    if df.empty:
        msg = '暂时无低算力机器。'
    else:
        accounts = df['账号'].unique()
        for account in accounts:
            container = ''
            head = '账号:' + str(account) + '\n'
            tail = '算力低于应有值，请查看。\n'
            lines = df[df['a'] == account]
            lens = len(lines)
            for line in range(lens):
                serial, realtime_hr = lines.iloc[line, 1], ('%6.2f' % lines.iloc[line, 2])
                container += '机器编号:{serial}\t实时算力:{realtime_hr}MH/s\n'.format(serial=serial, realtime_hr=realtime_hr)
            msg += head + container + tail + '\n'
    # 信息发送
    low_hr_dingding.send_text(msg=msg)


# 每小时离线报告
def hourly_offline_report(df):
    # 机器人设置
    webhook, secret = parse_yaml('conf.yml', 'hourly_offline_report').values()
    offline_dingding = DingtalkChatbot(webhook=webhook, secret=secret)

    # 获取每小时每个场地内离线的账户
    # 规则:实时算力等于0
    df = df[(df['实时算力'] == 0)][['账号', '机器编号', '实时算力']]

    # 发送内容
    msg = ''

    if df.empty:
        msg = '暂时无离线机器。'
    else:
        df['机器所属场地'] = df['机器编号'].map(lambda x: area_identify(x))
        groups = df.groupby(['机器所属场地', '账号'])
        area_name = ''
        for group_name, group_df in groups:
            if area_name != group_name[0]:
                msg += group_name[0] + '场地离线，请及时处理：' + '\n'
                area_name = group_name[0]
            serials = list(group_df['机器编号'])
            if len(serials) > 5:
                serials = str(list(group_df['机器编号'])[:5]) + '等大量离线'
            msg += str(group_df['账号'].values[0]) + '\t' + str(serials) + '\n'

    # 信息发送
    offline_dingding.send_text(msg=msg)


# 每小时离线机器编号写表
def hourly_offline_to_db(df):
    # 获取记录日期
    record_date = datetime.now().strftime('%Y%m%d')
    # 获取记录时分秒
    record_time = datetime.now().strftime('%H:%M:%S')

    # df处理：获取实时算力为0的机器
    df = df[(df['实时算力'] == 0)][['账号', '机器编号']]
    df.columns = ['account', 'serial']
    df['record_date'] = record_date
    df['record_time'] = record_time

    # 数据库连接配置
    conn_dict = parse_yaml('conf.yml', 'mysql_conn')
    user, password, host, port = conn_dict['user'], conn_dict['password'], conn_dict['host'], conn_dict['port']

    engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user, password, host, port, 'F2Pool'))

    # 发送数据到mysql
    df.to_sql('hourly_offline_serial_record', engine, index=False, if_exists='append')

# 每小时缺失记录
def miss_record():
    # 获取账户在每个场地中应有的机器数：返回[账户,场地,场地机器数]
    should_get_count = get_account_area_account()
    # 实际访问api后获得的每个账户在每个场地的机器数
    real_get_count = account_area_count(df)

    df = pd.merge(register_count, realtime_count, how='inner', on=['账号', '场地'])
    df['登记机器数'] = df['登记机器数'].map(lambda x: int(float(x)))
    df['diff'] = df['登记机器数']-df['count']
    df = df[df['diff']!=0]

    # 获取记录日期
    record_date = datetime.now().strftime('%Y%m%d')
    df['record_date'] = record_date

    # 数据库连接配置
    conn_dict = parse_yaml('conf.yml', 'mysql_conn')
    user, password, host, port = conn_dict['user'], conn_dict['password'], conn_dict['host'], conn_dict['port']
    engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user, password, host, port, 'F2Pool'))
    # 发送数据到mysql
    df.to_sql('hourly_miss_serial_record', engine, index=False, if_exists='append')