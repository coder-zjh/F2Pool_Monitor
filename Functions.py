#!/root/anaconda3/bin/python3
# -*- coding: UTF-8 -*-
# 功能类
import yaml
import pandas as pd
from dingtalkchatbot.chatbot import DingtalkChatbot
from Tools import *
from datetime import datetime


# 函数集在某时间范围内豁免执行
def stop_period_func(df, functions=[]):
    """
    :param df:
    :param functions: 需要对df执行的函数列表
    :return:
    """
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

    if len(df) == 0:
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

    # 规则:编号合理(非eth开头的) 且 实时算力等于0
    df = df[(df['实时算力'] == 0) & (~df['机器编号'].str.contains('eth'))][['账号', '机器编号', '实时算力']]

    # 发送内容
    msg = ''

    if len(df) == 0:
        msg = '暂时无离线机器。'
    else:
        accounts = df['账号'].unique()
        for account in accounts:
            container = '以下编号离线,请及时查看：\n'
            serials = list(df[df['账号'] == account]['机器编号'])
            if len(serials) > 10:
                container += account + '\t' + str(serials[:5]) + '等大量离线\n'
            else:
                container += account + '\t' + str(serials) + '\n'
            msg += container + '\n'
    # 信息发送
    offline_dingding.send_text(msg=msg)


# 24小时每小时离线机器记录
def day_offline_record(df):
    # 获取记录日期
    record_date = datetime.now().strftime('%Y%m%d')
    # 获取记录时分秒
    record_time = datetime.now().strftime('%H:%M:%S')

    # df处理：获取实时算力为0的机器
    df = df[(df['实时算力'] == 0) & (~df['机器编号'].str.contains('eth'))][['账号', '机器编号']]
    df['record_date'] = record_date
    df['record_time'] = record_time

    # 数据库连接配置
    conn_dict = parse_yaml('conf.yml', 'mysql_conn')
    user, password, host, port = conn_dict['user'], conn_dict['password'], conn_dict['host'], conn_dict['port']
    db_name = 'F2Pool'
    tb_name = 'hourly_offline_record'
    engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user, password, host, port, db_name))

    # 发送数据到mysql
    df.to_sql(tb_name, engine, index=False, if_exists='append')
