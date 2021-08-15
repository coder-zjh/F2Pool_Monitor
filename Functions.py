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


# 24小时中每小时离线情况记录=>邮件发送
# pickle做备份
def day_offline_record(df):
    with open('customers.yml', 'r') as f:
        yaml_cont = yaml.load(f, Loader=yaml.FullLoader)['account-record']

    hour = datetime.now().hour
    df = df[(df['过去1h平均算力'] == 0) & (~df['机器编号'].str.contains('eth'))][['账号', '机器编号']]
    df_new = df.groupby('账号')['机器编号'].count()
    dict_df_new = {'账号': df_new.index, hour: df_new.values}
    df_new = pd.DataFrame(dict_df_new)

    # 如果pkl文件不存在,创建。
    if not os.path.exists('daily_offline_record.pickle'):
        # 最左边的信息dataframe，包含账号及对应台数
        info_df = pd.DataFrame(columns=['账号', '台数'])
        info_df['账号'] = yaml_cont.keys()
        info_df['台数'] = yaml_cont.values()
        info_df.to_pickle('daily_offline_record.pickle')

    # 如果存在,与进入函数的 df_new 进行 merge
    df_old = pd.read_pickle('daily_offline_record.pickle')
    offline_record = pd.merge(df_old, df_new, on='账号', how='left').fillna(0)

    if len(offline_record.columns) == 26:
        df['今日该账户离线总时长'] = df['台数'] * 24 - df.iloc[:, 2:].sum(axis=1)
        yesterday = str(date.today() + timedelta(days=-1))
        csv_name = yesterday + ' 离线报告.csv'
        offline_record.to_csv(csv_name, index=False)
        send_email(yesterday, csv_name)
        os.remove(csv_name)
        offline_record = pd.DataFrame(columns=['账号', '台数'])
        offline_record['账号'] = yaml_cont.keys()
        offline_record['台数'] = yaml_cont.values()
        offline_record = pd.merge(df_old, df_new, on='账号', how='left').fillna(0)

    offline_record.to_pickle('daily_offline_record.pickle')

    return offline_record
