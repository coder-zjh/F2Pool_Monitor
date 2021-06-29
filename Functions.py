#!/root/anaconda3/bin/python
# -*- coding: UTF-8 -*-
# 功能类
import yaml
import pandas as pd
from dingtalkchatbot.chatbot import DingtalkChatbot
from Tools import *
import pickle
import os
from datetime import datetime, timedelta, date


# 低算力报告：取实时算力<200M且过去15min平均算力<200M
def low_hr_report(df):
    low_rate_result = ''
    account_list = []
    container = ''
    # 机器人设置
    webhook = 'https://oapi.dingtalk.com/robot/send?access_token' \
              '=cd7ad8453c6398ab3892cca6d80fe2268a627fea1b4dbcc1f4edf0981ed407a4'
    secret = 'SECa3242549289023e0ad9a7be9bcfcb2244af0876927f43e0890ba97d8944b9543'
    low_hr_dingding = DingtalkChatbot(webhook, secret=secret)
    # df处理:
    # 规则:
    #       1.实时算力>0,实时算力=0为离线情况
    #       2.实时算力<200 且 过去15min平均算力<200
    df = df[(df['实时算力'] > 0) & (df['实时算力'] < 200) & (df['过去15min平均算力'] < 200)][['账号', '机器编号', '实时算力']]
    if len(df) == 0:
        low_hr_dingding.send_text(msg='暂时无低算力机器。')
    else:
        for i in range(len(df)):
            account = df.iloc[i, 0]
            serial = df.iloc[i, 1]
            real_hr = ('%6.2f' % df.iloc[i, 2])
            row_record = '机器编号:{serial}\t实时算力:{real_hr}MH/s\n'.format(serial=serial, real_hr=real_hr)

            # 主逻辑
            if (account not in account_list) & (container != ''):
                low_rate_result += container + '算力低于应有值，请查看。\n\n'
                container = ''
            if account not in account_list:
                account_list.append(account)
                container = '账号:{account}\n'.format(account=account)
            if account in account_list:
                container += row_record
        # 信息发送
        low_hr_dingding.send_text(msg=low_rate_result)


# 每小时离线报告
def hourly_offline_report(df):
    offline_result = ''
    container = ''
    # 机器人设置
    webhook = 'https://oapi.dingtalk.com/robot/send?access_token' \
              '=5ae5268b0dc97a02360e46d90749146e0572c6eb91b5753af2aeb6279d85a73d'
    secret = 'SECf52c0f985d3e36b060ef53a2924a5cbbf90f972366cddabbc68cbf4dd73b0a55'
    offline_dingding = DingtalkChatbot(webhook, secret=secret)
    # df处理
    # 规则:
    #       编号合理(非eth开头的) 且 实时算力等于0
    df = df[(df['实时算力'] == 0) & (~df['机器编号'].str.contains('eth'))][['账号', '机器编号', '实时算力']]
    if len(df) == 0:
        offline_dingding.send_text(msg='暂时无离线机器')
    else:
        account_list = list(set(df['账号']))
        for account in account_list:
            container = '以下编号离线,请及时查看：\n'
            serial_list = list(df[df['账号'] == account]['机器编号'])
            if len(serial_list) > 10:
                container += account + '\t' + str(serial_list[:5]) + '等大量离线\n'
            else:
                container += account + '\t' + str(serial_list) + '\n'
            offline_result += container + '\n'
        # 信息发送
        offline_dingding.send_text(msg=offline_result)


# 24小时中每小时离线情况记录=>邮件发送
# pickle做备份
def day_offline_record(df):
    with open('customer_info.yml', 'r') as f:
        yaml_cont = yaml.load(f, Loader=yaml.FullLoader)['account-amount']

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
        df['今日该账户离线总时长'] = df['台数']*24-df.iloc[:,2:].sum(axis=1)
        yesterday = str(date.today() + timedelta(days=-1))
        csv_name = yesterday+' 离线报告.csv'
        offline_record.to_csv(csv_name, index=False)
        send_email(yesterday, csv_name)
        os.remove(csv_name)
        offline_record = pd.DataFrame(columns=['账号', '台数'])
        offline_record['账号'] = yaml_cont.keys()
        offline_record['台数'] = yaml_cont.values()
        offline_record = pd.merge(df_old, df_new, on='账号', how='left').fillna(0)


    offline_record.to_pickle('daily_offline_record.pickle')

    return offline_record
