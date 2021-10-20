#!/root/anaconda3/bin/python3
# -*- coding: UTF-8 -*-
# 工具类
import requests
import yagmail
import pandas as pd
import json
import yaml


# 解析yaml文件
def parse_yaml(file_name, obj):
    with open(file_name, 'r') as f:
        return yaml.load(f, Loader=yaml.FullLoader)[obj]


# 获取账户在每个场地中的机器数
def register_area_account():
    cus_info = pd.DataFrame(parse_yaml('customers.yml', 'account-record')).T.reset_index()
    cus_info.columns = ['账号', 'M', 'H', 'D']

    cus_info['场地拼接'] = cus_info['M'].map(str) + '-' + cus_info['H'].map(str) + '-' + cus_info['D'].map(str)
    cus_info['登记机器数'] = cus_info['场地拼接'].apply(lambda x: x.split('-'))
    cus_info = cus_info.explode('登记机器数')

    cus_info['场地'] = len(cus_info.groupby(['账号', '场地拼接'])) * ['M', 'H', 'D']
    cus_info = cus_info.loc[cus_info['登记机器数'] != 'nan'][['账号', '场地', '登记机器数']]
    return cus_info


# 发送邮件
def send_email(subject=None, contents=None, emails_receive=[], attachments=None):
    # 配置邮箱服务器
    user, password, host = parse_yaml('conf.yml', 'emails_send').values()
    yagmail_server = yagmail.SMTP(user=user, password=password, host=host)

    # 发送内容
    if len(emails_receive) == 0:
        yagmail_server.send(to=parse_yaml('conf.yml', 'emails_receive'), subject=subject,
                            contents=contents, attachments=attachments)
    else:
        yagmail_server.send(to=emails_receive, subject=subject,
                            contents=contents, attachments=attachments)
    yagmail_server.close()


# api返回信息处理:str=>dataframe
def get_response(account):
    url = 'https://api.f2pool.com/eth/{}'.format(account)
    res_raw = requests.get(url, timeout=30)
    # 如果状态码不为200，邮件告警并退出
    if res_raw.status_code != 200:
        error_content = '错误代码:' + str(res_raw.status_code)
        send_email('鱼池错误', error_content, ['541946578@qq.com'])
        return res_raw.status_code

    # 如果访问与接收没问题，开始取workers信息，即各编号矿机即时信息
    workers = json.loads(res_raw.text)['workers']
    if len(workers) == 0:
        return pd.DataFrame()
    else:
        df = pd.DataFrame(workers)[[0, 1, 2, 8]]
        df['账号'] = account
        df = df[['账号', 0, 1, 2, 8]]
        df.columns = ['账号', '机器编号', '过去15min平均算力', '过去1h平均算力', '实时算力']
        # 算力值保留2位小数
        df[['过去15min平均算力', '过去1h平均算力', '实时算力']] = round(df[['过去15min平均算力', '过去1h平均算力', '实时算力']] / 1000000, 2)
        # 返回 df=['账号', '机器编号', '过去15min平均算力', '过去1h平均算力', '实时算力']
        return df
