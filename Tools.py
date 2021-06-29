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
# 鱼池访问api限制，15次/分钟，设置延时4.5秒一次请求，每分钟13次请求。
def get_response(account):
    url = 'https://api.f2pool.com/eth/{}'.format(account)
    res_raw = requests.get(url, timeout=15)
    # 如果状态码不为200，邮件告警并退出
    if res_raw.status_code != 200:
        error_content = '错误代码:' + str(res_raw.status_code)
        send_email('鱼池错误', error_content, ['541946578@qq.com'])
        return res_raw.status_code

    # 如果访问与接收没问题，开始取workers信息，即各编号矿机即时信息
    res = json.loads(res_raw.text)['workers']
    if len(res) == 0:
        return pd.DataFrame()
    else:
        df = pd.DataFrame(res)[[0, 1, 2, 8]]
        df['账号'] = account
        df = df[['账号', 0, 1, 2, 8]]
        df.columns = ['账号', '机器编号', '过去15min平均算力', '过去1h平均算力', '实时算力']
        # 算力值保留2位小数
        df[['过去15min平均算力', '过去1h平均算力', '实时算力']] = round(df[['过去15min平均算力', '过去1h平均算力', '实时算力']] / 1000000, 2)
        # 返回 df=['账号', '机器编号', '过去15min平均算力', '过去1h平均算力', '实时算力']
        return df
