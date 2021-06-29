#!/root/anaconda3/bin/python
# -*- coding: UTF-8 -*-
# 工具类
import requests
import yagmail
import pandas as pd
import json


# api返回信息处理:str=>dataframe
# 鱼池访问api限制，15次/分钟，设置延时4.5秒一次请求，每分钟13次请求。
def get_response(account):
    url = 'https://api.f2pool.com/eth/{}'.format(account)
    res_raw = requests.get(url)
    # 如果状态码不为200，查看是否是鱼池出现问题
    if res_raw.status_code != 200:
        return 'null'
    else:
        res = json.loads(res_raw.text)['workers']
        if len(res) == 0:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(res)[[0, 1, 2, 8]]
            df['账号'] = account
            df = df[['账号', 0, 1, 2, 8]]
            df.columns = ['账号', '机器编号', '过去15min平均算力', '过去1h平均算力', '实时算力']
            # 保留2位小数
            df[['过去15min平均算力', '过去1h平均算力', '实时算力']] = round(df[['过去15min平均算力', '过去1h平均算力', '实时算力']] / 1000000, 2)
            # 返回 df=['账号', '机器编号', '过去15min平均算力', '过去1h平均算力', '实时算力']
            return df


# 发送邮件
def send_email(date, file):
    yagmail_server = yagmail.SMTP(user="邮箱号：需要设置", password="SMTP码", host="smtp.qq.com")
    email_to = ["邮箱号：需要设置"]
    title = date + '离线情况报告'
    email_title = [title]
    email_content = [title]
    email_attachment = [file]
    yagmail_server.send(to=email_to, subject=email_title, contents=email_content, attachments=email_attachment)
    yagmail_server.close()
