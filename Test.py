#!/root/anaconda3/bin/python3
# -*- coding: UTF-8 -*-
import pymysql
import pandas as pd
from Functions import *
from sqlalchemy import create_engine

df = pd.DataFrame({'account': ['zhangsan', 'lisi', 'wangwu', 'zhuliu'], 'serial': [1, 2, 3, 4],
                   'record_time': [1, 2, 3, 4]})
df['aa'] = [8, 9, 0, 5]
df['bb'] = [1, 2, 5, 6]
print(df)
