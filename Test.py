#!/root/anaconda3/bin/python3
# -*- coding: UTF-8 -*-
# 测试类

import pandas as pd
from Functions import *
from Tools import *
from datetime import datetime

df = pd.DataFrame([[1, 2, 3],
                   [2, 3, 4],
                   [3, 4, 5],
                   [1, 3, 6],
                   [2, 4, 4],
                   [3, 7, 9],
                   [1, 2, 3]],
                  columns=['a', 'b', 'c'])

accounts = df['a'].unique()
msg = ''

for account in accounts:
    container = ''
    head = '账号:' + str(account) + '\n'
    tail = '算力低于应有值，请查看。\n'
    lines = df[df['a'] == account]
    lens = len(lines)
    for line in range(lens):
        q, w = lines.iloc[line, 1], lines.iloc[line, 2]
        container += '机器编号:{serial}\t实时算力:{realtime_hr}MH/s\n'.format(serial=q, realtime_hr=w)
    msg += head + container + tail + '\n'

print(msg)
