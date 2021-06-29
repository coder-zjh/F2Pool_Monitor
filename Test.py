#!/root/anaconda3/bin/python
# -*- coding: UTF-8 -*-
# 测试类


import pandas as pd
from Functions import *

if __name__ == '__main__':
    df = get_response('zhouqifang')
    print(df[df['实时算力'] == 0])
