import pandas as pd
import numpy as np
import datetime
import jpholiday
import time

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 200)
ts = time.time()

# df.loc[df['sex'] == '不', 'sex'] = 0
# df.loc[df['sex'] == '男', 'sex'] = 1
# df.loc[df['sex'] == '女', 'sex'] = 2
# df.loc[df['trans'] == '現金出金', 'trans'] = 1
# df.loc[df['trans'] == '現金入金', 'trans'] = 2
# df.loc[df['trans'] == '振込', 'trans'] = 3
# df.loc[df['trans'] == '振替（出', 'trans'] = 4
# df.loc[df['trans'] == '振替（入', 'trans'] = 5
# df.loc[df['trans'] == 'その他', 'trans'] = 6
# df.loc[df['spot'] == '自', 'spot'] = 1
# df.loc[df['spot'] == 'セ', 'spot'] = 2
# df.loc[df['spot'] == 'ロ', 'spot'] = 3
# df.loc[df['spot'] == 'イ', 'spot'] = 4
# df.loc[df['spot'] == '他', 'spot'] = 5
# df.loc[df['spot'] == '海', 'spot'] = 6
# df.loc[df['spot'] == 'ゆ', 'spot'] = 7
#
#  #   Column         Non-Null Count  Dtype
# ---  ------         --------------  -----
#  0   ym             63985 non-null  object
#  1   hour           63985 non-null  uint8
#  2   is_offtime     63985 non-null  bool
#  3   is_individual  63985 non-null  bool
#  4   is_large       63985 non-null  bool
#  5   age            63985 non-null  int8
#  6   sex            63985 non-null  uint8
#  7   trans          63985 non-null  uint8
#  8   spot           63985 non-null  uint8
#  9   amount         63985 non-null  uint64
#  10  count          63985 non-null  uint32

files = [
    # '201404_201406',
    # '201407_201409',
    # '201410_201412',
    # '201501_201503',
    # '201504_201506',
    # '201507_201509',
    # '201510_201512',
    # '201601_201603',
    # '201604_201606',
    # '201607_201609',
    # '201610_201612',
    # '201701_201703',
    # '201704_201706',
    # '201707_201709',
    # '201710_201712',
    # '201801_201803',
    # '201804_201806',
    # '201807_201809',
    # '201810_201812',
    # '201901_201903',
    '201904_201906',
    '201907_201909',
    '201910_201912',
    '202001_202003'
]
result = pd.DataFrame()
grouping_columns = ['spot', 'trans']

for i,n in enumerate(files):
    df = pd.read_pickle('./input/' + n + '.pkl')
    # df = df[df['is_individual']]
    # df = df[df['trans'] == 1]
    df.loc[(df['spot']==1) & (df['is_offtime']),'spot'] = 5 # special!!!
    df['is_hokuyo'] = df.spot == 1
    df['year'] = df['ym'].str[:4].astype(int)
    df.loc[df['ym'].str[4:6].isin(['01','02','03']), 'year'] -= 1



    # ATM fee
    df['income_atm'] = 0
    # hokuyo
    a = df['is_hokuyo']
    b = df['is_offtime']
    c = (df['trans'] == 1) | (df['trans'] == 3)
    df.loc[a & b & c, 'income_atm'] = 110 * df.loc[a & b & c, 'count']
    # コンビニ
    df.loc[~a & ~b & ~(df['spot'] == 5) & ~(df['spot'] == 6), 'income_atm'] = 110 * df.loc[~a & ~b & ~(df['spot'] == 5) & ~(df['spot'] == 6), 'count']
    df.loc[~a & b & ~(df['spot'] == 5) & ~(df['spot'] == 6), 'income_atm'] = 220 * df.loc[~a & b & ~(df['spot'] == 5) & ~(df['spot'] == 6), 'count']

    # FURIKOMI fee
    df['income_furi'] = 0
    c = df['trans'] == 3
    d = df['is_large']
    df.loc[c & d, 'income_furi'] = 495 * df.loc[c & d, 'count']
    df.loc[c & ~d, 'income_furi'] = 330 * df.loc[c & ~d, 'count']

    # pay charge from hokuyo for using atm
    df['outgo_atm'] = 0
    # other bank
    df.loc[df['spot'] == 5, 'outgo_atm'] = 110 * df.loc[df['spot'] == 5, 'count']
    # seven
    df.loc[df['spot'] == 2, 'outgo_atm'] = 150 * df.loc[df['spot'] == 2, 'count']
    # lawson, e-net
    df.loc[df['spot'] == 3, 'outgo_atm'] = 130 * df.loc[df['spot'] == 3, 'count']
    df.loc[df['spot'] == 4, 'outgo_atm'] = 130 * df.loc[df['spot'] == 4, 'count']
    # jp
    a = df['spot'] == 7
    #b = df['取引金額'] > 110000 # 本当は11万超に境界があるがとりあえず3万で集計
    df.loc[a & b & d, 'outgo_atm'] = 440 * df.loc[a & b & d, 'count']
    df.loc[a & b & ~d, 'outgo_atm'] = 330 * df.loc[a & b & ~d, 'count']
    df.loc[a & ~b & d, 'outgo_atm'] = 330 * df.loc[a & ~b & d, 'count']
    df.loc[a & ~b & ~d, 'outgo_atm'] = 220 * df.loc[a & ~b & ~d, 'count']

    # outgo from hokuyo using furikomi
    df['outgo_furi'] = 0
    df.loc[c & d, 'outgo_furi'] = 162 * df.loc[c & d, 'count']
    df.loc[c & ~d, 'outgo_furi'] = 117 * df.loc[c & ~d, 'count']



    ans = df.groupby(grouping_columns, as_index=False).agg({'amount': 'sum', 'count': 'sum',
                                                            'income_atm': 'sum', 'income_furi': 'sum',
                                                            'outgo_atm': 'sum', 'outgo_furi': 'sum'})
    if i == 0:
        result = ans
    else:
        result = pd.concat([result, ans])
output = result.groupby(grouping_columns, as_index=False).agg({'amount': 'sum', 'count': 'sum',
                                                               'income_atm': 'sum', 'income_furi': 'sum',
                                                               'outgo_atm': 'sum', 'outgo_furi': 'sum'})
output['profit'] = output.loc[:,'income_atm':'income_furi'].sum(axis=1) \
                - output.loc[:,'outgo_atm':'outgo_furi'].sum(axis=1)
output.to_csv('output/output.txt', sep='\t', index=False)
print(output)


print('run time: ' + str(time.time() - ts))