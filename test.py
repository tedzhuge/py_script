#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import importlib
import sys
import array
import os

import datetime

years=['2013', '2014', '2015', '2016', '2017', '2018', '2019']
mms = ['01', '02', '03','04','05','06','07','08','09','10','11','12']
dds = ['01', '02', '03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18',
'19', '20','21','22','23','24','25','26','27','28', '29', '30', '31']

# for year in years:
#   for mm in mms:
#     for dd_ in dds:
#       dd = str(dd_)
#       ipath = f'/home/toto/entrance/DATA/DATA/us/sima/raw_prc.1/{year}/{mm}/raw_price.{year}{mm}{dd}'
#       print(ipath)
#       if os.path.exists(ipath):
#         opath = f'/home/toto/entrance/DATA/DATA/us/sima/us_prc/{year}{mm}{dd}'
#         with open(ipath, 'r') as rf:
#           with open(opath, 'w') as wf:
#             for ln in rf:
#               itms = ln.split(',')
#               wf.write(f'{itms[0]},{itms[3]},{itms[5]}\n')


# path='/home/toto/entrance/DATA/DATA/cn/'
# date= 20150105
# for date in [20150105, 20150106, 20150107]:
#   aa={}
#   bb={}
#   with open(path + f'raw_prc/2015/01/raw_prc.{date}' ,'r') as rf:
#     for ln in rf:
#       tokens = ln.split(',')
#       lst_ = tokens[0].split('.')
#       if lst_[1] == "SZ":
#         tk = "sz" + lst_[0]
#       elif lst_[1] == "SH":
#         tk = "sh" + lst_[0]
#       if tk in aa.keys():
#         print("aa",tk)
#         input()
#       aa[tk] = [tk, tokens[4], tokens[9]]


#   with open(path + f'stock_daily_close/2015/01/stock_daily_close.{date}.csv' ,'r') as rf:
#     for ln in rf:
#       tokens = ln.split('|')
#       if tokens[29] != "1":
#         continue
#       tk = tokens[0]#16st
#       if tk in bb.keys():
#         input()
#       bb[tk] = [tk, tokens[21], tokens[23]]


#   for key in aa.keys():
#     if abs(float(aa[key][1]) - float(bb[key][1])) > 1e-5 or \
#     abs(float(aa[key][2])* 1000 - float(bb[key][2]))  > 10:
#       print(aa[key],bb[key]) 

aa ={}
bb ={}

if False:
  with open('/home/toto/entrance/analytool/ana_d/raw_p', 'r') as rf:
    for ln in rf:
      tokens = ln.split(',')
        
      lst_ = tokens[0].split('.')
      if lst_[1] == "SZ":
        tk = "sz" + lst_[0]
      elif lst_[1] == "SH":
        tk = "sh" + lst_[0]
      if tk in aa.keys():
        print("aa",tk)
        input()
      aa[tk] = [tk]
      aa[tk].extend(tokens[1:])
      


  with open('/home/toto/entrance/analytool/ana_d/stock', 'r') as rf:
    for ln in rf:
      tokens = ln.split(',')

      tk = tokens[0]#16st
      if tk in bb.keys():
        input()
      bb[tk] = [tk]
      bb[tk].extend(tokens[1:])

  for key in aa.keys():
    for i in range(1, len(aa[key])):
      #print(key, float(aa[key][i]), float(bb[key][i]))
      if abs(float(aa[key][i]) - float(bb[key][i])) > 1e-9:
        print(aa[key],bb[key])    
else:
  with open('/home/toto/entrance/analytool/ana_d/raw_p.univ', 'r') as rf:
    for ln in rf:
      tokens = ln.split(',')
        
      lst_ = tokens[0].split('.')
      if lst_[1] == "SZ":
        tk = "sz" + lst_[0]
      elif lst_[1] == "SH":
        tk = "sh" + lst_[0]
      if tk in aa.keys():
        print("aa",tk)
        input()
      aa[tk] = [tk]
      aa[tk].extend(tokens[1:])
    
  with open('/home/toto/entrance/analytool/ana_d/stock.univ', 'r') as rf:
    for ln in rf:
      tokens = ln.split(',')

      tk = tokens[0]#16st
      if tk in bb.keys():
        input()
      bb[tk] = [tk]
      bb[tk].extend(tokens[1:])

  for key in aa.keys():
    for i in range(1, len(aa[key])):
      #print(key, float(aa[key][i]), float(bb[key][i]))
      if abs(int(aa[key][i]) - int(bb[key][i])) != 0:
        print(aa[key],bb[key])  
        break  
