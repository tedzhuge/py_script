#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import importlib
import sys
import array
import os
import mysql.connector
import datetime

gg1= {'user': 'reader',
  'password': 'reader',
  'host': '192.168.101.120',
  'port': 3306,
  'database': 'GOGOAL_V1',
  'raise_on_warnings': True
  }

def mkdir(dir):
  if not os.path.exists(dir):
    os.makedirs(dir)

def create_calendar_dates():
  base = datetime.datetime.today()
  date_list = [(base - datetime.timedelta(days=x)).strftime('%Y%m%d') for x in range(3000, -500, -1)]
  with open('DATA/iso/calendar.iso', 'w') as wf:
    for date_ in date_list:
      if int(date_) > 20150000:
        wf.write(f'{date_}\n')

def get_calendar_dates():
  dates =[]
  with open('DATA/iso/calendar.iso', 'r') as rf:
    for ln in rf:
      if len(ln) > 7:
        dates.append(ln[:8])
  return dates

def get_trd_dates(date_start=20150000):
  dates = []
  with open('DATA/iso/trade_dates.iso', 'r') as rf:
    for ln in rf:
      itm = ln.split(',')
      if len(itm) > 1:
        date = int(itm[0])
        if date > date_start:
          dates.append(itm[0])
  return dates
  
class Cn:
  def __init__(self):
    self.cn_ = mysql.connector.connect(**gg1)
    self.cursor = self.cn_.cursor()
    self.data = []

  def mkdir(self):
    dates = get_trd_dates()
    for date_ in dates:
      yyyy = date_[:4]
      mm = date_[4:6]
      dd = date_[6:8]
      mkdir(f'DATA/gg/data/{yyyy}/{mm}/{dd}')
        
  def output_dersub(self):
    calendar_dates = get_calendar_dates()
    cd_sz = len(calendar_dates)
    trd_dates = get_trd_dates()

    LIST_DER_REPORT_SUBTABLE = [
      'EntryDate',#0
      'EntryTime',#1
      'Time_Year',#2
      'Quarter',#3
      'Forecast_Income',#4
      'Forecast_Profit',#5
      'Forecast_Income_Share',#6
      'Forecast_Return_Cash_Share',#7
      'Forecast_Return_Capital_Share',#8
      'Forecast_Return',#9
      'R_Tar1',#10
      'R_Tar2',#11
      'R_Tar3',#12
      'R_Tar4',#13
      'R_Tar5',#14
      'R_Tar_Date1',#15
      'R_Tar_Date2',#16
      'Forecast_Income_0',#17
      'Forecast_Profit_0',#18
      'Profit_Flag'#19
    ]
    LIST_DER_REPORT_RESEARCH=[
      'Code',#0
      'Code_Name',#1
      'Title',#2
      'Type_ID',#3
      'Organ_ID',#4
      'Author',#5
      'Score_ID',#6
      'Organ_Score_ID',#7
      'Create_Date',#8
      'Into_Date',#9
      'Text1',#10
      'Text3',#11
      'Text5',#12
      'Text6',#13
      'Text8',#14
      'Price_Current',#15
      'Attention',#16
      'Attention_Name',#17
      'Score_Flag'#18
    ]


    _itms_DER_REPORT_SUBTABLE=','.join(LIST_DER_REPORT_SUBTABLE)    
    _itms_DER_REPORT_RESEARCH=','.join(LIST_DER_REPORT_RESEARCH)
    
    itms_DER_REPORT_SUBTABLE=','.join([f'a.{itm}' for itm in  LIST_DER_REPORT_SUBTABLE])
    itms_DER_REPORT_RESEARCH=','.join([f'b.{itm}' for itm in  LIST_DER_REPORT_RESEARCH])
    
    
    idx_ = 0
    for trd_date_ in trd_dates:
      
      yyyy = trd_date_[:4]
      mm = trd_date_[4:6]
      dd = trd_date_[6:8]

      output_path = f'DATA/gg/data/{yyyy}/{mm}/{dd}/dersub.{trd_date_}'
      wf=open(output_path, 'w')
      wf.close()
      while calendar_dates[idx_] <= trd_date_:
        c_date_ = calendar_dates[idx_]
        print(f'{c_date_} into dersub.{trd_date_}')
        self.cursor.execute(f"""
       SELECT  {itms_DER_REPORT_SUBTABLE}, {itms_DER_REPORT_RESEARCH} FROM 
               (SELECT Report_Search_ID, {_itms_DER_REPORT_SUBTABLE} FROM Der_Report_Subtable  WHERE EntryDate = {c_date_}) a
    LEFT JOIN (SELECT ID, {_itms_DER_REPORT_RESEARCH} FROM Der_Report_Research WHERE EntryDate = {c_date_}) b
        ON b.ID = a.Report_Search_ID
        """) 
        data = self.cursor.fetchall()
        
        with open(output_path, 'a') as f:
          id= 0
          for row in data:
            
            str_row = f'{id},{row[0].strftime('%Y%m%d')},{row[1].replace(':','')},{','.join([str(elem) for elem in row[2:]])}\n'
            f.write(f'{str_row}')
            id+=1
        idx_ += 1
      
      

  def fetchall(self):
    self.data = self.cursor.fetchall()

  def print(self):
    for row in self.data:
      print(row)

  def _output(self, name='tmp.txt', force_=False):
    data = self.cursor.fetchall()
    if force_ or (not os.path.exists(name)):
      with open(name, 'w') as f:
        for row in data:
          
          str_row = ','.join([str(elem) for elem in row])
          f.write(f'{str_row}\n')
    else:
      print(f"Can not write force = {force_}")
      
