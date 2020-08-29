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
    
    _itms_DER_REPORT_RESEARCH=' Code,Code_Name, Title, Type_ID,Organ_ID,Author, Score_ID,Organ_Score_ID, Create_Date, Pause_ID, Into_Date, Text1, Text3, Text5, Text6, Text8, Price_Current, Capital_Current, Forecast_Return, Attention, Attention_Name,Score_Flag'
    _itms_DER_REPORT_SUBTABLE='Time_Year, Quarter, Forecast_Income, Forecast_Profit,Forecast_Income_Share, Forecast_Return_Cash_Share, Forecast_Return_Capital_Share, Forecast_Return,  R_Tar1,R_Tar2, R_Tar3, R_Tar4, R_Tar5, R_Tar_Date1, R_Tar_Date2, Forecast_Income_0, Forecast_Profit_0, Profit_Flag, EntryDate, EntryTime '
    
    itms_DER_REPORT_RESEARCH=' b.Code, b.Code_Name, b.Title, b.Type_ID, b.Organ_ID,Author, b.Score_ID, b.Organ_Score_ID, b.Create_Date, b.Pause_ID, b.Into_Date, b.Text1, b.Text3, b.Text5, b.Text6, b.Text8, b.Price_Current, b.Capital_Current, b.Forecast_Return, b.Attention, b.Attention_Name, b.Score_Flag'
    itms_DER_REPORT_SUBTABLE=' a.Time_Year, a.Quarter, a.Forecast_Income, a.Forecast_Profit, a.Forecast_Income_Share, a.Forecast_Return_Cash_Share, a.Forecast_Return_Capital_Share, a.Forecast_Return, a.R_Tar1, a.R_Tar2, a.R_Tar3, a.R_Tar4, a.R_Tar5, a.R_Tar_Date1, a.R_Tar_Date2, a.Forecast_Income_0, a.Forecast_Profit_0, a.Profit_Flag, a.EntryDate, a.EntryTime '

    idx_ = 0
    for trd_date_ in trd_dates:
      
      yyyy = trd_date_[:4]
      mm = trd_date_[4:6]
      dd = trd_date_[6:8]

      output_path = f'DATA/gg/data1/{yyyy}/{mm}/{dd}/dersub.{trd_date_}'
      wf=open(output_path, 'w')
      wf.close()
      while calendar_dates[idx_] <= trd_date_:
        c_date_ = calendar_dates[idx_]
        print(f'{c_date_} into {trd_date_}')
        self.cursor.execute(f"""
       SELECT a.Report_Search_ID, {itms_DER_REPORT_SUBTABLE}, {itms_DER_REPORT_RESEARCH} FROM 
               (SELECT Report_Search_ID, {_itms_DER_REPORT_SUBTABLE} FROM Der_Report_Subtable  WHERE EntryDate = {c_date_}) a
    LEFT JOIN (SELECT ID, {_itms_DER_REPORT_RESEARCH} FROM Der_Report_Research WHERE EntryDate = {c_date_}) b
        ON b.ID = a.Report_Search_ID
        """) 
        data = self.cursor.fetchall()
        
        with open(output_path, 'a') as f:
          for row in data:
            
            str_row = ','.join([str(elem) for elem in row])
            f.write(f'{str_row}\n')

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
      


def create_forb():
  for yyyy in range(2015, 2023):
    for mm in ['01', '02', '03','04','05','06','07','08','09','10','11','12']:
      mkdir(f'DATA/forb/{yyyy}/{mm}')
  dates = []
  with open('DATA/iso/trade_dates.iso', 'r') as rf:
    for ln in rf:
      itm = ln.split(',')
      if len(itm) > 1:
        date = int(itm[0])
        if date > 20150000:
          dates.append(itm[0])
  
  for date in dates:
    yyyy = date[:4]
    mm = date[4:6]
    with open(f'DATA/forb/{yyyy}/{mm}/{date}', 'w') as f:
      continue
    print(date)

#cursor.execute("SHOW DATABASES")

# for x in cursor:
#   print(x)
# cn.close()
