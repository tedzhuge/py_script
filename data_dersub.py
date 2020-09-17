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
        
  def output_dersub(self, start_date):
    calendar_dates = get_calendar_dates()
    cd_sz = len(calendar_dates)
    trd_dates = get_trd_dates()

    LIST_DER_REPORT_SUBTABLE = [
      'Time_Year',#0,+3
      'Quarter',#1
      'Forecast_Income',#2
      'Forecast_Profit',#3
      'Forecast_Income_Share',#4
      'Forecast_Return_Cash_Share',#5
      'Forecast_Return_Capital_Share',#6
      'Forecast_Return',#7
      'R_Tar1',#8
      'R_Tar2',#9
      'R_Tar3',#10
      'R_Tar5',#11
      'Forecast_Income_0',#12
      'Forecast_Profit_0',#13
      'Profit_Flag'#14
    ]
    LIST_DER_REPORT_RESEARCH=[
      'Code',#0,+18
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
      'Text5',#11
      'Text8',#12
      'Price_Current',#13
      'Attention',#14
      'Attention_Name',#15
      'Score_Flag'#16
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
        
        if start_date <= trd_date_:
          print(f'{c_date_} into dersub.{trd_date_}')
          self.cursor.execute(f"""
          SELECT a.EntryDate, a.EntryTime, {itms_DER_REPORT_SUBTABLE}, {itms_DER_REPORT_RESEARCH}, a.Report_Search_ID 
          FROM Der_Report_Subtable a
          LEFT JOIN Der_Report_Research b
            ON b.ID = a.Report_Search_ID
          WHERE a.EntryDate = {c_date_} 
          ORDER BY a.EntryDate, a.EntryTime, b.Code
          """) 
          data = self.cursor.fetchall()
          
          with open(output_path, 'a') as f:
            id= 0
            for row in data:
              #3+15+17+1=36
              str_row = f"{id}|{row[0].strftime('%Y%m%d')}|{row[1].replace(':','')}|{'|'.join([str(elem) for elem in row[2:]])}\n"
              f.write(f'{str_row}')
              id+=1
        idx_ += 1
      
  def output_research(self):
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
      'Text5',#11
      'Text8',#12
      'Price_Current',#13
      'Attention',#14
      'Attention_Name',#15
      'Score_Flag'#16
    ]
    
    _itms_DER_REPORT_RESEARCH=','.join(LIST_DER_REPORT_RESEARCH)
    self.cursor.execute(f'SELECT ID, {_itms_DER_REPORT_RESEARCH},EntryDate,EntryTime FROM Der_Report_Research')
    data = self.cursor.fetchall()
    with open('DATA/gg/report.iso', 'w') as wf:
      for row in data:
        str_row = ','.join([str(elem) for elem in row])
        wf.write(f'{str_row}\n')


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
      
