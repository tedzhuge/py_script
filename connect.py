#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import importlib
import sys
import array
import os
import mysql.connector
import datetime
wd ={
      'user': 'reader',
  'password': 'reader',
  'host': '192.168.101.120',
  'port': 3306,
  'database': 'WindDB',
  'raise_on_warnings': True
}
gg1= {'user': 'reader',
  'password': 'reader',
  'host': '192.168.101.120',
  'port': 3306,
  'database': 'GOGOAL_V1',
  'raise_on_warnings': True
  }
gg = {
  'user': 'reader',
  'password': 'reader',
  'host': '192.168.101.120',
  'port': 3306,
  'database': 'GOGOAL',
  'raise_on_warnings': True
}
gg_other = {
  'user': 'reader',
  'password': 'reader',
  'host': '192.168.101.204',
  'port': 3306,
  'database': 'GOGOAL',
  'raise_on_warnings': True
}

sql_list_db="""Show Tables"""
sql_list_table= """Show Databases""" #"DESCRIBE name_of_table"

sql_cc = """Select Industriesname
          From ASHAREINDUSTRIESCODE
         Where Substr(A.Wind_ind_code, 1, 4) From ASHAREINDUSTRIESCLASS A """.upper()


sql_base = 'SELECT * FROM ASHAREEODPRICES WHERE TRADE_DT = 20200817'
sql_raw_ = """SELECT S_INFO_WINDCODE, CRNCY_CODE, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_CLOSE, S_DQ_HIGH, 
S_DQ_LOW, S_DQ_PCTCHANGE, S_DQ_VOLUME, S_DQ_AMOUNT, S_DQ_ADJPRECLOSE, S_DQ_ADJOPEN, S_DQ_ADJCLOSE, S_DQ_ADJHIGH
, S_DQ_LOW,S_DQ_ADJFACTOR, S_DQ_AVGPRICE, S_DQ_TRADESTATUS FROM ASHAREEODPRICES WHERE TRADE_DT = 20200817"""

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
  def __init__(self, name):
    self.cn_ = mysql.connector.connect(**name)
    self.cursor = self.cn_.cursor()
    self.data = []

  def mkdir(self):
    for yyyy in range(2015, 2023):
      for mm in ['01', '02', '03','04','05','06','07','08','09','10','11','12']:
        mkdir(f'DATA/raw_prc/{yyyy}/{mm}')
    dates = get_calendar_dates()
    for date_ in dates:
      yyyy = date_[:4]
      mm = date_[4:6]
      dd = date_[6:8]
      mkdir(f'DATA/gg/data/{yyyy}/{mm}/{dd}')
        
    mkdir(f'DATA/iso')



  def output_date(self):
    self.cursor.execute("""SELECT 
      TRADE_DAYS, GROUP_CONCAT(DISTINCT S_INFO_EXCHMARKET
                        SEPARATOR '|')
    FROM ASHARECALENDAR 
    GROUP BY TRADE_DAYS
    ORDER BY TRADE_DAYS""")
    self._output('DATA/iso/trade_dates.iso')


  def output_raw_prc(self, date_start=20150000, force_= False):
    dates = get_trd_dates(date_start)
    for date_ in dates:
      self.cursor.execute(f"""SELECT S_INFO_WINDCODE, CRNCY_CODE, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_CLOSE, S_DQ_HIGH, 
        S_DQ_LOW, S_DQ_PCTCHANGE, S_DQ_VOLUME, S_DQ_AMOUNT, S_DQ_ADJPRECLOSE, S_DQ_ADJOPEN, S_DQ_ADJCLOSE, S_DQ_ADJHIGH
        , S_DQ_ADJLOW,S_DQ_ADJFACTOR, S_DQ_AVGPRICE, S_DQ_TRADESTATUS, S_DQ_TRADESTATUSCODE
        FROM ASHAREEODPRICES 
        WHERE TRADE_DT = {date_}""")
      yyyy = date_[:4]
      mm = date_[4:6]
      self._output(f'DATA/raw_prc/{yyyy}/{mm}/raw_prc.{date_}', force_)
      print(f'raw_prc.{date_}')


  def output_citics_ind(self, force_= False):    
    self.cursor.execute(f"""SELECT a.S_INFO_WINDCODE, a.CITICS_IND_CODE, a.ENTRY_DT, a.REMOVE_DT, a.CUR_SIGN
      FROM ASHAREINDUSTRIESCLASSCITICS a
      ORDER BY S_INFO_WINDCODE, ENTRY_DT""")
    self._output(f'DATA/iso/citics_ind.iso', force_)
    self.cursor.execute(f"""SELECT INDUSTRIESCODE, INDUSTRIESNAME, LEVELNUM, USED, MEMO, CHINESEDEFINITION, WIND_NAME_ENG
      FROM ASHAREINDUSTRIESCODE 
      ORDER BY INDUSTRIESCODE, LEVELNUM""")
    self._output(f'DATA/iso/industry_code.iso', force_)

  def output_code_ind(self, force_= False):
    self.cursor.execute(f"""SELECT * FROM ASHAREINDUSTRIESCODE
        ORDER BY LEVELNUM, INDUSTRIESCODE
    """)
    self._output('DATA/iso/code_ind.iso', force_)


  def output_shares(self, force_ = False):
    self.cursor.execute(f"""SELECT S_INFO_WINDCODE, ANN_DT, CHANGE_DT, CHANGE_DT1, 
    TOT_SHR, FLOAT_SHR, NON_TRADABLE_SHR, S_SHARE_TOTALA, FLOAT_A_SHR, RESTRICTED_A_SHR,FLOAT_B_SHR, FLOAT_H_SHR, FLOAT_OVERSEAS_SHR,	
    S_SHARE_TOTALTRADABLE, S_SHARE_TOTALRESTRICTED
      	FROM ASHARECAPITALIZATION
    ORDER BY S_INFO_WINDCODE, CHANGE_DT""")
    self._output(f'DATA/iso/shares.iso', force_)


  def output_sample(self):
    date_ = 20150105
    
    # _itms_DER_REPORT_RESEARCH=' Code,Code_Name, Title, Type_ID,Organ_ID,Author, Score_ID,Organ_Score_ID, Create_Date, Pause_ID, Into_Date, Text1, Text3, Text5, Text6, Text8, Price_Current, Capital_Current, Forecast_Return, Attention, Attention_Name,Score_Flag'
    # _itms_DER_REPORT_SUBTABLE='Time_Year, Quarter, Forecast_Income, Forecast_Profit,Forecast_Income_Share, Forecast_Return_Cash_Share, Forecast_Return_Capital_Share, Forecast_Return,  R_Tar1,R_Tar2, R_Tar3, R_Tar4, R_Tar5, R_Tar_Date1, R_Tar_Date2, Forecast_Income_0, Forecast_Profit_0, Profit_Flag, EntryDate, EntryTime '
    
    itms_DER_REPORT_RESEARCH=' b.Code, b.Code_Name, b.Title, b.Type_ID, b.Organ_ID,Author, b.Score_ID, b.Organ_Score_ID, b.Create_Date, b.Pause_ID, b.Into_Date, b.Text1, b.Text3, b.Text5, b.Text6, b.Text8, b.Price_Current, b.Capital_Current, b.Forecast_Return, b.Attention, b.Attention_Name, b.Score_Flag'
    itms_DER_REPORT_SUBTABLE=' a.Time_Year, a.Quarter, a.Forecast_Income, a.Forecast_Profit, a.Forecast_Income_Share, a.Forecast_Return_Cash_Share, a.Forecast_Return_Capital_Share, a.Forecast_Return, a.R_Tar1, a.R_Tar2, a.R_Tar3, a.R_Tar4, a.R_Tar5, a.R_Tar_Date1, a.R_Tar_Date2, a.Forecast_Income_0, a.Forecast_Profit_0, a.Profit_Flag, a.EntryDate, a.EntryTime '
    
    

    # self.cursor.execute(f"""SELECT a.Report_Search_ID, {itms_DER_REPORT_SUBTABLE} FROM Der_Report_Subtable AS a
    #     WHERE EntryDate = {date_}""") 
    # data = self.cursor.fetchall()
    # with open('a.txt', 'w') as wf:
    #   for row in data:
    #     str_row = ','.join([str(elem) for elem in row])
    #     wf.write(f'{str_row}\n')
    
    
    # self.cursor.execute(f"""SELECT b.ID,  {itms_DER_REPORT_RESEARCH} FROM Der_Report_Research AS b
    #     WHERE EntryDate = {date_}""") 
    # data = self.cursor.fetchall()
    # with open('b.txt', 'w') as wf:
    #   for row in data:    
    #     str_row = ','.join([str(elem) for elem in row])
    #     wf.write(f'{str_row}\n')

    for date in dates:
      yyyy = date[:4]
      mm = date[4:6]
      with open(f'DATA/forb/{yyyy}/{mm}/{date}', 'w') as f:
        continue
      print(date)

      self.cursor.execute(f"""
        SELECT a.Report_Search_ID, {itms_DER_REPORT_SUBTABLE}, {itms_DER_REPORT_RESEARCH} FROM 
                (SELECT Report_Search_ID, {_itms_DER_REPORT_SUBTABLE} FROM Der_Report_Subtable  WHERE EntryDate = {date_}) a
      LEFT JOIN (SELECT ID, {_itms_DER_REPORT_RESEARCH} FROM Der_Report_Research WHERE EntryDate = {date_}) b
          ON b.ID = a.Report_Search_ID
          """) 
      data = self.cursor.fetchall()
      with open('c.txt', 'w') as wf:
        for row in data:
          str_row = ','.join([str(elem) for elem in row])
          wf.write(f'{str_row}\n')

  def output_forb():
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
          
          str_row = '|'.join([str(elem) for elem in row])
          f.write(f'{str_row}\n')
    else:
      print(f"Can not write force = {force_}")
      




