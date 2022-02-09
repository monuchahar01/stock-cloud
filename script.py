#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  8 11:32:53 2022

@author: monu
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  6 20:39:16 2022

@author: monu
"""

import time
import requests
import gspread
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import datetime


def analysis():
    values = {'username': 'Chaharmonu','password': 'Screener@123'}
    row_data=[]

    try:
        
        for page in range(1,116):
            
            url='https://www.screener.in/screens/605012/sample/?sort=market+capitalization&order=desc&page={}'.format(page)
            
            requests.post(url, data=values)
            time.sleep(1)
            texts=requests.get(url)
            text_utf=texts.text.encode('utf=8')
            soup = BeautifulSoup(text_utf, "html.parser")
            table_body=soup.find('table', {'class': 'data-table text-nowrap striped'})
            
            for row in table_body.find_all('tr'):
                col=row.find_all('td')
                col=[ele.text.strip() for ele in col]
                row_data.append(col)
                
            #for header         
        header=[]
        for i in soup.find_all('th'):
            col_name=i.text.strip().lower().replace(" ","")
            header.append(col_name)
        header=header[0:11]
        header
                 
        df_live=pd.DataFrame(row_data, columns=header)
        df_live= df_live.dropna(subset=['name'])
        df_live
             #df.to_csv('data_stocks.csv')
        scope=['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file',
           'https://www.googleapis.com/auth/drive','https://spreadsheets.google.com/feeds']
    
        cred=ServiceAccountCredentials.from_json_keyfile_name('stocks.json',scope)
        client=gspread.authorize(cred)
         
         #sample=client.open('sample').sheet1
         
         #sample.update_cell(1,2,"monu")
         
         
        sh = client.open('Stocks_Analysis')
        main = sh.worksheet("main")
        live=sh.worksheet('live')
        live.update([df_live.columns.values.tolist()] + df_live.values.tolist())
    
        df_main = pd.DataFrame(main.get_all_records())
            
    
        #df_live=df_live.drop(['Unnamed: 0'], axis = 1)
        df_live = df_live.drop_duplicates(subset = ['name'],keep = 'last').reset_index(drop = True)
        #df_main=df_main.drop(['Unnamed: 0'], axis = 1)
        df_main=pd.merge(df_main,df_live,how='left',on='name')
        df_list=['s.no._y','name','cmp\nrs._y',
           'p/e_y', 'marcap\nrs.cr._y', 'divyld\n%_y', 'npqtr\nrs.cr._y',
           'qtrprofitvar\n%_y', 'salesqtr\nrs.cr._y', 'qtrsalesvar\n%_y',
           'roce\n%_y','tmp1', 'tmp2', 'tmp3', 'tmp4', 'tmp5', 'tmp6', 'tmp7',
           'tmp8', 'tmp9', 'tmp10', 'L_Prc', 'H_Prc', 'B_Prc', 'T_Prc','B_Prc_date',
           'T_Prc_date', 'Prof_marg_days', 'Prof%','C_Prof%']
        df_main=df_main[df_list]
        df_main.rename(columns = {'s.no._y': 's.no.', 'cmp\nrs._y': 'cmp\nrs.','p/e_y':'p/e', 'marcap\nrs.cr._y':'marcap\nrs.cr.', 'divyld\n%_y':'divyld\n%', 'npqtr\nrs.cr._y':'npqtr\nrs.cr.',
           'qtrprofitvar\n%_y':'qtrprofitvar\n%', 'salesqtr\nrs.cr._y':'salesqtr\nrs.cr.', 'qtrsalesvar\n%_y':'qtrsalesvar\n%',
           'roce\n%_y':'roce\n%'}, inplace = True)
        file1 = open("data.txt","r")  
        number = file1.read() 
        file1.close() 
        run_col='tmp'+str(number)
        run_col=run_col.strip()
        print('the value of run_col is {}'.format(run_col))
        def update_tmp(x):
            df_main[x]=df_main['cmp\nrs.']
            #df_main[x].astype(str).astype(int)
        update_tmp(x=run_col) 
        df_main.dtypes
    
        tmp_lst=['tmp1','tmp2','tmp3','tmp4','tmp5','tmp6','tmp7','tmp8','tmp9','tmp10']
        today = datetime.date.today()
        rem_lst=['H_Prc','L_Prc','B_Prc','T_Prc','Prof%','C_Prof%','cmp\nrs.']
        con_num=tmp_lst + rem_lst
        for col in con_num:
            df_main[col] = pd.to_numeric(df_main[col])
            
        df_main['T_Prc_date'] =  pd.to_datetime(df_main['T_Prc_date'])
        df_main['B_Prc_date'] =  pd.to_datetime(df_main['B_Prc_date'])
        #df_main['Prof_marg_days'] =  pd.to_datetime(df_main['Prof_marg_days'] )                                   
                                                                                
        df_main.dtypes
        
        df_main['Today']=today.strftime("%Y-%m-%d")

           
        df_main['H_Prc']=df_main[tmp_lst].max(axis=1) 
        df_main['L_Prc']=df_main[tmp_lst].min(axis=1)

        df_main['B_Prc'].fillna(df_main['L_Prc'],inplace=True)
        df_main['T_Prc'].fillna(df_main['H_Prc'],inplace=True)
        df_main['B_Prc_date'].fillna(today,inplace=True)
        df_main['T_Prc_date'].fillna(today,inplace=True)
        
        df_main['T_Prc_date'] = np.where(df_main['H_Prc'] > df_main['T_Prc'], df_main['Today'], df_main['T_Prc_date'])
        df_main['T_Prc'] = np.where(df_main['H_Prc'] > df_main['T_Prc'], df_main['H_Prc'], df_main['T_Prc'])
        
        df_main['B_Prc_date'] = np.where(df_main['L_Prc'] < df_main['B_Prc'], df_main['Today'], df_main['B_Prc_date'])
        df_main['B_Prc'] = np.where(df_main['L_Prc'] < df_main['B_Prc'], df_main['L_Prc'], df_main['B_Prc'])
        
        df_main['T_Prc_date'] =  pd.to_datetime(df_main['T_Prc_date'],format='%Y-%m-%d')
        df_main['B_Prc_date'] =  pd.to_datetime(df_main['B_Prc_date'],format='%Y-%m-%d')
           
                

        df_main.dtypes
    
        df_main['Prof_marg_days']=df_main['T_Prc_date']-df_main['B_Prc_date']
        df_main['Prof%']=round(((df_main['H_Prc']-df_main['B_Prc'])/df_main['B_Prc'])*100,3)
        df_main['C_Prof%']=round(((df_main['cmp\nrs.']-df_main['B_Prc'])/df_main['B_Prc'])*100,3)
        df_main=df_main.sort_values(by=['Prof%','C_Prof%'],ascending=False)
    
        df_main.dtypes
    
        #before upload to google sheet make it as all columns as string
    
        col_lst=df_main.columns.values.tolist()
        for col in col_lst:
            df_main[col]=df_main[col].astype(str)
          
        df_main.dtypes    
    
        main.update([df_main.columns.values.tolist()] + df_main.values.tolist())
    
        file1 = open("data.txt","w")
        number=str(int(number)+1)
        if number == '11':
            number='1'
        number = file1.write(number)  
        file1.close()
        print('Job is completed')
    except Exception as e:
        print(e)
    
        
if __name__=="__main__":
    start_time=time.time()
    analysis()
    stop_time=time.time()
    print("Time taken {}".format(stop_time-start_time))
