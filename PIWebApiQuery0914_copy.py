# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 09:38:11 2019

@author: N000174156
"""

import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from datetime import datetime
from datetime import timedelta
from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient


def QueryData(client, pi_point, start, end, counts=150000):
    try:
        print(pi_point)
        data = pd.DataFrame()
        diff = 1             
        while(diff): 
#            print(diff)
            print(start)         
            data_temp = client.data.get_recorded_values(pi_point,
                                                        start_time=start, 
                                                        end_time=end, 
                                                        max_count=counts,
                                                        selected_fields="items.timestamp;items.value") 
#            data = data.append(data_temp)    
            data = pd.concat([data, data_temp], ignore_index=True)
            data = data.sort_values(by=['Timestamp'])
            diff = (pd.to_datetime(data.iloc[-1].Timestamp)-
                    pd.to_datetime(data.iloc[-2].Timestamp)).seconds
            # 更新後的 start 是 先取 end (抓資料8小時前的格式) , 加了8 小時之後, 再把 timezone 設為 none
            start = (pd.to_datetime(data.iloc[-1].Timestamp)+pd.Timedelta(8, unit='h')).tz_localize(tz = None)
            start = start.replace(microsecond=0,nanosecond=0)
            
        data['Timestamp'] = pd.to_datetime(data['Timestamp'])
        data['Timestamp'] = (data['Timestamp']+pd.Timedelta(8, unit='h')).apply(lambda x: x.tz_localize(tz = None))    
        data['Timestamp'] = data['Timestamp'].apply(lambda x: x.replace(microsecond=0,nanosecond=0)) 
        data['Value'] = pd.to_numeric(data['Value'], errors='coerce')
        data.sort_values(by=['Timestamp'], inplace=True)
        data.dropna(subset=['Value'], inplace=True)
        data.drop_duplicates(subset=['Timestamp'], inplace=True)
        data.reset_index(drop=True, inplace=True) 
        return data
    
    except Exception as e:
        print(e)
        raise
        # return pd.DataFrame(), e.status
 
    
def Get(client, point, start, end, interval):    
    try:
        data = QueryData(client, point, start, end, counts=150000)          
        if len(data)!=0:
            data.rename(columns={'Value': '{}'.format(point.split('\\')[-1])}, inplace=True)
            data.index = pd.to_datetime(data.Timestamp)                                       
            rule = "{}T".format(interval)
            data = data.resample(rule).mean() 
            # data = data.tail(1)
            return data 
    except Exception as e:
        print(e)
        raise 
  
        
def GetLast(client, point, start, end, count=60):    
    try:        
        data_temp = client.data.get_recorded_values(point,
                                                    start_time=start, 
                                                    end_time=end, 
                                                    max_count=count,
                                                    selected_fields="items.timestamp;items.value")
        if len(data_temp)!=0:
            data_temp = data_temp.tail(1)
            data_temp['Timestamp'] = pd.to_datetime(data_temp['Timestamp'])
            data_temp['Timestamp'] = (data_temp['Timestamp']+pd.Timedelta(8, unit='h')).apply(lambda x: x.tz_localize(tz = None))
            data_temp['Timestamp'] = data_temp['Timestamp'].apply(lambda x: x.replace(second=0, microsecond=0, nanosecond=0)) 
            # data_temp['Timestamp'] = end              
            return data_temp
    except Exception as e:
        print(e)
        return pd.DataFrame()
  
      
def PILogin():
    username = "N000183558"
    password = "83558"
    client = PIWebApiClient(baseUrl="https://10.114.134.43:20000/piwebapi",
                            useKerberos=False,
                            username="npcjirtpms\\{}".format(username),
                            password="{}".format(password),
                            verifySsl=False,
                            useNtlm=True)  
    return client

# 直接給時間應該可以，不用 datetime.strftime(datetime.now(), time_format)
def data_export1(start_time, end_time, tagpoint_list, time_interval = 10):
    client = PILogin()
    final_df = pd.DataFrame()
    df_list = []
    df_list = [Get(client, point, start_time, end_time, time_interval) for point in tagpoint_list]
    final_df = pd.concat(df_list, axis=1)

    return final_df

        
    
if __name__ == '__main__':    
    
    ############################################################
    ''' ### example 1(single points) '''
    
    client = PILogin()
    print(client)
    point = "pi:\\\\10.114.134.1\\JI4_HHR3_S1_TEMP"    
    start = datetime(2020,9,1,6,0,0) #datetime.now()-timedelta(hours=5)
    end = datetime(2020,9,1,8,00,0)   #datetime.now()
    data = QueryData(client, point, start, end, counts=150000)     
    ############################################################
    
    client = PILogin()
    print(client)
    point = "pi:\\\\10.114.134.1\\JI4_HHR3_S1_TEMP"    
    start = datetime(2022,5,1,0,0,0) #datetime.now()-timedelta(hours=5)
    end = datetime(2022,5,3,0,00,0)   #datetime.now()
    data  =QueryData(client, point, start, end, counts = 150000)
    

    ############################################################
    ''' ### example 2 (multiple points) '''
    
    client = PILogin()
    point_list = ["pi:\\10.114.134.1\\JI4_HHR2_S1_TEMP",
                  "pi:\\10.114.134.1\JI4_HHR2_S2_TEMP"]
    start = datetime.now()-timedelta(hours=5)
    end = datetime.now() 
    # start = datetime(2020,9,1,0,0,0) #datetime.now()-timedelta(hours=5)
    # end = datetime(2020,9,2,0,0,0)   #datetime.now() 
    interval = 1
        
    final_df = pd.DataFrame()       
    df_list = []
    df_list = [Get(client, point, start, end, interval) for point in point_list]
    final_df = pd.concat(df_list, axis=1)
    print(final_df)         
    
    ############################################################
