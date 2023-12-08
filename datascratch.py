import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from datetime import datetime
from datetime import timedelta
from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient

############################ Parameters
USERNAME = 'N000183558'
PASSWORD = '83558'
# PIWEB_API_URL = "https://10.114.134.43:20000/piwebapi"
# PI_SERVER_URL = 'pi:\\10.114.134.1\\'
# PARAMETERS_PATH = './Data_Scratch/parameters.xlsx'
# USERNAME = 'N11AppDev'
# PASSWORD = 'n11appdev'
PIWEB_API_URL = "https://10.114.134.43:20000/piwebapi"
PI_SERVER_URL = 'pi:\\10.114.134.1\\'
PARAMETERS_PATH = './Data_Scratch/parameters.xlsx'
############################

# def readData(path):
#     path_split = path.split(".")
#     data_form = path_split[-1]
    
#     if(data_form == "csv"):
#         data = pd.read_csv(path, engine = 'python')
#     elif(data_form == "xlsx"):
#         data = pd.read_excel(path)
        
#     return data

# 取得 PI 的參數名稱 (鈞宇寫的)
def getPIParameters(pi_server_url = PI_SERVER_URL):
    PI_parameters = readData(PARAMETERS_PATH)
    name_list = list(PI_parameters.iloc[:, 0])
    parameters_list = list(PI_parameters.iloc[:, 1])
    
    parse_parameters = []
    for para in parameters_list:
        new_para = pi_server_url + para
        parse_parameters.append(new_para)
    
    return parse_parameters, name_list
# 回傳 已登入client的資訊
def PILogin(username = USERNAME, password = PASSWORD, piweb_api_url = PIWEB_API_URL):
    username = username
    password = password
    client = PIWebApiClient(baseUrl=piweb_api_url,
                            useKerberos=False,
                            username="npcjirtpms\\{}".format(username),
                            password="{}".format(password),
                            verifySsl=False,
                            useNtlm=True)  
    return client

def PICatchParametersData(start_time, end_time, point_list=None, time_interval='20s'):
    client = PILogin()
    
    if(point_list==None):point_list, name_list = getPIParameters()
    
    #start = datetime(2021,10,25,18,0,0) #datetime.now()-timedelta(hours=5)
    #end = datetime(2021,10,25,18,10,0)   #datetime.now() 
    data = client.data.get_multiple_interpolated_values(point_list,
                                                        start_time=start_time,
                                                        end_time=end_time,
                                                        interval=time_interval)
    index=data.Timestamp1
    fliter = ['Value'+str(i+1) for i in range(len(point_list))]
    data=data[fliter]
    data.columns = [i.split('\\')[-1] for i in point_list]
    data['Timestamp']=index
    data['Timestamp'] = pd.to_datetime(data['Timestamp'])
    data['Timestamp'] = (data['Timestamp']+pd.Timedelta(8, unit='h')).apply(lambda x: x.tz_localize(tz = None))
    data.index=data['Timestamp']
    data=data.drop(columns=['Timestamp'])
    # 更動 point_list, 讓 column name 單純秀出 tag name 
    tag_name = [x[17:] for x in point_list]
    data.columns = tag_name
    
    return data

def data_export(start_time, end_time,tagpoint_list, time_interval='10m'):
    data = PICatchParametersData(start_time, end_time,point_list = tagpoint_list,time_interval=time_interval)
    #data = domain_knowhow_transform(data.reset_index())
    if data is None:
        # print('Calc Falied or I/O Timeout')
        return 
    
    return data