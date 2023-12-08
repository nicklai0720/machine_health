import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta

import warnings
warnings.filterwarnings("ignore")

def NameDict(df, k , v):
    d = dict([(a, b) for a, b in zip(df[k], df[v])])
    return d

def MachineDict(op):
    """
    輸入: op資料
    輸出: 字典，keys: (str)設備編號，values: (list) 中文點位list(第0元素) 跟 英文點位 list
    """
    dict_part = dict()
    for part in set(op['設備編號(ERP)'].values):
        T_ch = op[op['設備編號(ERP)']==part]['Ch_Name'].to_list()
        T_en = op[op['設備編號(ERP)']==part]['Tag_Name'].to_list()
        T = []
        T.append(T_ch)
        T.append(T_en)
        dict_part[part]= T

    return dict_part

def TagExtract(op):

    # 全部點位 list
    all_tag = list(op['Ch_Name'].values)

    # IJL1-7 有啟用值 的點位只有一個, 就是生產狀態，此部分相對好判斷，但適用性不高
    # 看嘉一射出 excel 其他的只要啟用值是T, 都是生產狀態
    # 用簡單判斷
    enable_tag = list(op[op['有無啟用值']=='T']['Ch_Name'].values)

    # 判斷 無啟用值, 有設定值 的關鍵字 list
    temp = list(op[((op['有無啟用值']=='F') & (op['有無設定值']=='T'))]['Ch_Name'].values)
    
    nj_sheet_names = ['NJ01', 'NJ02', 'NJ03', 'NJ04', 'NJ05', 'NJ06', 'CJ01', 'CJ02']

    if op['設備編號(ERP)'].values[0] in nj_sheet_names:
        set_tag = [item.split('設定')[0] for item in temp if '設定' in item]
    else:
        set_tag = [item.split('_SV')[0] for item in temp if '_SV' in item]
    
    return all_tag, enable_tag, set_tag

# 計算分數跟看是否正常生產寫在同一個func

# 現在有點位清單, 計算分數
# df 是中文column的資料, op 是 op卡
def HealthValue(df, op):
    # 這邊是中文點為, set_tag 是 key word
    all_tag, enable_tag, set_tag = TagExtract(op)
    df_res = pd.DataFrame(index = df.index, columns = set_tag)
    df_final = pd.DataFrame(index = ['residual', 'f_i1', 'F_i'], columns = set_tag)

    if (df[enable_tag[0]].dtypes == object) or (df[enable_tag[0]].mean()==0.0):
        work_state = '非正常生產狀態'
        df_res[df_res != 0] = 0
        df_final[df_final != 0] = 0
        score = 0
    
    elif (df[enable_tag[0]].dtypes != object) and (df[enable_tag[0]].mean()==1.0):
        work_state = '正常生產狀態'
        for i, col in enumerate(set_tag):
            usecol = [ item for item in all_tag if col in item]
            if (df[usecol[0]].dtypes !=object) and (df[usecol[1]].dtypes != object):
                df_res[col] = abs(df[usecol[0]]-df[usecol[1]])
            else:
                df_res[col] = np.zeros(len(df[usecol[0]]))
            limit = op[op['Ch_Name']==usecol[0]]['允差值'].values[0]
            meannow = df_res[col].mean()
            
            # 個別扣分跟點位分數
            f_i1 = 20*(meannow/limit)
            temp = 100-f_i1
            if temp < 0:
                temp =0
            df_final[col]=[limit, f_i1, temp]
        # df.loc['..']是 series, df.loc[['..']] 是 dataframe
        if df_final.loc['F_i'].min() <80:
            score = df_final.loc['F_i'].min()
        elif df_final.loc['F_i'].min() >= 80:
            score = df_final.loc['F_i'].mean()
    else:
        work_state ='正在啟動中'
        df_res[df_res != 0] = 0
        df_final[df_final != 0] = 0
        score = 0

    return work_state, df_res, df_final, score

# 輸出 errtable dataframe
# 輸入形式為index 時間 (一筆資料 / 1 row), column 各點位(加上總分), 值為健康度分數  
def ErrTable(df):
    time, tag, tagscore = [], [], []
    for i in df.columns.to_list():
        timestr = datetime.strftime(df[i].index.to_pydatetime()[0], '%Y-%m-%d %H:%M:%S')
        time.append(timestr)
        tag.append(i)
        tagscore.append(round(df[i][0], 2))

    temp = {
        'time': time,
        'tag': tag,
        'tagscore': tagscore,
    }
    ansdf = pd.DataFrame().from_dict(temp)
    #score = df['區段總分數']

    return ansdf