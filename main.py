import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objs as go
import random, calendar
from datetime import datetime
from datetime import timedelta
import pickle, time
import re
from dash import Dash, dcc, html, Input, Output, dash_table, State, callback_context
import dash_daq as daq

from datascratch import getPIParameters, PILogin, PICatchParametersData, data_export
from PIWebApiQuery0914_copy import QueryData, Get, GetLast, PILogin, data_export1

from DataPrepare import NameDict, MachineDict, TagExtract, HealthValue, ErrTable

app = Dash(__name__)

# step 1: 讀進 IJL1-7的 op 卡檔
# step 2: 形成 pi 點位路徑
# step 3: 建立英中轉換的 dict , key 英文點位, value 中文點位 / 之後以中文來做篩選
# step 4: 建立設備編號對應的字典，values 是中英文點位
pisource = 'pi:\\10.114.134.1\\'
# 射出op卡讀取
ij_allop = pd.ExcelFile('嘉一廠_OP_射出.xlsx')
ij_sheet_names = ij_allop.sheet_names

for i in range(len(ij_sheet_names)):
    tmp_name = ij_sheet_names[i]
    tmp_df = ij_allop.parse(tmp_name)
    globals()[tmp_name + '_op'] = tmp_df
    globals()[tmp_name + '_pitag'] = [pisource + elm for elm in list(tmp_df['Tag_Name'])]
    globals()[tmp_name + '_namedict'] = NameDict(tmp_df, 'Tag_Name', 'Ch_Name')
    globals()[tmp_name + '_machinedict'] = MachineDict(tmp_df)

# 棧板op卡讀取
nj_allop = pd.ExcelFile('嘉一廠_OP_棧板6.xlsx')
nj_sheet_names = nj_allop.sheet_names

for sheet in nj_sheet_names:
    tmp_df = nj_allop.parse(sheet)
    tmp_list = list(tmp_df['Ch_Name'].values)
    if sheet in ['NJ05', 'NJ06']:
        for i, col in enumerate(tmp_list):
            if '生產狀態' in col:
                continue
            elif '設定_':
                tmp_list[i] = tmp_list[i].replace('設定_','')
    elif sheet in ['CJ01','CJ02']:
        for i, col in enumerate(tmp_list):
            if '生產狀態' in col:
                continue
            else:
                reg = re.search('[0-9]',tmp_list[i])
                tmp_list[i] = tmp_list[i].replace(reg.group(),'').replace('溫度', '溫度'+reg.group())
    else:
        for i, col in enumerate(tmp_list):
            if '生產狀態' in col:
                continue
            else:
                reg = re.search('[0-9a-zA-Z]+', tmp_list[i])
                tmp_list[i] = tmp_list[i].replace(reg.group(), '').replace('_', reg.group())
        
    tmp_df['Ch_Name'] = tmp_list
    globals()[sheet+'_op'] = tmp_df
    globals()[sheet + '_pitag'] = [pisource + elm for elm in list(tmp_df['Tag_Name'])]
    globals()[sheet + '_namedict'] = NameDict(tmp_df, 'Tag_Name', 'Ch_Name')
    globals()[sheet + '_machinedict'] = MachineDict(tmp_df)


all_sheet_names = ij_sheet_names + nj_sheet_names

time_format="%y-%m-%d %H:%M:%S"

app.layout = html.Div(children=[
    html.Header([
        html.H1(
            className='title1',
            children=['南亞塑三部-設備健康度']),
        html.Br(),
        html.Nav(
            html.Ul(children=[
                html.Li(html.A(href='', children=['嘉義一廠'])),
                html.Li(html.A(href='', children=['嘉義二廠'])),
                html.Li(html.A(href='http://10.3.70.116:9999/', children=['嘉義四廠'])),
                html.Li(html.A(href='', children=['林口廠'])),
                html.Li(html.A(href='', children=['林園廠'])),
                html.Li(html.A(href='', children=['工塑廠'])),
                html.Li(html.A(href='', children=['仁武一廠']))
                ],className='menu')
            ),
        html.H1(children=['設備健康度']),
        html.H2(children=['嘉義一廠']),
        html.Br(),
        html.Div(children=[
            dcc.Dropdown(all_sheet_names, all_sheet_names[0], id='machine'),
            html.Button(children=['確認'], id='machine_sure', n_clicks=0)
            ],style={'width':'48%', 'display':'inline-block'}),

        html.H2(id= 'ifworking')
    ]),
    html.Br(),
    # 健康度分數
    html.Div(children=[
        html.Div([
            daq.Gauge(
                id='ijl_score',
                color = '#019858',
                showCurrentValue = True,
                label='健康度分數',
                value=80,
                min=0,
                max=100),
                
            # 呈現 errtable (LOADING)
            dcc.Loading(
                id="loading-1",
                children=[html.Div(children=[], id = 'ijl_err')],
                type="circle",
                fullscreen=True,)
        ],style={'width':'20%', 'display':'inline-block',
        'overflow':'auto',"margin-left": "5px","margin-right": "5px"}),

    ], style={'display': 'flex'}),

    dcc.Interval(
        id='interval_component',
        interval=300*1000, # 5分鐘跑一次, in milliseconds
        n_intervals=0
    )
],style ={ 'width': '90%', 'margin':'0 auto'})

@app.callback(
    Output('ifworking', 'children'),
    Output('ijl_score','value'),
    Output('ijl_score', 'label'),
    Output('ijl_err', 'children'),
    Input('machine_sure', 'n_clicks'),
    Input('interval_component','n_intervals'),
    State('machine','value')
)
def update_table(n_clicks, n, selected_machine):
    if selected_machine == None:
        return 0, '尚未選取確認', dash_table.DataTable()

    start = datetime.now() - timedelta(minutes = 15)
    end = datetime.now()

    op = globals()[selected_machine+'_op']
    pitag = globals()[selected_machine+'_pitag']
    namedict = globals()[selected_machine+'_namedict']
    machinedict = globals()[selected_machine+'_machinedict']

    df = data_export(datetime.strftime(datetime.now()-timedelta(minutes=15), time_format),
                datetime.strftime(datetime.now(), time_format),
                tagpoint_list=pitag, time_interval='1m')

    chdf = df.rename(columns = namedict)

    state, df_res, df_final, final_score = HealthValue(chdf, op)

    df_score = df_final.loc[['F_i'],:]
    df_score.index = [datetime.now()]

    df_err = ErrTable(df_score)
    df_err = df_err.sort_values(by = 'tagscore', ascending = True)

    return f'機台狀態: {state}', round(final_score,2), f'{selected_machine} 健康度分數', dash_table.DataTable(
                                                                                            df_err.to_dict('records'),
                                                                                            [{"name": i, "id": i} for i in df_err.columns],
                                                                                            page_size = 10,
                                                                                            fill_width=False,
                                                                                            style_data_conditional=[
                                                                                                {'if': {'filter_query': '{tagscore} < 80','column_id': 'tagscore'},
                                                                                                'backgroundColor': '#FF4136',
                                                                                                'color': 'white'},
                                                                                                ],
                                                                                            style_cell_conditional=[
                                                                                                {'if': {'column_id': 'time'},'width': '30%'},
                                                                                                {'if': {'column_id': 'tag'},'width': '30%'},
                                                                                                {'if': {'column_id': 'tagscore'},'width': '30%'},
                                                                                                ]
                                                                                                )

if __name__ == '__main__':
    app.run_server(debug=True, host='10.114.70.170', port=9999)


