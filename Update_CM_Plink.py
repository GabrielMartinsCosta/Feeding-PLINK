import pandas as pd
import os
from sqlalchemy import create_engine
import datetime as dt
import time

import Update_CM as uc

days_to_get = 7
plink = 'C:/PBI/Claro/data/extras/PLink/GENOPT/'
plink_file = '【NPOC】Task Table.csv'
db_user = 'Juliano Correa'

uc.__init__(db_user, days_to_get)
cm_form_open = uc.get_cm()

df_plink = pd.read_csv(plink + plink_file, parse_dates=True)
#df_plink_rfsh = df_plink[df_plink['Task List'] == "RF Shaping"].copy()
df_plink_rfsh = df_plink.copy()
df_plink_rfsh["Update Time"] = pd.to_datetime(df_plink_rfsh["Update Time"])
df_plink_rfsh = df_plink_rfsh[df_plink_rfsh["Update Time"].dt.date > uc.days_to_check(days_to_get)]
df_plink_rfsh.columns = df_plink_rfsh.columns.astype(str).str.replace(" ", "_")
df_plink_rfsh.columns = map(str.lower, df_plink_rfsh.columns)
#df_plink_rfsh_test = df_plink_rfsh[df_plink_rfsh.content == '55S01BAAMA9403']
#df_plink_rfsh_test.head()
df_plink_rfsh_test = df_plink_rfsh
df_plink_rfsh_test.head(2)

for row in df_plink_rfsh_test.itertuples():
    try:
        pl_tag = row.tag
        pl_object = row.content
        if len(pl_object) == 14:
            pl_cell = pl_object
            pl_node = ''
            if pl_cell[:1] == '5':
                pl_tec = 'nr'
            else:
                pl_tec = 'lte'
        elif len(pl_object) == 10:
            pl_cell = ''

            pl_node = pl_object[-7:]
            if '4G' in pl_tag:
                pl_tec = 'lte'
            elif '5G' in pl_tag:
                pl_tec = 'nr'
        elif len(pl_object) == 11:
            pl_cell = ''

            pl_node = pl_object[-7:]
            if '4G' in pl_tag:
                pl_tec = 'lte'
            elif '5G' in pl_tag:
                pl_tec = 'nr'
        pl_taskid = row.taskid
        pl_note = row.note
        pl_s_date = row.start_date
        pl_s_date = row.due_date
        pl_completed_or_not = row.completed_or_not
        pl_complete_time = str(row.complete_time)
        pl_update_time = str(row.update_time)
        pl_owner = ''
        pl_tasklist = row.task_list
    
        if pl_tag.find('RTL') != -1:
            pl_owner = 'RTL'
        elif pl_tag.find('GEN OPT') != -1:
            pl_owner = 'GEN OPT'
    
        
        _data_plan = pl_s_date[:10]
        _data_update = pl_update_time[:10]
        if pl_complete_time != 'nan':
            _data_term = pl_complete_time[:10]
        else:
            _data_term = ''    
        _acao = f'{pl_note}'
        if pl_tasklist == 'RF Shaping':
            uc.check_cell_task(pl_cell, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_note, pl_tec, cm_form_open, db_user,pl_completed_or_not, _data_update)
            print(pl_node, pl_cell,  pl_note, pl_owner,  pl_taskid, pl_s_date[:10], pl_tec, pl_completed_or_not, pl_complete_time, pl_tasklist)
            print('--------------')
        if pl_tasklist == 'Traffic Balance':
            uc.check_node_task(pl_node, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_note, pl_tec, cm_form_open, db_user,pl_completed_or_not, _data_update)
            print(pl_node, pl_cell,  pl_note, pl_owner,  pl_taskid, pl_s_date[:10], pl_tec, pl_completed_or_not, pl_complete_time, pl_tasklist)
            print('--------------')
        if pl_tasklist == 'Logic Parameter':
            uc.check_node_task(pl_node, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_note, pl_tec, cm_form_open, db_user,pl_completed_or_not, _data_update)
            print(pl_node, pl_cell,  pl_note, pl_owner,  pl_taskid, pl_s_date[:10], pl_tec, pl_completed_or_not, pl_complete_time, pl_tasklist)
            print('--------------') 
    except Exception as e:
        print(e, pl_object)
    

    #ULTIMO TESTADO