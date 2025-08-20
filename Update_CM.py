import pandas as pd
import os
from sqlalchemy import create_engine, text
import datetime as dt
import time
start_time = time.time()
today = dt.date.today()
today_str = today.strftime("%Y-%m-%d")
day_to_start = today - dt.timedelta(days=7)


def __init__(db_user, days_to_get):
    _db_user = db_user
    _days_to_get = days_to_get
    #cm_form_open = get_cm()


def days_to_check(days_to_get):
    start_time = time.time()
    today = dt.date.today()
    today_str = today.strftime("%Y-%m-%d")
    day_to_start = today - dt.timedelta(days=days_to_get)
    return day_to_start

# Get Conn
def get_conn():
    engine = create_engine('postgresql+psycopg2://biuser:Jdcmn1974@172.29.200.201:5432/npsmart')
    return engine

# Run query
def run_query(query):
    try:
        with get_conn().connect() as conn, conn.begin():
            conn.execute(text(query)) 
    except Exception as e:
        print(e)

# Get CM
def get_cm():
    query = """SELECT * FROM public.cellmapping_form_new where data_criacao > current_date - 180;"""
    with get_conn().connect() as conn, conn.begin():
        cm_form = pd.read_sql_query(query,conn)
#get open actions in db
    #cm_form_open = cm_form[cm_form['data_termino'].isna()]
    cm_form_open = cm_form
    return cm_form_open

# Insert Cell Action
def insert_cell_action(_tec, _cell, _comentario, _acao_n, _data_plan, _area, _usuario, _comentario_pessoal, _area_triage, _causa_triage):
    if _tec == 'lte':
        nr = False
        lte = True
        table = 'lte_control.cells'
    elif _tec == 'nr':
        nr = True
        lte = False
        table = 'nr_control.cells_database'
    else:
        nr = False
        lte = False
        table = ''
    query = f"""INSERT INTO public.cellmapping_form_new
                (year, week, region, uf, cidade, station, umts, lte, gsm, nr, data_criacao, comentario, acao, affected_cells, data_plan, data_termino,  
                area, usuario_criacao, usuario_ultima_edicao, comentario_pessoal,area_triage,causa_triage) 
                select extract(year from current_timestamp) as "year", extract(week from current_timestamp) as "week", "region", "uf" as "uf",  "cidade"
                ,  "station", false as "umts", {lte} as "lte", false as "gsm", {nr} as "nr", current_date as "data_criacao"
                , '{_comentario}'  as "comentario"
                , '{_acao_n}' as "acao"
                , concat(' ',"cell") as affected_cells, '{_data_plan}' as data_plan, null as data_termino
                , '{_area}' as "area"
                ,'{_usuario}' as usuario_criacao
                ,'{_usuario}' as usuario_ultima_edicao
                , '{_comentario_pessoal}' as comentario_pessoal
                , '{_area_triage}' as area_triage
                , '{_causa_triage}' as causa_triage
                from {table} vlep
                where "cell" = '{_cell}';"""
    run_query(query)

# Insert Node Action
def insert_node_action(_tec, _node, _comentario, _acao_n, _data_plan, _area, _usuario, _comentario_pessoal, _area_triage, _causa_triage):
    if _tec == 'lte':
        nr = False
        lte = True
        table = 'lte_control.cells'
    elif _tec == 'nr':
        nr = True
        lte = False
        table = 'nr_control.cells_database'
    else:
        nr = False
        lte = False
        table = ''
    query = f"""INSERT INTO public.cellmapping_form_new
                    (year, week, region, uf, cidade, station, umts, lte, gsm, data_criacao, comentario, acao, affected_cells, data_plan, data_termino,  
                    area, usuario_criacao, usuario_ultima_edicao, comentario_pessoal,area_triage,causa_triage,nr) 
                    select extract(year from current_timestamp) as "year", extract(week from current_timestamp) as "week", "region", "uf" as "uf", "cidade"
                    , "station", false as "umts", {lte} as "lte", false as "gsm", current_date as "data_criacao"
                    , '{_comentario}'  as "comentario"
                    , '{_acao_n}' as "acao"
                    ,  concat(' ',"affected_cells") as affected_cells, '{_data_plan}' as data_plan, null as data_termino
                    , '{_area}' as "area"
                    ,'{_usuario}' as usuario_criacao
                    ,'{_usuario}' as usuario_ultima_edicao 
                    , '{_comentario_pessoal}' as comentario_pessoal
                    , '{_area_triage}' as area_triage
                    , '{_causa_triage}' as causa_triage, {nr} as "nr"
                    from (select region, uf, cidade, station , affected_cells from
                    (select  station as node , string_agg(cell ,' ') as affected_cells, 
                    region,uf, cidade, station 
                    from {table} group by region, uf,cidade, station) as tempo
                    where station = '{_node}') as tempo2;"""
    #print(query)
    run_query(query)

# Update Cell Action data_plan
def update_cell_action_data_plan(db_id, _data_plan, _db_user):
    query = f"""UPDATE public.cellmapping_form_new 
        set data_plan='{_data_plan}', usuario_ultima_edicao='{_db_user}' where id={db_id};"""
    print(f'{_data_plan} updated')
    run_query(query)

# Update Cell Action data_term
def update_cell_action_data_term(db_id, _data_term, _db_user):
    query = f"""UPDATE public.cellmapping_form_new 
        set data_termino='{_data_term}', usuario_ultima_edicao='{_db_user}' where id={db_id};"""
    print(f'{_data_term} updated')
    run_query(query)

# Update Cell Action comments
def update_cell_action_comment(db_id, acao, _acao, _db_user):
    query = f"""UPDATE public.cellmapping_form_new 
        set acao='{acao};{_acao}' , usuario_ultima_edicao='{_db_user}' where id={db_id};"""
    run_query(query)

# Insert Cell Class
def insert_cell_class(pl_cell, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_tec, db_user):
    print(pl_tasklist)
    if pl_tasklist == 'RF Shaping':
        _comentario = 'Setor Overshooter'
        _comentario_pessoal = f'{today_str}_RFSH'
        _area = 'OTM'
        _area_triage = 'OTM Vendor'
        _causa_triage = 'Parametrização'
        _acao_n = f'{_acao} {pl_owner} {pl_taskid}'

    elif pl_tasklist == 'Cell Power':
        _comentario = 'Parâmetros de potência fora do baselne'
        _comentario_pessoal = f'{today_str}_CPWR'
        _area = 'OTM'
        _area_triage = 'OTM Vendor'
        _causa_triage = 'Parametrização'
        _acao_n = f'{_acao} {pl_owner} {pl_taskid}'
    elif pl_tasklist == 'Traffic Balance':
        _comentario = 'Tráfego desbalanceado'
        _comentario_pessoal = f'{today_str}_TRAFBAL'
        _area = 'OTM'
        _area_triage = 'OTM Vendor'
        _causa_triage = 'Parametrização'
        _acao_n = f'{_acao} {pl_owner} {pl_taskid}'

    insert_cell_action(pl_tec, pl_cell, _comentario, _acao_n, _data_plan, _area, db_user, _comentario_pessoal, _area_triage, _causa_triage)

# Insert Node Class
def insert_node_class(pl_node, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_tec, db_user):
    #print(pl_tasklist)
    if pl_tasklist == 'Traffic Balance':
        _comentario = 'Tráfego desbalanceado'
        _comentario_pessoal = f'{today_str}_TRAFBAL'
        _area = 'OTM'
        _area_triage = 'OTM Vendor'
        _causa_triage = 'Parametrização'
        _acao_n = f'{_acao} {pl_owner} {pl_taskid}'

    elif pl_tasklist == 'Logic Parameter':
        _comentario = 'Parâmetros fora do baseline'
        _comentario_pessoal = f'{today_str}_LOGPAR'
        _area = 'OTM'
        _area_triage = 'OTM Vendor'
        _causa_triage = 'Parametrização'
        _acao_n = f'{_acao} {pl_owner} {pl_taskid}'

    insert_node_action(pl_tec, pl_node, _comentario, _acao_n, _data_plan, _area, db_user, _comentario_pessoal, _area_triage, _causa_triage)


# Check Cell Task
def check_cell_task(pl_cell, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_note, pl_tec, cm_form_open, _db_user,pl_completed_or_not, _data_update):    
    print(pl_cell)
    cm_form_object = cm_form_open[cm_form_open['affected_cells'].str.contains(pl_cell)]
    cm_form_task = cm_form_open[cm_form_open['acao'].str.contains(pl_taskid)]
    # Check cell in db
    if cm_form_object.empty:
        print('insert cell action')
        # insert action
        insert_cell_class(pl_cell, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_tec, _db_user )
    elif not cm_form_object.empty:
        print('cell action exist')
        if cm_form_task.empty:
            print('task not included')
            #same action?
            acao = cm_form_open['acao'].values[0]
            if acao.find(_acao) == -1:
                insert_cell_class(pl_cell, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_tec, _db_user )
            print(cm_form_object[['comentario','acao','data_plan','data_termino']])
            # insert action
        elif not cm_form_task.empty:
            print('task included')
            if cm_form_task['data_plan'].values[0].strftime("%Y-%m-%d") == _data_plan:
                print('data plan updated')
            elif cm_form_task['data_plan'].values[0].strftime("%Y-%m-%d") != _data_plan:
                if pl_completed_or_not == 'Y':
                    update_cell_action_data_term(cm_form_task['id'].values[0], _data_plan, _db_user)
                if pl_completed_or_not == 'N':
                    update_cell_action_data_plan(cm_form_task['id'].values[0], _data_plan, _db_user)
                #print(f'{_data_plan} updated')
            if cm_form_task['data_termino'].values[0] is not None:
                #if cm_form_task['data_termino'].values[0].strftime("%Y-%m-%d") != _data_update:
                #if cm_form_task['data_termino'].values[0].strftime("%Y-%m-%d") != _data_plan:
                if cm_form_task['data_termino'].values[0].strftime("%Y-%m-%d") != _data_term:
                    if _data_update is not None:
                        if _data_update != '':
                            if pl_completed_or_not == 'Y':
                                update_cell_action_data_term(cm_form_task['id'].values[0], _data_update, _db_user)
                            # ajustar # update_cell_action_data_plan(cm_form_task['id'].values[0], _data_update, _db_user)
            elif cm_form_task['data_termino'].isna:
                if pl_completed_or_not != 'N':
                    update_cell_action_data_term(cm_form_task['id'].values[0], _data_update, _db_user)
                    update_cell_action_data_plan(cm_form_task['id'].values[0], _data_update, _db_user)
                elif _data_update == '':
                    print('data term updated')
            acao = cm_form_task['acao'].values[0]
            if acao.find(_acao) == -1:
                update_cell_action_comment(cm_form_task['id'].values[0], acao, _acao, _db_user)
                print('comment updated')
            elif acao.find(_acao) != -1:
                print('updated')
                
                #update_cell_action(db_id, acao, _acao)
            #print(f'Action finished')
    cm_form_task.head()


# Check node Task
def check_node_task(pl_node, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_note, pl_tec, cm_form_open, _db_user, pl_completed_or_not, _data_update):    
    print(pl_node)
    cm_form_object = cm_form_open[cm_form_open['station'].str.contains(pl_node)]
    cm_form_task = cm_form_open[cm_form_open['acao'].str.contains(pl_taskid)]
    # Check cell in db
    if not cm_form_object.empty:
        print('node action exist')
        if cm_form_task.empty:
            print('task not included')
            #same action?
            insert_node_class(pl_node, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_tec, _db_user )                
            print(cm_form_object[['comentario','acao','data_plan','data_termino']])
            # insert action
        elif not cm_form_task.empty:
            print('task included')
            if cm_form_task['data_plan'].values[0].strftime("%Y-%m-%d") == _data_plan:
                print('data plan updated')
            elif cm_form_task['data_plan'].values[0].strftime("%Y-%m-%d") != _data_plan:
                if pl_completed_or_not == 'Y':
                    update_cell_action_data_term(cm_form_task['id'].values[0], _data_plan, _db_user)
                if pl_completed_or_not == 'N':
                    update_cell_action_data_plan(cm_form_task['id'].values[0], _data_plan, _db_user)
                #print(f'{_data_plan} updated')

            if cm_form_task['data_termino'].values[0] is not None:
                #if cm_form_task['data_termino'].values[0].strftime("%Y-%m-%d") != _data_update:
                if cm_form_task['data_termino'].values[0].strftime("%Y-%m-%d") != _data_term:
                    if _data_update is not None:
                        if _data_update != '':
                            if pl_completed_or_not == 'Y':
                                update_cell_action_data_term(cm_form_task['id'].values[0], _data_update, _db_user)
                            #update_cell_action_data_plan(cm_form_task['id'].values[0], _data_update, _db_user)
            elif cm_form_task['data_termino'].isna:
                if pl_completed_or_not != 'N':
                    update_cell_action_data_term(cm_form_task['id'].values[0], _data_update, _db_user)
                    update_cell_action_data_plan(cm_form_task['id'].values[0], _data_update, _db_user)
                elif _data_update == '':
                    print('data term updated')
            acao = cm_form_task['acao'].values[0]
            if acao.find(_acao) == -1:
                update_cell_action_comment(cm_form_task['id'].values[0], acao, _acao, _db_user)
                print('comment updated')
            elif not acao.find(_acao) != -1:
                print('updated')
                #update_cell_action(db_id, acao, _acao)       
    elif cm_form_object.empty:
        insert_node_class(pl_node, pl_taskid, _data_plan, _data_term, _acao, pl_tasklist, pl_owner, pl_tec, _db_user )
    #cm_form_task.head()

