# -*- coding: UTF-8 -*-


import os

from common  import *
from myfunc  import *
import pymssql


def get_table_name(_dict):
    table_name = _dict['res_table']
    return table_name


def get_log_table_name(_orig_table_name):
    table_name = _orig_table_name + '_log'
    return table_name


# relation_id: src_db => tgt_db
def convert_to_new_rel_id(_row):

    # relation_id
    rel = 'relation_id'
    if _row.has_key(rel):
        relation_id = _row[rel]
        # log_debug('org rel_id: %s', relation_id)
        new_rel_id  = translate_relation_id(relation_id, MyCtx.cursorX, MyCtx.cursorY)
        if len(new_rel_id) == 0:
            log_error('error: translate_relation_id')
            return -1

        _row[rel] = new_rel_id

        if relation_id != new_rel_id:
            log_debug('rel_id changed: %s => %s', relation_id, new_rel_id)
    else:
        # log_debug('not found relation_id')
        pass


    return 0



# res_id, like elem_id
def generate_new_res_id(_row):
    new_res_id = ''

    # elem_id, and more...
    for item in MyCtx.id_mng_map:
        # log_debug("id_type[%s] -- '%s'", item, MyCtx.id_mng_map[item])

        if _row.has_key(item):
            id_type = MyCtx.id_mng_map[item] # '16'
            id_val  = _row[item]
            # log_debug("%s -- '%s'", item, id_val)
            new_res_id = my_get_next_id(id_type, MyCtx.cursorY)
            _row[item] = new_res_id
            if id_val != new_res_id:
                log_debug('GENERATED: [%s]: %s => %s', item, id_val, new_res_id)
        else:
            # log_debug('not found %s', item)
            pass

    return new_res_id






def generate_insert_cm(_tgt_table, _data_table, _oper_id, _get_res_id_mode):

    # SQL -- get target table's column definition
    sql   = 'sp_columns %s' % (_tgt_table)
    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()

    # SQL -- get source table's data
    sql   = "select * from %s where oper_id = '%s'" % (_data_table, _oper_id)
    MyCtx.cursorX.execute(sql)
    list2 = MyCtx.cursorX.fetchall()
    if len(list2) != 1:
        log_error('error: no data: %s', sql)
        return ""
    log_row = list2[0]

    """
    # use new RELATION_ID 2018-5-10
    # log_debug(log_row)
    if convert_to_new_rel_id(log_row) < 0:
        log_error('error: convert_to_new_rel_id')
        return ""

    # mode: 1, id-table; 2, main-table
    if _get_res_id_mode == 1:
        # use new RES_ID:ELEM_ID 2018-5-10
        # log_debug('need to convert res id')
        generate_new_res_id(log_row)
#   elif _get_res_id_mode == 2:
#       # get the res_id(like func_id) from target db by res_name(like func_name)
#       log_debug('sub-insert')
#       # query_current_res_id(_tgt_table, log_row, MyCtx.cursorX, MyCtx.cursorY)
    else:
        # log_debug('no need to convert res id')
        pass
    """

    #  convert new relation-id
    convert_relation_id(_tgt_table, log_row, MyCtx.cursorX, MyCtx.cursorY)

    #  generate new resource-id
    generate_res_id(_tgt_table, log_row, MyCtx.cursorY)

    #  query resource-id in target DB
    convert_res_id(_tgt_table, log_row, MyCtx.cursorX, MyCtx.cursorY)

    # change MAP_ID to 'AUTOM' 2018-5-17
    # this can mark the LOG is inserted by script, instead of real user
    mark_as_me = 'map_id'
    if log_row.has_key(mark_as_me):
        log_row[mark_as_me] = 'AUTOM'


    column_name_list    = []
    column_value_list   = []
    for row in list1:
        column_name = row['COLUMN_NAME']
        column_type = row['TYPE_NAME']

        if log_row[column_name] is None:
            column_value= "null"
        else:
            column_value= "'" + str(log_row[column_name]).strip() + "'"

        if column_type == 'int identity':
            # log_debug('%s -- %s', column_name, column_type)
            continue
        column_name_list.append(column_name)
        column_value_list.append(column_value)
        # log_debug('[%s] => [%s]', column_name, column_value)

    buf1 = ', '.join(column_name_list)

    buf2 = ', '.join(column_value_list)

    sql = "insert into %s (%s) values (%s)" % (_tgt_table, buf1, buf2)

    return sql




# taskid-map
def generate_insert_map(_column_list, _value_list, _table):

    buf1 = ', '.join(_column_list)

    buf2 = ', '.join(_value_list)

    sql = "insert into %s (%s) values (%s)" % (_table, buf1, buf2)

    return sql



# delete from est_func_param where func_id = (select func_id from est_func where func_name = 'get_date_time')
def delete_sub_resource(_table, _res_name):

    res_name    = _res_name # 'get_date_time'

    sub_table   = _table # est_func_param
    sub_key     = sai_conf_get(sub_table, 'KEY') # func_id

    if not MyCtx.sub_main_table_map.has_key(sub_table):
        log_error('error: sub_main_table_map not find [%s]', sub_table)
        raise  Exception

    main_list   = MyCtx.sub_main_table_map[sub_table]
    main_table  = main_list[0] # est_func
    main_key    = main_list[1] # func_name


    # sql = "delete from $TABLE where $KEY = (select $KEY from $MAIN_TABLE where $MAIN_KEY = '$RES_NAME')"
    sql = "delete from %s where %s = (select %s from %s where %s = '%s')" % (sub_table, sub_key, sub_key, main_table, main_key, res_name)

    # log_debug('delete-sub: %s', sql)

    return sql



def generate_log_sql(_dict):
    table_name      = get_table_name(_dict)
    log_table_name  = get_log_table_name(table_name)
    oper_id         = _dict['oper_id']
    get_res_id_mode = 0

    sql = generate_insert_cm(log_table_name, log_table_name, oper_id, get_res_id_mode)

    return sql



def generate_insert_res(_dict):
    table_name      = get_table_name(_dict)
    log_table_name  = get_log_table_name(table_name)
    oper_id         = _dict['oper_id']
    res_name        = _dict['res_name']
    get_res_id_mode = 1

    # mark no need to generate new res-id
    if _dict['res_table_class'] == 'SUBT':
        get_res_id_mode = 2
        # log_debug('is-sub-insert')


    sql = generate_insert_cm(table_name, log_table_name, oper_id, get_res_id_mode)

    return sql


def generate_update_res(_dict):
    res_table_name  = get_table_name(_dict)

    # get key list
    key_list, cmp_list, dis_list = get_config_list(res_table_name)
    if len(key_list) == 0:
        log_error("sorry, please config cnf/my.cnf [%s] KEY=xxx", res_table_name)
        return ""

    sql = "update %s set %s = '%s' where %s = '%s'" % \
           (res_table_name, _dict['res_column'],\
            _dict['dst_val'], \
            key_list[0], _dict['res_name'])

    return sql



def generate_delete_res(_dict):
    res_table = get_table_name(_dict)
    res_name  = _dict['res_name']

    # get key list
    key_list, cmp_list, dis_list = get_config_list(res_table)
    if len(key_list) == 0:
        log_error("sorry, please config cnf/my.cnf [%s] KEY=xxx", res_table)
        return ""


    if _dict['res_table_class'] == 'SUBT':
        sql = delete_sub_resource(res_table, res_name)
        log_debug('sub-delete: [%s]', sql)
    else:
        sql = "delete from %s where %s = '%s'" % (res_table, key_list[0], _dict['res_name'])

    return sql



def convert_to_dict(_words):

    my_dict = {}

    # update
    if len(_words) == 10:
        # log_debug('is update dict')
        my_dict = dict(zip(MyCtx.update_list, _words))
    # insert/delete
    elif len(_words) == 7:
        # log_debug('is short dict')
        my_dict = dict(zip(MyCtx.insert_list, _words))
    else:
        log_error('error')
        return None

    return my_dict



# XXX: only clear the LOG generated by script, instead of real user.
def clear_log_by_taskid(_table_name, _task_id):
    sql = "delete from %s where task_id = '%s'" % (_table_name, _task_id)

    log_debug('clear the previous related LOG: [%s -- %s]', _table_name, _task_id)

    MyCtx.cursorY.execute(sql)

    return 0



# line is of submission.txt
def process_one_line(_line):

    words = _line.split(',')
    for i in range(len(words)):
        words[i] = words[i].strip()
    log_debug('submission: %s', words)

    table_class = words[0]
    table_name  = words[1]
    oper_id     = words[2]
    task_id     = words[4]
    oper_type   = words[5]
    res_name    = words[6]

    my_dict = convert_to_dict(words)
    # log_debug(my_dict)

    # error context
    MyCtx.err_context = my_dict

    table_name      = get_table_name(my_dict)
    log_table_name  = get_log_table_name(table_name)

    # for item in sorted(my_dict.keys()):
    #     log_debug("'%s' -- '%s'", item, my_dict[item].strip())

    # Check whether need to clear the log
    task_uniq_id = '%s+%s' % (log_table_name, task_id)
    if task_uniq_id in MyCtx.task_id_uniq:
        # nothing to do
        # log_debug('task_id: %s already exists', task_uniq_id)
        pass
    else:
        MyCtx.task_id_uniq[task_uniq_id] = '1'
        # first time, to clear the log related with task-id
        clear_log_by_taskid(log_table_name, task_id)


    # delete from sub-resource-table
    if oper_type == 'insert' and table_class == 'SUBT':

        subt_uniq_id = '%s+%s' % (table_name, res_name)
        if subt_uniq_id in MyCtx.subt_id_uniq:
            # log_info('SUB: subt_uniq_id[%s] already exists, dont delete again', subt_uniq_id)
            pass
        else:
            MyCtx.subt_id_uniq[subt_uniq_id] = '1'
            sub_sql = delete_sub_resource(table_name, res_name)
            log_debug('SUB: delete for first time: [%s]', sub_sql)
            MyCtx.cursorY.execute(sub_sql)
            log_debug('deleted count: %s', MyCtx.cursorY.rowcount)

    sql = ''
    if oper_type == 'insert':
        sql = generate_insert_res(my_dict)
    elif oper_type == 'update':
        sql = generate_update_res(my_dict)
    elif oper_type == 'delete':
        sql = generate_delete_res(my_dict)
    else:
        log_error('error: %s', oper_type)
        return -1

    if len(sql) == 0:
        log_error('error: generate RES sql failure')
        return -1

    log_debug('res-sql -- %s', sql)
    MyCtx.res_content.append(sql)
    MyCtx.cursorY.execute(sql)  # res
    # log_debug('affected rowcount:    %s', MyCtx.cursorY.rowcount)


    # A original log can be seperated several lines, but only the first line need to be record(insert)
    log_uniq_id = '%s+%s' % (table_name, oper_id)
    if log_uniq_id in MyCtx.oper_id_uniq:
        log_info('LOG: log_uniq_id[%s] already exists, lets next', log_uniq_id)
        return 0
    else:
        MyCtx.oper_id_uniq[log_uniq_id] = '1'

    if oper_type == 'update' and MyCtx.cursorY.rowcount == 0:
        log_debug("sorry, update 0 rows for %s", res_name)
        return -1

    # log_debug('dict: %s', my_dict)
    sql = ''
    sql = generate_log_sql(my_dict)
    if len(sql) == 0:
        log_error('error: generate LOG sql failure')
        return -1

    MyCtx.log_content.append(sql)

    log_debug('log-sql -- %s', sql)
    MyCtx.cursorY.execute(sql) # log

    return 0






def replay_log_and_res(_date):
    rv = 0

    file_name = "submission.%s.txt" % (_date)
    file_path = get_data_file_path(file_name)
    log_debug('input: %s', file_path)

    fo = None
    fo = open(file_path, "r")

    try:
        lines = fo.readlines()
        lines.sort()
        pre_process_lines(lines)
        log_debug(MyCtx.seperator)
        for line in lines:
            # log_debug('%s', line.strip())
            line = line.strip()
            if process_one_line(line) != 0:
                log_error("error: row [%s] failuer", line)
                rv = -1
                break
            log_debug('-'*80)

        log_file_name = 'log.%s.txt' % (_date)
        res_file_name = 'res.%s.txt' % (_date)
        put_list_to_file(MyCtx.log_content, log_file_name)
        put_list_to_file(MyCtx.res_content, res_file_name)

        if rv == 0:
            log_debug('coMMit!')
            MyCtx.connY.commit()
        else:
            log_debug('failure, do not commit!')

    except Exception, e:
        log_error('exception: [%s]', e)
        # log_error('exception: [%s]', dir(e))
        # log_error('exception: [%s], %d, %s', type(e.args), len(e.args), e.args)
        # log_error('exception: [%s], %s', type(e.message), e.message)
        if len(e.args) >= 2 and 'duplicate key row' in str(e.args[1]):
            log_debug('error: is duplicate key row')
            if len(MyCtx.err_context) > 0:
                # dump_my_dict(MyCtx.err_context)
                # print('error: resource: %s is duplicate' %  MyCtx.err_context['res_name'])
                print('error: %s found duplicate key row: %s' % 
                        (MyCtx.err_context['res_table'], MyCtx.err_context['res_name']))
        else:
            log_debug('error: [%s]', e)
        rv = -1
    finally:
        # log_info('finally')
        MyCtx.connY.rollback()
        if fo:
            fo.close()

    return rv




def replay_taskid_map(_date):
    rv = 0

    file_name = "taskidmap.%s.txt" % (_date)
    file_path = get_data_file_path(file_name)
    log_debug('input: %s', file_path)

    fo = None
    fo = open(file_path, "r")
    lines = fo.readlines()
    fo.close()


    try:
        table = 'est_taskid_map'

        import_to_table_ignore_dup(lines, table, MyCtx.cursorY)

        log_debug('CoMMiT!')
        MyCtx.connY.commit()
        
    except Exception, e:
        log_error('exception: [%s]', e)
        if 'duplicate key row' in str(e.args[1]):
            print('error: %s found duplicate key row' % (table))
    finally:
        # log_info('finally')
        MyCtx.connY.rollback()

    return rv


def replay_taskid(_date):
    rv = 0

    file_name = "taskid.%s.txt" % (_date)
    file_path = get_data_file_path(file_name)
    log_debug('input: %s', file_path)

    fo = None
    fo = open(file_path, "r")
    lines = fo.readlines()
    fo.close()


    try:
        table = 'est_taskid_mng'

        import_to_table_ignore_dup(lines, table, MyCtx.cursorY)

        log_debug('CoMMiT!')
        MyCtx.connY.commit()
        
    except Exception, e:
        log_error('exception: [%s]', e)
        if 'duplicate key row' in str(e.args[1]):
            print('error: %s found duplicate key row' % (table))
    finally:
        # log_info('finally')
        MyCtx.connY.rollback()

    return rv


def replay_init():
    rv = 0

    src_dbname = 'SRC-DB'
    tgt_dbname = 'TGT-DB'

    args = sai_get_args()
    if len(args) >= 2:
        src_dbname = args[0] # 'SIT'
        tgt_dbname = args[1] # 'UAT'
        log_debug('move [%s] -> [%s]', src_dbname, tgt_dbname)

    if len(args) >= 3:
        file_name = args[2]
        log_debug('file_name1: %s', file_name)

    if len(args) == 0:
        src_dbname = sai_conf_get('RUNTIME', 'SOURCE')
        tgt_dbname = sai_conf_get('RUNTIME', 'TARGET')

    MyCtx.log_content = []
    MyCtx.res_content = []

    MyCtx.connX     = db_init(src_dbname)
    MyCtx.cursorX   = MyCtx.connX.cursor(as_dict=True)

    MyCtx.connY     = db_init(tgt_dbname)
    MyCtx.cursorY   = MyCtx.connY.cursor(as_dict=True)

    MyCtx.target_project = sai_conf_get('RUNTIME', 'TARGET_PROJECT')

    return rv


def replay_doit():

    file_date = sai_get_today()


    # import taskid_mng for foreign key constraint
    log_debug('--- TASK ---')
    if replay_taskid(file_date) < 0:
        log_error('error: replay_taskid')
        return -1

    log_debug('--- MAP ---')
    if replay_taskid_map(file_date) < 0:
        log_error('error: replay_taskid_map')
        return -1

    log_debug('--- LOG && RES ---')
    if replay_log_and_res(file_date) < 0:
        log_error('error: replay_log_and_res')
        return -1

    return 0


def replay_done():

    #
    MyCtx.connX.close()
    MyCtx.connY.close()

    return 0


def worker():

    replay_init()

    replay_doit()

    replay_done()

    return 0


# main()
if __name__=="__main__":
    sailog_set("replay.log")
    worker()

