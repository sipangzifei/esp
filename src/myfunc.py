# -*- coding: UTF-8 -*-

import os

from common import *


class MyCtx():
    conn   = None
    cursor = None


    # src-database
    connX  = None
    cursorX= None

    # tgt-database
    connY  = None
    cursorY= None

    oper_id_uniq= {} # ensure insert once for each update in log-table
    task_id_uniq= {} # ensure delete once for each task in log-table
    subt_id_uniq= {} # ensure delete once for sub-table

    exp_content = []

    log_content = []
    res_content = []

    err_context = {}

    seperator   = '+' * 72


    ##################################################################
    # export  ########################################################

    # primary key of table
    table_key_map = {
        'est_element'       : ['elem_id'],
        'est_func'          : ['func_id'],
        'est_enum'          : ['enum_id'],
        'est_format'        : ['fmt_id'],
        'est_flow'          : ['flow_id'],
        'est_flw_end'       : ['flow_id'],
        'est_flow_step'     : ['flow_id'],
    }


    # primary key and unique-index of table
    main_table_key_map2 = {
        'est_element'       : ['elem_name', 'elem_id'],
        'est_func'          : ['func_name', 'func_id'],
        'est_enum'          : ['enum_name', 'enum_id'],
        'est_format'        : ['fmt_name',  'fmt_id'],
        'est_flow'          : ['flow_name', 'flow_id'],
    }


    # primary key for sub-tables
    subt_table_key_map = {
        'est_flw_end'       : ['flow_id', 'step_no'],
        'est_flow_step'     : ['flow_id', 'step_no'],
    }


    # primary key group for sub-tables
    subt_table_group_key_map = {
        'est_func_param'    : ['func_id'],

        'est_enum_value'    : ['enum_id'],

        'est_fmt_item'      : ['fmt_id'],
        'est_sign_item'     : ['fmt_id'],

        'est_flw_end'       : ['flow_id'],
        'est_flow_step'     : ['flow_id'],
    }


    main_subt_table_map = {
        'est_func'          : ['est_func_param'],
        'est_enum'          : ['est_enum_value'],
        'est_format'        : ['est_fmt_item', 'est_sign_item'],
        'est_flow'          : ['est_flw_end', 'est_flow_step'],
    }


    ####################################################### export  ##
    ##################################################################



    # for update
    update_list = ['res_table_class', 'res_table', 'oper_id', 'oper_date_time', 'task_id', 'oper_type_log',
    'res_name', 'res_column', 'src_val', 'dst_val']

    # for insert/delete
    insert_list = ['res_table_class', 'res_table', 'oper_id', 'oper_date_time', 'task_id', 'oper_type_log',
    'res_name' ]



    # convert RELATION-ID
    rel_conv_list_map = {
        'est_element'       : ['relation_id'],
        'est_func'          : ['relation_id'],
        'est_enum'          : ['relation_id'],
        'est_format'        : ['relation_id'],
    }


    # generate NEW res-id
    res_gen_list_map = {
        'est_element'       : ['elem_id'],

        'est_func'          : ['func_id'],

        'est_enum'          : ['enum_id'],

        'est_format'        : ['fmt_id'],
    }

    new_res_id  = ''
    id_mng_map  = {
        'elem_id'   :   '16',
        'func_id'   :   '08',
        'enum_id'   :   '25',
        'fmt_id'    :   '03',
    }


    # convert res-id for foreign-key dependant
    # list_map + rule_map
    res_cvt_list_map = {
        'est_element_log'   : ['elem_id'],

        'est_func_log'      : ['func_id'],
        'est_func_param'    : ['func_id'],
        'est_func_param_log': ['func_id'],

        'est_enum'          : ['enum_fmem_id', 'enum_smem_id'],
        'est_enum_log'      : ['enum_id', 'enum_fmem_id', 'enum_smem_id'],
        'est_enum_value'    : ['enum_id'],
        'est_enum_value_log': ['enum_id'],

        'est_format_log'    : ['fmt_id'],
        'est_fmt_item'      : ['fmt_id', 'elem_id'],
        'est_sign_item'     : ['fmt_id', 'elem_id'],
        'est_fmt_item_log'  : ['fmt_id', 'elem_id'],
        'est_sign_item_log' : ['fmt_id', 'elem_id'],
    }


    res_cvt_rule_map = {
        'est_element_log-elem_id'       : ['est_element',   'elem_id',  'elem_name'],

        'est_func_log-func_id'          : ['est_func',      'func_id',  'func_name'],
        'est_func_param-func_id'        : ['est_func',      'func_id',  'func_name'],
        'est_func_param_log-func_id'    : ['est_func',      'func_id',  'func_name'],

        'est_enum-enum_fmem_id'         : ['est_element',   'elem_id',  'elem_name'],
        'est_enum-enum_smem_id'         : ['est_element',   'elem_id',  'elem_name'],
        'est_enum_log-enum_id'          : ['est_enum',      'enum_id',  'enum_name'],
        'est_enum_log-enum_fmem_id'     : ['est_element',   'elem_id',  'elem_name'],
        'est_enum_log-enum_smem_id'     : ['est_element',   'elem_id',  'elem_name'],
        'est_enum_value-enum_id'        : ['est_enum',      'enum_id',  'enum_name'],
        'est_enum_value_log-enum_id'    : ['est_enum',      'enum_id',  'enum_name'],

        'est_format_log-fmt_id'         : ['est_format',    'fmt_id',   'fmt_name'],
        'est_fmt_item-fmt_id'           : ['est_format',    'fmt_id',   'fmt_name'],
        'est_fmt_item-elem_id'          : ['est_element',   'elem_id',  'elem_name'],
        'est_sign_item-fmt_id'          : ['est_format',    'fmt_id',   'fmt_name'],
        'est_sign_item-elem_id'         : ['est_element',   'elem_id',  'elem_name'],
        'est_fmt_item_log-fmt_id'       : ['est_format',    'fmt_id',   'fmt_name'],
        'est_fmt_item_log-elem_id'      : ['est_element',   'elem_id',  'elem_name'],
        'est_sign_item_log-fmt_id'      : ['est_format',    'fmt_id',   'fmt_name'],
        'est_sign_item_log-elem_id'     : ['est_element',   'elem_id',  'elem_name'],

        'est_flow_step-GET_RES_ID'      : ['est_flow',      'flow_id',  'flow_name'],
        'est_flw_end-GET_RES_ID'        : ['est_flow',      'flow_id',  'flow_name'],
    }


    # for TARGET db
    sub_res_id_map  = {
        'est_func_param': ['est_func', 'func_id', 'func_name']
    }

    # for SOURCE db
    sub_res_name_map  = {
        'est_func_param': ['est_func', 'func_name', 'func_id']
    }


    # from delete
    sub_main_table_map  = {
        'est_func_param'    :   ['est_func',      'func_name'],
        'est_enum_value'    :   ['est_enum',      'enum_name'],
        'est_fmt_item'      :   ['est_format',    'fmt_name'],
        'est_sign_item'     :   ['est_format',    'fmt_name'],

        'est_flow_step'     :   ['est_flow',      'flow_name'],
        'est_flw_end'       :   ['est_flow',      'flow_name'],
    } 

    target_project = ''


    def __init__(self):
        print('inited')



# get compare list
def get_compare_list(_table):
    # ---------------------------------- #
    cmp_str  = sai_conf_get(_table, "CMP")

    cmp_list = []
    cmp_list = cmp_str.split(',')


    return cmp_list


# get CONFIG list
def get_config_list(_table):
    # ---------------------------------- #
    cmp_str  = sai_conf_get(_table, "CMP")

    cmp_list = []
    cmp_list = cmp_str.split(',')

    # ---------------------------------- #
    key_str  = sai_conf_get(_table, "KEY")

    key_list = []
    key_list = key_str.split(',')

    # ---------------------------------- #
    dis_str  = sai_conf_get(_table, "DIS")

    dis_list = []
    dis_list = dis_str.split(',')

    # ---------------------------------- #

    return key_list, cmp_list, dis_list

#



def trim_list(_list):

    for i in range(len(_list)):
        _list[i] = _list[i].strip()

    return _list



# TODO: more elegant
def get_type_name_id0(_table):

    key_str  = sai_conf_get(_table, "KEY")

    key_list = key_str.split(',')

    trim_list(key_list)

    # TODO: name, id, more check
    type_name = key_list[0]
    type_id   = key_list[1]

    return type_name, type_id


#
def get_type_name_id1(_table):

    my_key = _table

    if MyCtx.main_table_key_map.has_key(my_key):
        key_list = MyCtx.main_table_key_map[my_key]
    else:
        log_error('error: [%s] not configured', my_key)
        return -1, -1

    type_name = key_list[0]
    type_id   = key_list[1]

    return type_name, type_id


#
def get_type_id(_table):

    my_key = _table

    if MyCtx.table_key_map.has_key(my_key):
        key_list = MyCtx.table_key_map[my_key]
    else:
        log_error('error: [%s] not configured', my_key)
        return -1

    type_id   = key_list[0]

    return type_id


#
def get_sub_type_id(_table):

    my_key = _table

    if MyCtx.table_key_map.has_key(my_key):
        key_list = MyCtx.table_key_map[my_key]
    else:
        log_error('error: [%s] not configured', my_key)
        return -1

    type_id   = key_list[0]

    return type_id



def get_data_file_path(_file_name):
    file_path = "%s/data/%s" % (os.getenv('ESP_HOME'), _file_name)
    return file_path


def put_list_to_file(_content_list, _file_name):
    file_path = get_data_file_path(_file_name)

    try:
        my_fp = open(file_path, 'w')
        for line in _content_list:
            # log_debug('%s', line)
            my_fp.write(line+'\n')
    except Exception, e:
        log_error('exception: %s', e)
    finally:
        if my_fp:
            my_fp.close()


def dump_my_dict(_dict):
    for item in sorted(_dict.keys()):
        log_debug("'%s' -- '%s'", item, _dict[item].strip())


def get_column_name_list(_table_name, _cursor):
    column_sql  = 'sp_columns %s' % (_table_name)
    # log_debug('column-sql: %s', column_sql)

    _cursor.execute(column_sql)
    list1 = _cursor.fetchall()

    column_name_list    = []
    for row in list1:
        column_name = row['COLUMN_NAME']
        column_type = row['TYPE_NAME']
        if column_type == 'int identity':
            continue
        column_name_list.append(column_name)

    return column_name_list


# db.table ==> list
def export_from_table(_table_name, _where, _cursor):
    content_list = []

    line_seperator  = sai_conf_get('MY', 'SEPERATOR')

    column_name_list = get_column_name_list(_table_name, _cursor)
    log_debug('%s', line_seperator.join(column_name_list))

    # line = line_seperator.join(column_name_list)
    # content_list.append(line)


    data_sql = 'select * from %s %s' % (_table_name, _where)
    # log_debug('data-sql: %s', data_sql)

    _cursor.execute(data_sql)
    list2 = _cursor.fetchall()

    line = ''
    for row in list2:
        # log_debug('-'*72)
        # log_debug('row = %s', (row))

        val_list = []
        for item in column_name_list:
            item = item.strip()
            val  = row[item].strip()
            val_list.append(val)
            # log_debug('[%s] => [%s]', item, val.encode('gb18030'))
            # log_debug('[%s] => [%s]', item, val)
            line = line_seperator.join(val_list)

        content_list.append(line)

    return content_list


# system
# list ==> db.table
def sys_import_to_table(_line_list, _table_name, _cursor, _ignore_duplicate):
    rv = 0

    line_seperator  = sai_conf_get('MY', 'SEPERATOR')
    charset = sai_conf_get('MY', 'LANG')

    column_name_list = get_column_name_list(_table_name, _cursor)

    for i in range(len(column_name_list)):
        column_name_list[i] = column_name_list[i].strip()
    # log_debug('column_name_list: %s', column_name_list)

    for item in _line_list:
        # log_debug('-'*80)

        item = item.strip()
        # log_debug('item: [%s]', item)


        value_list = item.split(line_seperator)
        # log_debug('%s -- %s', value_list, value_list[3])

        for i in range(len(value_list)):
            value_list[i] = "'" + value_list[i].strip().decode(charset) + "'"


        buf1 = ', '.join(column_name_list)
        buf2 = ', '.join(value_list)
        sql = "insert into %s (%s) values (%s)" % (_table_name, buf1, buf2)

        # log_debug('sql -- %s', sql)
        # log_debug('x')
        # log_debug('y')

        try:
            _cursor.execute(sql)
        except Exception, e:
            if 'duplicate key row' in str(e.args[1]):
                if _ignore_duplicate:
                    log_debug('is duplicate, but ignore')
                else:
                    log_error('is duplicate %s, need raise it', e.args[0])
                    raise(e)
            elif 'PRIMARY KEY constraint' in str(e.args[1]):
                if _ignore_duplicate:
                    log_debug('is duplicate, but ignore')
                else:
                    log_error('is duplicate %s, need raise it', e.args[0])
                    raise(e)
            else:
                log_error('other exception %s, need raise it', e.args[0])
                log_error('error: [%s]', sql)
                raise(e)

    return rv


# list ==> db.table
def import_to_table(_line_list, _table_name, _cursor):
    ignore_duplicate = False

    return sys_import_to_table(_line_list, _table_name, _cursor, ignore_duplicate)


# list ==> db.table
# but ignore duplicate
def import_to_table_ignore_dup(_line_list, _table_name, _cursor):
    ignore_duplicate = True

    return sys_import_to_table(_line_list, _table_name, _cursor, ignore_duplicate)


my_meta_id = '00000'
my_fail_id = '!@#$%'


def my_is_meta_id(_id):
    global my_meta_id
    return _id == my_meta_id

def my_is_fail_id(_id):
    global my_fail_id
    return _id == my_fail_id


def parse_relation_id(_relation_id):
    # log_debug('%s', _relation_id)

    project_id = _relation_id[0:5]
    bus_id     = _relation_id[5:10]
    sub_bus_id = _relation_id[10:15]

    # log_debug('project    -- %s', project_id)
    # log_debug('bus_id     -- %s', bus_id)
    # log_debug('sub_bus_id -- %s', sub_bus_id)

    return project_id, bus_id, sub_bus_id


def get_target_project_id(_relation_id, _src_cursor, _tgt_cursor):
    global my_meta_id
    global my_fail_id

    project_id, bus_id, sub_id = parse_relation_id(_relation_id)

    if my_is_meta_id(project_id):
        # log_debug('project-id is meta: %s', project_id)
        return  project_id

    sql = "select project_name from est_project where project_id='%s'" % (project_id)

    _src_cursor.execute(sql)
    list1 = _src_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    project_name = row['project_name'].strip()
    # log_debug('src - project_name -- [%s]', project_name)

    #  get target project-name from config file 2018-5-14
    project_name = MyCtx.target_project
    # log_debug('tgt - project_name -- [%s]', project_name)


    sql = "select project_id from est_project where project_name='%s'" % (project_name)

    _tgt_cursor.execute(sql)
    list1 = _tgt_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    project_id = row['project_id']
    # log_debug('tgt - project_id   -- %s', project_id)

    return project_id


def get_target_bus_id(_src_relation_id, _tgt_relation_id, _src_cursor, _tgt_cursor):
    global my_meta_id
    global my_fail_id

    project_id, bus_id, sub_id = parse_relation_id(_src_relation_id)

    if my_is_meta_id(bus_id):
        # log_debug('bus-id is meta: %s', bus_id)
        return bus_id

    sql = "select bus_name from est_bus where project_id = '%s' and  bus_id='%s'" % (project_id, bus_id)
    _src_cursor.execute(sql)
    list1 = _src_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    bus_name = row['bus_name'].strip()
    # log_debug('src - bus_name -- %s', bus_name)


    project_id2, bus_id2, sub_id2 = parse_relation_id(_tgt_relation_id)
    sql = "select bus_id from est_bus where project_id = '%s' and bus_name='%s'" % (project_id2, bus_name)
    _tgt_cursor.execute(sql)
    list1 = _tgt_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    bus_id = row['bus_id']
    # log_debug('tgt - bus_id   -- %s', bus_id)

    return bus_id



def get_target_sub_bus_id(_src_relation_id, _tgt_relation_id, _src_cursor, _tgt_cursor):
    global my_meta_id
    global my_fail_id

    project_id, bus_id, sub_id = parse_relation_id(_src_relation_id)

    if my_is_meta_id(sub_id):
        # log_debug('sub-bus-id is meta: %s', sub_id)
        return sub_id

    sql = "select sub_bus_name from est_sub_bus where project_id = '%s' and  bus_id='%s' and sub_bus_id = '%s'" % (project_id, bus_id, sub_id)
    _src_cursor.execute(sql)
    list1 = _src_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    sub_name = row['sub_bus_name']
    # log_debug('src - sub_bus_name -- %s', sub_name)


    project_id2, bus_id2, sub_id2 = parse_relation_id(_tgt_relation_id)
    sql = "select sub_bus_id from est_sub_bus where project_id = '%s' and bus_id='%s' and sub_bus_name = '%s'" % (project_id2, bus_id2, sub_name)
    _tgt_cursor.execute(sql)
    list1 = _tgt_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    sub_bus_id = row['sub_bus_id']
    # log_debug('tgt - sub_bus_id   -- %s', sub_bus_id)

    return sub_bus_id



def translate_relation_id(_src_relation_id, _src_cursor, _tgt_cursor):
    global my_fail_id

    project_id, bus_id, sub_id = parse_relation_id(_src_relation_id)

    project_id2 = get_target_project_id(_src_relation_id, _src_cursor, _tgt_cursor)
    if project_id2 == my_fail_id:
        log_error('error: get_target_project_id')
        return ""

    new_relation_id = project_id2+bus_id+sub_id
    # log_debug('2: %s => %s', project_id2, new_relation_id)

    bus_id2 = get_target_bus_id(_src_relation_id, new_relation_id, _src_cursor, _tgt_cursor)
    if bus_id2 == my_fail_id:
        log_error('error: get_target_bus_id')
        return ""
    new_relation_id = project_id2+bus_id2+sub_id
    # log_debug('2: %s => %s', bus_id2, new_relation_id)

    sub_id2 = get_target_sub_bus_id(_src_relation_id, new_relation_id, _src_cursor, _tgt_cursor)
    if sub_id2 == my_fail_id:
        log_error('error: get_target_sub_bus_id')
        return ""
    new_relation_id = project_id2+bus_id2+sub_id2
    # log_debug('2: %s => %s', sub_id2, new_relation_id)

    return new_relation_id



def my_get_next_id(_id_type, _cursor):
    next_id = ""

    sql = "SELECT ID_TYPE, ID_NO FROM est_id_mng WITH( TABLOCKX ) WHERE id_type = '%s'" % (_id_type)
    # log_debug(sql)

    _cursor.execute(sql)
    list1 = _cursor.fetchall()

    sql = ""
    this_id = 0
    next_id = 0
    if len(list1) == 0:
        # log_info('first time')
        next_id = 1
        sql = "INSERT INTO est_id_mng WITH(TABLOCKX, UPDLOCK) (id_type , id_desc , id_no ) VALUES ( '%s' , '%s' , '%05d' ) " % ( _id_type , "AUTOM", next_id)
    else:
        # log_info('old guy')
        row = list1[0]
        this_id = int(row['ID_NO'])
        next_id = this_id + 1
        if next_id > 99999:
            elog("error: too big: %d", next_id)
            return "-1"

        sql = "UPDATE est_id_mng WITH(TABLOCKX, UPDLOCK) SET id_no= '%05d' WHERE id_type = '%s'" % (next_id, _id_type)

    # log_debug("sql -- [%s]", sql)
    _cursor.execute(sql)


    return '%05d' % next_id


# select flow_id from est_flow where flow_name = 'pub'
# input -- sub-table-name
def sub_get_res_id(_sub_table, _res_name, _cursor):

    table_name  = ''  # est_flow
    res_id_col  = ''  # flow_id
    res_name_col= ''  # flow_name

    my_key = '%s-GET_RES_ID' % (_sub_table)

    if not MyCtx.res_cvt_rule_map.has_key(my_key):
        log_error('error: cvt-rule not found: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.res_cvt_rule_map[my_key]
        table_name  = val_list[0]
        res_id_col  = val_list[1]
        res_name_col= val_list[2]


    sql = "select %s from %s where %s = '%s'" % (res_id_col, table_name, res_name_col, _res_name)
    # log_debug('%s', sql)

    _cursor.execute(sql)
    list0 = _cursor.fetchall()
    if len(list0) <= 0:
        log_error('error: no data: %s', sql)
        return ""

    row = list0[0]
    res_id = row[res_id_col]
    log_debug('sub-update res_id [%s => %s]', res_id_col, res_id)

    return res_id



# select func_name from est_func where func_id = '00234'
def sub_get_res_name(_sub_table, _log_row, _cursor):
    res_name = ''

    if not MyCtx.sub_res_name_map.has_key(_sub_table):
        log_error('error: sub_get_res_name: %s', _sub_table)
        return -1

    val_list = MyCtx.sub_res_name_map[_sub_table]

    main_table  = val_list[0]
    res_name_col= val_list[1]
    res_id_col  = val_list[2]
    res_id_val  = _log_row[res_id_col]

    sql = "select %s from %s where %s = '%s'" % (res_name_col, main_table, res_id_col, res_id_val)
    log_debug('[%s]', sql)

    _cursor.execute(sql)
    list0 = _cursor.fetchall()
    if len(list0) <= 0:
        log_error('error: no data: %s', sql)
        return res_name

    row = list0[0]
    res_name = row[res_name_col]
    log_debug('res_name[%s => %s]', res_name_col, res_name)

    return res_name





# dev.func_id => dev.func_name => sit.func_id
# step1. get RES-name from DEV
# step2. get RES-id   from SIT
def convert_res_id_one(_table, _column, _column_value, _src_cursor, _tgt_cursor):
    new_res_id = ''

    # select func_id from est_func where func_name = 'get_time'

    table_name  = ''  # est_func
    res_id_col  = ''  # func_id
    res_name_col= ''  # func_name
    res_name_val= ''  # as 'get_time'

    my_key = '%s-%s' % (_table, _column)


    if not MyCtx.res_cvt_rule_map.has_key(my_key):
        log_error('error: cvt-rule not found: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.res_cvt_rule_map[my_key]
        table_name  = val_list[0]
        res_id_col  = val_list[1]
        res_name_col= val_list[2]

    res_id_val = _column_value
    sql = "select %s from %s where %s = '%s'" % (res_name_col, table_name, res_id_col, res_id_val)
    # log_debug('sql1: [%s]', sql)

    _src_cursor.execute(sql)
    list0 = _src_cursor.fetchall()
    if len(list0) <= 0:
        log_error('error: no data: %s', sql)
        return res_name

    row = list0[0]
    res_name = row[res_name_col].strip()
    # log_debug('res_name[%s => %s]', res_name_col, res_name)

    # log_debug('%s, %s, %s', res_id_col, res_name_col, res_name_val)

    sql = "select %s from %s where %s = '%s'" % (res_id_col, table_name, res_name_col, res_name)
    # log_debug('%s', sql)

    _tgt_cursor.execute(sql)
    list0 = _tgt_cursor.fetchall()
    if len(list0) <= 0:
        log_error('error: no data: %s', sql)
        return res_name

    row = list0[0]
    new_res_id = row[res_id_col]
    # log_debug('res_id [%s => %s]', res_id_col, new_res_id)


    return new_res_id



def convert_res_id(_table, _log_row, _src_cursor, _tgt_cursor):

    my_key = '%s' % (_table)

    if not MyCtx.res_cvt_list_map.has_key(my_key):
        # log_info('no need to change res-id for: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.res_cvt_list_map[my_key]
        # log_debug('convert-list: %s => %s', my_key, val_list)


    for column in val_list:
        column_value = _log_row[column]
        # log_debug('column need to changed: [%s], [%s]', column, column_value)

        new_column_value = convert_res_id_one(_table, column, column_value, _src_cursor, _tgt_cursor)

        _log_row[column] = new_column_value

        # log_debug('column changed: [%s.%s], [%s => %s]', _table, column, column_value, new_column_value)

    return



def generate_res_id(_table, _log_row, _tgt_cursor):

    my_key = '%s' % (_table)

    if not MyCtx.res_gen_list_map.has_key(my_key):
        # log_info('no need to generate res-id for: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.res_gen_list_map[my_key]
        # log_debug('generate-list: %s => %s', my_key, val_list)


    for column in val_list:
        # column                           # elem_id

        if not MyCtx.id_mng_map.has_key(column):
            continue

        id_type = MyCtx.id_mng_map[column] # '16'

        column_value = _log_row[column]    # 00079
        # log_debug('column need to generate: [%s], [%s]', column, column_value)

        new_res_id = my_get_next_id(id_type, _tgt_cursor)

        _log_row[column] = new_res_id

        # log_debug('ID generated: [%s.%s], [%s => %s]', _table, column, column_value, new_res_id)

    return



def convert_relation_id(_table, _log_row, _src_cursor, _tgt_cursor):

    my_key = '%s' % (_table)

    if not MyCtx.rel_conv_list_map.has_key(my_key):
        # log_info('no need to convert rel-id for: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.rel_conv_list_map[my_key]
        # log_debug('convert-rel-list: %s => %s', my_key, val_list)


    for column in val_list:
        # column                            # relation_id

        relation_id = _log_row[column]      # 000040000100000
        # log_debug('column need to generate: [%s], [%s]', column, column_value)

        new_relation_id  = translate_relation_id(relation_id, _src_cursor, _tgt_cursor)
        if len(new_relation_id) == 0:
            log_error('error: translate_relation_id: %s', relation_id)
            return -1

        _log_row[column] = new_relation_id

        # log_debug('REL converted: [%s.%s], [%s => %s]', _table, column, relation_id, new_relation_id)


    return



def pre_process_lines(_lines):

    idx = 0

    last_oper_type = ''

    map1 = {}  # line list
    map2 = {}  # line_no seq
    del_list = []

    for line in _lines:
        # log_debug('+%s+', line)
        words = line.split(',')
        trim_list(words)
        table_class = words[0]
        table_name  = words[1]
        oper_type   = words[5]
        res_name    = words[6]

        if table_class != 'SUBT':
            # log_debug('ignore %s', table_class)
            idx += 1
            continue
        else:
            # log_debug('%s, %s, %s', table_name, oper_type, res_name)
            pass

        key_str = '%s-%s' % (table_name, res_name)

        if map1.has_key(key_str):
            # check changed
            if (oper_type == 'delete' or last_oper_type == 'delete') and oper_type != last_oper_type:
                # means changed

                # 1. delete item from MAP
                # 2. mark these lines to be deleted
                val_list = []
                val_list.append(line)
                map1[key_str] = val_list
                seq_list = map2[key_str]
                del_list = del_list + seq_list
                seq_list = []
                seq_list.append(idx)
                map2[key_str] = seq_list
                # log_debug('change: %s => %d, %s, %s', key_str, idx, val_list, seq_list)
            else:
                # NOT changed
                # 1. add this line to list
                val_list = map1[key_str]
                val_list.append(line)
                map1[key_str] = val_list
                seq_list = map2[key_str]
                seq_list.append(idx)
                map2[key_str] = seq_list
                # log_debug('append: %s => %d, %s, %s', key_str, idx, val_list, seq_list)
        else:
            # first time
            val_list = []
            val_list.append(line)
            seq_list = []
            seq_list.append(idx)
            map1[key_str] = val_list
            map2[key_str] = seq_list
            # log_debug('first: %s => %d, %s, %s', key_str, idx, val_list, seq_list)


        last_oper_type = oper_type
        idx += 1

    # log_debug('del: %s', del_list)


    del_list.sort(reverse=True)
    for del_idx in del_list:
        # log_debug('del: %d', del_idx)
        del _lines[del_idx]

    return 0

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
        log_error('error: %d -- %s', len(_words), _words)
        return None

    return my_dict



#######################################################################



# main()
if __name__=="__main__":
    sailog_set("myfunc.log")

    file_name = "test.%s.txt" % (sai_get_today())
    file_path = get_data_file_path(file_name)
    log_debug('input: %s', file_path)


    charset = sai_conf_get('MY', 'LANG')

    ###########################################################################
    conn = db_init('DEV')
    curr = conn.cursor(as_dict=True)

    """
    table = 'est_taskid_map'
    where = "where task_id in ('%s', '%s') " % ('PE-001', 'PE-002')
    content_list = export_from_table(table, where, curr)

    fo = open(file_path, 'w')
    for item in content_list:
        log_debug('%s', item.encode(charset))
        fo.write(item.encode(charset)+'\n')
    fo.close()
    ###########################################################################


    fo = open(file_path, "r")
    lines = fo.readlines()
    fo.close()

    conn = db_init('SIT')
    curr = conn.cursor(as_dict=True)
    table = 'est_taskid_map'

    import_to_table(lines, table, curr)
    conn.commit()

    rel = '000040000100000'
    rel = '000000000000000'
    rel = '00004PROST00000'

    # parse_relation_id(rel)
    # get_target_project_id(rel, curr, curr)
    # get_target_bus_id(rel, rel, curr, curr)
    # get_target_sub_bus_id(rel, rel, curr, curr)

    new = translate_relation_id(rel, curr, curr)
    log_debug('%s => %s', rel, new)

    my_get_next_id('88', curr)
    """

    file_name = "submission.%s.txt" % (sai_get_today())
    file_path = get_data_file_path(file_name)
    fo = open(file_path, "r")
    lines = fo.readlines()
    lines.sort()
    pre_process_lines(lines) 
    for line in lines:
        log_debug('%s', line)
    fo.close()

    ###########################################################################
    conn.commit()
    db_end(conn)


#
