# -*- coding: UTF-8 -*-


import os

from common  import *
from myfunc  import *



def check_sys_user():
    my_user = 'yase'

    sql = "select user_name from est_user_mng_new where user_name = '%s'" % (my_user)

    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()

    if len(list1) > 0:
        log_debug('has user %s', my_user)
        return 0

    sql = "insert into est_user_mng_new values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
           (my_user, 'sys-user', '1', 'yase', '1', ' ', '20180711-123456', '20180711-123456', '111111111100000000111111100000000000000000000000000000')
    log_debug('create new user %s', my_user)
    MyCtx.cursorX.execute(sql)
    MyCtx.connX.commit()

    return 0


def dist_get_relation_id(_ala_name, _cursor):

    if len(_ala_name) == 0:
        return ''

    sql  = "select project_id + bus_id + sub_bus_id rel_id, sub_bus_name from est_sub_bus where sub_bus_name = '%s'" % (_ala_name)

    _cursor.execute(sql)
    list1 = _cursor.fetchall()

    rel_id = ''

    row = list1[0]
    rel_id  = row['rel_id']
    log_debug('rel_id: %s', rel_id)

    return rel_id


def dist_generate_res_id(_table, _cursor):

    gen_newid_list_map = {
        'est_sub_bus'       : ['sub_bus_id',    '19'],
        'est_svc_logic'     : ['service_id',    '10'],
    }


    my_key = '%s' % (_table)

    if not gen_newid_list_map.has_key(my_key):
        return '', ''

    val_list    = gen_newid_list_map[my_key]

    id_name = val_list[0]  # sub_bus_id
    id_type = val_list[1]  # '19'

    new_res_id = my_get_next_id(id_type, _cursor)

    # log_debug('ID generated: [%s.%s] => [%s]', _table, id_name, new_res_id)

    return id_name, new_res_id


def dist_convert_res_id(_table):

    cvt_resid_list_map = {
        'est_svc_logic_revs'    : ['svc_logic_id'],
        'est_svc_proc'          : ['service_id'],
    }

    my_key = '%s' % (_table)

    if not cvt_resid_list_map.has_key(my_key):
        return '', ''

    val_list    = cvt_resid_list_map[my_key]

    id_col = val_list[0]  # svc_logic_id

    new_res_id = MyCtx.new_svc_id

    log_debug('ID converted: [%s.%s] => [%s]', _table, id_col, new_res_id)

    return id_col, new_res_id



def dist_generate_res_name(_table, _seq):

    gen_newname_list_map = {
        'est_sub_bus'   : 'sub_bus_name',
        'est_svc_logic' : 'svc_name',
    }


    my_key = '%s' % (_table)

    if not gen_newname_list_map.has_key(my_key):
        return '', ''

    col_name = gen_newname_list_map[my_key]

    suffix = '%03d' % (_seq)

    return col_name, suffix


def dist_generate_res_desc(_table, _seq):

    gen_newdesc_list_map = {
        'est_sub_bus'   : 'sub_bus_desc',
        'est_svc_logic' : 'svc_desc',
    }

    my_key = '%s' % (_table)

    if not gen_newdesc_list_map.has_key(my_key):
        return '', ''

    col_name = gen_newdesc_list_map[my_key]

    suffix = '%03d' % (_seq)

    return col_name, suffix



def dist_generate_insert_cm(_table, _key_value, _seq):
    log_debug('-'*80)
    table_key_column_map = {
        'est_sub_bus'           : 'sub_bus_name',
        'est_svc_logic'         : 'svc_name',
        'est_svc_logic_revs'    : 'svc_logic_id',
        'est_svc_proc'          : 'service_id',
    }

    sql_list = []

    ###################################################################
    # SQL -- get table structure
    sql   = 'sp_columns %s' % (_table)
    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()

    ###################################################################
    # SQL -- get source table's data
    key_column = table_key_column_map[_table]
    sql   = "select * from %s where %s = '%s'" % (_table, key_column , _key_value)
    MyCtx.cursorX.execute(sql)
    list2 = MyCtx.cursorX.fetchall()
    if len(list2) <= 0:
        log_info('warn: no data: %s', sql)
        return []
    #  data_row = list2[0]

    ###################################################################
    for data_row in list2:

        # use system user
        if data_row.has_key('crt_user'):
            data_row['crt_user'] = 'yase'
        if data_row.has_key('lst_mod_user'):
            data_row['lst_mod_user'] = 'yase'

        # use new relation-id
        if data_row.has_key('relation_id'):
            new_rel_id = dist_get_relation_id(MyCtx.new_ala_name, MyCtx.cursorX)
            if len(new_rel_id) > 0:
                old_rel_id = data_row['relation_id']
                log_debug('CHANGE-relation: [%s] => [%s]', old_rel_id, new_rel_id)
                data_row['relation_id'] = new_rel_id

        # generate new name
        res_name_col, res_name_suffix = dist_generate_res_name(_table, _seq)
        if len(res_name_suffix) > 0:
            old_res_name = data_row[res_name_col].strip()
            new_res_name = "%s_%s" % (old_res_name, res_name_suffix)
            log_debug('CHANGE-name: [%s] => [%s]', old_res_name, new_res_name)
            data_row[res_name_col] = new_res_name

            if _table == 'est_sub_bus':
                MyCtx.new_ala_name = new_res_name
                log_debug('new ala-name: %s', MyCtx.new_ala_name)
            elif _table == 'est_svc_logic':
                MyCtx.new_svc_name = new_res_name
                # log_debug('new svc-name: %s', MyCtx.new_svc_name)
            else:
                log_debug('xxx')


        # generate new desc
        res_name_col, suffix = dist_generate_res_desc(_table, _seq)
        if len(suffix) > 0:
            old_res_name = data_row[res_name_col].strip()
            new_res_name = "%s_%s" % (old_res_name, suffix)
            log_debug('CHANGE-name: [%s] => [%s]', old_res_name, new_res_name)
            data_row[res_name_col] = new_res_name


        # generate new resource-id
        res_id_col, new_res_id = dist_generate_res_id(_table, MyCtx.cursorX)
        if len(new_res_id) > 0:
            log_debug('CHANGE-id: [%s] => [%s]', data_row[res_id_col], new_res_id)
            data_row[res_id_col] = new_res_id

            if _table == 'est_svc_logic':
                MyCtx.new_svc_id = new_res_id
                log_debug('new svc-id: %s', MyCtx.new_svc_id)


        # convert resource-id
        res_id_col, cvt_res_id = dist_convert_res_id(_table)
        if len(cvt_res_id) > 0:
            log_debug('CHANGE-id2: [%s] => [%s]', data_row[res_id_col], cvt_res_id)
            data_row[res_id_col] = cvt_res_id


        column_name_list    = []
        column_value_list   = []
        for row in list1:
            column_name = row['COLUMN_NAME']
            column_type = row['TYPE_NAME']

            if data_row[column_name] is None:
                column_value= "null"
            else:
                column_value= "'" + str(data_row[column_name]).strip() + "'"

            if column_type == 'int identity':
                # log_debug('%s -- %s', column_name, column_type)
                continue

            column_name_list.append(column_name)
            column_value_list.append(column_value)
            # log_debug('[%s] => [%s]', column_name, column_value)

        buf1 = ', '.join(column_name_list)

        buf2 = ', '.join(column_value_list)

        sql = "insert into %s (%s) values (%s)" % (_table, buf1, buf2)
        sql_list.append(sql)
        log_debug('\n%s', sql)

    return sql_list



def dist_generate_one(_ala_name, _svc_name, _svc_id, _seq):


    ###################################################################
    # service logic
    table_name = 'est_svc_logic'
    sql = dist_generate_insert_cm(table_name, _svc_name, _seq)
    # log_debug('%s', sql)

    for one in sql:
        MyCtx.cursorX.execute(one)

    ###################################################################
    # sub-table-1
    table_name = 'est_svc_logic_revs'
    sql = dist_generate_insert_cm(table_name, _svc_id, _seq)
    # log_debug('%s', sql)
    for one in sql:
        MyCtx.cursorX.execute(one)

    ###################################################################
    # sub-table-2
    table_name = 'est_svc_proc'
    sql = dist_generate_insert_cm(table_name, _svc_id, _seq)
    # log_debug('%s', sql)
    for one in sql:
        MyCtx.cursorX.execute(one)


    return 0



def dist_duplicate_sub_bus(_ala_name, _seq):

    ###################################################################
    sql  = "select project_id + bus_id + sub_bus_id rel_id, sub_bus_name from est_sub_bus where sub_bus_name = '%s'" % (_ala_name)

    log_debug('%s', sql)

    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    rel_id  = ''
    ala_name= ''
    for row in list1:
        rel_id  = row['rel_id']
        ala_name= row['sub_bus_name']
        log_debug('rel_id: %s, name: %s', rel_id, ala_name)

    if len(rel_id) == 0:
        print("error: not found ALA: '%s'" % _ala_name)
        return -1


    # do generate
    table_name = 'est_sub_bus'
    MyCtx.new_ala_name = ''
    sql = dist_generate_insert_cm(table_name, _ala_name, _seq)
    # log_debug('%s', sql)

    for one in sql:
        MyCtx.cursorX.execute(one)

    ###################################################################

    sql = "select * from est_svc_logic where relation_id = '%s'" % (rel_id)
    # log_debug('%s', sql)

    MyCtx.cursorX.execute(sql)

    list2 = MyCtx.cursorX.fetchall()

    # more than 1 usually
    for row in list2:
        svc_id  = row['service_id']
        svc_name= row['svc_name']
        # log_debug('%s -- %s', svc_id, svc_name)
        MyCtx.new_svc_name = ''
        MyCtx.new_svc_id   = ''
        dist_generate_one(ala_name, svc_name, svc_id, _seq)

    ###################################################################


    return 0


# 
def dist_init():
    src_dbname = 'DEV'

    tasks = ''

    args = sai_get_args()

    if len(args) == 0:
        tasks = 'DEMO_ALA1'
    else:
        tasks = args[0]

    log_debug('ala-name: %s', tasks)

    # DB connection
    MyCtx.connX  = db_init(src_dbname)
    MyCtx.cursorX= MyCtx.connX.cursor(as_dict=True)

    check_sys_user()

    return tasks


def dist_doit(tasks):

    log_debug('--- RES and LOG ---')

    start_str   = sai_conf_get("DUP", "START")
    last_str    = sai_conf_get("DUP", "LAST")

    start = int(start_str)
    last  = int(last_str)

    if start > last:
        print('error: invalid config: start[%d] > last[%d]', start, last)
        return -1

    for seq in range(start, last+1):
        log_debug('seq: %d', seq)
        rv = dist_duplicate_sub_bus(tasks, seq)
        if rv < 0:
            print("error: dist_duplicate_sub_bus: '%s, %s'" % (tasks, seq))
            return -1

    MyCtx.connX.commit()

    return 0


def dist_done():
    # finally
    MyCtx.connX.rollback()
    MyCtx.connX.close()

    return 0


def dist_run():
    rv = 0

    tasks = dist_init()

    dist_doit(tasks)

    dist_done()


    return rv



# main()
if __name__=="__main__":
    sailog_set("dist.log")
    dist_run()

# dist.py
