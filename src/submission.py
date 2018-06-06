# -*- coding: UTF-8 -*-


import os

from common  import *
from myfunc  import *


# 
def get_elem_log_list(_task_id, _elem_name, _elem_id, _table):
    log_table = '%s_log' % (_table)

    # TODO: name, id

    sql = """select * from %s
where task_id = '%s' and elem_name = '%s' and elem_id = '%s' 
order by oper_id""" % (log_table, _task_id, _elem_name, _elem_id)
    # log_debug("log: %s", sql)

    MyCtx.cursorX.execute(sql)

    listx = MyCtx.cursorX.fetchall()

    return listx


# function:
#   get the next record after input
#   must has value, or failure
#
# implement
#   1. query log table 
#   2. query res table
#
# input
#   _row is of LOG-table
def get_elem_next_record(_row, _table):

    # res-table-name, log-table-name
    res_table = _table
    log_table = '%s_log' % (_table)


    elem_id   = _row['elem_id']
    elem_name = _row['elem_name']
    oper_date_time = _row['oper_date_time']
    oper_id = _row['oper_id']



    # TODO: elem_id, elem_name

    # log
    sql = "select top 1 * from %s where elem_name='%s' and elem_id = '%s' and oper_id > %s  order by oper_id" % (log_table, elem_name, elem_id, oper_id)
    # log_debug("%s", sql)
    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()
    if len(list1) == 0:
        log_debug("NOT found in LOG-table, let's query in RES-table")
        # res
        sql = "select * from %s where elem_name = '%s' and elem_id = '%s' " % (res_table, elem_name, elem_id)
        # log_debug("%s", sql)

        MyCtx.cursorX.execute(sql)
        list1 = MyCtx.cursorX.fetchall()
    else:
        log_debug("Found in LOG-table")
        pass


    return list1





# 2018-5-17
def get_res_log_list(_task_id, _res_name, _res_id, _table):
    log_table = '%s_log' % (_table)

    # get name, id as 'elem_name', 'elem_id'
    type_name, type_id = get_type_name_id(_table)
    if type_name == -1:
        log_error('error: get_type_name_id')
        return []

    sql = """select * from %s
where task_id = '%s' and %s = '%s' and %s = '%s' 
order by oper_id""" % (log_table, _task_id, type_name, _res_name, type_id, _res_id)
    # log_debug("log: %s", sql)

    MyCtx.cursorX.execute(sql)

    listx = MyCtx.cursorX.fetchall()

    return listx


# 2018-5-17
# function:
#   get the next record after input
#   must has value, or failure
#
# implement
#   1. query log table 
#   2. query res table
#
# input
#   _row is of LOG-table
def get_res_next_record(_row, _table):

    # res-table-name, log-table-name
    res_table = _table
    log_table = '%s_log' % (_table)

    # get name, id as 'elem_name', 'elem_id'
    type_name, type_id = get_type_name_id(_table)
    if type_name == -1:
        log_error('error: get_type_name_id')
        return []

    res_id   = _row[type_id]
    res_name = _row[type_name]
    oper_date_time = _row['oper_date_time']
    oper_id = _row['oper_id']


    # log
    sql = "select top 1 * from %s where %s = '%s' and %s = '%s' and oper_id > %s  order by oper_id" % (log_table, type_name, res_name, type_id, res_id, oper_id)
    # log_debug("%s", sql)
    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()
    if len(list1) == 0:
        log_debug("NOT found in LOG-table, let's query in RES-table")
        # res
        sql = "select * from %s where %s = '%s' and %s = '%s' " % (res_table, type_name, res_name, type_id, res_id)
        # log_debug("%s", sql)

        MyCtx.cursorX.execute(sql)
        list1 = MyCtx.cursorX.fetchall()
    else:
        log_debug("Found in LOG-table")
        pass


    return list1


def get_map_where_str(_task_list):
    where_section = ""

    tmp = ""
    for task in _task_list:
        task = task.strip()
        log_debug("-- [%s] --", task)

        if len(tmp) > 0:
            tmp += ", "

        tmp += "'%s'" % (task)

    if len(tmp) > 0:
        where_section = "where task_id in (%s)" % (tmp)

    return where_section




# very common
def compare_two_row(_row1, _row2, _table_config):

    buffer = ""
    buffer_list = []
    # Get config list
    key_list, cmp_list, dis_list = get_config_list(_table_config)
    if len(key_list) == 0:
        log_error("sorry, please config cnf/my.cnf KEY=xxx")
    elif len(cmp_list) == 0:
        log_error("sorry, please config cnf/my.cnf CMP=xxx")
    else:
        # log_debug("%s", cmp_list)
        pass

    key_str = ""
    for column in key_list:
        column = column.strip().lower()
        v      = _row1[column].strip()
        if len(key_str) > 0:
            key_str += "+"
        key_str += "%s" % (v)

    for column in cmp_list:
        column = column.strip()
        # log_debug('column = %s', column)
        v1 = _row1[column]
        v2 = _row2[column]
        if isinstance(v1, int):
            # log_debug("---int")
            pass
        elif isinstance(v1, str):
            # log_debug("---string")
            pass
        elif isinstance(v1, float):
            # log_debug("---float")
            pass
        elif isinstance(v1, bytes):
            # log_debug("---bytes")
            pass
        elif isinstance(v1, unicode):
            # log_debug("---unicode")
            v1 = v1.rstrip().encode("gbk")
            v2 = v2.rstrip().encode("gbk")
        elif v1 is None:
            # log_debug("---%s: None",  column)
            pass
        else:
            log_info("---other")
            log_info('---type = %s', type(v1))

        # log_debug('---[%s] : [%s]',  v1, v2)

        if v1 != v2:
            log_info('>>> [%s] %s: (%s => %s)', key_str, column, v1, v2)
            buffer = "%s, %s, %s" % (column, v1, v2)
            buffer_list.append(buffer)
            pass
        else:
            # log_debug('---the same[%s] : [%s]', v1, v2)
            pass

    return buffer_list



# row is of est_taskid_map
def deal_element(_row):
    content = ""

    task_id     = _row['task_id'].strip()
    elem_name   = _row['res_name'].strip()
    elem_id     = _row['res_no'].strip()
    table_name  = _row['res_type'].strip()
    table_class = _row['res_type_class'].strip()


    log_debug("task_id   = [%s]", task_id)
    log_debug("elem_id   = [%s]", elem_id)
    log_debug("elem_name = [%s]", elem_name)
    log_debug("table     = [%s]", table_name)


    # query record from LOG table
    list2 = get_elem_log_list(task_id, elem_name, elem_id, table_name)
    if len(list2) == 0:
        log_debug("error: not found in log-table: %s, %s", task_id, elem_name)
        return ""


    # step: compare within log table
    buffer = ""
    next_row = {}
    for row in list2:
        log_debug("------------------------------------------------------------------")
        oper_type      = row['oper_type']
        oper_date_time = row['oper_date_time']
        oper_id        = row['oper_id']
        log_debug('is %s', oper_type)
        # log_debug('opertime: %s', oper_date_time)

        buffer = ""
        buffer_list = []

        if oper_type == 'insert':
            buffer = "%s, %-20s, %06d, %s, %s, %s, %s" % (table_class, table_name, oper_id, oper_date_time, task_id, oper_type, elem_name)
            content += "%s\n" % (buffer)
        elif oper_type == 'delete':
            buffer = "%s, %-20s, %06d, %s, %s, %s, %s" % (table_class, table_name, oper_id, oper_date_time, task_id, oper_type, elem_name)
            content += "%s\n" % (buffer)
        elif oper_type == 'update':
            #  get-next-row
            list3 = get_elem_next_record(row, table_name)
            if len(list3) == 0:
                log_error("error: get_elem_next_record: not data")
                return ""

            next_row = list3[0]

            # 'meta' means got the row from RES-table, instead of LOG-table
            next_oper_type      = next_row.get('oper_type',      'meta')
            next_oper_date_time = next_row.get('oper_date_time', 'meta')
            next_oper_id        = next_row.get('oper_id',        0)
            log_debug('next_row: %s -- %s -- %s', next_oper_type, next_oper_date_time, next_oper_id)
            if next_oper_type == 'update' or next_oper_type == 'delete' or next_oper_type == 'meta':
                # the next row of 'update'
                log_debug('compare two row')
                buffer_list = compare_two_row(row, next_row, table_name)

                # format to submission
                for item in buffer_list:
                    buffer = item
                    buffer = "%s, %-20s, %06d, %s, %s, %s, %s, %s" % (table_class, table_name, oper_id, oper_date_time, task_id, 'update', elem_name, buffer)
                    content += "%s\n" % (buffer)
            else:
                # next_oper_type is 'insert' means something missed
                log_error('error: invalid operation after update: %s', next_oper_type)
                return ""
        else:
            log_error('error: invalid oper_type: %s', oper_type)
            return ""
        # format to submission


    return content



# this is a general function than deal element
# row is of est_taskid_map
def deal_resource2(_row):
    content = ""

    task_id     = _row['task_id'].strip()
    res_name    = _row['res_name'].strip()
    res_id      = _row['res_no'].strip()
    table_name  = _row['res_type'].strip()
    table_class = _row['res_type_class'].strip()


    log_debug("task_id   = [%s]", task_id)
    log_debug("res_id    = [%s]", res_id)
    log_debug("res_name  = [%s]", res_name)
    log_debug("table     = [%s]", table_name)


    # query record from LOG table
    list2 = get_res_log_list(task_id, res_name, res_id, table_name)
    if len(list2) == 0:
        log_debug("error: not found in log-table: %s, %s", task_id, res_name)
        return ""


    # step: compare within log table
    buffer = ""
    next_row = {}
    for row in list2:
        log_debug("------------------------------------------------------------------")
        oper_type      = row['oper_type']
        oper_date_time = row['oper_date_time']
        oper_id        = row['oper_id']
        log_debug('is %s', oper_type)
        # log_debug('opertime: %s', oper_date_time)

        buffer = ""
        buffer_list = []

        if oper_type == 'insert':
            buffer = "%s, %-20s, %06d, %s, %s, %s, %s" % (table_class, table_name, oper_id, oper_date_time, task_id, oper_type, res_name)
            content += "%s\n" % (buffer)
        elif oper_type == 'delete':
            buffer = "%s, %-20s, %06d, %s, %s, %s, %s" % (table_class, table_name, oper_id, oper_date_time, task_id, oper_type, res_name)
            content += "%s\n" % (buffer)
        elif oper_type == 'update':
            #  get-next-row
            list3 = get_res_next_record(row, table_name)
            if len(list3) == 0:
                log_error("error: get_res_next_record: not data")
                return ""

            next_row = list3[0]

            # 'meta' means got the row from RES-table, instead of LOG-table
            next_oper_type      = next_row.get('oper_type',      'meta')
            next_oper_date_time = next_row.get('oper_date_time', 'meta')
            next_oper_id        = next_row.get('oper_id',        0)
            log_debug('next_row: %s -- %s -- %s', next_oper_type, next_oper_date_time, next_oper_id)
            if next_oper_type == 'update' or next_oper_type == 'delete' or next_oper_type == 'meta':
                # the next row of 'update'
                log_debug('compare two row')
                buffer_list = compare_two_row(row, next_row, table_name)

                # format to submission
                for item in buffer_list:
                    buffer = item
                    buffer = "%s, %-20s, %06d, %s, %s, %s, %s, %s" % (table_class, table_name, oper_id, oper_date_time, task_id, 'update', res_name, buffer)
                    content += "%s\n" % (buffer)

                if len(buffer_list) == 0:
                    log_info("warn: two rows are the same")
            else:
                # next_oper_type is 'insert' means something missed
                log_error('error: invalid operation after update: %s', next_oper_type)
                return ""
        else:
            log_error('error: invalid oper_type: %s', oper_type)
            return ""
        # format to submission


    return content


# 2018-5-21
def get_sub_type_name_id(_table):

    key_column = ''

    my_key = _table

    if MyCtx.subt_table_key_map.has_key(my_key):
        key_list = MyCtx.subt_table_key_map[my_key]
        key_column = key_list[0]
    else:
        log_error("error: not found: [%s]", my_key)
        key_column = -1


    return key_column


# 2018-5-21
def get_sub_res_log_list(_task_id, _res_id, _table):
    log_table = '%s_log' % (_table)

    # get id-name as 'func_id'
    sub_type_id = get_sub_type_name_id(_table)
    log_debug('sub-type: %s', sub_type_id)

    sql = """select * from %s
where task_id = '%s' and %s = '%s' 
order by oper_id""" % (log_table, _task_id, sub_type_id, _res_id)
    log_debug("log: %s", sql)

    MyCtx.cursorX.execute(sql)

    listx = MyCtx.cursorX.fetchall()

    return listx


# 2018-5-21
# row is of est_taskid_map still
# sub-table as 'est_func_param'
def deal_subtype_resource(_row):
    content = ""

    task_id     = _row['task_id'].strip()
    res_name    = _row['res_name'].strip()
    res_id      = _row['res_no'].strip()
    table_name  = _row['sub_res_type'].strip() # changed
    table_class = _row['res_type_class'].strip()

    log_debug("the following is for sub")
    log_debug("sub-task_id   = [%s]", task_id)
    log_debug("sub-res_id    = [%s]", res_id)
    log_debug("sub-res_name  = [%s]", res_name)
    log_debug("sub-table     = [%s]", table_name)

    # query record from LOG table
    list2 = get_sub_res_log_list(task_id, res_id, table_name)
    if len(list2) == 0:
        log_info("warn: sub: not found in log-table: %s, %s", task_id, res_id)
        return ""


    # step: compare within log table
    buffer = ""
    next_row = {}
    for row in list2:
        log_debug('$'*100)
        oper_type      = row['oper_type']
        oper_date_time = row['oper_date_time']
        oper_id        = row['oper_id']
        log_debug('is %s', oper_type)
        # log_debug('opertime: %s', oper_date_time)

        buffer = ""
        buffer_list = []

        if oper_type == 'insert':
            buffer = "%s, %-20s, %06d, %s, %s, %s, %s" % (table_class, table_name, oper_id, oper_date_time, task_id, oper_type, res_name)
            content += "%s\n" % (buffer)
        elif oper_type == 'delete':
            buffer = "%s, %-20s, %06d, %s, %s, %s, %s" % (table_class, table_name, oper_id, oper_date_time, task_id, oper_type, res_name)
            content += "%s\n" % (buffer)
        elif oper_type == 'update':
            log_error('error: invalid operation as update: %s', oper_type)
            return ""
        else:
            log_error('error: invalid oper_type: %s', oper_type)
            return ""
        # format to submission


    return content



def get_resource_list(_task_list):

    where_section = get_map_where_str(_task_list)

    sql = "select distinct task_id, res_type, res_name, res_no from est_taskid_map %s order by 1, 2, 3" % (where_section)
    log_debug(sql)

    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    return list1


def export_resource(_task_list):
    rv = 0

    file_name = "submission.%s.txt" % (sai_get_today())
    file_path = get_data_file_path(file_name)

    fo = open(file_path, "w")

    list1 = get_resource_list(_task_list)

    # for row in list1:
    #     log_debug(row)

    for row in list1:
        log_debug("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        res_type = row['res_type'].strip()
        row['res_type_class']   = 'MAIN'
        #log_debug('row = %s', (row))
        if res_type == 'est_element':
            # log_debug("is element")
            content = deal_element(row)
            if len(content) > 0:
                # log_debug("\n%s", content)
                fo.write(content)
            else:
                log_error("error: deal_element failure")
        elif res_type == 'est_func':
            # log_debug("is func")
            content = deal_resource2(row)
            if len(content) > 0:
                # log_debug("\n%s", content)
                fo.write(content)
            else:
                log_info("warn: deal_resource2 not succeeds")

            # sub - est_func_param
            row['res_type_class']   = 'SUBT'
            row['sub_res_type']     = 'est_func_param'
            content = deal_subtype_resource(row)
            if len(content) > 0:
                # log_debug("\n%s", content)
                fo.write(content)
            else:
                log_info("warn: sub: deal_resource no data")
        elif res_type == 'est_enum':
            # log_debug("is enum")
            content = deal_resource2(row)
            if len(content) > 0:
                # log_debug("\n%s", content)
                fo.write(content)
            else:
                log_info("warn: deal_resource2 not succeeds")

            # sub - est_enum_value
            row['res_type_class']   = 'SUBT'
            row['sub_res_type']     = 'est_enum_value'
            content = deal_subtype_resource(row)
            if len(content) > 0:
                # log_debug("\n%s", content)
                fo.write(content)
            else:
                log_info("warn: sub: deal_resource no data")

        elif res_type == 'est_format':
            # log_debug("is format")
            content = deal_resource2(row)
            if len(content) > 0:
                # log_debug("\n%s", content)
                fo.write(content)
            else:
                log_info("warn: deal_resource2 not succeeds")


            # sub1 - est_fmt_item
            row['res_type_class']   = 'SUBT'
            row['sub_res_type']     = 'est_fmt_item'
            content = deal_subtype_resource(row)
            if len(content) > 0:
                # log_debug("\n%s", content)
                fo.write(content)
            else:
                log_info("warn: sub: deal_resource no data")

            # sub2 - est_sign_item
            row['res_type_class']   = 'SUBT'
            row['sub_res_type']     = 'est_sign_item'
            content = deal_subtype_resource(row)
            if len(content) > 0:
                # log_debug("\n%s", content)
                fo.write(content)
            else:
                log_info("warn: sub: deal_resource no data")

        else:
            log_error("error: other resource: [%s]", res_type)


    fo.close()


    fo = open(file_path, "r")
    lines = fo.readlines()
    lines.sort()
    log_debug('--- CHG ---')
    for line in lines:
        log_debug('[%s]', line.strip())
    fo.close()

    return rv



def export_taskid_map(_task_list):
    rv = 0

    charset = sai_conf_get('MY', 'LANG')

    file_name = "taskidmap.%s.txt" % (sai_get_today())
    file_path = get_data_file_path(file_name)

    where_section = get_map_where_str(_task_list)

    table_name = 'est_taskid_map'
    content_list = export_from_table(table_name, where_section, MyCtx.cursorX)

    fo = open(file_path, "w")

    for line in content_list:
        log_debug("%s", line.encode(charset))
        fo.write(line.encode(charset)+'\n')

    fo.close()

    return rv


def export_taskid(_task_list):
    rv = 0

    charset = sai_conf_get('MY', 'LANG')

    file_name = "taskid.%s.txt" % (sai_get_today())
    file_path = get_data_file_path(file_name)

    where_section = get_map_where_str(_task_list)

    table_name = 'est_taskid_mng'
    content_list = export_from_table(table_name, where_section, MyCtx.cursorX)

    fo = open(file_path, "w")

    for line in content_list:
        log_debug("%s", line.encode(charset))
        fo.write(line.encode(charset)+'\n')

    fo.close()

    return rv


def usage():
    print('usage:')
    print('    python %s db-name task1 task2 ...' % (__file__))
    print('')
    print('Here continued by using $PROJECT/cnf/my.conf')


def submission_init():
    src_dbname = ''

    task_list = ['task1', 'task2']
    task_list = ['task2', 'task3']
    task_list = ['task1', 'task3']
    task_list = ['task3']
    task_list = ['task2']
    task_list = ['task1', 'task2', 'task3']
    task_list = ['task1']
    task_list = ['PE-001']

    args = sai_get_args()

    if len(args) == 0:
        usage()

        tasks = sai_conf_get('RUNTIME', 'TASK_LIST')
        task_list = tasks.split(',')
        trim_list(task_list)
        log_debug('task-list0: %s', task_list)

        src_dbname = sai_conf_get('RUNTIME', 'SOURCE').strip()
        log_debug('source: [%s]', src_dbname)

    elif len(args) == 1:
        src_dbname= args[0]
        log_debug('dbname1:    %s',src_dbname)
        log_debug('task-list1: %s', task_list)

    elif len(args) > 1:
        src_dbname= args[0]
        del args[0]
        task_list = args
        log_debug('dbname2:    %s',src_dbname)
        log_debug('task-list2: %s', task_list)


    # DB connection
    MyCtx.connX  = db_init(src_dbname)
    MyCtx.cursorX= MyCtx.connX.cursor(as_dict=True)

    return task_list


def submission_doit(task_list):


    # export resource and log
    log_debug(MyCtx.seperator)
    log_debug('--- RES and LOG ---')
    rv = export_resource(task_list)
    if rv < 0:
        log_error('error: export_resource')
        return -1


    # export taskid-map 2018-5-2
    log_debug(MyCtx.seperator)
    log_debug('--- MAP ---')
    rv = export_taskid_map(task_list)
    if rv < 0:
        log_error('error: export_taskid_map')
        return -1


    # export taskid_mng 2018-5-16
    log_debug(MyCtx.seperator)
    log_debug('--- TASK ---')
    rv = export_taskid(task_list)
    if rv < 0:
        log_error('error: export_taskid')
        return -1

    log_debug(MyCtx.seperator)

    return 0


def submission_done():
    # finally
    MyCtx.connX.close()

    return 0


def submission_run():
    rv = 0

    task_list = submission_init()

    submission_doit(task_list)

    submission_done()


    return rv



# main()
if __name__=="__main__":
    sailog_set("submission.log")
    submission_run()

# submission.py