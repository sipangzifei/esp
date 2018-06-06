# -*- coding: UTF-8 -*-


from sailog  import *
from saiconf import *
from saiutil import *
from saidb   import *


# this is a simulator of ESBuilder


# 1. query current-table
# 2. insert into log-table
def simulate_add(_task_id, _element_name, _cursor):
    sql = "select * from est_element where elem_name = '%s'" % (_element_name)
    log_debug(sql)

    _cursor.execute(sql)

    list1 = _cursor.fetchall()
    if len(list1) == 0:
        log_error("error: not found")
        return -1

    row = list1[0]
    sql = """insert into est_element_his values 
    ('insert', '%s', '00000', '%s', 
     '%s', '%s', '%s', '%s', 
     '%s', '%s', '%s', '%s', 
     '%s', '%s', '%s', '%s', 
     '%s', '%s', '%s', '%s') """ % (sai_get_date_time(), _task_id,
    row['elem_id'], row['relation_id'], row['hash_id'], row['elem_name'],
    row['elem_desc'], row['elem_class'], row['elem_type'], row['def_val'],
    row['max_len'], row['elem_prec'], row['resource_id'], row['grp_id'],
    row['crt_user'], row['crt_date_time'], row['lst_mod_user'], row['lst_mod_date_time'])

    log_debug(sql)
    _cursor.execute(sql)

    return 0


# 1. query current-table
# 2. insert into log-table
# 取最后一行
def simulate_update(_task_id, _element_name, _cursor):
    # sql = "select top 1 * from est_element_updated where elem_name = '%s' order by lst_mod_date_time desc" % (_element_name)
    sql = "select top 1 * from (select top 2 * from est_element_updated where elem_name='%s' order by lst_mod_date_time desc) as t1 order by lst_mod_date_time" % (_element_name)
    log_debug(sql)

    _cursor.execute(sql)

    list1 = _cursor.fetchall()
    if len(list1) == 0:
        log_error("error: not found")
        return -1

    row = list1[0]
    sql = """insert into est_element_his values 
    ('update', '%s', '00000', '%s', 
     '%s', '%s', '%s', '%s', 
     '%s', '%s', '%s', '%s', 
     '%s', '%s', '%s', '%s', 
     '%s', '%s', '%s', '%s') """ % (sai_get_date_time(), _task_id,
    row['elem_id'], row['relation_id'], row['hash_id'], row['elem_name'],
    row['elem_desc'], row['elem_class'], row['elem_type'], row['def_val'],
    row['max_len'], row['elem_prec'], row['resource_id'], row['grp_id'],
    row['crt_user'], row['crt_date_time'], row['lst_mod_user'], row['lst_mod_date_time'])

    log_debug(sql)
    _cursor.execute(sql)

    return 0


# 1. query deleted-table
# 2. insert into log-table
# 还原map表
# 还原element表
def simulate_delete(_task_id, _element_name, _cursor):
    # step1: element
    sql = "select top 1 * from est_element_deleted where elem_name = '%s' order by lst_mod_date_time desc" % (_element_name)
    log_debug(sql)

    _cursor.execute(sql)

    list1 = _cursor.fetchall()
    if len(list1) == 0:
        log_error("error: not found")
        return -1

    row = list1[0]
    sql = """insert into est_element_his values 
    ('delete', '%s', '00000', '%s', 
     '%s', '%s', '%s', '%s', 
     '%s', '%s', '%s', '%s', 
     '%s', '%s', '%s', '%s', 
     '%s', '%s', '%s', '%s') """ % (sai_get_date_time(), _task_id,
    row['elem_id'], row['relation_id'], row['hash_id'], row['elem_name'],
    row['elem_desc'], row['elem_class'], row['elem_type'], row['def_val'],
    row['max_len'], row['elem_prec'], row['resource_id'], row['grp_id'],
    row['crt_user'], row['crt_date_time'], row['lst_mod_user'], row['lst_mod_date_time'])

    log_debug(sql)
    _cursor.execute(sql)

    # step2: map
    sql = "insert into est_taskid_map select * from est_taskid_map_deleted where res_name = '%s'" % (_element_name)
    log_debug(sql)

    _cursor.execute(sql)


    return 0

def usage():
    print("python simulator.py operation res-name task-id")
    print(" -- operation: update/insert/delete")
    print("python simulator.py update E1 task1")
    return 0


def worker():

    argv = sai_get_args()
    log_debug(argv)

    if len(argv) != 3:
        print("invalid usage:")
        usage()
        return -1

    operation = argv[0]
    res_name  = argv[1]
    task_id   = argv[2]

    if operation != 'insert' and operation != 'update' and operation != 'delete':
        print("invalid operation: %s" % operation)
        return -1

    conn = db_init()

    cursor = conn.cursor(as_dict=True)

    if operation == 'insert':
        log_debug("simulate insert: res[%s], task[%s]", res_name, task_id)
        simulate_add(task_id, res_name, cursor)
    elif operation == 'update':
        log_debug("simulate update: res[%s], task[%s]", res_name, task_id)
        simulate_update(task_id, res_name, cursor)
    elif operation == 'delete':
        log_debug("simulate delete: res[%s], task[%s]", res_name, task_id)
        simulate_delete(task_id, res_name, cursor)
    else:
        log_error("error: invalid command: %s", operation)
        return -1;


    conn.commit();
    conn.close()

# main()
if __name__=="__main__":
    sailog_set("simulator.log")
    worker()


# simulator.py
