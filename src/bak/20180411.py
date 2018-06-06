
def rescue_deleted(_row, _cursor):
    elem_name = _row['elem_name']
    oper_date_time = _row['oper_date_time']
    sql = "select top 1 * from est_element_his where elem_name='%s' and oper_date_time > '%s'  order by oper_date_time" % (elem_name, oper_date_time)

    log_debug("%s", sql)

    _cursor.execute(sql)

    list1 = _cursor.fetchall()

    return list1



            list3 = rescue_deleted(last_row, _cursor)
            if len(list3) == 0:
                log_error("error: not found in meta table")
                return content
            else:
                log_debug("recovered from log-table: %s", list3)
                pass
