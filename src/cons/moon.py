import functools

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from werkzeug.security import check_password_hash, generate_password_hash
from cons.db import get_db


from common  import *
from submission import submission_init2, submission_doit, submission_done
from myfunc import MyCtx, convert_to_dict, trim_list

bp = Blueprint('moon', __name__, url_prefix='/moon')


@bp.route('/export', methods=('GET', 'POST'))
def export():
    """...

    """
    print('method is: [%s]' % request.method)
    if request.method == 'POST':

        tasks = request.form['tasklist_name']
        print('-- task-list: %s -- ' % tasks)

        error = None

        result = []
        one = {'name':'wang', 'age':'28' }
        result.append(one)
        one = {'name':'zhang', 'age':'18' }
        result.append(one)

        task_list = submission_init2(tasks)
        submission_doit(task_list)
        submission_done()

        mission = []
        for item in MyCtx.exp_content:
            words   = item.split(',')
            trim_list(words)
            my_dict = convert_to_dict(words)
            log_debug('!-- %s - ', my_dict)
            mission.append(my_dict)


        # return redirect(url_for('auth.login'))
        return render_template('moon/export.html', result=[], mission=mission, tasks=tasks)


    return render_template('moon/export.html')


