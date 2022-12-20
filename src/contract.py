from config.db import init_connection
import base64


db, cr, odoo, O_DB, O_UID, O_PWD = init_connection()

archieved_condition = ['|', ['active', '=', True], ['active', '=', False]]

from config.env import FILESTORE_PATH



def insert_1():

    cr.execute("""
        SELECT
            id,
            name,
            active,
            employee_id,
            date_start::TEXT,
            date_end::TEXT,
            trial_date_end::TEXT,
            company_id,
            kanban_state,
            date_generated_from::date::TEXT,
            date_generated_to::date::TEXT,
            legal_leave,
            sick_leave,
            transportation::integer,
            second_shift::integer,
            site_lunch_allowance::integer,
            wage::integer,
            state,
            structure_type_id,
            department_id,
            job_id
        FROM
            hr_contract
        WHERE
            company_id = 1
        ORDER BY
            id
    """)

    db_contracts = [{
        'old_id': con[0],
        'name': con[1],
        'active': con[2],
        'employee_id': con[3],
        'date_start': con[4],
        'date_end': con[5],
        'trial_date_end': con[6],
        'company_id': con[7],
        'kanban_state': con[8],
        'date_generated_from': con[9],
        'date_generated_to': con[10],
        'legal_leave': con[11],
        'sick_leave': con[12],
        'transportation': con[13],
        'second_shift': con[14],
        'site_lunch_allowance': con[15],
        'wage': con[16],
        'state': con[17],
        'structure_type_id': con[18],
        'department_id': con[19],
        'job_id': con[20],
        'contract_approver': 2,

    } for con in cr.fetchall()]

    for index, con in enumerate(db_contracts):
        emp_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', con.get('employee_id')], *archieved_condition]])
        if emp_id:
            con.update({'employee_id': emp_id[0]})
            
            dep = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'search', [[['old_id', '=', con.get('department_id')], *archieved_condition]])
            con.update({'department_id': dep and dep[0] or False})

            job = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.job', 'search', [[['old_id', '=', con.get('job_id')]]])
            con.update({'job_id': job and job[0] or False})

            odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.contract', 'create', [con])
            print(f'{index+1} / {len(db_contracts)}')



def insert_2():
    cr.execute("""
        SELECT
            res_id,
            json_agg(json_build_object('name', att.name, 'path', att.store_fname))
        FROM
            ir_attachment att INNER JOIN hr_contract con ON con.id = att.res_id
        WHERE
            res_model = 'hr.contract'
        AND
            con.company_id = 1
        GROUP BY
            res_id
        ORDER BY
            res_id
    """)
    db_attachments = cr.fetchall()
    for index, rec in enumerate(db_attachments):
        contract_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.contract', 'search', [[['old_id', '=', rec[0]], *archieved_condition]])
        for att in rec[1]:
            try:
                file = open(f"{FILESTORE_PATH}/{att.get('path')}", "rb")
                odoo.execute_kw(O_DB, O_UID, O_PWD, 'ir.attachment', 'create', [{
                    'res_id': contract_id[0],
                    'res_model': 'hr.contract',
                    'name': att.get('name'),
                    'datas': base64.encodebytes(file.read()).decode('utf-8'),
                }])
            except Exception as e:
                print('Error writing attachment', e)
        print(f'{index+1} / {len(db_attachments)}')

# Run each one separately (All others should be commented each time)

# insert_1()
# insert_2()
