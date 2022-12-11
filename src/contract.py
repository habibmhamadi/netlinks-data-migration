from config.db import init_connection
import base64


db, cr, odoo, O_DB, O_UID, O_PWD = init_connection()

archieved_condition = ['|', ['active', '=', True], ['active', '=', False]]



def insert_1():

    cr.execute("SELECT contract_signatory FROM res_company LIMIT 1")
    contract_signatory_id = cr.fetchone()[0]

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
            vacancy_number,
            duration,
            job_summary,
            legal_leave,
            sick_leave,
            transportation,
            second_shift,
            site_lunch_allowance,
            wage,
            state,
            contract_signatory,
            structure_type_id,
            department_id,
            job_id
        FROM
            hr_contract
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
        'vacancy_number': con[11],
        'duration': con[12],
        'job_summary': con[13],
        'legal_leave': con[14],
        'sick_leave': con[15],
        'transportation': con[16],
        'second_shift': con[17],
        'site_lunch_allowance': con[18],
        'wage': con[19],
        'state': con[20],
        'contract_signatory': contract_signatory_id,
        'structure_type_id': con[22],
        'department_id': con[23],
        'job_id': con[24],
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
            print(f'{index+1}/{len(db_contracts)}')





def insert_2():
    cr.execute("""
        SELECT
            res_id,
            json_agg(json_build_object('name', name, 'path', store_fname))
        FROM
            ir_attachment
        WHERE
            res_model = 'hr.contract'
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
                file = open(f"filestore/{att.get('path')}", "rb")
                odoo.execute_kw(O_DB, O_UID, O_PWD, 'ir.attachment', 'create', [{
                    'res_id': contract_id[0],
                    'res_model': 'hr.contract',
                    'name': att.get('name'),
                    'datas': base64.encodebytes(file.read()).decode('utf-8'),
                }])
            except Exception as e:
                print('Error writing attachment', e)
        print(f'{index+1}/{len(db_attachments)}')

# Run each one separately (All others should be commented each time)

# insert_1()
# insert_2()
