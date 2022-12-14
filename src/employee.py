from config.db import init_connection
import base64


db, cr, odoo, O_DB, O_UID, O_PWD = init_connection()

archieved_condition = ['|', ['active', '=', True], ['active', '=', False]]

from config.env import FILESTORE_PATH

# HR Department
def get_departments():
    cr.execute("""
        SELECT
            id,
            name,
            active,
            parent_id,
            manager_id,
            company_id
        FROM
            hr_department
        WHERE
            id > 2
        AND
            company_id = 1 or company_id is null
        ORDER BY
            parent_id NULLS FIRST""")
    return [{'old_id': dep[0], 'name':dep[1], 'active':dep[2], 'company_id': 1} for dep in cr.fetchall()]


# HR Employee
def get_employees():
    cr.execute("""
        SELECT
            id,
            name,
            idc_no,
            address_id,
            fingerprint_id,
            join_date::TEXT,
            personal_mobile,
            personal_email,
            work_email,
            work_phone,
            coach_id,
            parent_id,
            department_id,
            job_id,
            resource_calendar_id,
            country_id,
            identification_id,
            passport_id,
            passport_expiry_date::TEXT,
            tin_no,
            hr_bank_account,
            blood_group_type,
            father_name,
            grand_father_name,
            mother_name,
            current_address,
            permanent_address,
            birthday::TEXT,
            place_of_birth::TEXT,
            gender,
            marital,
            children,
            completed_police_clearance,
            completed_pikaz_clearance,
            company_id,
            active
        FROM
            hr_employee
        WHERE
            id > 1
        AND
            company_id = 1
        ORDER BY
            parent_id NULLS FIRST, id""")

    return [{
        'old_id': emp[0],
        'name': emp[1],
        'idc_no': emp[2],
        'address_id': None,
        'fingerprint_id': emp[4],
        'join_date': emp[5],
        'personal_mobile': emp[6],
        'personal_email': emp[7],
        'work_email': emp[8],
        'work_phone': emp[9],
        'coach_id': None,
        'parent_id': None,
        'department_id': emp[12],
        'job_id': emp[13],
        'resource_calendar_id': 1,
        'country_id': emp[15],
        'identification_id': emp[16],
        'passport_id': emp[17],
        'passport_expiry_date': emp[18],
        'tin_no': emp[19],
        'bank_account': emp[20],
        'blood_group': emp[21],
        'father_name': emp[22],
        'grand_father_name': emp[23],
        'mother_name': emp[24],
        'current_address': emp[25],
        'permanent_address': emp[26],
        'birthday': emp[27],
        'place_of_birth': emp[28],
        'gender': emp[29],
        'marital': emp[30],
        'children': emp[31],
        'completed_police_clearance': emp[32],
        'completed_pikaz_clearance':emp[33],
        'company_id':emp[34],
        'active': emp[35]
    } for emp in cr.fetchall()]



def insert_part_1():

    # COMPANY
    cr.execute("""
        SELECT 
            name,
            email,
            phone
        FROM
            res_company
        WHERE
            id = 1""")

    company = cr.fetchone()
    company = {'name': company[0], 'email': company[1], 'phone': company[2]}

    # COMPANY CONTACT
    cr.execute("""
        SELECT 
            name,
            company_id,
            website,
            street,
            zip,
            city,
            country_id,
            email,
            phone,
            commercial_company_name,
            email_normalized,
            phone_sanitized
        FROM
            res_partner
        WHERE
            id = 1 """)

    company_partner = cr.fetchone()
    company_partner = {
        'name': company_partner[0],
        'company_id': company_partner[1],
        'website': company_partner[2],
        'street': company_partner[3],
        'zip': company_partner[4],
        'city': company_partner[5],
        'country_id': company_partner[6],
        'email': company_partner[7],
        'phone': company_partner[8],
        'commercial_company_name': company_partner[9],
        'email_normalized': company_partner[10],
        'phone_sanitized': company_partner[11],
    }

    # HR JOB
    cr.execute("""
        SELECT
            id,
            name,
            company_id
        FROM
            hr_job
        WHERE company_id = 1 or company_id is null
            """)

    jobs = [{'old_id': job[0], 'name':job[1], 'company_id': 1} for job in cr.fetchall()]
    try:
        odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.company', 'write', [[1], company])
    except Exception as e:
        pass
    odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.partner', 'write', [[1], company_partner])
    odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.job', 'create', [jobs])
    odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'create', [get_departments()])
    print('*** Migration 1 Success ***')


def insert_part_2():
    emps = get_employees()
    for index, emp in enumerate(emps):
        if emp.get('department_id'):
            department_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'search', [[['old_id', '=', emp.get('department_id')], *archieved_condition]])
            if department_id:
                emp.update({'department_id': department_id[0]})
        if emp.get('job_id'):
            job_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.job', 'search', [[['old_id', '=', emp.get('job_id')]]])
            if job_id:
                emp.update({'job_id': job_id[0]})
        # if emp.get('address_id'):
        #     if not odoo.execute_kw(O_DB, O_UID, O_PWD, 'res.partner', 'search', [[['id', '=', emp.get('address_id')], *archieved_condition]]):
        #         emp.update({'address_id': None})
        try:
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'create', [emp])
            print(f'{index+1} / {len(emps)}')
        except Exception as e:
            print(' =====> ',e, '\n', emp)
            break
    print('*** Migration 2 Success ***')


def insert_part_3():
    deps = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'search_read', [[['old_id', 'in', [dep.get('old_id') for dep in get_departments()]], *archieved_condition]])
    for index, dep in enumerate(deps):
        cr.execute("SELECT manager_id, parent_id FROM hr_department WHERE id = %s", (dep.get('old_id'),))
        dep_data = cr.fetchone()
        new_manager_id = False
        new_parent_id = False
        if dep_data and dep_data[0]:
            new_manager_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', dep_data[0]], *archieved_condition]])
            if new_manager_id:
                new_manager_id = new_manager_id[0]
        if dep_data and dep_data[1]:
            new_parent_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'search', [[['old_id', '=', dep_data[1]], *archieved_condition]])
            if new_parent_id:
                new_parent_id = new_parent_id[0]
        odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.department', 'write', [[dep.get('id')], {'manager_id': new_manager_id, 'parent_id': new_parent_id}])
        print(f'{index+1} / {len(deps)}')
        

    emps = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search_read', [[['old_id', 'in', [emp.get('old_id') for emp in get_employees()]], *archieved_condition]])
    for index, emp in enumerate(emps):
        cr.execute("SELECT coach_id, parent_id FROM hr_employee WHERE id = %s", (emp.get('old_id'),))
        emp_data = cr.fetchone()
        new_coach_id = False
        new_parent_id = False
        if emp_data and emp_data[0]:
            new_coach_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', emp_data[0]], *archieved_condition]])
            if new_coach_id:
                new_coach_id = new_coach_id[0]
        if emp_data and emp_data[1]:
            new_parent_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', emp_data[1]], *archieved_condition]])
            if new_parent_id:
                new_parent_id = new_parent_id[0]
        odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'write', [[emp.get('id')], {'coach_id': new_coach_id, 'parent_id': new_parent_id}])
        print(f'{index+1} / {len(emps)}')
    print('*** Migration 3 Success ***')
    

    
def insert_part_4():

    # Emergency Contact
    cr.execute("""
        SELECT
            contacts,
            json_agg(
                json_build_object(
                    'name', name,
                    'relationship', relationship,
                    'number', number
                )
            )
        FROM
            emergency_contacts
        WHERE
            name is not null
        GROUP BY
            contacts""")
    emergency_contacts = [{'old_emp_id': emg[0], 'contacts': [{
                            'name': contact.get('name'),
                            'relationship': contact.get('relationship'),
                            'number': contact.get('number'),
                          } for contact in emg[1]]} for emg in cr.fetchall()]

    index = 1
    for emp in emergency_contacts:
        if emp.get('contacts'):
            emp_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', emp.get('old_emp_id')], *archieved_condition]])
            if emp_id:
                contacts = list(map(lambda x: {**x, **{'contact_id': emp_id[0]}}, emp.get('contacts')))
                odoo.execute_kw(O_DB, O_UID, O_PWD, 'emergency.contact', 'create', [contacts])
                print(f'{index}')
                index+=1


    # Employee Reference
    cr.execute("""
        SELECT
            empl_id,
            json_agg(
                json_build_object(
                    'name', name,
                    'job_title', job_title,
                    'organization', organization,
                    'contact_number', contact_number,
                    'email', email,
                    'checked', checked
                )
            )
        FROM
            employee_references
        WHERE
            name is not null
        GROUP BY
            empl_id""")
    emp_references = [{'old_emp_id': emg[0], 'references': [{
                            'name': ref.get('name'),
                            'job_title': ref.get('job_title'),
                            'organization': ref.get('organization'),
                            'contact_number': ref.get('contact_number'),
                            'email': ref.get('email'),
                            'checked': ref.get('checked'),
                          } for ref in emg[1]]} for emg in cr.fetchall()]
    index = 1
    for emp in emp_references:
        if emp.get('references'):
            emp_id = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search', [[['old_id', '=', emp.get('old_emp_id')], *archieved_condition]])
            if emp_id:
                refs = list(map(lambda x: {**x, **{'employee_id': emp_id[0]}} , emp.get('references')))
                odoo.execute_kw(O_DB, O_UID, O_PWD, 'employee.reference', 'create', [refs])
                print(f'{index}')
                index+=1

    print('*** Migration 4 Success ***')
    

def insert_part_5():
    emp_ids = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search_read', [[*archieved_condition]])
    for index, emp in enumerate(emp_ids):
        cr.execute("SELECT store_fname FROM ir_attachment WHERE res_model = 'hr.employee' AND res_id = %s AND res_field = 'image_1920' LIMIT 1", (emp.get('old_id'),))
        f_name = cr.fetchone()
        byte_data = None
        if f_name:
            try:
                file = open(f'{FILESTORE_PATH}/{f_name[0]}', "rb")
                byte_data = base64.b64encode(file.read()).decode('utf-8')
            except Exception as e:
                print(f"ERROR reading attachment for employee {emp.get('id')}", e)
        if byte_data:
            odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'write', [[emp.get('id')], {'image_1920': byte_data}])
        print(f'{index+1} / {len(emp_ids)}')
    print('*** Migration 5 Success ***')


def insert_part_6():
    cr.execute("""
        SELECT 
            doc.emp_id,
            json_agg(json_build_object('name', doc.file_name, 'file', att.store_fname))
        FROM 
            employee_document doc INNER JOIN ir_attachment att ON att.res_id = doc.id INNER JOIN hr_employee emp ON emp.id = doc.emp_id
        WHERE
            att.res_model = 'employee.document'
        AND
            att.res_field = 'name'
        AND
            emp.company_id = 1
        GROUP BY
            doc.emp_id
    """)
    counter = 0
    for rec in cr.fetchall():
        emp = odoo.execute_kw(O_DB, O_UID, O_PWD, 'hr.employee', 'search_read', [[['old_id', '=', rec[0]], *archieved_condition]])
        if emp:
            emp = emp[0]
            for doc in rec[1]:
                byte_data = None
                try:
                    file = open(f"{FILESTORE_PATH}/{doc.get('file')}", "rb")
                    byte_data = base64.b64encode(file.read()).decode('utf-8')
                    odoo.execute_kw(O_DB, O_UID, O_PWD, 'employee.document', 'create', [{'employee_id': emp.get('id'), 'name': doc.get('name'), 'document': byte_data}])
                except Exception as e:
                    print(f"ERROR reading attachment for employee {emp.get('id')}", doc.get('name'))
                print(f'{counter}')
                counter+=1
    
    print('*** Migration 6 Success ***')


# Run each one separately (All others should be commented each time)

# insert_part_1()
# insert_part_2()
# insert_part_3()
# insert_part_4()
# insert_part_5()
# insert_part_6()

