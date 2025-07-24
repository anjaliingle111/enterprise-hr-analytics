import psycopg2
import random
from datetime import date, timedelta

conn = psycopg2.connect(host='localhost', database='hrdb', user='hr_user', password='hr_pass')
cursor = conn.cursor()

print('🚀 GENERATING EMPLOYEES...')

companies = [(1, 315), (2, 210), (3, 198), (4, 167), (5, 160)]
names = ['Sarah', 'Mike', 'Lisa', 'David', 'Jennifer']
surnames = ['Chen', 'Johnson', 'Wang', 'Smith', 'Taylor']

total = 0
for company_id, target in companies:
    for i in range(target):
        cursor.execute('''
            INSERT INTO employees (company_id, department_id, business_unit_id, job_code_id,
            first_name, last_name, hire_date, current_salary, employment_status, created_at, updated_at)
            VALUES (%s, 1, 1, 1, %s, %s, %s, %s, 'active', NOW(), NOW())
        ''', (company_id, random.choice(names), random.choice(surnames), 
              date.today() - timedelta(days=random.randint(100, 1000)),
              random.randint(70000, 150000)))
        total += 1
    conn.commit()

print(f'Created {total} employees')
cursor.close()
conn.close()