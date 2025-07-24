import psycopg2
import time
from datetime import datetime
import random

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password=''
)

print('🔥 REAL-TIME HR ANALYTICS STREAMING!')
print('=' * 50)

iteration = 1
while True:
    cursor = conn.cursor()
    
    print(f'📊 STREAMING CYCLE #{iteration}')
    print(f'⏰ {datetime.now().strftime("%H:%M:%S")}')
    
    # Update random employee salaries
    cursor.execute('''
        SELECT employee_id, first_name, last_name, current_salary, department, company 
        FROM employees 
        ORDER BY RANDOM() 
        LIMIT 3
    ''')
    
    employees = cursor.fetchall()
    
    for emp_id, first, last, salary, dept, company in employees:
        raise_amount = random.randint(1000, 8000)
        new_salary = salary + raise_amount
        
        cursor.execute('''
            UPDATE employees 
            SET current_salary = %s, updated_at = %s 
            WHERE employee_id = %s
        ''', (new_salary, datetime.now(), emp_id))
        
        print(f'💰 {first} {last} ({dept}, {company})')
        print(f'   📈 ${salary:,} → ${new_salary:,} (+${raise_amount:,})')
    
    conn.commit()
    
    # Show stats
    cursor.execute('''
        SELECT COUNT(*) as total, AVG(current_salary)::int as avg_salary
        FROM employees
    ''')
    total, avg_sal = cursor.fetchone()
    
    print(f'📊 METRICS: {total} employees, avg salary ${avg_sal:,}')
    print('=' * 50)
    
    cursor.close()
    iteration += 1
    time.sleep(10)