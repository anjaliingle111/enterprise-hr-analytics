import psycopg2
import random
from datetime import datetime

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password=''
)

conn.autocommit = True
cursor = conn.cursor()

print('🔧 Creating HR demo database...')

# Create employees table
cursor.execute('''
    DROP TABLE IF EXISTS employees;
    CREATE TABLE employees (
        employee_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        current_salary INTEGER,
        department VARCHAR(50),
        company VARCHAR(50),
        updated_at TIMESTAMP DEFAULT NOW()
    )
''')

# Insert 50 demo employees
employees = [
    ('Sarah', 'Chen', 95000, 'Engineering', 'TechCorp'),
    ('Mike', 'Johnson', 87000, 'Sales', 'TechCorp'),
    ('Lisa', 'Wang', 92000, 'Marketing', 'TechCorp'),
    ('David', 'Smith', 78000, 'HR', 'TechCorp'),
    ('Jennifer', 'Taylor', 105000, 'Engineering', 'TechCorp'),
    ('Alex', 'Brown', 83000, 'Sales', 'InnovateInc'),
    ('Emily', 'Davis', 91000, 'Engineering', 'InnovateInc'),
    ('James', 'Wilson', 76000, 'Marketing', 'InnovateInc'),
    ('Maria', 'Garcia', 89000, 'Engineering', 'DataFlow'),
    ('Robert', 'Miller', 94000, 'Sales', 'DataFlow')
]

for first, last, salary, dept, company in employees:
    cursor.execute('''
        INSERT INTO employees (first_name, last_name, current_salary, department, company)
        VALUES (%s, %s, %s, %s, %s)
    ''', (first, last, salary, dept, company))

cursor.execute('SELECT COUNT(*) FROM employees')
count = cursor.fetchone()[0]
print(f'✅ Created {count} employees')

cursor.close()
conn.close()
print('🎉 Demo HR database ready!')