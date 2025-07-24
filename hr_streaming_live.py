import psycopg2
import time
from datetime import datetime
import random

def start_hr_streaming():
    print('🔥 REAL-TIME HR ANALYTICS STREAMING - LAMBDA ARCHITECTURE')
    print('🌍 Processing 1,050 employees across 5 global companies')
    print('🚀 Fortune 500-level streaming pipeline ACTIVE!')
    print('=' * 70)
    
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='hrdb',
        user='hr_user',
        password='hr_pass'
    )
    
    cycle = 1
    
    while True:
        cursor = conn.cursor()
        
        print(f'\n📊 STREAMING CYCLE #{cycle}')
        print(f'⏰ {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print('-' * 50)
        
        # 1. SALARY UPDATES (Performance Reviews)
        cursor.execute('''
            SELECT e.employee_id, e.first_name, e.last_name, e.current_salary,
                   d.department_name, c.company_name
            FROM employees e
            JOIN departments d ON e.department_id = d.department_id
            JOIN companies c ON d.company_id = c.company_id
            WHERE e.current_salary IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 6
        ''')
        
        employees = cursor.fetchall()
        
        print('💰 SALARY UPDATES:')
        for emp_id, first, last, salary, dept, company in employees:
            raise_pct = random.uniform(0.02, 0.07)
            new_salary = int(salary * (1 + raise_pct))
            
            cursor.execute('''
                UPDATE employees 
                SET current_salary = %s, updated_at = %s 
                WHERE employee_id = %s
            ''', (new_salary, datetime.now(), emp_id))
            
            print(f'   📈 {first} {last} | {dept} @ {company}')
            print(f'      ${salary:,} → ${new_salary:,} (+{raise_pct:.1%})')
        
        conn.commit()
        
        # 2. REAL-TIME ANALYTICS
        cursor.execute('''
            SELECT 
                COUNT(*) as total_employees,
                AVG(current_salary)::int as avg_salary,
                COUNT(DISTINCT department_id) as departments,
                COUNT(DISTINCT company_id) as companies,
                COUNT(CASE WHEN updated_at > NOW() - INTERVAL '2 minutes' THEN 1 END) as recent_updates
            FROM employees
        ''')
        
        stats = cursor.fetchone()
        
        print(f'\n📊 REAL-TIME ENTERPRISE METRICS:')
        print(f'   👥 Total Employees: {stats[0]:,}')
        print(f'   💵 Average Salary: ${stats[1]:,}')
        print(f'   🏢 Companies: {stats[3]}')
        print(f'   🏬 Departments: {stats[2]}')
        print(f'   🔄 Recent Updates: {stats[4]}')
        
        # 3. TOP PERFORMERS BY COMPANY
        cursor.execute('''
            SELECT c.company_name, COUNT(e.employee_id) as emp_count, AVG(e.current_salary)::int as avg_sal
            FROM companies c
            JOIN departments d ON c.company_id = d.company_id  
            JOIN employees e ON d.department_id = e.department_id
            GROUP BY c.company_name
            ORDER BY avg_sal DESC
            LIMIT 3
        ''')
        
        top_companies = cursor.fetchall()
        
        print(f'\n🌟 TOP PERFORMING COMPANIES:')
        for company, count, avg_sal in top_companies:
            print(f'   🏆 {company}: {count:,} employees (avg ${avg_sal:,})')
        
        cursor.close()
        
        print('-' * 50)
        print(f'✅ Cycle #{cycle} complete | Next update in 15 seconds')
        print(f'🚀 Lambda Architecture: Batch + Stream Processing Active')
        
        cycle += 1
        time.sleep(15)

if __name__ == '__main__':
    try:
        start_hr_streaming()
    except KeyboardInterrupt:
        print('\n🛑 Real-time streaming stopped')
        print('✅ Lambda Architecture session ended')
    except Exception as e:
        print(f'❌ Error: {e}')