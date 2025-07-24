import psycopg2
import json
import time
from datetime import datetime
import random

def create_realtime_hr_stream():
    conn = psycopg2.connect(
        host='localhost', 
        port=5432,
        database='hrdb', 
        user='hr_user', 
        password='hr_pass'
    )
    
    print('🔥 REAL-TIME HR ANALYTICS STREAMING STARTED!')
    print('🌟 Processing 1000+ Employee Database in Real-time!')
    print('=' * 60)
    
    iteration = 1
    
    while True:
        cursor = conn.cursor()
        
        # Generate real-time HR events
        print(f'📊 STREAMING CYCLE #{iteration}')
        print(f'⏰ {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        
        # 1. Employee Salary Updates (Performance Reviews)
        cursor.execute('''
            SELECT e.employee_id, e.first_name, e.last_name, e.current_salary, 
                   d.department_name, c.company_name
            FROM employees e
            JOIN departments d ON e.department_id = d.department_id  
            JOIN companies c ON d.company_id = c.company_id
            WHERE e.current_salary IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 5
        ''')
        
        employees = cursor.fetchall()
        
        for emp_id, first, last, salary, dept, company in employees:
            # Simulate performance-based raises
            raise_percent = random.uniform(0.03, 0.08)  # 3-8% raises
            new_salary = int(salary * (1 + raise_percent))
            
            cursor.execute('''
                UPDATE employees 
                SET current_salary = %s, updated_at = %s 
                WHERE employee_id = %s
            ''', (new_salary, datetime.now(), emp_id))
            
            print(f'💰 SALARY UPDATE: {first} {last} ({dept}, {company})')
            print(f'   📈 ${salary:,} → ${new_salary:,} (+{raise_percent:.1%})')
        
        # 2. New Training Enrollments
        cursor.execute('''
            SELECT e.employee_id, e.first_name, tp.program_name
            FROM employees e
            CROSS JOIN training_programs tp
            ORDER BY RANDOM()
            LIMIT 3
        ''')
        
        training_data = cursor.fetchall()
        
        for emp_id, name, program in training_data:
            cursor.execute('''
                INSERT INTO training_enrollments 
                (employee_id, training_program_id, enrollment_date, status)
                SELECT %s, tp.training_program_id, %s, 'enrolled'
                FROM training_programs tp 
                WHERE tp.program_name = %s
                ON CONFLICT DO NOTHING
            ''', (emp_id, datetime.now().date(), program))
            
            print(f'🎓 TRAINING ENROLLMENT: {name} → {program}')
        
        # 3. Performance Review Updates
        cursor.execute('''
            SELECT e.employee_id, e.first_name, e.last_name
            FROM employees e
            ORDER BY RANDOM()
            LIMIT 2
        ''')
        
        review_employees = cursor.fetchall()
        
        for emp_id, first, last in review_employees:
            rating = random.uniform(3.5, 5.0)  # Performance rating
            cursor.execute('''
                INSERT INTO performance_reviews 
                (employee_id, review_period_start, review_period_end, 
                 overall_rating, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            ''', (emp_id, datetime.now().date(), datetime.now().date(), 
                  rating, datetime.now(), datetime.now()))
            
            print(f'⭐ PERFORMANCE REVIEW: {first} {last} - Rating: {rating:.1f}/5.0')
        
        conn.commit()
        cursor.close()
        
        # Show summary stats
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                COUNT(*) as total_employees,
                AVG(current_salary)::int as avg_salary,
                COUNT(DISTINCT department_id) as departments,
                COUNT(DISTINCT CASE WHEN updated_at > NOW() - INTERVAL '1 minute' THEN employee_id END) as recent_updates
            FROM employees
        ''')
        
        stats = cursor.fetchone()
        cursor.close()
        
        print(f'📊 REAL-TIME METRICS:')
        print(f'   👥 Total Employees: {stats[0]:,}')
        print(f'   💵 Average Salary: ${stats[1]:,}')
        print(f'   🏢 Departments: {stats[2]}')
        print(f'   🔄 Recent Updates: {stats[3]}')
        print('=' * 60)
        
        iteration += 1
        time.sleep(15)  # Stream every 15 seconds

if __name__ == '__main__':
    try:
        create_realtime_hr_stream()
    except KeyboardInterrupt:
        print('\n🛑 Real-time streaming stopped by user')
        print('✅ HR Analytics Pipeline Complete!')