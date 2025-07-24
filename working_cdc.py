import psycopg2
import time
from datetime import datetime

def run_working_cdc():
    print('🚀 REAL-TIME HR CDC STARTED')
    print('🌍 Monitoring 1,050 employees across 5 companies')
    print('=' * 60)
    
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
        
        print(f'\n📊 CDC CYCLE #{cycle} - {datetime.now().strftime("%H:%M:%S")}')
        print('-' * 50)
        
        # Update employee salaries in real-time
        cursor.execute("""
            UPDATE employees 
            SET current_salary = current_salary * (1.02 + RANDOM() * 0.06),
                updated_at = NOW()
            WHERE employee_id IN (
                SELECT employee_id FROM employees 
                WHERE employment_status = 'active'
                ORDER BY RANDOM() LIMIT 5
            )
            RETURNING first_name, last_name, current_salary
        """)
        
        updates = cursor.fetchall()
        
        print('💰 SALARY UPDATES:')
        for first, last, salary in updates:
            print(f'   📈 {first} {last}: ${salary:,.0f}')
        
        # Update fact table with new salary data  
        cursor.execute("""
            UPDATE fact_employee_daily_snapshot feds
            SET current_salary = e.current_salary,
                last_updated = NOW()
            FROM employees e
            JOIN dim_employee de ON e.employee_id::TEXT = de.employee_id
            WHERE feds.employee_key = de.employee_key
            AND e.updated_at > NOW() - INTERVAL '1 minute'
            AND de.is_current = TRUE
        """)
        
        updated_snapshots = cursor.rowcount
        
        # Get current business metrics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_employees,
                AVG(current_salary)::INT as avg_salary,
                COUNT(DISTINCT company_id) as companies,
                SUM(current_salary)::BIGINT as total_payroll
            FROM employees 
            WHERE employment_status = 'active'
        """)
        
        total, avg_sal, companies, payroll = cursor.fetchone()
        
        print(f'\n📊 REAL-TIME BUSINESS METRICS:')
        print(f'   👥 Active Employees: {total:,}')
        print(f'   💵 Average Salary: ${avg_sal:,}')
        print(f'   🏢 Companies: {companies}')
        print(f'   💰 Total Annual Payroll: ${payroll:,}')
        print(f'   🔄 Updated Records: {updated_snapshots}')
        
        # Show top companies by average salary
        cursor.execute("""
            SELECT 
                c.company_name,
                COUNT(e.employee_id) as employees,
                AVG(e.current_salary)::INT as avg_salary
            FROM companies c
            JOIN employees e ON c.company_id = e.company_id
            WHERE e.employment_status = 'active'
            GROUP BY c.company_name
            ORDER BY avg_salary DESC
            LIMIT 3
        """)
        
        top_companies = cursor.fetchall()
        
        print(f'\n🌟 TOP COMPANIES BY SALARY:')
        for company, emp_count, avg_sal in top_companies:
            print(f'   🏆 {company}: {emp_count} employees (${avg_sal:,} avg)')
        
        conn.commit()
        cursor.close()
        
        print('-' * 50)
        print(f'✅ Cycle #{cycle} complete | Next update in 20 seconds')
        
        cycle += 1
        time.sleep(20)

if __name__ == '__main__':
    try:
        run_working_cdc()
    except KeyboardInterrupt:
        print('\n🛑 CDC stopped by user')
        print('✅ Enterprise HR streaming session ended')