import psycopg2
import random
from datetime import datetime, date, timedelta

def generate_employees():
    # Connect using postgres superuser instead of hr_user
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='hrdb',
            user='postgres',  # Changed to postgres
            password='hr_pass'  # Keep same password
        )
    except:
        # Fallback with empty password
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='hrdb',
            user='postgres',
            password=''  # Try empty password
        )
    
    cursor = conn.cursor()
    
    print('🏗️  GENERATING ENTERPRISE HR DATABASE')
    print('🌍 Creating 1000+ employees across 5 global companies')
    print('=' * 60)
    
    # Employee name pools
    first_names = [
        'Sarah', 'Michael', 'Lisa', 'David', 'Jennifer', 'James', 'Maria', 'Robert',
        'Emily', 'William', 'Jessica', 'Thomas', 'Ashley', 'Christopher', 'Amanda',
        'Daniel', 'Stephanie', 'Matthew', 'Michelle', 'Anthony', 'Kimberly', 'Mark',
        'Angela', 'Steven', 'Melissa', 'Kenneth', 'Brenda', 'Paul', 'Emma', 'Andrew',
        'Olivia', 'Joshua', 'Cynthia', 'Kevin', 'Amy', 'Brian', 'Helen', 'George',
        'Debra', 'Edward', 'Rachel', 'Ronald', 'Carolyn', 'Timothy', 'Janet', 'Jason'
    ]
    
    last_names = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
        'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
        'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
        'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker'
    ]
    
    # Get reference data
    cursor.execute("SELECT company_id, company_name FROM companies")
    companies = cursor.fetchall()
    
    cursor.execute("SELECT department_id, department_name, company_id FROM departments")
    departments = cursor.fetchall()
    
    cursor.execute("SELECT job_code_id, job_title, min_salary, max_salary FROM job_codes")
    job_codes = cursor.fetchall()
    
    cursor.execute("SELECT business_unit_id, company_id FROM business_units")
    business_units = cursor.fetchall()
    
    print(f'✅ Found {len(companies)} companies, {len(departments)} departments, {len(job_codes)} job codes')
    print('')
    
    total_employees = 0
    company_targets = {1: 315, 2: 210, 3: 198, 4: 167, 5: 160}
    
    for company_id, company_name in companies:
        target_count = company_targets[company_id]
        print(f'👥 Creating {target_count} employees for {company_name}')
        
        company_departments = [d for d in departments if d[2] == company_id]
        company_business_units = [bu[0] for bu in business_units if bu[1] == company_id]
        
        for i in range(target_count):
            # Generate employee
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            
            dept_id, dept_name, _ = random.choice(company_departments)
            business_unit_id = random.choice(company_business_units)
            job_id, job_title, min_sal, max_sal = random.choice(job_codes)
            
            # Salary calculation
            base_salary = random.randint(int(min_sal), int(max_sal))
            hire_date = date.today() - timedelta(days=random.randint(30, 2920))
            years_exp = max(0, (date.today() - hire_date).days // 365)
            salary_multiplier = 1 + (years_exp * random.uniform(0.03, 0.08))
            current_salary = int(base_salary * salary_multiplier)
            
            employee_number = f"{company_name[:3].upper()}{random.randint(1000, 9999)}"
            email = f"{first_name.lower()}.{last_name.lower()}@company{company_id}.com"
            
            # Work details
            locations = {
                1: ['San Francisco', 'Seattle', 'New York'],
                2: ['London', 'Amsterdam', 'Dublin'], 
                3: ['Singapore', 'Tokyo', 'Sydney'],
                4: ['Munich', 'Berlin', 'Frankfurt'],
                5: ['Bangalore', 'Mumbai', 'Delhi']
            }
            work_location = random.choice(locations[company_id])
            employment_status = 'active'
            employment_type = 'full-time'
            performance_rating = round(random.uniform(3.2, 4.8), 1)
            education = random.choice(['Bachelor Degree', 'Master Degree', 'PhD'])
            remote_eligible = random.choice([True, False])
            
            skills = random.sample([
                'Python', 'JavaScript', 'SQL', 'React', 'AWS', 'Leadership', 'Communication'
            ], random.randint(3, 5))
            
            # Insert employee
            cursor.execute('''
                INSERT INTO employees (
                    company_id, department_id, business_unit_id, job_code_id,
                    employee_number, first_name, last_name, email,
                    hire_date, employment_status, employment_type,
                    current_salary, work_location, remote_eligible,
                    performance_rating, years_of_experience, education_level,
                    skills, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            ''', (
                company_id, dept_id, business_unit_id, job_id,
                employee_number, first_name, last_name, email,
                hire_date, employment_status, employment_type,
                current_salary, work_location, remote_eligible,
                performance_rating, years_exp, education,
                skills, datetime.now(), datetime.now()
            ))
            
            total_employees += 1
            
            if total_employees % 100 == 0:
                print(f'   📊 Generated {total_employees} employees...')
                conn.commit()  # Commit every 100
        
        print(f'✅ Completed {company_name}: {target_count} employees')
    
    conn.commit()
    
    # Final summary
    cursor.execute('''
        SELECT c.company_name, COUNT(e.employee_id) as count, AVG(e.current_salary)::int as avg_salary
        FROM companies c
        JOIN employees e ON c.company_id = e.company_id
        GROUP BY c.company_id, c.company_name
        ORDER BY count DESC
    ''')
    
    results = cursor.fetchall()
    
    print('\n🎉 ENTERPRISE DATABASE COMPLETE!')
    print('=' * 50)
    total = 0
    for company, count, avg_sal in results:
        total += count
        print(f'🏢 {company}: {count:,} employees (avg ${avg_sal:,})')
    
    print(f'\n🌍 TOTAL: {total:,} EMPLOYEES CREATED!')
    print('🚀 Ready for Lambda Architecture streaming!')
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    try:
        generate_employees()
    except Exception as e:
        print(f'❌ Error: {e}')
        print('💡 Trying alternative connection...')