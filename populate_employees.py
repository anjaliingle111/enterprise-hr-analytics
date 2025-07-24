import psycopg2
import random
from datetime import datetime, date, timedelta

def generate_employees():
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='hrdb',
        user='hr_user',
        password='hr_pass'
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
        'Debra', 'Edward', 'Rachel', 'Ronald', 'Carolyn', 'Timothy', 'Janet', 'Jason',
        'Virginia', 'Jeffrey', 'Catherine', 'Ryan', 'Frances', 'Jacob', 'Christine',
        'Gary', 'Samantha', 'Nicholas', 'Deborah', 'Eric', 'Rachel', 'Jonathan',
        'Laura', 'Stephen', 'Sharon', 'Larry', 'Anna', 'Justin', 'Shirley', 'Scott',
        'Ruth', 'Brandon', 'Diane', 'Benjamin', 'Julie', 'Samuel', 'Joyce', 'Gregory'
    ]
    
    last_names = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
        'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
        'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
        'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
        'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill',
        'Flores', 'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell',
        'Mitchell', 'Carter', 'Roberts', 'Gomez', 'Phillips', 'Evans', 'Turner',
        'Diaz', 'Parker', 'Cruz', 'Edwards', 'Collins', 'Reyes', 'Stewart', 'Morris'
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
    
    # Generate employees for each company
    company_targets = {1: 315, 2: 210, 3: 198, 4: 167, 5: 160}  # Realistic distribution
    
    for company_id, company_name in companies:
        target_count = company_targets[company_id]
        print(f'👥 Creating {target_count} employees for {company_name}')
        
        company_departments = [d for d in departments if d[2] == company_id]
        company_business_units = [bu[0] for bu in business_units if bu[1] == company_id]
        
        for i in range(target_count):
            # Generate basic info
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            
            # Department and business unit
            dept_id, dept_name, _ = random.choice(company_departments)
            business_unit_id = random.choice(company_business_units)
            
            # Job and salary
            job_id, job_title, min_sal, max_sal = random.choice(job_codes)
            base_salary = random.randint(int(min_sal), int(max_sal))
            
            # Experience and salary adjustment
            hire_date = date.today() - timedelta(days=random.randint(30, 2920))
            years_exp = (date.today() - hire_date).days // 365
            salary_multiplier = 1 + (years_exp * random.uniform(0.03, 0.10))
            current_salary = int(base_salary * salary_multiplier)
            
            # Employee details
            employee_number = f"{company_name[:3].upper()}{random.randint(1000, 9999)}"
            email = f"{first_name.lower()}.{last_name.lower()}@company{company_id}.com"
            
            # Work attributes
            locations = {
                1: ['San Francisco', 'Seattle', 'New York', 'Austin'],
                2: ['London', 'Amsterdam', 'Manchester', 'Dublin'], 
                3: ['Singapore', 'Tokyo', 'Sydney', 'Hong Kong'],
                4: ['Munich', 'Berlin', 'Hamburg', 'Frankfurt'],
                5: ['Bangalore', 'Mumbai', 'Delhi', 'Hyderabad']
            }
            work_location = random.choice(locations[company_id])
            
            employment_status = random.choices(['active', 'inactive'], weights=[95, 5])[0]
            employment_type = random.choices(['full-time', 'part-time'], weights=[90, 10])[0]
            performance_rating = round(random.uniform(3.0, 4.8), 1)
            education = random.choice(['Bachelor Degree', 'Master Degree', 'PhD', 'Associate Degree'])
            remote_eligible = random.choice([True, False])
            
            skills = random.sample([
                'Python', 'JavaScript', 'SQL', 'React', 'AWS', 'Docker', 'Git', 
                'Leadership', 'Communication', 'Project Management', 'Data Analysis'
            ], random.randint(3, 7))
            
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
    generate_employees()