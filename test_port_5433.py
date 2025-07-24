import psycopg2

def test_port_5433():
    print('🔍 Testing port 5433 for your enterprise HR database...')
    
    # Test different credentials on port 5433
    configs = [
        {'host': 'localhost', 'port': 5433, 'database': 'hrdb', 'user': 'hr_user', 'password': 'hr_pass'},
        {'host': 'localhost', 'port': 5433, 'database': 'hrdb', 'user': 'postgres', 'password': 'hr_pass'},
        {'host': 'localhost', 'port': 5433, 'database': 'postgres', 'user': 'postgres', 'password': ''},
    ]
    
    for i, config in enumerate(configs):
        try:
            print(f'🔍 Attempt {i+1}: {config["user"]}@{config["database"]}:{config["port"]}')
            
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            
            cursor.execute('SELECT current_user, current_database()')
            user, db = cursor.fetchone()
            print(f'✅ Connected as {user} to {db}')
            
            # Check for our enterprise tables
            cursor.execute('''
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('employees', 'companies', 'departments', 'business_units', 'training_programs')
                ORDER BY table_name
            ''')
            
            tables = [row[0] for row in cursor.fetchall()]
            print(f'📊 Found tables: {tables}')
            
            if 'employees' in tables:
                # Check employee count
                cursor.execute('SELECT COUNT(*) FROM employees')
                emp_count = cursor.fetchone()[0]
                print(f'👥 Employees: {emp_count:,}')
                
                if emp_count > 100:  # Our enterprise DB should have 1000+
                    print('🎉 THIS LOOKS LIKE YOUR ENTERPRISE DATA!')
                    
                    # Verify with company data
                    if 'companies' in tables:
                        cursor.execute('SELECT company_name FROM companies')
                        companies = [row[0] for row in cursor.fetchall()]
                        print(f'🏢 Companies found: {companies}')
                        
                        # Check for our specific companies
                        enterprise_companies = ['GlobalTech Solutions Inc', 'European Tech Ltd', 'Asia Pacific Holdings']
                        if any(comp in str(companies) for comp in enterprise_companies):
                            print('🚀 CONFIRMED: This is your 1000+ employee enterprise database!')
                            cursor.close()
                            conn.close()
                            return config
                    
                    # Show sample employee data
                    cursor.execute('SELECT first_name, last_name, current_salary FROM employees LIMIT 3')
                    sample_employees = cursor.fetchall()
                    print('👤 Sample employees:')
                    for first, last, salary in sample_employees:
                        print(f'   • {first} {last}: ${salary:,}' if salary else f'   • {first} {last}: No salary')
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f'❌ Failed: {e}')
    
    return None

if __name__ == '__main__':
    working_config = test_port_5433()
    if working_config:
        print(f'✅ SUCCESS! Use this config: {working_config}')
    else:
        print('❌ Enterprise data not found on port 5433')