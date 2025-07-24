import psycopg2

def find_enterprise_database():
    print('🔍 Comprehensive search for your enterprise HR database...')
    
    # Test both ports with various authentication
    test_configs = [
        # Port 5432 variations
        {'host': 'localhost', 'port': 5432, 'database': 'hrdb', 'user': 'hr_user', 'password': 'hr_pass'},
        {'host': 'localhost', 'port': 5432, 'database': 'hrdb', 'user': 'postgres', 'password': 'hr_pass'},
        {'host': 'localhost', 'port': 5432, 'database': 'postgres', 'user': 'postgres', 'password': ''},
        {'host': 'localhost', 'port': 5432, 'database': 'postgres', 'user': 'hr_user', 'password': 'hr_pass'},
        
        # Port 5433 variations (different passwords)
        {'host': 'localhost', 'port': 5433, 'database': 'hrdb', 'user': 'hr_user', 'password': 'hr_pass'},
        {'host': 'localhost', 'port': 5433, 'database': 'hrdb', 'user': 'postgres', 'password': 'hr_pass'},
        {'host': 'localhost', 'port': 5433, 'database': 'hrdb', 'user': 'postgres', 'password': ''},
        {'host': 'localhost', 'port': 5433, 'database': 'hrdb', 'user': 'postgres', 'password': 'postgres'},
        {'host': 'localhost', 'port': 5433, 'database': 'postgres', 'user': 'postgres', 'password': ''},
        {'host': 'localhost', 'port': 5433, 'database': 'postgres', 'user': 'postgres', 'password': 'postgres'},
        {'host': 'localhost', 'port': 5433, 'database': 'hrdb', 'user': 'hr_user', 'password': ''},
    ]
    
    for i, config in enumerate(test_configs):
        try:
            print(f'🔍 Test {i+1}: {config["user"]}@{config["database"]}:{config["port"]} (pwd: "{config["password"]}")')
            
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            
            cursor.execute('SELECT current_user, current_database(), version()')
            user, db, version = cursor.fetchone()
            print(f'✅ SUCCESS: {user}@{db}')
            print(f'   PostgreSQL: {version.split()[1]}')
            
            # Look for tables
            cursor.execute('''
                SELECT schemaname, tablename 
                FROM pg_tables 
                WHERE schemaname = 'public' 
                ORDER BY tablename
            ''')
            
            tables = [row[1] for row in cursor.fetchall()]
            print(f'📊 Tables found: {len(tables)} total')
            
            # Check for enterprise tables
            enterprise_tables = ['employees', 'companies', 'departments', 'business_units', 'training_programs', 'benefit_plans']
            found_enterprise_tables = [t for t in enterprise_tables if t in tables]
            
            if found_enterprise_tables:
                print(f'🏢 Enterprise tables: {found_enterprise_tables}')
                
                if 'employees' in tables:
                    cursor.execute('SELECT COUNT(*) FROM employees')
                    emp_count = cursor.fetchone()[0]
                    print(f'👥 Employee count: {emp_count:,}')
                    
                    if emp_count >= 1000:  # Our enterprise DB has 1000+
                        print('🎉 FOUND YOUR ENTERPRISE DATABASE!')
                        
                        # Show companies
                        if 'companies' in tables:
                            cursor.execute('SELECT company_name FROM companies')
                            companies = [row[0] for row in cursor.fetchall()]
                            print(f'🌍 Companies: {companies}')
                        
                        # Show sample data
                        cursor.execute('''
                            SELECT e.first_name, e.last_name, e.current_salary, d.department_name, c.company_name
                            FROM employees e
                            LEFT JOIN departments d ON e.department_id = d.department_id
                            LEFT JOIN companies c ON d.company_id = c.company_id
                            LIMIT 3
                        ''')
                        
                        employees = cursor.fetchall()
                        print('👤 Sample employees:')
                        for emp in employees:
                            print(f'   • {emp[0]} {emp[1]}: ${emp[2]:,} ({emp[3]}, {emp[4]})')
                        
                        cursor.close()
                        conn.close()
                        return config
                
                elif len(found_enterprise_tables) >= 2:
                    print('🔍 Found enterprise tables but no employee count available')
            
            else:
                print('⚠️  No enterprise tables found')
                if tables:
                    print(f'   Available tables: {tables[:5]}...' if len(tables) > 5 else f'   Available tables: {tables}')
            
            cursor.close()
            conn.close()
            print('')
            
        except Exception as e:
            print(f'❌ Failed: {str(e)[:80]}...' if len(str(e)) > 80 else f'❌ Failed: {e}')
            print('')
    
    print('❌ Enterprise database not found with any tested configuration')
    return None

if __name__ == '__main__':
    config = find_enterprise_database()
    if config:
        print(f'🚀 READY TO START STREAMING WITH: {config}')
    else:
        print('💡 May need to check Docker container status or create demo data')