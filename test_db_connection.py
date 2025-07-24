import psycopg2

def test_connections():
    configs = [
        {'host': 'localhost', 'port': 5432, 'database': 'hrdb', 'user': 'postgres', 'password': 'hr_pass'},
        {'host': 'localhost', 'port': 5432, 'database': 'hrdb', 'user': 'postgres', 'password': ''},
        {'host': 'localhost', 'port': 5432, 'database': 'postgres', 'user': 'postgres', 'password': ''},
        {'host': 'localhost', 'port': 5432, 'database': 'hrdb', 'user': 'hr_user', 'password': 'hr_pass'},
    ]
    
    for i, config in enumerate(configs):
        try:
            print(f'🔍 Test {i+1}: {config["user"]}@{config["database"]}')
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            cursor.execute('SELECT current_user, current_database()')
            user, db = cursor.fetchone()
            print(f'✅ SUCCESS: Connected as {user} to {db}')
            
            # Test if companies table exists
            cursor.execute("SELECT COUNT(*) FROM companies")
            count = cursor.fetchone()[0]
            print(f'   📊 Found {count} companies')
            
            cursor.close()
            conn.close()
            return config
            
        except Exception as e:
            print(f'❌ Failed: {e}')
    
    return None

if __name__ == '__main__':
    working_config = test_connections()
    if working_config:
        print(f'\n✅ Use this config for employee generation: {working_config}')
    else:
        print('\n❌ No working connection found')