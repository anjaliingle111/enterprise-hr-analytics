import psycopg2
from datetime import datetime

def test_connection():
    # Try different user configurations
    configs = [
        {'host': 'localhost', 'port': 5432, 'database': 'hrdb', 'user': 'hr_user', 'password': 'hr_pass'},
        {'host': 'localhost', 'port': 5432, 'database': 'hrdb', 'user': 'postgres', 'password': 'hr_pass'},  
        {'host': 'localhost', 'port': 5432, 'database': 'postgres', 'user': 'postgres', 'password': ''},
        {'host': 'localhost', 'port': 5432, 'database': 'hrdb', 'user': 'postgres', 'password': ''}
    ]
    
    for i, config in enumerate(configs):
        try:
            print(f'🔍 Testing connection {i+1}: {config["user"]}@{config["database"]}')
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            cursor.execute('SELECT current_user, current_database()')
            user, db = cursor.fetchone()
            print(f'✅ SUCCESS: Connected as {user} to database {db}')
            
            # Test if our tables exist
            cursor.execute("SELECT COUNT(*) FROM employees")
            emp_count = cursor.fetchone()[0]
            print(f'👥 Found {emp_count:,} employees!')
            
            cursor.close()
            conn.close()
            return config
            
        except Exception as e:
            print(f'❌ Failed: {e}')
            continue
    
    print('❌ All connection attempts failed')
    return None

if __name__ == '__main__':
    working_config = test_connection()
    if working_config:
        print(f'✅ Use this config: {working_config}')
    else:
        print('❌ No working configuration found')