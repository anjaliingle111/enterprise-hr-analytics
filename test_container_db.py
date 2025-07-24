import psycopg2
from datetime import datetime

def test_container_connection():
    try:
        print('🔍 Testing container database via port forwarding...')
        
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='hrdb',
            user='hr_user',
            password='hr_pass'
        )
        
        cursor = conn.cursor()
        cursor.execute('SELECT current_user, current_database()')
        user, db = cursor.fetchone()
        print(f'✅ SUCCESS: Connected as {user} to {db}')
        
        cursor.execute('SELECT COUNT(*) FROM employees')
        count = cursor.fetchone()[0]
        print(f'🎉 FOUND YOUR DATA: {count:,} employees!')
        
        cursor.execute('''
            UPDATE employees 
            SET current_salary = current_salary + 100 
            WHERE employee_id = 1
            RETURNING first_name, current_salary
        ''')
        result = cursor.fetchone()
        if result:
            print(f'💰 Test Update: {result[0]} salary now ${result[1]:,}')
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f'❌ Container connection failed: {e}')
        return False

if __name__ == '__main__':
    if test_container_connection():
        print('🚀 YOUR HR DATABASE IS ACCESSIBLE!')
        print('✅ Ready for real-time streaming!')
    else:
        print('❌ Need alternative approach')