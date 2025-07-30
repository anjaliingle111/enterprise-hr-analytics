#!/usr/bin/env python3
import psycopg2
import sys

def connect_db():
    try:
        conn = psycopg2.connect(
            host='localhost', port=5432, user='hr_user', 
            password='hr_pass', database='hrdb'
        )
        cursor = conn.cursor()
        print("✅ Database connection established")
        return conn, cursor
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        sys.exit(1)

def execute_sql_file(cursor, conn, filename):
    try:
        with open(f'sql/{filename}', 'r') as f:
            sql_content = f.read()
        print(f"📄 Executing {filename}...")
        cursor.execute(sql_content)
        conn.commit()
        print(f"✅ {filename} executed successfully")
        return True
    except Exception as e:
        print(f"❌ Error executing {filename}: {e}")
        conn.rollback()
        return False

def verify_setup(cursor):
    print("\n🔍 Verifying HR Analytics Setup...")
    print("=" * 50)
    
    tables = ['performance_reviews', 'training_programs', 'employee_training', 'employee_surveys', 'skills_matrix', 'leave_balances']
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            print(f"✅ {table}: {count:,} records")
        except:
            print(f"❌ {table}: Table not found")
    
    cursor.execute("SELECT company_name, parent_company FROM companies ORDER BY company_id;")
    print(f"\n🏢 Company Structure:")
    for company_name, parent_company in cursor.fetchall():
        print(f"  • {company_name} (Parent: {parent_company or 'None'})")
    
    cursor.execute("SELECT COUNT(*), ROUND(SUM(current_salary)/1000000.0, 1) FROM dim_employee WHERE is_current = true;")
    total_emp, total_payroll = cursor.fetchone()
    print(f"\n📊 Platform Statistics:")
    print(f"  • Total Employees: {total_emp:,}")
    print(f"  • Total Payroll: ${total_payroll}M")
    print(f"\n🎉 HR Analytics Platform Setup Complete!")

def main():
    print("🚀 STARTING HR ANALYTICS PLATFORM ENHANCEMENT")
    print("=" * 60)
    
    conn, cursor = connect_db()
    
    try:
        # Step 1: Transform companies to departments
        if not execute_sql_file(cursor, conn, 'transform_to_single_company.sql'):
            return
            
        # Step 2: Create HR tables
        if not execute_sql_file(cursor, conn, 'create_advanced_hr_tables_simple.sql'):
            return
            
        # Step 3: Populate HR data
        if not execute_sql_file(cursor, conn, 'populate_hr_data.sql'):
            return
        
        # Step 4: Verify setup
        verify_setup(cursor)
        
        print("\n🎯 NEXT STEPS:")
        print("1. Run analytics: python scripts/comprehensive_hr_analytics.py")
        print("2. Start streaming: python enterprise_streaming_final.py")
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
    finally:
        cursor.close()
        conn.close()
        print("\n🔐 Database connection closed")

if __name__ == "__main__":
    main()
