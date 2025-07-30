#!/usr/bin/env python3
import psycopg2

print("🚀 POPULATING HR DATA WITH REAL TABLE STRUCTURE")
print("=" * 50)

try:
    conn = psycopg2.connect(host='localhost', port=5432, user='hr_user', password='hr_pass', database='hrdb')
    cursor = conn.cursor()
    print("✅ Database connection established")
    
    with open('sql/populate_real_hr_data.sql', 'r') as f:
        sql_content = f.read()
    
    print("📄 Executing HR data population...")
    cursor.execute(sql_content)
    conn.commit()
    print("✅ HR data populated successfully!")
    
    # Verify results
    cursor.execute("SELECT company_name, parent_company FROM companies ORDER BY company_id;")
    print("\n🏢 Department Structure:")
    for company_name, parent_company in cursor.fetchall():
        print(f"  • {company_name} (Parent: {parent_company})")
    
    # Check HR data counts
    cursor.execute("""
        SELECT 'performance_reviews' as table_name, COUNT(*) as records FROM performance_reviews
        UNION ALL SELECT 'employee_surveys', COUNT(*) FROM employee_surveys
        UNION ALL SELECT 'skills_matrix', COUNT(*) FROM skills_matrix
        UNION ALL SELECT 'leave_balances', COUNT(*) FROM leave_balances
        ORDER BY table_name;
    """)
    
    print("\n📊 HR Data Populated:")
    for table, count in cursor.fetchall():
        print(f"  • {table}: {count:,} records")
        
    print("\n🎉 Transformation and HR data population complete!")
    
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    cursor.close()
    conn.close()
