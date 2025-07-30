import psycopg2
import random

print("🚀 ENTERPRISE STREAMING TEST")
print("=" * 50)

# Connect with correct credentials
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    user='hr_user',
    password='hr_pass',
    database='hrdb'
)
cursor = conn.cursor()

# Get top executives (simple query that works)
cursor.execute("""
    SELECT de.employee_key, de.first_name, de.last_name, 
           de.current_salary, c.company_name, de.company_id
    FROM dim_employee de
    JOIN companies c ON de.company_id = c.company_id
    WHERE de.is_current = true AND de.current_salary > 250000
    ORDER BY de.current_salary DESC
    LIMIT 10;
""")

executives = cursor.fetchall()
print(f"🎉 Found {len(executives)} executives for streaming!")
print()

# Stream them with realistic updates
for i, exec_data in enumerate(executives):
    name = f"{exec_data[1]} {exec_data[2]}"
    company = exec_data[4]
    salary = float(exec_data[3])
    new_salary = salary * random.uniform(1.05, 1.15)
    
    print(f"👤 {name} | {company}")
    print(f"   �� ${salary:,.0f} → ${new_salary:,.0f} (+{((new_salary-salary)/salary)*100:.1f}%)")
    print(f"   📊 Budget Impact: {(new_salary-salary)/138300000*100:.3f}%")
    print()

conn.close()
print("✅ Enterprise streaming test completed successfully!")
