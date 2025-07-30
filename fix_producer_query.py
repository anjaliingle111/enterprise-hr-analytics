import re

# Read the current producer
with open('optimized_enterprise_producer_working.py', 'r') as f:
    content = f.read()

# Find and replace the complex query with the simple working one
old_query = r'WITH ranked_employees AS \(.*?\) ORDER BY current_salary DESC'
new_query = '''SELECT de.employee_key, de.first_name, de.last_name, 
                   de.current_salary, c.company_name, 'Executive' as job_title,
                   'Executive Suite' as department_name, de.company_id
            FROM dim_employee de
            JOIN companies c ON de.company_id = c.company_id
            WHERE de.is_current = true AND de.current_salary > 250000
            ORDER BY de.current_salary DESC
            LIMIT 50'''

# Replace the query
content = re.sub(old_query, new_query, content, flags=re.DOTALL)

# Write the fixed version
with open('optimized_enterprise_producer_working.py', 'w') as f:
    f.write(content)

print("✅ Producer query fixed!")
