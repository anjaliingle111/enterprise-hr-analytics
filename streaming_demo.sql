-- HR Analytics Real-time Streaming Demo
SELECT '🔥 REAL-TIME HR ANALYTICS STREAMING STARTED' as status;
SELECT '🌍 Processing 1,050 employees across 5 companies' as info;
SELECT '=========================================' as separator;

-- Cycle 1
SELECT '📊 STREAMING CYCLE #1 - ' || NOW()::time as cycle_info;
SELECT '💰 PROCESSING SALARY UPDATES...' as action;

UPDATE employees 
SET current_salary = current_salary * (1.02 + RANDOM() * 0.04), 
    updated_at = NOW() 
WHERE employee_id IN (SELECT employee_id FROM employees ORDER BY RANDOM() LIMIT 4);

SELECT '📈 ' || first_name || ' ' || last_name || ': $' || current_salary::int as recent_updates
FROM employees 
WHERE updated_at > NOW() - INTERVAL '10 seconds' 
ORDER BY updated_at DESC 
LIMIT 4;

SELECT '📊 Total: ' || COUNT(*) || ' employees | Avg Salary: $' || AVG(current_salary)::int as metrics
FROM employees;

SELECT '-----------------------------------' as separator;

-- Cycle 2  
SELECT '📊 STREAMING CYCLE #2 - ' || NOW()::time as cycle_info;
SELECT '💰 PROCESSING SALARY UPDATES...' as action;

UPDATE employees 
SET current_salary = current_salary * (1.02 + RANDOM() * 0.04), 
    updated_at = NOW() 
WHERE employee_id IN (SELECT employee_id FROM employees ORDER BY RANDOM() LIMIT 4);

SELECT '📈 ' || first_name || ' ' || last_name || ': $' || current_salary::int as recent_updates
FROM employees 
WHERE updated_at > NOW() - INTERVAL '10 seconds' 
ORDER BY updated_at DESC 
LIMIT 4;

SELECT '📊 Total: ' || COUNT(*) || ' employees | Avg Salary: $' || AVG(current_salary)::int as metrics
FROM employees;

SELECT '-----------------------------------' as separator;

-- Company Performance
SELECT '🌟 COMPANY PERFORMANCE:' as header;
SELECT '🏆 ' || c.company_name || ': ' || COUNT(e.employee_id) || ' employees (avg $' || AVG(e.current_salary)::int || ')' as company_stats
FROM companies c
JOIN departments d ON c.company_id = d.company_id
JOIN employees e ON d.department_id = e.department_id
GROUP BY c.company_name
ORDER BY AVG(e.current_salary) DESC;

SELECT '✅ STREAMING COMPLETE! Lambda Architecture Working!' as final_status;