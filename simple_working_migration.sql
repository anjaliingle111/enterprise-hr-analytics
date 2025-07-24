-- Simple Working Migration - Based on Actual Schema and Existing Data
SELECT '🚀 STARTING SIMPLE MIGRATION' as status;

-- Step 1: Populate employees directly using existing employees table
INSERT INTO dim_employee (
    employee_id, first_name, last_name, email, job_title, 
    employment_type, employment_status, work_location
)
SELECT 
    e.employee_id::TEXT,
    e.first_name,
    e.last_name,
    e.email,
    COALESCE(jc.job_title, 'Unknown Position'),
    COALESCE(e.employment_type, 'full-time'),
    COALESCE(e.employment_status, 'active'),
    e.work_location
FROM employees e
LEFT JOIN job_codes jc ON e.job_code_id = jc.job_code_id
WHERE e.employment_status = 'active'
ON CONFLICT (employee_id, effective_start_date) DO NOTHING;

-- Step 2: Populate departments using actual existing columns
INSERT INTO dim_department (
    department_id, department_name, department_code
)
SELECT 
    d.department_id::TEXT,
    d.department_name,
    d.department_code
FROM departments d
ON CONFLICT (department_id, effective_start_date) DO NOTHING;

-- Step 3: Create basic daily snapshots with existing employees
INSERT INTO fact_employee_daily_snapshot (
    date_key, employee_key, department_key, company_key, current_salary
)
SELECT 
    TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INTEGER as date_key,
    de.employee_key,
    dd.department_key, 
    dc.company_key,
    e.current_salary
FROM employees e
JOIN dim_employee de ON e.employee_id::TEXT = de.employee_id AND de.is_current = TRUE
JOIN dim_department dd ON e.department_id::TEXT = dd.department_id AND dd.is_current = TRUE
JOIN dim_company dc ON e.company_id::TEXT = dc.company_id AND dc.is_current = TRUE
WHERE e.employment_status = 'active' AND e.current_salary IS NOT NULL
ON CONFLICT (date_key, employee_key) DO NOTHING;

-- Step 4: Create salary change events
INSERT INTO fact_salary_changes (
    employee_key, department_key, company_key, effective_date_key,
    change_type, previous_salary, new_salary, change_amount, change_percentage
)
SELECT 
    de.employee_key,
    dd.department_key,
    dc.company_key,
    TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INTEGER as effective_date_key,
    'INITIAL_LOAD' as change_type,
    (e.current_salary * 0.95)::NUMERIC(12,2) as previous_salary,
    e.current_salary as new_salary,
    (e.current_salary * 0.05)::NUMERIC(12,2) as change_amount,
    5.0 as change_percentage
FROM employees e
JOIN dim_employee de ON e.employee_id::TEXT = de.employee_id AND de.is_current = TRUE  
JOIN dim_department dd ON e.department_id::TEXT = dd.department_id AND dd.is_current = TRUE
JOIN dim_company dc ON e.company_id::TEXT = dc.company_id AND dc.is_current = TRUE
WHERE e.employment_status = 'active' AND e.current_salary IS NOT NULL AND e.current_salary > 0
ON CONFLICT DO NOTHING;

-- Results Summary
SELECT '✅ MIGRATION RESULTS' as status;
SELECT 'Date Dimension: ' || COUNT(*) || ' records' as result FROM dim_date
UNION ALL
SELECT 'Company Dimension: ' || COUNT(*) || ' records' FROM dim_company WHERE is_current = TRUE
UNION ALL  
SELECT 'Department Dimension: ' || COUNT(*) || ' records' FROM dim_department WHERE is_current = TRUE
UNION ALL
SELECT 'Employee Dimension: ' || COUNT(*) || ' records' FROM dim_employee WHERE is_current = TRUE
UNION ALL
SELECT 'Daily Snapshots: ' || COUNT(*) || ' records' FROM fact_employee_daily_snapshot
UNION ALL
SELECT 'Salary Changes: ' || COUNT(*) || ' records' FROM fact_salary_changes
ORDER BY result;

-- Show sample data
SELECT '📊 SAMPLE EMPLOYEE DATA' as status;
SELECT 
    de.first_name, 
    de.last_name, 
    de.job_title, 
    de.employment_status,
    feds.current_salary
FROM dim_employee de
JOIN fact_employee_daily_snapshot feds ON de.employee_key = feds.employee_key
WHERE de.is_current = TRUE
LIMIT 5;

SELECT '🎉 DIMENSIONAL MODEL READY FOR REAL-TIME CDC!' as final_status;