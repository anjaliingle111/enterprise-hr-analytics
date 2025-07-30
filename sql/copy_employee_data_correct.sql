-- Copy employee data from dim_employee to employees with proper column mapping
BEGIN;

-- Copy data mapping columns correctly
INSERT INTO employees (
    employee_id, 
    company_id, 
    department_id, 
    job_code_id,
    first_name, 
    last_name, 
    email, 
    hire_date, 
    current_salary,
    employment_status,
    employment_type,
    currency
)
SELECT 
    employee_id,
    company_id,
    company_id as department_id, -- Use company_id as department_id since they're the same
    job_code_id,
    first_name,
    last_name,
    email,
    hire_date,
    current_salary,
    'active' as employment_status,
    'full-time' as employment_type,
    'USD' as currency
FROM dim_employee 
WHERE is_current = true
ON CONFLICT (employee_id) DO NOTHING;

COMMIT;

SELECT 'Employee data copied successfully!' as status;
SELECT COUNT(*) as total_employees FROM employees;
