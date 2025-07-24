-- =================================================================
-- PRACTICAL HR ANALYTICS - REAL BUSINESS INSIGHTS
-- Focus on data engineering, not predictions
-- =================================================================

SELECT '📊 PRACTICAL HR ANALYTICS - ENTERPRISE INSIGHTS' as section;

-- 1. HEADCOUNT BY COMPANY AND DEPARTMENT
SELECT 
    '👥 HEADCOUNT ANALYSIS' as analysis_type,
    c.company_name,
    d.department_name,
    COUNT(*) as employee_count,
    ROUND(SUM(de.current_salary)/1000, 0) as total_salary_k,
    ROUND(AVG(de.current_salary), 0) as avg_salary
FROM dim_employee de
JOIN companies c ON de.company_id = c.company_id
JOIN departments d ON de.department_id = d.department_id
WHERE de.is_current = true
GROUP BY c.company_name, d.department_name
ORDER BY c.company_name, employee_count DESC;

-- 2. SALARY DISTRIBUTION BY JOB LEVEL
SELECT 
    '💰 SALARY DISTRIBUTION' as analysis_type,
    jc.job_level,
    COUNT(*) as employee_count,
    ROUND(MIN(de.current_salary), 0) as min_salary,
    ROUND(AVG(de.current_salary), 0) as avg_salary,
    ROUND(MAX(de.current_salary), 0) as max_salary,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY de.current_salary), 0) as median_salary
FROM dim_employee de
JOIN job_codes jc ON de.job_code_id = jc.job_code_id
WHERE de.is_current = true
GROUP BY jc.job_level
ORDER BY avg_salary DESC;

-- 3. LEAVE UTILIZATION ANALYSIS
SELECT 
    '🏖️ LEAVE UTILIZATION' as analysis_type,
    lt.leave_type_name,
    COUNT(elb.employee_key) as employees_with_leave,
    ROUND(AVG(elb.used_days), 1) as avg_days_used,
    ROUND(AVG(elb.remaining_days), 1) as avg_days_remaining,
    ROUND(AVG(elb.used_days / elb.allocated_days * 100), 1) as utilization_percentage
FROM employee_leave_balances elb
JOIN leave_types lt ON elb.leave_type_id = lt.leave_type_id
WHERE elb.year = 2025
GROUP BY lt.leave_type_name
ORDER BY utilization_percentage DESC;

-- 4. ATTENDANCE PATTERNS BY WORK LOCATION
SELECT 
    '🏢 WORK LOCATION ANALYSIS' as analysis_type,
    ar.work_location,
    COUNT(*) as total_records,
    ROUND(AVG(ar.total_hours_worked), 2) as avg_hours_per_day,
    ROUND(SUM(ar.overtime_hours), 1) as total_overtime_hours,
    COUNT(DISTINCT ar.employee_key) as unique_employees
FROM attendance_records ar
GROUP BY ar.work_location
ORDER BY avg_hours_per_day DESC;

-- 5. PAYROLL ANALYSIS BY COMPANY
SELECT 
    '💸 PAYROLL ANALYSIS' as analysis_type,
    c.company_name,
    COUNT(epd.employee_key) as employees_paid,
    ROUND(SUM(epd.gross_pay), 0) as total_gross_pay,
    ROUND(AVG(epd.gross_pay), 0) as avg_gross_pay,
    ROUND(SUM(epd.taxes_withheld), 0) as total_taxes,
    ROUND(SUM(epd.net_pay), 0) as total_net_pay,
    ROUND(AVG(epd.overtime_hours), 2) as avg_overtime_hours
FROM employee_payroll_details epd
JOIN dim_employee de ON epd.employee_key = de.employee_key
JOIN companies c ON de.company_id = c.company_id
WHERE de.is_current = true
GROUP BY c.company_name
ORDER BY total_gross_pay DESC;

-- 6. TENURE ANALYSIS
SELECT 
    '⏰ TENURE ANALYSIS' as analysis_type,
    CASE 
        WHEN EXTRACT(days FROM (CURRENT_DATE - de.hire_date)) < 365 THEN '< 1 Year'
        WHEN EXTRACT(days FROM (CURRENT_DATE - de.hire_date)) < 1095 THEN '1-3 Years'
        WHEN EXTRACT(days FROM (CURRENT_DATE - de.hire_date)) < 1825 THEN '3-5 Years'
        ELSE '5+ Years'
    END as tenure_group,
    COUNT(*) as employee_count,
    ROUND(AVG(de.current_salary), 0) as avg_salary,
    ROUND(AVG(EXTRACT(days FROM (CURRENT_DATE - de.hire_date))), 0) as avg_days_tenure
FROM dim_employee de
WHERE de.is_current = true
GROUP BY 
    CASE 
        WHEN EXTRACT(days FROM (CURRENT_DATE - de.hire_date)) < 365 THEN '< 1 Year'
        WHEN EXTRACT(days FROM (CURRENT_DATE - de.hire_date)) < 1095 THEN '1-3 Years'
        WHEN EXTRACT(days FROM (CURRENT_DATE - de.hire_date)) < 1825 THEN '3-5 Years'
        ELSE '5+ Years'
    END
ORDER BY avg_salary DESC;

SELECT '✅ PRACTICAL HR ANALYTICS COMPLETE' as status;