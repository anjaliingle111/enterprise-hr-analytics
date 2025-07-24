-- =================================================================
-- FIXED HR ANALYTICS - ENTERPRISE 1000 EMPLOYEE ANALYSIS
-- All SQL syntax errors corrected for PostgreSQL
-- =================================================================

SELECT '📊 ENTERPRISE HR ANALYTICS - 1000 EMPLOYEES' as section;

-- 1. EXECUTIVE SUMMARY - TOP LEVEL METRICS
SELECT 
    '🏢 EXECUTIVE SUMMARY' as analysis_type,
    COUNT(*) as total_employees,
    ROUND(SUM(current_salary)/1000000.0, 1) as total_payroll_millions,
    ROUND(AVG(current_salary), 0) as average_salary,
    MAX(current_salary) as highest_salary,
    MIN(current_salary) as lowest_salary
FROM dim_employee 
WHERE is_current = true;

-- 2. SALARY DISTRIBUTION BY JOB LEVEL (FIXED)
SELECT 
    '💰 SALARY DISTRIBUTION' as analysis_type,
    jc.job_level,
    COUNT(*) as employee_count,
    ROUND(MIN(de.current_salary)::numeric, 0) as min_salary,
    ROUND(AVG(de.current_salary)::numeric, 0) as avg_salary,
    ROUND(MAX(de.current_salary)::numeric, 0) as max_salary,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY de.current_salary)::numeric, 0) as median_salary
FROM dim_employee de
JOIN job_codes jc ON de.job_code_id = jc.job_code_id
WHERE de.is_current = true
GROUP BY jc.job_level
ORDER BY avg_salary DESC;

-- 3. COMPANY COMPARISON ANALYSIS
SELECT 
    '🏢 COMPANY ANALYSIS' as analysis_type,
    c.company_name,
    COUNT(*) as employee_count,
    ROUND(SUM(de.current_salary)/1000000.0, 1) as payroll_millions,
    ROUND(AVG(de.current_salary), 0) as avg_salary,
    ROUND(SUM(de.current_salary)/SUM(SUM(de.current_salary)) OVER () * 100, 1) as payroll_percentage
FROM dim_employee de
JOIN companies c ON de.company_id = c.company_id
WHERE de.is_current = true
GROUP BY c.company_name
ORDER BY payroll_millions DESC;

-- 4. DEPARTMENT EFFICIENCY ANALYSIS
SELECT 
    '📊 DEPARTMENT EFFICIENCY' as analysis_type,
    d.department_name,
    COUNT(*) as total_employees,
    ROUND(SUM(de.current_salary)/1000.0, 0) as total_salary_k,
    ROUND(AVG(de.current_salary), 0) as avg_salary,
    ROUND(d.budget_allocation/1000000.0, 1) as budget_millions,
    ROUND((SUM(de.current_salary) / d.budget_allocation) * 100, 1) as budget_utilization_pct
FROM dim_employee de
JOIN dim_department d ON de.department_id = d.department_id
WHERE de.is_current = true AND d.is_current = true
GROUP BY d.department_name, d.budget_allocation
ORDER BY total_employees DESC;

-- 5. TENURE ANALYSIS (FIXED)
SELECT 
    '⏰ TENURE ANALYSIS' as analysis_type,
    CASE 
        WHEN (CURRENT_DATE - de.hire_date) < INTERVAL '1 year' THEN '< 1 Year'
        WHEN (CURRENT_DATE - de.hire_date) < INTERVAL '3 years' THEN '1-3 Years'
        WHEN (CURRENT_DATE - de.hire_date) < INTERVAL '5 years' THEN '3-5 Years'
        ELSE '5+ Years'
    END as tenure_group,
    COUNT(*) as employee_count,
    ROUND(AVG(de.current_salary), 0) as avg_salary,
    ROUND(AVG(EXTRACT(days FROM (CURRENT_DATE - de.hire_date))), 0) as avg_days_tenure
FROM dim_employee de
WHERE de.is_current = true
GROUP BY 
    CASE 
        WHEN (CURRENT_DATE - de.hire_date) < INTERVAL '1 year' THEN '< 1 Year'
        WHEN (CURRENT_DATE - de.hire_date) < INTERVAL '3 years' THEN '1-3 Years'
        WHEN (CURRENT_DATE - de.hire_date) < INTERVAL '5 years' THEN '3-5 Years'
        ELSE '5+ Years'
    END
ORDER BY avg_salary DESC;

-- 6. TOP EARNERS BY COMPANY
SELECT 
    '⭐ TOP EARNERS' as analysis_type,
    c.company_name,
    de.first_name || ' ' || de.last_name as employee_name,
    jc.job_title,
    de.current_salary,
    ROW_NUMBER() OVER (PARTITION BY c.company_name ORDER BY de.current_salary DESC) as company_rank
FROM dim_employee de
JOIN companies c ON de.company_id = c.company_id
JOIN job_codes jc ON de.job_code_id = jc.job_code_id
WHERE de.is_current = true
ORDER BY c.company_name, de.current_salary DESC;

-- 7. JOB FAMILY ANALYSIS
SELECT 
    '👨‍💼 JOB FAMILY ANALYSIS' as analysis_type,
    jc.job_family,
    COUNT(*) as employee_count,
    ROUND(AVG(de.current_salary), 0) as avg_salary,
    ROUND(MIN(de.current_salary), 0) as min_salary,
    ROUND(MAX(de.current_salary), 0) as max_salary,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 1) as percentage_of_workforce
FROM dim_employee de
JOIN job_codes jc ON de.job_code_id = jc.job_code_id
WHERE de.is_current = true
GROUP BY jc.job_family
ORDER BY employee_count DESC;

-- 8. SALARY BAND DISTRIBUTION
SELECT 
    '💵 SALARY BAND DISTRIBUTION' as analysis_type,
    CASE 
        WHEN current_salary < 75000 THEN '< $75K'
        WHEN current_salary < 100000 THEN '$75K - $100K'
        WHEN current_salary < 150000 THEN '$100K - $150K'
        WHEN current_salary < 200000 THEN '$150K - $200K'
        WHEN current_salary < 300000 THEN '$200K - $300K'
        ELSE '$300K+'
    END as salary_band,
    COUNT(*) as employee_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 1) as percentage
FROM dim_employee
WHERE is_current = true
GROUP BY 
    CASE 
        WHEN current_salary < 75000 THEN '< $75K'
        WHEN current_salary < 100000 THEN '$75K - $100K'
        WHEN current_salary < 150000 THEN '$100K - $150K'
        WHEN current_salary < 200000 THEN '$150K - $200K'
        WHEN current_salary < 300000 THEN '$200K - $300K'
        ELSE '$300K+'
    END
ORDER BY MIN(current_salary);

SELECT '✅ ENTERPRISE HR ANALYTICS COMPLETE - 1000 EMPLOYEES' as status;