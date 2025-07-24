-- =================================================================
-- GENERATE 1,000 ENTERPRISE EMPLOYEES (200 per company)
-- Fortune 500-level workforce data generation
-- =================================================================

SELECT '🚀 GENERATING 1,000 ENTERPRISE EMPLOYEES' as status;

-- First, let's add more job codes to support 1,000 employees
INSERT INTO job_codes (job_title, job_level, job_family, salary_band) VALUES
-- Engineering roles
('Junior Software Engineer', 'junior', 'engineering', 'L3'),
('Software Engineer II', 'mid', 'engineering', 'L4'), 
('Senior Software Engineer II', 'senior', 'engineering', 'L5'),
('Staff Software Engineer', 'staff', 'engineering', 'L6'),
('Principal Software Engineer', 'principal', 'engineering', 'L7'),
('Distinguished Engineer', 'distinguished', 'engineering', 'L8'),

-- Product roles
('Associate Product Manager', 'junior', 'product', 'L3'),
('Product Manager II', 'mid', 'product', 'L4'),
('Senior Product Manager II', 'senior', 'product', 'L5'),
('Staff Product Manager', 'staff', 'product', 'L6'),
('Principal Product Manager', 'principal', 'product', 'L7'),

-- Data roles  
('Data Analyst', 'junior', 'data', 'L3'),
('Senior Data Analyst', 'mid', 'data', 'L4'),
('Data Scientist II', 'mid', 'data', 'L4'),
('Senior Data Scientist II', 'senior', 'data', 'L5'),
('Staff Data Scientist', 'staff', 'data', 'L6'),

-- Operations roles
('DevOps Engineer', 'mid', 'operations', 'L4'),
('Senior DevOps Engineer', 'senior', 'operations', 'L5'),
('Site Reliability Engineer', 'mid', 'operations', 'L4'),
('Senior SRE', 'senior', 'operations', 'L5'),

-- Business roles
('Business Analyst', 'junior', 'business', 'L3'),
('Senior Business Analyst', 'mid', 'business', 'L4'),
('Program Manager', 'senior', 'business', 'L5'),
('Director of Engineering', 'director', 'leadership', 'L8'),
('Director of Product', 'director', 'leadership', 'L8'),
('VP of Engineering', 'vp', 'executive', 'L9');

-- Add more departments to support larger workforce
INSERT INTO departments (department_name, department_head, budget_allocation) VALUES
('Mobile Engineering', 'Alex Thompson', 15000000.00),
('Platform Engineering', 'Jordan Kim', 18000000.00), 
('Security Engineering', 'Morgan Davis', 12000000.00),
('Quality Assurance', 'Taylor Wilson', 8000000.00),
('Customer Success', 'Riley Johnson', 6000000.00),
('Business Intelligence', 'Casey Martinez', 7000000.00),
('Legal & Compliance', 'Drew Anderson', 4000000.00),
('IT Support', 'Jamie Brown', 5000000.00);

SELECT '✅ EXPANDED JOB CODES AND DEPARTMENTS' as status;

-- Now let's generate employees systematically
-- Company 1: GlobalTech Solutions Inc (200 employees)
WITH employee_generator AS (
    SELECT 
        1000 + row_number() OVER () as employee_id,
        CASE (row_number() OVER () % 20)
            WHEN 0 THEN 'Alex' WHEN 1 THEN 'Taylor' WHEN 2 THEN 'Jordan' WHEN 3 THEN 'Casey'
            WHEN 4 THEN 'Riley' WHEN 5 THEN 'Morgan' WHEN 6 THEN 'Avery' WHEN 7 THEN 'Quinn'
            WHEN 8 THEN 'Sage' WHEN 9 THEN 'River' WHEN 10 THEN 'Phoenix' WHEN 11 THEN 'Blake'
            WHEN 12 THEN 'Drew' WHEN 13 THEN 'Skylar' WHEN 14 THEN 'Rowan' WHEN 15 THEN 'Finley'
            WHEN 16 THEN 'Emery' WHEN 17 THEN 'Peyton' WHEN 18 THEN 'Reese' ELSE 'Cameron'
        END as first_name,
        CASE (row_number() OVER () % 25)
            WHEN 0 THEN 'Smith' WHEN 1 THEN 'Johnson' WHEN 2 THEN 'Williams' WHEN 3 THEN 'Brown'
            WHEN 4 THEN 'Jones' WHEN 5 THEN 'Garcia' WHEN 6 THEN 'Miller' WHEN 7 THEN 'Davis'
            WHEN 8 THEN 'Rodriguez' WHEN 9 THEN 'Martinez' WHEN 10 THEN 'Hernandez' WHEN 11 THEN 'Lopez'
            WHEN 12 THEN 'Gonzalez' WHEN 13 THEN 'Wilson' WHEN 14 THEN 'Anderson' WHEN 15 THEN 'Thomas'
            WHEN 16 THEN 'Taylor' WHEN 17 THEN 'Moore' WHEN 18 THEN 'Jackson' WHEN 19 THEN 'Martin'
            WHEN 20 THEN 'Lee' WHEN 21 THEN 'Perez' WHEN 22 THEN 'Thompson' WHEN 23 THEN 'White'
            ELSE 'Harris'
        END as last_name,
        -- Distribute across job codes (11-36 are our new job codes)
        11 + (row_number() OVER () % 26) as job_code_id,
        -- Distribute across departments (1-18 total departments)  
        1 + (row_number() OVER () % 18) as department_id,
        -- Company 1
        1 as company_id,
        -- Realistic salary based on job level
        CASE 
            WHEN (row_number() OVER () % 26) BETWEEN 0 AND 4 THEN 75000 + (random() * 25000)::int  -- Junior
            WHEN (row_number() OVER () % 26) BETWEEN 5 AND 10 THEN 95000 + (random() * 35000)::int -- Mid
            WHEN (row_number() OVER () % 26) BETWEEN 11 AND 16 THEN 130000 + (random() * 45000)::int -- Senior
            WHEN (row_number() OVER () % 26) BETWEEN 17 AND 21 THEN 165000 + (random() * 55000)::int -- Staff
            WHEN (row_number() OVER () % 26) BETWEEN 22 AND 24 THEN 200000 + (random() * 80000)::int -- Principal
            ELSE 280000 + (random() * 120000)::int -- Director/VP
        END as current_salary,
        -- Random hire dates in last 5 years
        CURRENT_DATE - (random() * 1825)::int as hire_date,
        TRUE as is_current
    FROM generate_series(1, 200) as gs
)
INSERT INTO dim_employee (
    employee_id, first_name, last_name, email, hire_date, 
    job_code_id, department_id, company_id, current_salary, is_current
)
SELECT 
    employee_id,
    first_name,
    last_name,
    lower(first_name) || '.' || lower(last_name) || employee_id || '@globaltech.com',
    hire_date,
    job_code_id,
    department_id, 
    company_id,
    current_salary,
    is_current
FROM employee_generator;

SELECT '✅ GENERATED 200 EMPLOYEES FOR GLOBALTECH SOLUTIONS' as status;