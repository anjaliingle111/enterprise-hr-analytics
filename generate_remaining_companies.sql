-- =================================================================
-- GENERATE REMAINING 800 EMPLOYEES (200 per company for companies 2-5)
-- =================================================================

SELECT '🚀 GENERATING 800 MORE EMPLOYEES FOR COMPANIES 2-5' as status;

-- Company 2: European Tech Ltd (200 employees, IDs 2000-2199)
WITH employee_generator_company2 AS (
    SELECT 
        2000 + row_number() OVER () as employee_id,
        CASE (row_number() OVER () % 20)
            WHEN 0 THEN 'Emma' WHEN 1 THEN 'Oliver' WHEN 2 THEN 'Sophia' WHEN 3 THEN 'Liam'
            WHEN 4 THEN 'Ava' WHEN 5 THEN 'Noah' WHEN 6 THEN 'Isabella' WHEN 7 THEN 'Lucas'
            WHEN 8 THEN 'Mia' WHEN 9 THEN 'Ethan' WHEN 10 THEN 'Charlotte' WHEN 11 THEN 'Mason'
            WHEN 12 THEN 'Amelia' WHEN 13 THEN 'Logan' WHEN 14 THEN 'Harper' WHEN 15 THEN 'Jackson'
            WHEN 16 THEN 'Ella' WHEN 17 THEN 'Sebastian' WHEN 18 THEN 'Aria' ELSE 'Aiden'
        END as first_name,
        CASE (row_number() OVER () % 25)
            WHEN 0 THEN 'Clarke' WHEN 1 THEN 'Wright' WHEN 2 THEN 'Evans' WHEN 3 THEN 'Edwards'
            WHEN 4 THEN 'Collins' WHEN 5 THEN 'Stewart' WHEN 6 THEN 'Morris' WHEN 7 THEN 'Rogers'
            WHEN 8 THEN 'Reed' WHEN 9 THEN 'Cook' WHEN 10 THEN 'Bailey' WHEN 11 THEN 'Cooper'
            WHEN 12 THEN 'Richardson' WHEN 13 THEN 'Cox' WHEN 14 THEN 'Howard' WHEN 15 THEN 'Ward'
            WHEN 16 THEN 'Torres' WHEN 17 THEN 'Peterson' WHEN 18 THEN 'Gray' WHEN 19 THEN 'Ramirez'
            WHEN 20 THEN 'James' WHEN 21 THEN 'Watson' WHEN 22 THEN 'Brooks' WHEN 23 THEN 'Kelly'
            ELSE 'Sanders'
        END as last_name,
        11 + (row_number() OVER () % 26) as job_code_id,
        1 + (row_number() OVER () % 18) as department_id,
        2 as company_id,
        CASE 
            WHEN (row_number() OVER () % 26) BETWEEN 0 AND 4 THEN 70000 + (random() * 20000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 5 AND 10 THEN 85000 + (random() * 30000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 11 AND 16 THEN 115000 + (random() * 40000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 17 AND 21 THEN 145000 + (random() * 50000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 22 AND 24 THEN 180000 + (random() * 70000)::int
            ELSE 260000 + (random() * 100000)::int
        END as current_salary,
        CURRENT_DATE - (random() * 1825)::int as hire_date,
        TRUE as is_current
    FROM generate_series(1, 200) as gs
)
INSERT INTO dim_employee (
    employee_id, first_name, last_name, email, hire_date, 
    job_code_id, department_id, company_id, current_salary, is_current
)
SELECT 
    employee_id, first_name, last_name,
    lower(first_name) || '.' || lower(last_name) || employee_id || '@eurotech.com',
    hire_date, job_code_id, department_id, company_id, current_salary, is_current
FROM employee_generator_company2;

SELECT '✅ GENERATED 200 EMPLOYEES FOR EUROPEAN TECH LTD' as status;

-- Company 3: Asia Pacific Holdings (200 employees, IDs 3000-3199) 
WITH employee_generator_company3 AS (
    SELECT 
        3000 + row_number() OVER () as employee_id,
        CASE (row_number() OVER () % 20)
            WHEN 0 THEN 'Wei' WHEN 1 THEN 'Yuki' WHEN 2 THEN 'Raj' WHEN 3 THEN 'Mei'
            WHEN 4 THEN 'Hiroshi' WHEN 5 THEN 'Priya' WHEN 6 THEN 'Takeshi' WHEN 7 THEN 'Anjali'
            WHEN 8 THEN 'Chen' WHEN 9 THEN 'Sakura' WHEN 10 THEN 'Arjun' WHEN 11 THEN 'Lin'
            WHEN 12 THEN 'Kenji' WHEN 13 THEN 'Sita' WHEN 14 THEN 'Ming' WHEN 15 THEN 'Yumi'
            WHEN 16 THEN 'Rohit' WHEN 17 THEN 'Xiao' WHEN 18 THEN 'Akira' ELSE 'Kavya'
        END as first_name,
        CASE (row_number() OVER () % 25)
            WHEN 0 THEN 'Zhang' WHEN 1 THEN 'Tanaka' WHEN 2 THEN 'Singh' WHEN 3 THEN 'Wang'
            WHEN 4 THEN 'Yamamoto' WHEN 5 THEN 'Patel' WHEN 6 THEN 'Li' WHEN 7 THEN 'Sato'
            WHEN 8 THEN 'Kumar' WHEN 9 THEN 'Liu' WHEN 10 THEN 'Suzuki' WHEN 11 THEN 'Sharma'
            WHEN 12 THEN 'Chen' WHEN 13 THEN 'Watanabe' WHEN 14 THEN 'Gupta' WHEN 15 THEN 'Wu'
            WHEN 16 THEN 'Nakamura' WHEN 17 THEN 'Reddy' WHEN 18 THEN 'Yang' WHEN 19 THEN 'Ito'
            WHEN 20 THEN 'Agarwal' WHEN 21 THEN 'Zhou' WHEN 22 THEN 'Kato' WHEN 23 THEN 'Jain'
            ELSE 'Xu'
        END as last_name,
        11 + (row_number() OVER () % 26) as job_code_id,
        1 + (row_number() OVER () % 18) as department_id,
        3 as company_id,
        CASE 
            WHEN (row_number() OVER () % 26) BETWEEN 0 AND 4 THEN 65000 + (random() * 25000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 5 AND 10 THEN 90000 + (random() * 35000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 11 AND 16 THEN 125000 + (random() * 45000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 17 AND 21 THEN 160000 + (random() * 55000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 22 AND 24 THEN 195000 + (random() * 75000)::int
            ELSE 275000 + (random() * 115000)::int
        END as current_salary,
        CURRENT_DATE - (random() * 1825)::int as hire_date,
        TRUE as is_current
    FROM generate_series(1, 200) as gs
)
INSERT INTO dim_employee (
    employee_id, first_name, last_name, email, hire_date, 
    job_code_id, department_id, company_id, current_salary, is_current
)
SELECT 
    employee_id, first_name, last_name,
    lower(first_name) || '.' || lower(last_name) || employee_id || '@asiapacific.com',
    hire_date, job_code_id, department_id, company_id, current_salary, is_current
FROM employee_generator_company3;

SELECT '✅ GENERATED 200 EMPLOYEES FOR ASIA PACIFIC HOLDINGS' as status;

-- Company 4: German Innovation GmbH (200 employees, IDs 4000-4199)
WITH employee_generator_company4 AS (
    SELECT 
        4000 + row_number() OVER () as employee_id,
        CASE (row_number() OVER () % 20)
            WHEN 0 THEN 'Hans' WHEN 1 THEN 'Anna' WHEN 2 THEN 'Klaus' WHEN 3 THEN 'Eva'
            WHEN 4 THEN 'Stefan' WHEN 5 THEN 'Maria' WHEN 6 THEN 'Andreas' WHEN 7 THEN 'Julia'
            WHEN 8 THEN 'Michael' WHEN 9 THEN 'Petra' WHEN 10 THEN 'Thomas' WHEN 11 THEN 'Sabine'
            WHEN 12 THEN 'Wolfgang' WHEN 13 THEN 'Brigitte' WHEN 14 THEN 'Jurgen' WHEN 15 THEN 'Ingrid'
            WHEN 16 THEN 'Rainer' WHEN 17 THEN 'Monika' WHEN 18 THEN 'Dieter' ELSE 'Gabriele'
        END as first_name,
        CASE (row_number() OVER () % 25)
            WHEN 0 THEN 'Mueller' WHEN 1 THEN 'Schmidt' WHEN 2 THEN 'Schneider' WHEN 3 THEN 'Fischer'
            WHEN 4 THEN 'Weber' WHEN 5 THEN 'Meyer' WHEN 6 THEN 'Wagner' WHEN 7 THEN 'Becker'
            WHEN 8 THEN 'Schulz' WHEN 9 THEN 'Hoffmann' WHEN 10 THEN 'Schafer' WHEN 11 THEN 'Koch'
            WHEN 12 THEN 'Richter' WHEN 13 THEN 'Klein' WHEN 14 THEN 'Wolf' WHEN 15 THEN 'Schroder'
            WHEN 16 THEN 'Neumann' WHEN 17 THEN 'Schwarz' WHEN 18 THEN 'Zimmermann' WHEN 19 THEN 'Braun'
            WHEN 20 THEN 'Kruger' WHEN 21 THEN 'Hartmann' WHEN 22 THEN 'Lange' WHEN 23 THEN 'Schmitt'
            ELSE 'Werner'
        END as last_name,
        11 + (row_number() OVER () % 26) as job_code_id,
        1 + (row_number() OVER () % 18) as department_id,
        4 as company_id,
        CASE 
            WHEN (row_number() OVER () % 26) BETWEEN 0 AND 4 THEN 68000 + (random() * 22000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 5 AND 10 THEN 88000 + (random() * 32000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 11 AND 16 THEN 120000 + (random() * 42000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 17 AND 21 THEN 155000 + (random() * 52000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 22 AND 24 THEN 190000 + (random() * 72000)::int
            ELSE 270000 + (random() * 110000)::int
        END as current_salary,
        CURRENT_DATE - (random() * 1825)::int as hire_date,
        TRUE as is_current
    FROM generate_series(1, 200) as gs
)
INSERT INTO dim_employee (
    employee_id, first_name, last_name, email, hire_date, 
    job_code_id, department_id, company_id, current_salary, is_current
)
SELECT 
    employee_id, first_name, last_name,
    lower(first_name) || '.' || lower(last_name) || employee_id || '@german-innovation.com',
    hire_date, job_code_id, department_id, company_id, current_salary, is_current
FROM employee_generator_company4;

SELECT '✅ GENERATED 200 EMPLOYEES FOR GERMAN INNOVATION GMBH' as status;

-- Company 5: India Software Services (200 employees, IDs 5000-5199)
WITH employee_generator_company5 AS (
    SELECT 
        5000 + row_number() OVER () as employee_id,
        CASE (row_number() OVER () % 20)
            WHEN 0 THEN 'Rahul' WHEN 1 THEN 'Priya' WHEN 2 THEN 'Arjun' WHEN 3 THEN 'Anita'
            WHEN 4 THEN 'Vikram' WHEN 5 THEN 'Sita' WHEN 6 THEN 'Raj' WHEN 7 THEN 'Kavya'
            WHEN 8 THEN 'Suresh' WHEN 9 THEN 'Meera' WHEN 10 THEN 'Anil' WHEN 11 THEN 'Pooja'
            WHEN 12 THEN 'Deepak' WHEN 13 THEN 'Shreya' WHEN 14 THEN 'Kiran' WHEN 15 THEN 'Nisha'
            WHEN 16 THEN 'Manoj' WHEN 17 THEN 'Ritu' WHEN 18 THEN 'Ajay' ELSE 'Sunita'
        END as first_name,
        CASE (row_number() OVER () % 25)
            WHEN 0 THEN 'Sharma' WHEN 1 THEN 'Patel' WHEN 2 THEN 'Singh' WHEN 3 THEN 'Kumar'
            WHEN 4 THEN 'Gupta' WHEN 5 THEN 'Agarwal' WHEN 6 THEN 'Reddy' WHEN 7 THEN 'Jain'
            WHEN 8 THEN 'Yadav' WHEN 9 THEN 'Mishra' WHEN 10 THEN 'Tiwari' WHEN 11 THEN 'Srivastava'
            WHEN 12 THEN 'Chandra' WHEN 13 THEN 'Verma' WHEN 14 THEN 'Chopra' WHEN 15 THEN 'Bansal'
            WHEN 16 THEN 'Malhotra' WHEN 17 THEN 'Saxena' WHEN 18 THEN 'Kapoor' WHEN 19 THEN 'Mehta'
            WHEN 20 THEN 'Shah' WHEN 21 THEN 'Bhatia' WHEN 22 THEN 'Aggarwal' WHEN 23 THEN 'Goyal'
            ELSE 'Mittal'
        END as last_name,
        11 + (row_number() OVER () % 26) as job_code_id,
        1 + (row_number() OVER () % 18) as department_id,
        5 as company_id,
        CASE 
            WHEN (row_number() OVER () % 26) BETWEEN 0 AND 4 THEN 45000 + (random() * 20000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 5 AND 10 THEN 65000 + (random() * 25000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 11 AND 16 THEN 85000 + (random() * 35000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 17 AND 21 THEN 110000 + (random() * 45000)::int
            WHEN (row_number() OVER () % 26) BETWEEN 22 AND 24 THEN 140000 + (random() * 60000)::int
            ELSE 200000 + (random() * 90000)::int
        END as current_salary,
        CURRENT_DATE - (random() * 1825)::int as hire_date,
        TRUE as is_current
    FROM generate_series(1, 200) as gs
)
INSERT INTO dim_employee (
    employee_id, first_name, last_name, email, hire_date, 
    job_code_id, department_id, company_id, current_salary, is_current
)
SELECT 
    employee_id, first_name, last_name,
    lower(first_name) || '.' || lower(last_name) || employee_id || '@indiasoft.com',
    hire_date, job_code_id, department_id, company_id, current_salary, is_current
FROM employee_generator_company5;

SELECT '✅ GENERATED 200 EMPLOYEES FOR INDIA SOFTWARE SERVICES' as status;
SELECT '🎉 TOTAL: 1,000 ENTERPRISE EMPLOYEES GENERATED!' as final_status;