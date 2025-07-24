DO $$ 
DECLARE 
    i INT; 
    company_id INT; 
    total INT := 0;
    first_names TEXT[] := ARRAY['Sarah','Mike','Lisa','David','Jennifer','Alex','Emily','James','Maria','Robert'];
    last_names TEXT[] := ARRAY['Chen','Johnson','Wang','Smith','Taylor','Brown','Davis','Wilson','Garcia','Miller'];
BEGIN
    RAISE NOTICE 'Starting employee generation...';
    
    FOR company_id IN 1..5 LOOP
        FOR i IN 1..(CASE company_id WHEN 1 THEN 315 WHEN 2 THEN 210 WHEN 3 THEN 198 WHEN 4 THEN 167 ELSE 160 END) LOOP
            INSERT INTO employees (
                company_id, department_id, business_unit_id, job_code_id, 
                first_name, last_name, hire_date, current_salary, 
                employment_status, created_at, updated_at
            ) VALUES (
                company_id, 
                1 + (RANDOM() * 25)::INT, 
                1 + (RANDOM() * 10)::INT, 
                1 + (RANDOM() * 41)::INT,
                first_names[1 + (RANDOM() * 9)::INT],
                last_names[1 + (RANDOM() * 9)::INT],
                CURRENT_DATE - (RANDOM() * 1000)::INT,
                70000 + (RANDOM() * 100000)::INT,
                'active',
                NOW(),
                NOW()
            );
            total := total + 1;
            
            IF total % 200 = 0 THEN
                RAISE NOTICE 'Generated % employees...', total;
            END IF;
        END LOOP;
        RAISE NOTICE 'Company % complete', company_id;
    END LOOP;
    
    RAISE NOTICE 'SUCCESS: Created % employees total!', total;
END $$;

SELECT COUNT(*) as total_employees FROM employees;

SELECT 
    c.company_name, 
    COUNT(e.employee_id) as employees,
    AVG(e.current_salary)::int as avg_salary
FROM companies c 
LEFT JOIN employees e ON c.company_id = e.company_id 
GROUP BY c.company_id, c.company_name 
ORDER BY employees DESC;