-- Transform Companies and Populate Existing HR Tables
BEGIN;

-- Step 1: Transform companies to departments
ALTER TABLE companies ADD COLUMN IF NOT EXISTS parent_company VARCHAR(100);

UPDATE companies SET 
    company_name = CASE company_id
        WHEN 1 THEN 'Engineering Department'
        WHEN 2 THEN 'Product Management Department'  
        WHEN 3 THEN 'Sales & Marketing Department'
        WHEN 4 THEN 'Operations Department'
        WHEN 5 THEN 'Finance & HR Department'
    END,
    industry = 'Technology',
    parent_company = 'TechCorp Solutions Inc';

-- Step 2: Populate Performance Reviews
INSERT INTO performance_reviews (employee_id, review_period, overall_rating, goals_met, goals_total, manager_rating, review_date)
SELECT 
    de.employee_id,
    'Q3-2024',
    (random() * 2 + 3)::DECIMAL(3,2),
    (random() * 6 + 4)::INTEGER,
    (random() * 3 + 8)::INTEGER,
    (random() * 2 + 3)::DECIMAL(3,2),
    CURRENT_DATE - (random() * 90)::INTEGER
FROM dim_employee de 
WHERE de.is_current = true 
AND random() > 0.3
LIMIT 700;

-- Step 3: Populate Employee Surveys
INSERT INTO employee_surveys (employee_id, survey_date, engagement_score, satisfaction_score, work_life_balance, career_development, compensation_satisfaction, management_effectiveness, would_recommend_company, likely_to_leave)
SELECT 
    de.employee_id,
    CURRENT_DATE - (random() * 180)::INTEGER,
    (random() * 4 + 6)::INTEGER,
    (random() * 4 + 6)::INTEGER,
    (random() * 5 + 5)::INTEGER,
    (random() * 4 + 6)::INTEGER,
    (random() * 5 + 5)::INTEGER,
    (random() * 4 + 6)::INTEGER,
    random() > 0.2,
    (random() * 7 + 1)::INTEGER
FROM dim_employee de 
WHERE de.is_current = true 
AND random() > 0.5
LIMIT 500;

-- Step 4: Populate Skills Matrix
INSERT INTO skills_matrix (employee_id, skill_category, skill_name, proficiency_level, last_assessed, assessor_id, target_level)
SELECT 
    de.employee_id,
    CASE de.company_id 
        WHEN 1 THEN 'Technical'
        WHEN 2 THEN 'Product'
        WHEN 3 THEN 'Sales'
        WHEN 4 THEN 'Operations'
        ELSE 'Business'
    END,
    'Core Job Skills',
    (random() * 3 + 2)::INTEGER,
    CURRENT_DATE - (random() * 180)::INTEGER,
    1000,
    (random() * 2 + 4)::INTEGER
FROM dim_employee de 
WHERE de.is_current = true 
AND random() > 0.6
LIMIT 400;

-- Step 5: Populate Leave Balances
INSERT INTO leave_balances (employee_id, leave_type, annual_allocation, used_days, remaining_days, year)
SELECT 
    de.employee_id,
    leave_type,
    allocation,
    (random() * allocation * 0.6)::INTEGER,
    allocation - (random() * allocation * 0.6)::INTEGER,
    2024
FROM dim_employee de
CROSS JOIN (VALUES ('Vacation', 25), ('Sick Leave', 12), ('Personal', 5)) AS lt(leave_type, allocation)
WHERE de.is_current = true;

-- Step 6: Populate Employee Training (using existing training_programs)
INSERT INTO employee_training (employee_id, training_program_id, enrollment_date, completion_date, status, score, certification_earned)
SELECT 
    de.employee_id,
    (SELECT training_program_id FROM training_programs ORDER BY random() LIMIT 1),
    CURRENT_DATE - (random() * 365)::INTEGER,
    CASE WHEN random() > 0.3 THEN CURRENT_DATE - (random() * 180)::INTEGER ELSE NULL END,
    CASE WHEN random() > 0.3 THEN 'Completed' ELSE 'In Progress' END,
    (random() * 40 + 60)::DECIMAL(5,2),
    random() > 0.8
FROM dim_employee de 
WHERE de.is_current = true 
AND random() > 0.6
LIMIT 400;

COMMIT;

SELECT 'Companies transformed and HR data populated successfully!' as status;
