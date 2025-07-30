-- Populate HR Tables Using employees table (final version)
BEGIN;

-- Populate Performance Reviews
INSERT INTO performance_reviews (employee_id, review_period_start, review_period_end, overall_rating, goals_rating, competencies_rating, reviewer_employee_id, review_status)
SELECT 
    e.employee_id,
    '2024-07-01'::DATE,
    '2024-09-30'::DATE,
    (random() * 2 + 3)::DECIMAL(3,1),
    (random() * 2 + 3)::DECIMAL(3,1),
    (random() * 2 + 3)::DECIMAL(3,1),
    (SELECT MIN(employee_id) FROM employees),
    'completed'
FROM employees e 
WHERE random() > 0.4
LIMIT 600;

-- Populate Employee Surveys
INSERT INTO employee_surveys (employee_id, survey_date, engagement_score, satisfaction_score, work_life_balance, career_development, compensation_satisfaction, management_effectiveness, would_recommend_company, likely_to_leave)
SELECT 
    e.employee_id,
    CURRENT_DATE - (random() * 180)::INTEGER,
    (random() * 4 + 6)::INTEGER,
    (random() * 4 + 6)::INTEGER,
    (random() * 5 + 5)::INTEGER,
    (random() * 4 + 6)::INTEGER,
    (random() * 5 + 5)::INTEGER,
    (random() * 4 + 6)::INTEGER,
    random() > 0.2,
    (random() * 7 + 1)::INTEGER
FROM employees e 
WHERE random() > 0.5
LIMIT 500;

-- Populate Skills Matrix
INSERT INTO skills_matrix (employee_id, skill_category, skill_name, proficiency_level, last_assessed, target_level)
SELECT 
    e.employee_id,
    CASE (e.employee_id % 5) + 1
        WHEN 1 THEN 'Technical'
        WHEN 2 THEN 'Product'
        WHEN 3 THEN 'Sales'
        WHEN 4 THEN 'Operations'
        ELSE 'Business'
    END,
    'Core Job Skills',
    (random() * 3 + 2)::INTEGER,
    CURRENT_DATE - (random() * 180)::INTEGER,
    (random() * 2 + 4)::INTEGER
FROM employees e 
WHERE random() > 0.6
LIMIT 400;

-- Populate Leave Balances
INSERT INTO leave_balances (employee_id, leave_type, annual_allocation, used_days, remaining_days, year)
SELECT 
    e.employee_id,
    leave_type,
    allocation,
    (random() * allocation * 0.6)::INTEGER,
    allocation - (random() * allocation * 0.6)::INTEGER,
    2024
FROM employees e
CROSS JOIN (VALUES ('Vacation', 25), ('Sick Leave', 12), ('Personal', 5)) AS lt(leave_type, allocation);

COMMIT;

SELECT 'HR data populated successfully!' as status;
