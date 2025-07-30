-- Populate HR Tables Using dim_employee (correct table)
BEGIN;

-- Populate Performance Reviews (using dim_employee)
INSERT INTO performance_reviews (employee_id, review_period_start, review_period_end, overall_rating, goals_rating, competencies_rating, reviewer_employee_id, review_status)
SELECT 
    de.employee_id,
    '2024-07-01'::DATE,
    '2024-09-30'::DATE,
    (random() * 2 + 3)::DECIMAL(3,1),
    (random() * 2 + 3)::DECIMAL(3,1),
    (random() * 2 + 3)::DECIMAL(3,1),
    1001, -- Default reviewer
    'completed'
FROM dim_employee de 
WHERE de.is_current = true
AND random() > 0.3
LIMIT 600;

-- Populate Employee Surveys (using dim_employee)
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

-- Populate Skills Matrix (using dim_employee)
INSERT INTO skills_matrix (employee_id, skill_category, skill_name, proficiency_level, last_assessed, target_level)
SELECT 
    de.employee_id,
    CASE de.company_id 
        WHEN 1 THEN 'Technical'
        WHEN 2 THEN 'Product'
        WHEN 3 THEN 'Sales'
        WHEN 4 THEN 'Operations'
        ELSE 'Business'
    END,
    CASE (random() * 5)::INTEGER 
        WHEN 0 THEN 'Leadership'
        WHEN 1 THEN 'Communication'
        WHEN 2 THEN 'Technical Skills'
        WHEN 3 THEN 'Project Management'
        ELSE 'Problem Solving'
    END,
    (random() * 3 + 2)::INTEGER,
    CURRENT_DATE - (random() * 180)::INTEGER,
    (random() * 2 + 4)::INTEGER
FROM dim_employee de 
WHERE de.is_current = true
AND random() > 0.6
LIMIT 400;

-- Populate Leave Balances (using dim_employee)
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

COMMIT;

SELECT 'HR data populated successfully using dim_employee table!' as status;
