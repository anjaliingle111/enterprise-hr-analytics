-- Populate HR Tables with Realistic Data
-- Author: HR Analytics Project
-- Purpose: Add realistic HR data for comprehensive analytics

BEGIN;

-- Insert Training Programs
INSERT INTO training_programs (program_name, program_type, duration_hours, cost, provider, is_mandatory, department_specific) VALUES
('Python for Data Analysis', 'Technical', 40, 1500.00, 'DataCamp', FALSE, 1),
('Leadership Fundamentals', 'Leadership', 24, 2000.00, 'LinkedIn Learning', FALSE, NULL),
('Agile Project Management', 'Technical', 16, 800.00, 'Scrum Alliance', TRUE, 2),
('Financial Planning Basics', 'Business', 12, 600.00, 'Internal Training', FALSE, 5),
('Sales Techniques Advanced', 'Sales', 20, 1200.00, 'SalesForce', FALSE, 3),
('Compliance Training 2024', 'Compliance', 4, 200.00, 'Internal Legal', TRUE, NULL),
('Machine Learning Fundamentals', 'Technical', 60, 3000.00, 'Coursera', FALSE, 1),
('Communication Skills', 'Soft Skills', 8, 400.00, 'Dale Carnegie', FALSE, NULL)
ON CONFLICT DO NOTHING;

-- Insert Employee Training Records
INSERT INTO employee_training (employee_id, program_id, enrollment_date, completion_date, status, score, certification_earned, cost)
SELECT 
    (1000 + (random() * 1000)::INTEGER),
    (1 + (random() * 7)::INTEGER),
    CURRENT_DATE - (random() * 365)::INTEGER,
    CASE 
        WHEN random() > 0.25 THEN CURRENT_DATE - (random() * 180)::INTEGER 
        ELSE NULL 
    END,
    CASE 
        WHEN random() > 0.25 THEN 'Completed' 
        WHEN random() > 0.15 THEN 'In Progress'
        ELSE 'Enrolled'
    END,
    (random() * 40 + 60)::DECIMAL(5,2),
    random() > 0.7,
    (random() * 2000 + 200)::DECIMAL(10,2)
FROM generate_series(1, 400);

-- Insert Performance Reviews
INSERT INTO performance_reviews (employee_id, review_period, overall_rating, goals_met, goals_total, manager_rating, peer_rating, self_rating, review_date)
SELECT 
    de.employee_id,
    CASE (random() * 4)::INTEGER 
        WHEN 0 THEN 'Q1-2024'
        WHEN 1 THEN 'Q2-2024' 
        WHEN 2 THEN 'Q3-2024'
        ELSE 'Annual-2024'
    END,
    (random() * 2 + 3)::DECIMAL(3,2),
    (random() * 6 + 4)::INTEGER,
    (random() * 3 + 8)::INTEGER,
    (random() * 2 + 3)::DECIMAL(3,2),
    (random() * 2 + 3)::DECIMAL(3,2),
    (random() * 2 + 3)::DECIMAL(3,2),
    CURRENT_DATE - (random() * 90)::INTEGER
FROM dim_employee de 
WHERE de.is_current = true 
AND random() > 0.3
LIMIT 700;

-- Insert Employee Surveys
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

-- Insert Skills Matrix
INSERT INTO skills_matrix (employee_id, skill_category, skill_name, proficiency_level, last_assessed, target_level)
SELECT 
    de.employee_id,
    CASE de.company_id 
        WHEN 1 THEN 'Technical'
        WHEN 2 THEN 'Product Strategy'
        WHEN 3 THEN 'Sales'
        WHEN 4 THEN 'Operations'
        ELSE 'Business Skills'
    END,
    CASE (random() * 6)::INTEGER 
        WHEN 0 THEN 'Python Programming'
        WHEN 1 THEN 'Data Analysis'
        WHEN 2 THEN 'Leadership'
        WHEN 3 THEN 'Communication'
        WHEN 4 THEN 'Project Management'
        ELSE 'Strategic Planning'
    END,
    (random() * 3 + 2)::INTEGER,
    CURRENT_DATE - (random() * 180)::INTEGER,
    (random() * 2 + 4)::INTEGER
FROM dim_employee de 
WHERE de.is_current = true 
AND random() > 0.6
LIMIT 400;

-- Insert Leave Balances
INSERT INTO leave_balances (employee_id, leave_type, annual_allocation, used_days, remaining_days, year)
SELECT 
    de.employee_id,
    leave_types.leave_type,
    leave_types.allocation,
    (random() * leave_types.allocation * 0.7)::INTEGER,
    leave_types.allocation - (random() * leave_types.allocation * 0.7)::INTEGER,
    2024
FROM dim_employee de
CROSS JOIN (
    VALUES 
        ('Vacation', 25),
        ('Sick Leave', 12),
        ('Personal', 5)
) AS leave_types(leave_type, allocation)
WHERE de.is_current = true;

COMMIT;

SELECT 'HR data populated successfully!' as status;
