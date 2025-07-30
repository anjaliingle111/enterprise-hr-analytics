-- Advanced HR Tables (Simple Version)
BEGIN;

CREATE TABLE IF NOT EXISTS performance_reviews (
    review_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    review_period VARCHAR(20),
    overall_rating DECIMAL(3,2),
    goals_met INTEGER,
    goals_total INTEGER,
    manager_rating DECIMAL(3,2),
    peer_rating DECIMAL(3,2),
    self_rating DECIMAL(3,2),
    review_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS training_programs (
    program_id SERIAL PRIMARY KEY,
    program_name VARCHAR(200),
    program_type VARCHAR(50),
    duration_hours INTEGER,
    cost DECIMAL(10,2),
    provider VARCHAR(100),
    is_mandatory BOOLEAN DEFAULT FALSE,
    department_specific INTEGER
);

CREATE TABLE IF NOT EXISTS employee_training (
    training_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    program_id INTEGER,
    enrollment_date DATE,
    completion_date DATE,
    status VARCHAR(20),
    score DECIMAL(5,2),
    certification_earned BOOLEAN DEFAULT FALSE,
    cost DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS employee_surveys (
    survey_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    survey_date DATE,
    engagement_score INTEGER,
    satisfaction_score INTEGER,
    work_life_balance INTEGER,
    career_development INTEGER,
    compensation_satisfaction INTEGER,
    management_effectiveness INTEGER,
    would_recommend_company BOOLEAN,
    likely_to_leave INTEGER
);

CREATE TABLE IF NOT EXISTS skills_matrix (
    skill_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    skill_category VARCHAR(50),
    skill_name VARCHAR(100),
    proficiency_level INTEGER,
    last_assessed DATE,
    target_level INTEGER
);

CREATE TABLE IF NOT EXISTS leave_balances (
    balance_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    leave_type VARCHAR(30),
    annual_allocation INTEGER,
    used_days INTEGER,
    remaining_days INTEGER,
    year INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS salary_history (
    history_id SERIAL PRIMARY KEY,
    employee_id INTEGER,
    effective_date DATE,
    previous_salary DECIMAL(10,2),
    new_salary DECIMAL(10,2),
    increase_percentage DECIMAL(5,2),
    reason VARCHAR(100)
);

COMMIT;

SELECT 'Advanced HR tables created successfully!' as status;
