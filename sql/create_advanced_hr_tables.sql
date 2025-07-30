-- Advanced HR Tables for Enterprise Analytics
-- Author: HR Analytics Project  
-- Purpose: Add comprehensive HR data tables used by Fortune 500 companies

BEGIN;

-- 1. PERFORMANCE MANAGEMENT
CREATE TABLE IF NOT EXISTS performance_reviews (
    review_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES dim_employee(employee_id),
    review_period VARCHAR(20),
    overall_rating DECIMAL(3,2),
    goals_met INTEGER,
    goals_total INTEGER,
    manager_rating DECIMAL(3,2),
    peer_rating DECIMAL(3,2),
    self_rating DECIMAL(3,2),
    improvement_areas TEXT,
    strengths TEXT,
    review_date DATE,
    reviewer_id INTEGER,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. TRAINING & DEVELOPMENT
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
    employee_id INTEGER REFERENCES dim_employee(employee_id),
    program_id INTEGER REFERENCES training_programs(program_id),
    enrollment_date DATE,
    completion_date DATE,
    status VARCHAR(20),
    score DECIMAL(5,2),
    certification_earned BOOLEAN DEFAULT FALSE,
    expiry_date DATE,
    cost DECIMAL(10,2)
);

-- 3. EMPLOYEE ENGAGEMENT & SATISFACTION
CREATE TABLE IF NOT EXISTS employee_surveys (
    survey_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES dim_employee(employee_id),
    survey_date DATE,
    engagement_score INTEGER,
    satisfaction_score INTEGER,
    work_life_balance INTEGER,
    career_development INTEGER,
    compensation_satisfaction INTEGER,
    management_effectiveness INTEGER,
    would_recommend_company BOOLEAN,
    likely_to_leave INTEGER,
    comments TEXT
);

-- 4. SKILLS MATRIX
CREATE TABLE IF NOT EXISTS skills_matrix (
    skill_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES dim_employee(employee_id),
    skill_category VARCHAR(50),
    skill_name VARCHAR(100),
    proficiency_level INTEGER,
    last_assessed DATE,
    assessor_id INTEGER,
    target_level INTEGER,
    development_plan TEXT
);

-- 5. LEAVE MANAGEMENT
CREATE TABLE IF NOT EXISTS leave_balances (
    balance_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES dim_employee(employee_id),
    leave_type VARCHAR(30),
    annual_allocation INTEGER,
    used_days INTEGER,
    remaining_days INTEGER,
    year INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. COMPENSATION HISTORY
CREATE TABLE IF NOT EXISTS salary_history (
    history_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES dim_employee(employee_id),
    effective_date DATE,
    previous_salary DECIMAL(10,2),
    new_salary DECIMAL(10,2),
    increase_percentage DECIMAL(5,2),
    reason VARCHAR(100),
    approved_by INTEGER,
    notes TEXT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_performance_employee ON performance_reviews(employee_id);
CREATE INDEX IF NOT EXISTS idx_training_employee ON employee_training(employee_id);
CREATE INDEX IF NOT EXISTS idx_surveys_employee ON employee_surveys(employee_id);
CREATE INDEX IF NOT EXISTS idx_skills_employee ON skills_matrix(employee_id);

COMMIT;

SELECT 'Advanced HR tables created successfully!' as status;
