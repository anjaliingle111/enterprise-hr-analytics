-- =================================================================
-- ENTERPRISE HR ANALYTICS DATA WAREHOUSE SCHEMA
-- =================================================================

SELECT '🚀 CREATING ENTERPRISE DATA WAREHOUSE SCHEMA' as status;

-- === DIMENSIONAL TABLES ===

-- Companies dimension with SCD Type 2
CREATE TABLE IF NOT EXISTS companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL UNIQUE,
    industry VARCHAR(50),
    headquarters_location VARCHAR(100),
    employee_count INTEGER,
    annual_revenue DECIMAL(15,2),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiry_date TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE
);

-- Job codes dimension
CREATE TABLE IF NOT EXISTS job_codes (
    job_code_id SERIAL PRIMARY KEY,
    job_title VARCHAR(100) NOT NULL,
    job_level VARCHAR(20),
    job_family VARCHAR(50),
    salary_band VARCHAR(20),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Departments dimension
CREATE TABLE IF NOT EXISTS departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL,
    department_head VARCHAR(100),
    budget_allocation DECIMAL(12,2),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Date dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    week_of_year INTEGER,
    is_weekend BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Employee dimension with SCD Type 2
CREATE TABLE IF NOT EXISTS dim_employee (
    employee_key SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    hire_date DATE,
    job_code_id INTEGER REFERENCES job_codes(job_code_id),
    department_id INTEGER REFERENCES departments(department_id),
    company_id INTEGER REFERENCES companies(company_id),
    manager_id INTEGER,
    employment_status VARCHAR(20) DEFAULT 'active',
    current_salary DECIMAL(10,2),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiry_date TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Department dimension
CREATE TABLE IF NOT EXISTS dim_department (
    department_key SERIAL PRIMARY KEY,
    department_id INTEGER NOT NULL,
    department_name VARCHAR(100),
    department_head VARCHAR(100),
    budget_allocation DECIMAL(12,2),
    company_id INTEGER REFERENCES companies(company_id),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiry_date TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE
);

-- === FACT TABLES ===

-- Employee daily snapshot fact table
CREATE TABLE IF NOT EXISTS fact_employee_daily_snapshot (
    snapshot_key SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dim_date(date_key),
    employee_key INTEGER REFERENCES dim_employee(employee_key),
    department_key INTEGER REFERENCES dim_department(department_key),
    salary_amount DECIMAL(10,2),
    fte DECIMAL(3,2) DEFAULT 1.0,
    is_active BOOLEAN DEFAULT TRUE,
    tenure_days INTEGER,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Salary changes fact table
CREATE TABLE IF NOT EXISTS fact_salary_changes (
    change_key SERIAL PRIMARY KEY,
    employee_key INTEGER REFERENCES dim_employee(employee_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    previous_salary DECIMAL(10,2),
    new_salary DECIMAL(10,2),
    salary_change_amount DECIMAL(10,2),
    salary_change_percentage DECIMAL(5,2),
    change_reason VARCHAR(100),
    effective_date DATE,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- === INDEXES FOR PERFORMANCE ===

CREATE INDEX IF NOT EXISTS idx_dim_employee_current ON dim_employee(employee_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_employee_company ON dim_employee(company_id, is_current);
CREATE INDEX IF NOT EXISTS idx_fact_snapshot_date ON fact_employee_daily_snapshot(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_snapshot_employee ON fact_employee_daily_snapshot(employee_key);
CREATE INDEX IF NOT EXISTS idx_fact_salary_changes_employee ON fact_salary_changes(employee_key);
CREATE INDEX IF NOT EXISTS idx_fact_salary_changes_date ON fact_salary_changes(date_key);

-- === SEED DATA ===

-- Insert companies
INSERT INTO companies (company_name, industry, headquarters_location, employee_count, annual_revenue) VALUES
('GlobalTech Solutions Inc', 'Technology', 'San Francisco, CA', 210, 45000000.00),
('European Tech Ltd', 'Software', 'London, UK', 198, 38000000.00),
('Asia Pacific Holdings', 'Technology', 'Singapore', 225, 52000000.00),
('German Innovation GmbH', 'Engineering', 'Berlin, Germany', 187, 41000000.00),
('India Software Services', 'IT Services', 'Bangalore, India', 230, 35000000.00)
ON CONFLICT (company_name) DO NOTHING;

-- Insert departments
INSERT INTO departments (department_name, department_head, budget_allocation) VALUES
('Machine Learning', 'Dr. Sarah Johnson', 5200000.00),
('Data Science', 'Michael Chen', 4800000.00),
('Backend Engineering', 'Jennifer Rodriguez', 6100000.00),
('Frontend Engineering', 'David Kim', 4900000.00),
('DevOps', 'Lisa Wang', 3700000.00),
('Product Management', 'Robert Taylor', 4200000.00),
('Marketing', 'Amanda Wilson', 3100000.00),
('Sales', 'James Miller', 5800000.00),
('Human Resources', 'Maria Garcia', 2300000.00),
('Finance', 'Thomas Anderson', 2900000.00),
('Quality Assurance', 'Rachel Thompson', 2800000.00),
('Customer Success', 'Daniel Brown', 3400000.00),
('Business Intelligence', 'Sophie Davis', 3600000.00),
('Security', 'Alex Kumar', 4100000.00),
('Infrastructure', 'Emma Johnson', 3900000.00),
('Research & Development', 'Oliver Singh', 5500000.00),
('Legal', 'Grace Liu', 1800000.00),
('Operations', 'Noah Patel', 3200000.00),
('Training & Development', 'Zoe Martin', 1900000.00),
('Procurement', 'Lucas Zhang', 1600000.00),
('Facilities Management', 'Mia Williams', 1400000.00),
('IT Support', 'Jacob Wilson', 2100000.00),
('Compliance', 'Isabella Moore', 1700000.00),
('Strategy', 'Ethan Garcia', 2500000.00),
('Communications', 'Ava Rodriguez', 1500000.00),
('Partnerships', 'Logan Anderson', 2200000.00);

-- Insert job codes
INSERT INTO job_codes (job_title, job_level, job_family, salary_band) VALUES
('Senior Data Scientist', 'Senior', 'Data & Analytics', '140-180K'),
('Principal Engineer', 'Principal', 'Engineering', '160-200K'),
('VP Product Management', 'Executive', 'Product', '180-240K'),
('Senior ML Engineer', 'Senior', 'Engineering', '150-190K'),
('Staff Engineer', 'Staff', 'Engineering', '170-210K'),
('DevOps Director', 'Director', 'Infrastructure', '180-220K'),
('Lead Data Scientist', 'Lead', 'Data & Analytics', '155-195K'),
('Senior Product Manager', 'Senior', 'Product', '135-175K'),
('Principal Data Engineer', 'Principal', 'Engineering', '165-205K'),
('Senior Software Engineer', 'Senior', 'Engineering', '140-180K');

-- Populate date dimension for current year
INSERT INTO dim_date (date_key, full_date, year, quarter, month, day, day_of_week, week_of_year, is_weekend, fiscal_year, fiscal_quarter)
SELECT 
    TO_CHAR(date_series, 'YYYYMMDD')::INTEGER as date_key,
    date_series::DATE as full_date,
    EXTRACT(YEAR FROM date_series)::INTEGER as year,
    EXTRACT(QUARTER FROM date_series)::INTEGER as quarter,
    EXTRACT(MONTH FROM date_series)::INTEGER as month,
    EXTRACT(DAY FROM date_series)::INTEGER as day,
    EXTRACT(DOW FROM date_series)::INTEGER as day_of_week,
    EXTRACT(WEEK FROM date_series)::INTEGER as week_of_year,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0,6) THEN TRUE ELSE FALSE END as is_weekend,
    CASE 
        WHEN EXTRACT(MONTH FROM date_series) >= 4 THEN EXTRACT(YEAR FROM date_series)::INTEGER
        ELSE EXTRACT(YEAR FROM date_series)::INTEGER - 1
    END as fiscal_year,
    CASE 
        WHEN EXTRACT(MONTH FROM date_series) IN (4,5,6) THEN 1
        WHEN EXTRACT(MONTH FROM date_series) IN (7,8,9) THEN 2
        WHEN EXTRACT(MONTH FROM date_series) IN (10,11,12) THEN 3
        ELSE 4
    END as fiscal_quarter
FROM generate_series('2024-01-01'::date, '2025-12-31'::date, '1 day'::interval) as date_series
ON CONFLICT (date_key) DO NOTHING;

SELECT '✅ ENTERPRISE SCHEMA CREATED SUCCESSFULLY' as status;
SELECT 'Ready for dimensional data loading and real-time CDC streaming!' as next_step;