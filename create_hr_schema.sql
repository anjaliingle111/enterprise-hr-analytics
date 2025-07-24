-- Enterprise HR Analytics Database Schema Recreation
-- Fortune 500-level HR system with global companies

-- Create Companies table (5 Global Companies)
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(50) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    founded_year INTEGER,
    industry VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Business Units table
CREATE TABLE business_units (
    business_unit_id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(company_id),
    unit_name VARCHAR(100) NOT NULL,
    location VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Departments table
CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(company_id),
    business_unit_id INTEGER REFERENCES business_units(business_unit_id),
    department_name VARCHAR(100) NOT NULL,
    department_code VARCHAR(10),
    manager_employee_id INTEGER,
    budget DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Job Codes table
CREATE TABLE job_codes (
    job_code_id SERIAL PRIMARY KEY,
    job_code VARCHAR(20) NOT NULL UNIQUE,
    job_title VARCHAR(100) NOT NULL,
    job_family VARCHAR(50),
    job_level INTEGER,
    min_salary DECIMAL(12,2),
    max_salary DECIMAL(12,2),
    currency VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Training Programs table
CREATE TABLE training_programs (
    training_program_id SERIAL PRIMARY KEY,
    program_name VARCHAR(100) NOT NULL,
    program_type VARCHAR(50),
    duration_weeks INTEGER,
    cost DECIMAL(10,2),
    provider VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Benefit Plans table
CREATE TABLE benefit_plans (
    benefit_plan_id SERIAL PRIMARY KEY,
    plan_name VARCHAR(100) NOT NULL,
    plan_type VARCHAR(50),
    coverage_type VARCHAR(50),
    monthly_cost DECIMAL(10,2),
    company_contribution_pct DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Employees table (Main table)
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(company_id),
    department_id INTEGER REFERENCES departments(department_id),
    business_unit_id INTEGER REFERENCES business_units(business_unit_id),
    job_code_id INTEGER REFERENCES job_codes(job_code_id),
    employee_number VARCHAR(20) UNIQUE,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    date_of_birth DATE,
    hire_date DATE NOT NULL,
    termination_date DATE,
    employment_status VARCHAR(20) DEFAULT 'active',
    employment_type VARCHAR(20) DEFAULT 'full-time',
    current_salary DECIMAL(12,2),
    currency VARCHAR(3) DEFAULT 'USD',
    manager_id INTEGER REFERENCES employees(employee_id),
    work_location VARCHAR(100),
    remote_eligible BOOLEAN DEFAULT FALSE,
    performance_rating DECIMAL(3,1),
    last_promotion_date DATE,
    years_of_experience INTEGER,
    education_level VARCHAR(50),
    skills TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Performance Reviews table
CREATE TABLE performance_reviews (
    review_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(employee_id),
    review_period_start DATE NOT NULL,
    review_period_end DATE NOT NULL,
    overall_rating DECIMAL(3,1),
    goals_rating DECIMAL(3,1),
    competencies_rating DECIMAL(3,1),
    reviewer_employee_id INTEGER REFERENCES employees(employee_id),
    reviewer_comments TEXT,
    employee_comments TEXT,
    review_status VARCHAR(20) DEFAULT 'draft',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(employee_id, review_period_start)
);

-- Create Training Enrollments table
CREATE TABLE training_enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(employee_id),
    training_program_id INTEGER REFERENCES training_programs(training_program_id),
    enrollment_date DATE NOT NULL,
    completion_date DATE,
    status VARCHAR(20) DEFAULT 'enrolled',
    score DECIMAL(5,2),
    certificate_earned BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Benefit Enrollments table
CREATE TABLE benefit_enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(employee_id),
    benefit_plan_id INTEGER REFERENCES benefit_plans(benefit_plan_id),
    enrollment_date DATE NOT NULL,
    effective_date DATE NOT NULL,
    end_date DATE,
    status VARCHAR(20) DEFAULT 'active',
    employee_contribution DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Payroll Runs table
CREATE TABLE payroll_runs (
    payroll_run_id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL UNIQUE,
    pay_period_start DATE NOT NULL,
    pay_period_end DATE NOT NULL,
    total_employees INTEGER,
    total_gross_pay DECIMAL(15,2),
    total_net_pay DECIMAL(15,2),
    status VARCHAR(20) DEFAULT 'draft',
    processed_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert Companies (5 Global Fortune 500 Companies)
INSERT INTO companies (company_name, country, currency, founded_year, industry) VALUES
('GlobalTech Solutions Inc', 'USA', 'USD', 2010, 'Technology'),
('European Tech Ltd', 'UK', 'GBP', 2008, 'Software Development'),
('Asia Pacific Holdings', 'Singapore', 'SGD', 2012, 'Digital Services'),
('German Innovation GmbH', 'Germany', 'EUR', 2015, 'AI & Machine Learning'),
('India Software Services', 'India', 'INR', 2011, 'IT Consulting');

-- Insert Business Units
INSERT INTO business_units (company_id, unit_name, location, region) VALUES
-- GlobalTech Solutions Inc
(1, 'North America Engineering', 'San Francisco', 'Americas'),
(1, 'Product Development', 'Seattle', 'Americas'),
(1, 'Global Sales', 'New York', 'Americas'),
-- European Tech Ltd  
(2, 'UK Development', 'London', 'EMEA'),
(2, 'European Sales', 'Amsterdam', 'EMEA'),
-- Asia Pacific Holdings
(3, 'APAC Engineering', 'Singapore', 'APAC'),
(3, 'Innovation Lab', 'Tokyo', 'APAC'),
-- German Innovation GmbH
(4, 'AI Research', 'Munich', 'EMEA'),
(4, 'European Operations', 'Berlin', 'EMEA'),
-- India Software Services
(5, 'Bangalore Development', 'Bangalore', 'APAC'),
(5, 'Mumbai Operations', 'Mumbai', 'APAC');

-- Insert Departments (26+ departments across companies)
INSERT INTO departments (company_id, business_unit_id, department_name, department_code, budget) VALUES
-- GlobalTech Solutions Inc
(1, 1, 'Backend Engineering', 'BE', 2500000.00),
(1, 1, 'Frontend Engineering', 'FE', 2200000.00),
(1, 1, 'DevOps Engineering', 'DO', 1800000.00),
(1, 1, 'Data Engineering', 'DE', 2100000.00),
(1, 2, 'Product Management', 'PM', 1500000.00),
(1, 2, 'UX/UI Design', 'UX', 1200000.00),
(1, 3, 'Enterprise Sales', 'ES', 3000000.00),
(1, 3, 'Marketing', 'MK', 2000000.00),
(1, 3, 'Customer Success', 'CS', 1600000.00),
-- European Tech Ltd
(2, 4, 'Software Development', 'SD', 1800000.00),
(2, 4, 'Quality Assurance', 'QA', 1200000.00),
(2, 4, 'Technical Writing', 'TW', 800000.00),
(2, 5, 'Regional Sales', 'RS', 2200000.00),
(2, 5, 'Partner Relations', 'PR', 1000000.00),
-- Asia Pacific Holdings
(3, 6, 'Mobile Development', 'MD', 1500000.00),
(3, 6, 'Cloud Infrastructure', 'CI', 2000000.00),
(3, 7, 'Research & Development', 'RD', 2500000.00),
(3, 7, 'Innovation Strategy', 'IS', 1200000.00),
-- German Innovation GmbH
(4, 8, 'Machine Learning', 'ML', 3000000.00),
(4, 8, 'Data Science', 'DS', 2800000.00),
(4, 9, 'Business Operations', 'BO', 1500000.00),
(4, 9, 'Human Resources', 'HR', 1200000.00),
-- India Software Services
(5, 10, 'Full Stack Development', 'FS', 2000000.00),
(5, 10, 'System Architecture', 'SA', 2200000.00),
(5, 11, 'Client Services', 'CL', 1800000.00),
(5, 11, 'Technical Support', 'TS', 1400000.00);

-- Insert Job Codes (39 job codes across levels and functions)
INSERT INTO job_codes (job_code, job_title, job_family, job_level, min_salary, max_salary, currency) VALUES
-- Engineering Job Codes
('ENG001', 'Junior Software Engineer', 'Engineering', 1, 65000, 85000, 'USD'),
('ENG002', 'Software Engineer', 'Engineering', 2, 85000, 110000, 'USD'),
('ENG003', 'Senior Software Engineer', 'Engineering', 3, 110000, 140000, 'USD'),
('ENG004', 'Principal Engineer', 'Engineering', 4, 140000, 180000, 'USD'),
('ENG005', 'Staff Engineer', 'Engineering', 5, 180000, 220000, 'USD'),
('ENG006', 'Junior DevOps Engineer', 'Engineering', 1, 70000, 90000, 'USD'),
('ENG007', 'DevOps Engineer', 'Engineering', 2, 90000, 115000, 'USD'),
('ENG008', 'Senior DevOps Engineer', 'Engineering', 3, 115000, 145000, 'USD'),
('ENG009', 'Data Engineer', 'Engineering', 2, 95000, 125000, 'USD'),
('ENG010', 'Senior Data Engineer', 'Engineering', 3, 125000, 155000, 'USD'),
-- Product Management
('PM001', 'Associate Product Manager', 'Product', 1, 80000, 100000, 'USD'),
('PM002', 'Product Manager', 'Product', 2, 100000, 130000, 'USD'),
('PM003', 'Senior Product Manager', 'Product', 3, 130000, 160000, 'USD'),
('PM004', 'Principal Product Manager', 'Product', 4, 160000, 200000, 'USD'),
-- Design
('DES001', 'UI/UX Designer', 'Design', 2, 75000, 95000, 'USD'),
('DES002', 'Senior UI/UX Designer', 'Design', 3, 95000, 120000, 'USD'),
('DES003', 'Principal Designer', 'Design', 4, 120000, 150000, 'USD'),
-- Sales
('SAL001', 'Sales Development Rep', 'Sales', 1, 60000, 80000, 'USD'),
('SAL002', 'Account Executive', 'Sales', 2, 80000, 120000, 'USD'),
('SAL003', 'Senior Account Executive', 'Sales', 3, 120000, 160000, 'USD'),
('SAL004', 'Sales Manager', 'Sales', 4, 140000, 180000, 'USD'),
-- Marketing
('MKT001', 'Marketing Specialist', 'Marketing', 2, 65000, 85000, 'USD'),
('MKT002', 'Marketing Manager', 'Marketing', 3, 85000, 110000, 'USD'),
('MKT003', 'Senior Marketing Manager', 'Marketing', 4, 110000, 140000, 'USD'),
-- Data Science & ML
('DS001', 'Data Scientist', 'Data Science', 2, 100000, 130000, 'USD'),
('DS002', 'Senior Data Scientist', 'Data Science', 3, 130000, 165000, 'USD'),
('DS003', 'Principal Data Scientist', 'Data Science', 4, 165000, 200000, 'USD'),
('ML001', 'ML Engineer', 'Machine Learning', 2, 105000, 135000, 'USD'),
('ML002', 'Senior ML Engineer', 'Machine Learning', 3, 135000, 170000, 'USD'),
('ML003', 'Principal ML Engineer', 'Machine Learning', 4, 170000, 210000, 'USD'),
-- Customer Success
('CS001', 'Customer Success Specialist', 'Customer Success', 2, 65000, 85000, 'USD'),
('CS002', 'Customer Success Manager', 'Customer Success', 3, 85000, 110000, 'USD'),
-- Quality Assurance
('QA001', 'QA Engineer', 'Quality Assurance', 2, 70000, 90000, 'USD'),
('QA002', 'Senior QA Engineer', 'Quality Assurance', 3, 90000, 115000, 'USD'),
-- Technical Writing
('TW001', 'Technical Writer', 'Technical Writing', 2, 65000, 85000, 'USD'),
('TW002', 'Senior Technical Writer', 'Technical Writing', 3, 85000, 105000, 'USD'),
-- HR
('HR001', 'HR Specialist', 'Human Resources', 2, 60000, 80000, 'USD'),
('HR002', 'HR Manager', 'Human Resources', 3, 80000, 105000, 'USD'),
('HR003', 'Senior HR Manager', 'Human Resources', 4, 105000, 130000, 'USD'),
-- Operations
('OP001', 'Operations Specialist', 'Operations', 2, 65000, 85000, 'USD'),
('OP002', 'Operations Manager', 'Operations', 3, 85000, 110000, 'USD'),
('OP003', 'Senior Operations Manager', 'Operations', 4, 110000, 140000, 'USD');

-- Insert Training Programs (25 programs)
INSERT INTO training_programs (program_name, program_type, duration_weeks, cost, provider) VALUES
('Leadership Development', 'Leadership', 8, 2500.00, 'Corporate University'),
('Technical Skills Bootcamp', 'Technical', 12, 3500.00, 'Tech Academy'),
('Data Science Fundamentals', 'Technical', 10, 3000.00, 'Data Institute'),
('Machine Learning Advanced', 'Technical', 16, 4500.00, 'AI Academy'),
('Project Management Certification', 'Management', 6, 2000.00, 'PM Institute'),
('Agile Methodology', 'Process', 4, 1500.00, 'Agile Training Co'),
('Cloud Computing Essentials', 'Technical', 8, 2800.00, 'Cloud Academy'),
('Cybersecurity Awareness', 'Security', 2, 800.00, 'Security First'),
('Communication Skills', 'Soft Skills', 3, 1200.00, 'Comm Excellence'),
('Sales Excellence', 'Sales', 6, 2200.00, 'Sales Pro Training'),
('Customer Service Mastery', 'Customer Service', 4, 1600.00, 'Service Academy'),
('Digital Marketing', 'Marketing', 8, 2400.00, 'Marketing Institute'),
('Financial Analysis', 'Finance', 6, 2000.00, 'Finance Academy'),
('Supply Chain Management', 'Operations', 10, 3200.00, 'SCM Institute'),
('Quality Management', 'Quality', 8, 2600.00, 'Quality First'),
('Innovation Workshop', 'Innovation', 2, 1000.00, 'Innovation Lab'),
('Diversity & Inclusion', 'HR', 3, 1200.00, 'D&I Consultants'),
('Mental Health Awareness', 'Wellness', 2, 800.00, 'Wellness Corp'),
('Time Management', 'Productivity', 2, 600.00, 'Productivity Plus'),
('Public Speaking', 'Communication', 4, 1400.00, 'Speaker Academy'),
('Negotiation Skills', 'Business Skills', 3, 1800.00, 'Negotiation Pro'),
('Strategic Thinking', 'Strategy', 6, 2500.00, 'Strategy Institute'),
('Change Management', 'Management', 5, 2100.00, 'Change Leaders'),
('Ethics in Business', 'Compliance', 2, 900.00, 'Ethics First'),
('Remote Work Best Practices', 'Remote Work', 3, 1000.00, 'Remote Academy');

-- Create indexes for better performance
CREATE INDEX idx_employees_company ON employees(company_id);
CREATE INDEX idx_employees_department ON employees(department_id);
CREATE INDEX idx_employees_status ON employees(employment_status);
CREATE INDEX idx_employees_salary ON employees(current_salary);
CREATE INDEX idx_employees_hire_date ON employees(hire_date);
CREATE INDEX idx_employees_updated ON employees(updated_at);
CREATE INDEX idx_performance_employee ON performance_reviews(employee_id);
CREATE INDEX idx_training_employee ON training_enrollments(employee_id);
CREATE INDEX idx_benefit_employee ON benefit_enrollments(employee_id);