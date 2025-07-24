-- =================================================================
-- PRACTICAL HR TABLES - STEP 1
-- Real industry-standard tables every company uses
-- =================================================================

-- ATTENDANCE & TIME TRACKING
CREATE TABLE attendance_records (
    attendance_id SERIAL PRIMARY KEY,
    employee_key INTEGER REFERENCES dim_employee(employee_key),
    work_date DATE NOT NULL,
    check_in_time TIME,
    check_out_time TIME,
    total_hours_worked DECIMAL(4,2),
    overtime_hours DECIMAL(4,2) DEFAULT 0,
    work_location VARCHAR(50), -- 'office', 'remote', 'client_site'
    attendance_status VARCHAR(20) DEFAULT 'present', -- 'present', 'absent', 'partial'
    approved_by_manager BOOLEAN DEFAULT FALSE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- LEAVE MANAGEMENT
CREATE TABLE leave_types (
    leave_type_id SERIAL PRIMARY KEY,
    leave_type_name VARCHAR(50) NOT NULL, -- 'vacation', 'sick', 'personal'
    is_paid BOOLEAN DEFAULT TRUE,
    max_days_per_year INTEGER,
    requires_approval BOOLEAN DEFAULT TRUE,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE employee_leave_balances (
    balance_id SERIAL PRIMARY KEY,
    employee_key INTEGER REFERENCES dim_employee(employee_key),
    leave_type_id INTEGER REFERENCES leave_types(leave_type_id),
    year INTEGER NOT NULL,
    allocated_days DECIMAL(4,1),
    used_days DECIMAL(4,1) DEFAULT 0,
    remaining_days DECIMAL(4,1),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- OFFICE LOCATIONS
CREATE TABLE office_locations (
    location_id SERIAL PRIMARY KEY,
    location_code VARCHAR(10) UNIQUE NOT NULL,
    location_name VARCHAR(100) NOT NULL,
    city VARCHAR(50),
    country VARCHAR(50),
    capacity INTEGER,
    monthly_rent DECIMAL(10,2),
    is_active BOOLEAN DEFAULT TRUE
);

-- PAYROLL RUNS
CREATE TABLE payroll_runs (
    payroll_run_id SERIAL PRIMARY KEY,
    pay_period_start DATE NOT NULL,
    pay_period_end DATE NOT NULL,
    pay_date DATE NOT NULL,
    total_gross_pay DECIMAL(15,2),
    total_net_pay DECIMAL(15,2),
    employee_count INTEGER,
    status VARCHAR(20) DEFAULT 'processed',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- EMPLOYEE PAYROLL DETAILS
CREATE TABLE employee_payroll_details (
    payroll_detail_id SERIAL PRIMARY KEY,
    payroll_run_id INTEGER REFERENCES payroll_runs(payroll_run_id),
    employee_key INTEGER REFERENCES dim_employee(employee_key),
    gross_pay DECIMAL(10,2),
    taxes_withheld DECIMAL(8,2),
    benefits_deducted DECIMAL(8,2),
    net_pay DECIMAL(10,2),
    overtime_hours DECIMAL(4,2) DEFAULT 0,
    overtime_pay DECIMAL(8,2) DEFAULT 0
);

SELECT '✅ PRACTICAL HR TABLES CREATED' as status;