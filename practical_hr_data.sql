-- =================================================================
-- PRACTICAL HR SAMPLE DATA - STEP 2
-- Real data for the tables we just created
-- =================================================================

SELECT '🚀 LOADING PRACTICAL HR DATA' as status;

-- OFFICE LOCATIONS
INSERT INTO office_locations (location_code, location_name, city, country, capacity, monthly_rent) VALUES
('SF01', 'San Francisco HQ', 'San Francisco', 'USA', 150, 45000.00),
('LON01', 'London Office', 'London', 'UK', 80, 25000.00),
('SG01', 'Singapore Office', 'Singapore', 'Singapore', 100, 20000.00),
('BER01', 'Berlin Office', 'Berlin', 'Germany', 60, 18000.00),
('BLR01', 'Bangalore Office', 'Bangalore', 'India', 120, 8000.00),
('RMT01', 'Remote Workers', 'Various', 'Global', 500, 0.00);

-- LEAVE TYPES
INSERT INTO leave_types (leave_type_name, is_paid, max_days_per_year, requires_approval) VALUES
('Annual Vacation', TRUE, 25, TRUE),
('Sick Leave', TRUE, 12, FALSE),
('Personal Day', TRUE, 5, TRUE),
('Maternity Leave', TRUE, 90, TRUE),
('Bereavement', TRUE, 3, TRUE);

-- EMPLOYEE LEAVE BALANCES (for our 30 employees)
INSERT INTO employee_leave_balances (employee_key, leave_type_id, year, allocated_days, used_days, remaining_days) VALUES
-- Top executives get more vacation days
(1, 1, 2025, 30.0, 8.5, 21.5),
(1, 2, 2025, 12.0, 2.0, 10.0),
(3, 1, 2025, 28.0, 12.0, 16.0),
(3, 2, 2025, 12.0, 1.5, 10.5),
(5, 1, 2025, 27.0, 6.0, 21.0),
(5, 2, 2025, 12.0, 0.5, 11.5),

-- Regular employees
(6, 1, 2025, 25.0, 10.5, 14.5),
(6, 2, 2025, 12.0, 3.0, 9.0),
(7, 1, 2025, 25.0, 15.0, 10.0),
(7, 2, 2025, 12.0, 4.5, 7.5),
(10, 1, 2025, 25.0, 8.0, 17.0),
(10, 2, 2025, 12.0, 1.0, 11.0),

-- Junior employees
(11, 1, 2025, 22.0, 5.5, 16.5),
(11, 2, 2025, 12.0, 1.5, 10.5),
(15, 1, 2025, 20.0, 3.0, 17.0),
(15, 2, 2025, 12.0, 0.0, 12.0);

-- ATTENDANCE RECORDS (last few days for key employees)
INSERT INTO attendance_records (employee_key, work_date, check_in_time, check_out_time, total_hours_worked, overtime_hours, work_location, attendance_status) VALUES
-- Sarah Chen (high performer - works extra hours)
(3, '2025-01-20', '08:30:00', '18:15:00', 9.75, 1.75, 'office', 'present'),
(3, '2025-01-21', '08:45:00', '17:30:00', 8.75, 0.75, 'office', 'present'),
(3, '2025-01-22', '09:00:00', '17:45:00', 8.75, 0.75, 'remote', 'present'),

-- Jennifer Miller (remote worker)
(1, '2025-01-20', '09:15:00', '18:00:00', 8.75, 0.75, 'remote', 'present'),
(1, '2025-01-21', '09:00:00', '17:30:00', 8.50, 0.50, 'remote', 'present'),
(1, '2025-01-22', '08:30:00', '17:00:00', 8.50, 0.50, 'remote', 'present'),

-- Regular employees
(6, '2025-01-20', '09:00:00', '17:00:00', 8.00, 0.00, 'office', 'present'),
(6, '2025-01-21', '09:00:00', '17:00:00', 8.00, 0.00, 'office', 'present'),
(7, '2025-01-20', '09:30:00', '17:30:00', 8.00, 0.00, 'remote', 'present'),
(10, '2025-01-20', '08:45:00', '17:15:00', 8.50, 0.50, 'office', 'present'),
(11, '2025-01-20', '09:00:00', '17:00:00', 8.00, 0.00, 'office', 'present');

-- PAYROLL RUN (January 2025)
INSERT INTO payroll_runs (pay_period_start, pay_period_end, pay_date, total_gross_pay, total_net_pay, employee_count, status) VALUES
('2025-01-01', '2025-01-15', '2025-01-20', 978333.33, 710200.00, 30, 'processed');

-- EMPLOYEE PAYROLL DETAILS (for key employees)
INSERT INTO employee_payroll_details (payroll_run_id, employee_key, gross_pay, taxes_withheld, benefits_deducted, net_pay, overtime_hours, overtime_pay) VALUES
-- Top executives
(1, 1, 10208.33, 2349.27, 1200.00, 6659.06, 2.5, 312.50),
(1, 3, 12291.67, 2834.68, 1440.00, 8016.99, 4.0, 590.32),
(1, 5, 11458.33, 2640.42, 1375.00, 7442.91, 1.5, 229.17),

-- Mid-level
(1, 6, 7708.33, 1775.92, 925.00, 5007.41, 0.0, 0.00),
(1, 7, 8125.00, 1871.88, 975.00, 5278.12, 1.0, 121.88),
(1, 10, 6875.00, 1584.38, 825.00, 4465.62, 0.5, 85.94);

SELECT '✅ PRACTICAL HR DATA LOADED SUCCESSFULLY' as status;