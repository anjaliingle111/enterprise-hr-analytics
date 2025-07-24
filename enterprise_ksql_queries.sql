-- =================================================================
-- ENTERPRISE KSQLDB STREAM PROCESSING QUERIES
-- Lambda Architecture Serving Layer Implementation
-- =================================================================

-- Create stream from executive updates topic
CREATE STREAM hr_executive_stream (
    event_type VARCHAR,
    employee_id INTEGER,
    employee_name VARCHAR,
    job_title VARCHAR,
    company VARCHAR,
    department VARCHAR,
    previous_salary DOUBLE,
    new_salary DOUBLE,
    salary_increase DOUBLE,
    increase_percentage DOUBLE,
    timestamp VARCHAR,
    cycle INTEGER
) WITH (
    KAFKA_TOPIC='hr-executive-updates',
    VALUE_FORMAT='JSON'
);

-- Create stream from business intelligence topic
CREATE STREAM hr_intelligence_stream (
    event_type VARCHAR,
    cycle INTEGER,
    executive_updates_processed INTEGER,
    total_salary_increases DOUBLE,
    average_increase_percentage DOUBLE,
    projected_annual_impact DOUBLE,
    market_trend_classification VARCHAR,
    analysis_timestamp VARCHAR
) WITH (
    KAFKA_TOPIC='hr-enterprise-intelligence',
    VALUE_FORMAT='JSON'
);

-- Create competitive intelligence stream
CREATE STREAM hr_competitive_stream (
    event_type VARCHAR,
    cycle INTEGER,
    analysis_timestamp VARCHAR
) WITH (
    KAFKA_TOPIC='hr-competitive-intelligence',
    VALUE_FORMAT='JSON'
);

-- =================================================================
-- ADVANCED STREAM ANALYTICS
-- =================================================================

-- Real-time salary increase aggregations
CREATE TABLE salary_increases_by_company AS
SELECT 
    company,
    COUNT(*) as update_count,
    AVG(increase_percentage) as avg_increase_pct,
    SUM(salary_increase) as total_increase_amount,
    MAX(new_salary) as highest_salary
FROM hr_executive_stream
WINDOW TUMBLING (SIZE 2 MINUTES)
GROUP BY company
EMIT CHANGES;

-- High-impact executive changes (>10% increases)
CREATE STREAM high_impact_changes AS
SELECT 
    employee_name,
    company,
    job_title,
    previous_salary,
    new_salary,
    increase_percentage,
    timestamp
FROM hr_executive_stream
WHERE increase_percentage > 10.0
EMIT CHANGES;

-- Executive compensation trends by job title
CREATE TABLE compensation_trends_by_title AS
SELECT 
    job_title,
    COUNT(*) as updates_count,
    AVG(new_salary) as avg_current_salary,
    AVG(increase_percentage) as avg_increase_pct,
    SUM(salary_increase) as total_increases
FROM hr_executive_stream
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY job_title
EMIT CHANGES;

-- Real-time market trend analysis
CREATE TABLE market_trend_analysis AS
SELECT 
    market_trend_classification,
    COUNT(*) as trend_occurrences,
    AVG(average_increase_percentage) as avg_market_increase,
    SUM(projected_annual_impact) as total_annual_impact
FROM hr_intelligence_stream
WINDOW TUMBLING (SIZE 3 MINUTES)
GROUP BY market_trend_classification
EMIT CHANGES;

-- Enterprise intelligence dashboard data
CREATE STREAM executive_dashboard_feed AS
SELECT 
    'EXECUTIVE_UPDATE' as feed_type,
    employee_name as metric_name,
    CAST(new_salary AS VARCHAR) as metric_value,
    company as category,
    timestamp as event_time
FROM hr_executive_stream
EMIT CHANGES;

-- =================================================================
-- ENTERPRISE ALERTING STREAMS
-- =================================================================

-- Alert for massive salary increases (>15%)
CREATE STREAM salary_spike_alerts AS
SELECT 
    'SALARY_SPIKE_ALERT' as alert_type,
    employee_name,
    company,
    previous_salary,
    new_salary,
    increase_percentage,
    timestamp
FROM hr_executive_stream
WHERE increase_percentage > 15.0
EMIT CHANGES;

-- Company-wide compensation budget alerts
CREATE STREAM budget_impact_alerts AS
SELECT 
    'BUDGET_IMPACT_ALERT' as alert_type,
    company,
    total_increase_amount,
    update_count,
    TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss') as window_start
FROM salary_increases_by_company
WHERE total_increase_amount > 100000
EMIT CHANGES;

-- =================================================================
-- DATA MESH DOMAIN STREAMS
-- =================================================================

-- Finance domain stream
CREATE STREAM finance_domain_stream AS
SELECT 
    employee_name,
    company,
    previous_salary,
    new_salary,
    salary_increase,
    increase_percentage,
    'FINANCE_DOMAIN' as domain,
    timestamp
FROM hr_executive_stream
WHERE increase_percentage > 5.0
EMIT CHANGES;

-- HR domain stream  
CREATE STREAM hr_domain_stream AS
SELECT 
    employee_name,
    job_title,
    company,
    increase_percentage,
    'HR_DOMAIN' as domain,
    timestamp
FROM hr_executive_stream
EMIT CHANGES;

-- Executive domain stream
CREATE STREAM executive_domain_stream AS
SELECT 
    employee_name,
    job_title,
    company,
    new_salary,
    'EXECUTIVE_DOMAIN' as domain,
    timestamp
FROM hr_executive_stream
WHERE new_salary > 250000
EMIT CHANGES;