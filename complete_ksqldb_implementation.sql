-- =================================================================
-- COMPLETE ksqlDB STREAM PROCESSING IMPLEMENTATION
-- Real-time stream analytics for enterprise HR platform
-- =================================================================

-- Set processing mode to earliest for complete data processing
SET 'auto.offset.reset' = 'earliest';

-- 1. CREATE STREAMS FROM KAFKA TOPICS
CREATE STREAM hr_executive_updates_stream (
    event_type VARCHAR,
    cycle INTEGER,
    employee_key INTEGER,
    employee_name VARCHAR,
    company VARCHAR,
    job_title VARCHAR,
    department VARCHAR,
    previous_salary DOUBLE,
    new_salary DOUBLE,
    increase_amount DOUBLE,
    increase_percentage DOUBLE,
    timestamp VARCHAR,
    market_factor DOUBLE
) WITH (
    KAFKA_TOPIC = 'hr-executive-updates',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp',
    TIMESTAMP_FORMAT = 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'
);

CREATE STREAM hr_enterprise_intelligence_stream (
    event_type VARCHAR,
    cycle INTEGER,
    timestamp VARCHAR,
    employees_updated INTEGER,
    total_increase_amount DOUBLE,
    average_increase DOUBLE,
    projected_annual_impact DOUBLE,
    market_trend VARCHAR,
    budget_impact_percentage DOUBLE
) WITH (
    KAFKA_TOPIC = 'hr-enterprise-intelligence',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'timestamp',
    TIMESTAMP_FORMAT = 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'
);

CREATE STREAM hr_competitive_intelligence_stream (
    event_type VARCHAR,
    market_moves ARRAY<STRUCT<
        competitor VARCHAR,
        action VARCHAR,
        impact_percentage DOUBLE,
        confidence_score DOUBLE,
        timestamp VARCHAR
    >>,
    analysis_timestamp VARCHAR,
    market_volatility VARCHAR
) WITH (
    KAFKA_TOPIC = 'hr-competitive-intelligence',
    VALUE_FORMAT = 'JSON'
);

-- 2. REAL-TIME ANALYTICS TABLES
-- High-value executives (salary > 250K)
CREATE TABLE high_value_executives AS
SELECT 
    company,
    COUNT(*) as executive_count,
    AVG(new_salary) as avg_salary,
    SUM(increase_amount) as total_increases,
    MAX(increase_percentage) as max_increase_pct
FROM hr_executive_updates_stream
WHERE new_salary > 250000
GROUP BY company
EMIT CHANGES;

-- Salary increase trends by company
CREATE TABLE company_salary_trends AS
SELECT 
    company,
    department,
    WINDOWSTART as window_start,
    WINDOWEND as window_end,
    COUNT(*) as updates_count,
    AVG(increase_percentage) as avg_increase_pct,
    SUM(increase_amount) as total_increase_amount
FROM hr_executive_updates_stream
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY company, department
EMIT CHANGES;

-- Market trend analysis
CREATE TABLE market_trend_analysis AS
SELECT 
    market_trend,
    WINDOWSTART as window_start,
    COUNT(*) as trend_count,
    AVG(total_increase_amount) as avg_total_increase,
    AVG(budget_impact_percentage) as avg_budget_impact
FROM hr_enterprise_intelligence_stream
WINDOW TUMBLING (SIZE 10 MINUTES)
GROUP BY market_trend
EMIT CHANGES;

-- 3. REAL-TIME ALERTS
-- High compensation pressure alert
CREATE STREAM high_compensation_alerts AS
SELECT 
    'HIGH_COMPENSATION_PRESSURE' as alert_type,
    timestamp,
    total_increase_amount,
    budget_impact_percentage,
    'Budget impact exceeds 2% threshold' as alert_message
FROM hr_enterprise_intelligence_stream
WHERE budget_impact_percentage > 2.0
EMIT CHANGES;

-- Executive salary spike alert  
CREATE STREAM executive_salary_spikes AS
SELECT 
    'EXECUTIVE_SALARY_SPIKE' as alert_type,
    employee_name,
    company,
    previous_salary,
    new_salary,
    increase_percentage,
    'Executive salary increase exceeds 15%' as alert_message
FROM hr_executive_updates_stream
WHERE increase_percentage > 0.15
EMIT CHANGES;

-- Competitive market volatility alert
CREATE STREAM market_volatility_alerts AS
SELECT 
    'MARKET_VOLATILITY_HIGH' as alert_type,
    analysis_timestamp,
    market_volatility,
    ARRAY_LENGTH(market_moves) as competitor_moves_count,
    'High market volatility detected' as alert_message
FROM hr_competitive_intelligence_stream
WHERE market_volatility = 'high'
EMIT CHANGES;

-- 4. ADVANCED ANALYTICS
-- Department performance comparison
CREATE TABLE department_performance AS
SELECT 
    department,
    company,
    COUNT(*) as employee_updates,
    AVG(increase_percentage) as avg_increase_pct,
    STDDEV_SAMP(increase_percentage) as increase_std_dev,
    MIN(increase_percentage) as min_increase,
    MAX(increase_percentage) as max_increase
FROM hr_executive_updates_stream
GROUP BY department, company
EMIT CHANGES;

-- Real-time budget impact tracking
CREATE TABLE budget_impact_tracker AS
SELECT 
    WINDOWSTART as window_start,
    COUNT(*) as intelligence_events,
    AVG(budget_impact_percentage) as avg_budget_impact,
    MAX(budget_impact_percentage) as max_budget_impact,
    SUM(total_increase_amount) as total_compensation_increase
FROM hr_enterprise_intelligence_stream
WINDOW TUMBLING (SIZE 15 MINUTES)
GROUP BY 1
EMIT CHANGES;

-- 5. COMPETITIVE INTELLIGENCE ANALYTICS
-- Flatten competitive moves for analysis
CREATE STREAM competitive_moves_flat AS
SELECT 
    analysis_timestamp,
    market_volatility,
    EXPLODE(market_moves) as move
FROM hr_competitive_intelligence_stream
EMIT CHANGES;

-- Competitor action frequency
CREATE TABLE competitor_action_frequency AS
SELECT 
    move->competitor as competitor,
    move->action as action,
    COUNT(*) as action_count,
    AVG(move->impact_percentage) as avg_impact,
    AVG(move->confidence_score) as avg_confidence
FROM competitive_moves_flat
GROUP BY move->competitor, move->action
EMIT CHANGES;

-- 6. EXECUTIVE SUMMARY DASHBOARD DATA
CREATE TABLE executive_dashboard_summary AS
SELECT 
    LATEST_BY_OFFSET(timestamp) as last_update,
    COUNT(*) as total_intelligence_events,
    AVG(total_increase_amount) as avg_increase_per_cycle,
    AVG(budget_impact_percentage) as avg_budget_impact,
    LATEST_BY_OFFSET(market_trend) as current_market_trend,
    SUM(total_increase_amount) as cumulative_increases
FROM hr_enterprise_intelligence_stream
GROUP BY 1
EMIT CHANGES;

-- Print completion status
SELECT 'ksqlDB STREAM PROCESSING IMPLEMENTATION COMPLETE' as status;