from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
import requests
import logging
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'enterprise-hr-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the main enterprise DAG
enterprise_dag = DAG(
    'enterprise_hr_analytics_orchestration',
    default_args=default_args,
    description='Complete Enterprise HR Analytics Orchestration Pipeline',
    schedule_interval=timedelta(hours=1),  # Run every hour
    tags=['enterprise', 'hr', 'analytics', 'real-time'],
    max_active_runs=1
)

def extract_employee_metrics(**context):
    """Extract key employee metrics from PostgreSQL"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get current employee statistics
    employee_stats_query = """
    SELECT 
        COUNT(*) as total_employees,
        ROUND(AVG(current_salary), 2) as avg_salary,
        ROUND(SUM(current_salary), 2) as total_payroll,
        COUNT(DISTINCT company_id) as companies,
        COUNT(DISTINCT department_id) as departments
    FROM dim_employee 
    WHERE is_current = true;
    """
    
    stats = postgres_hook.get_first(employee_stats_query)
    
    # Get top salary changes in last 24 hours
    salary_changes_query = """
    SELECT 
        de.first_name || ' ' || de.last_name as employee_name,
        c.company_name,
        fsc.previous_salary,
        fsc.new_salary,
        fsc.salary_change_amount,
        fsc.salary_change_percentage,
        fsc.effective_date
    FROM fact_salary_changes fsc
    JOIN dim_employee de ON fsc.employee_key = de.employee_key
    JOIN companies c ON de.company_id = c.company_id
    WHERE fsc.effective_date >= CURRENT_DATE - INTERVAL '24 hours'
    ORDER BY fsc.salary_change_amount DESC
    LIMIT 10;
    """
    
    recent_changes = postgres_hook.get_records(salary_changes_query)
    
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'employee_stats': {
            'total_employees': stats[0],
            'avg_salary': float(stats[1]),
            'total_payroll': float(stats[2]),
            'companies': stats[3],
            'departments': stats[4]
        },
        'recent_salary_changes': len(recent_changes),
        'top_changes': [
            {
                'employee': change[0],
                'company': change[1],
                'previous_salary': float(change[2]),
                'new_salary': float(change[3]),
                'increase': float(change[4]),
                'percentage': float(change[5]),
                'date': change[6].isoformat() if change[6] else None
            } for change in recent_changes
        ]
    }
    
    # Store in XCom for next tasks
    context['task_instance'].xcom_push(key='employee_metrics', value=metrics)
    logging.info(f"Extracted metrics for {stats[0]} employees")
    return metrics

def analyze_budget_impact(**context):
    """Analyze budget impact and generate recommendations"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get department budget utilization
    budget_query = """
    SELECT 
        d.department_name,
        d.budget_allocation,
        COUNT(de.employee_key) as employee_count,
        SUM(de.current_salary) as department_payroll,
        ROUND((SUM(de.current_salary) / d.budget_allocation) * 100, 2) as utilization_pct
    FROM dim_department d
    LEFT JOIN dim_employee de ON d.department_id = de.department_id AND de.is_current = true
    WHERE d.is_current = true
    GROUP BY d.department_name, d.budget_allocation
    ORDER BY utilization_pct DESC;
    """
    
    budget_analysis = postgres_hook.get_records(budget_query)
    
    # Identify over-budget departments
    over_budget = [dept for dept in budget_analysis if dept[4] > 100]
    
    # Generate recommendations
    recommendations = []
    for dept in over_budget[:5]:  # Top 5 over-budget departments
        recommendations.append({
            'department': dept[0],
            'budget_allocation': float(dept[1]),
            'current_payroll': float(dept[2]),
            'utilization_pct': float(dept[4]),
            'recommendation': f"Department exceeds budget by {dept[4] - 100:.1f}% - consider budget reallocation",
            'priority': 'HIGH' if dept[4] > 150 else 'MEDIUM'
        })
    
    analysis = {
        'timestamp': datetime.now().isoformat(),
        'total_departments': len(budget_analysis),
        'over_budget_departments': len(over_budget),
        'recommendations': recommendations,
        'avg_utilization': sum([dept[4] for dept in budget_analysis]) / len(budget_analysis)
    }
    
    context['task_instance'].xcom_push(key='budget_analysis', value=analysis)
    logging.info(f"Analyzed {len(budget_analysis)} departments, {len(over_budget)} over budget")
    return analysis

def generate_executive_report(**context):
    """Generate executive summary report"""
    # Get data from previous tasks
    employee_metrics = context['task_instance'].xcom_pull(task_ids='extract_employee_metrics', key='employee_metrics')
    budget_analysis = context['task_instance'].xcom_pull(task_ids='analyze_budget_impact', key='budget_analysis')
    
    # Create comprehensive executive report
    report = {
        'report_timestamp': datetime.now().isoformat(),
        'report_type': 'EXECUTIVE_SUMMARY',
        'period': 'HOURLY',
        'enterprise_overview': {
            'total_employees': employee_metrics['employee_stats']['total_employees'],
            'total_payroll_millions': round(employee_metrics['employee_stats']['total_payroll'] / 1000000, 2),
            'avg_salary': employee_metrics['employee_stats']['avg_salary'],
            'active_companies': employee_metrics['employee_stats']['companies'],
            'active_departments': employee_metrics['employee_stats']['departments']
        },
        'compensation_activity': {
            'recent_changes': employee_metrics['recent_salary_changes'],
            'top_increases': employee_metrics['top_changes'][:3],
            'total_increase_amount': sum([change['increase'] for change in employee_metrics['top_changes']])
        },
        'budget_health': {
            'total_departments': budget_analysis['total_departments'],
            'over_budget_count': budget_analysis['over_budget_departments'],
            'avg_utilization_pct': round(budget_analysis['avg_utilization'], 2),
            'high_priority_alerts': len([r for r in budget_analysis['recommendations'] if r['priority'] == 'HIGH'])
        },
        'key_recommendations': budget_analysis['recommendations'][:3],
        'next_review': (datetime.now() + timedelta(hours=1)).isoformat()
    }
    
    # Store report
    context['task_instance'].xcom_push(key='executive_report', value=report)
    logging.info(f"Generated executive report with {len(report['key_recommendations'])} recommendations")
    return report

def publish_to_kafka(**context):
    """Publish executive report to Kafka for real-time consumption"""
    import json
    from kafka import KafkaProducer
    
    try:
        # Get the executive report
        report = context['task_instance'].xcom_pull(task_ids='generate_executive_report', key='executive_report')
        
        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=['enterprise-kafka:9092'],
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
        )
        
        # Publish to executive reports topic
        producer.send('hr-executive-reports', value=report)
        producer.flush()
        producer.close()
        
        logging.info("Published executive report to Kafka topic: hr-executive-reports")
        return "SUCCESS"
        
    except Exception as e:
        logging.error(f"Failed to publish to Kafka: {str(e)}")
        return "FAILED"

def update_data_mesh_domains(**context):
    """Update Data Mesh domain data products"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Update HR Domain data product
    hr_domain_query = """
    INSERT INTO data_mesh_hr_domain_product 
    SELECT 
        CURRENT_TIMESTAMP as update_timestamp,
        'HR_ANALYTICS' as product_name,
        COUNT(*) as total_employees,
        AVG(current_salary) as avg_compensation,
        COUNT(DISTINCT department_id) as departments_active,
        'ACTIVE' as status
    FROM dim_employee 
    WHERE is_current = true;
    """
    
    # Update Finance Domain data product  
    finance_domain_query = """
    INSERT INTO data_mesh_finance_domain_product
    SELECT 
        CURRENT_TIMESTAMP as update_timestamp,
        'PAYROLL_ANALYTICS' as product_name,
        SUM(current_salary) as total_payroll,
        COUNT(*) as employee_count,
        AVG(current_salary) as avg_salary,
        'ACTIVE' as status
    FROM dim_employee 
    WHERE is_current = true;
    """
    
    try:
        postgres_hook.run(sql=hr_domain_query)
        postgres_hook.run(sql=finance_domain_query)
        logging.info("Updated Data Mesh domain data products")
        return "SUCCESS"
    except Exception as e:
        logging.error(f"Failed to update Data Mesh domains: {str(e)}")
        return "FAILED"

# Define task dependencies
extract_metrics_task = PythonOperator(
    task_id='extract_employee_metrics',
    python_callable=extract_employee_metrics,
    dag=enterprise_dag
)

analyze_budget_task = PythonOperator(
    task_id='analyze_budget_impact', 
    python_callable=analyze_budget_impact,
    dag=enterprise_dag
)

generate_report_task = PythonOperator(
    task_id='generate_executive_report',
    python_callable=generate_executive_report,
    dag=enterprise_dag
)

publish_kafka_task = PythonOperator(
    task_id='publish_to_kafka',
    python_callable=publish_to_kafka,
    dag=enterprise_dag
)

update_domains_task = PythonOperator(
    task_id='update_data_mesh_domains',
    python_callable=update_data_mesh_domains,
    dag=enterprise_dag
)

# Health check task
health_check_task = BashOperator(
    task_id='health_check_services',
    bash_command='''
    echo "Checking enterprise services health..."
    docker ps --format "table {{.Names}}\t{{.Status}}" | grep enterprise
    echo "Services health check complete"
    ''',
    dag=enterprise_dag
)

# Set task dependencies - proper orchestration workflow
health_check_task >> [extract_metrics_task, analyze_budget_task]
[extract_metrics_task, analyze_budget_task] >> generate_report_task
generate_report_task >> [publish_kafka_task, update_domains_task]

# Additional real-time monitoring DAG
monitoring_dag = DAG(
    'enterprise_hr_monitoring',
    default_args=default_args,
    description='Real-time Enterprise HR Monitoring and Alerting',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    tags=['monitoring', 'alerts', 'real-time']
)

def monitor_salary_anomalies(**context):
    """Monitor for salary anomalies and generate alerts"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check for unusual salary increases
    anomaly_query = """
    WITH salary_stats AS (
        SELECT 
            AVG(salary_change_percentage) as avg_increase,
            STDDEV(salary_change_percentage) as std_increase
        FROM fact_salary_changes 
        WHERE effective_date >= CURRENT_DATE - INTERVAL '7 days'
    )
    SELECT 
        de.first_name || ' ' || de.last_name as employee_name,
        c.company_name,
        fsc.salary_change_percentage,
        fsc.salary_change_amount,
        fsc.effective_date
    FROM fact_salary_changes fsc
    JOIN dim_employee de ON fsc.employee_key = de.employee_key
    JOIN companies c ON de.company_id = c.company_id
    CROSS JOIN salary_stats ss
    WHERE fsc.effective_date >= CURRENT_DATE - INTERVAL '1 hour'
    AND fsc.salary_change_percentage > (ss.avg_increase + 2 * ss.std_increase)
    ORDER BY fsc.salary_change_percentage DESC;
    """
    
    anomalies = postgres_hook.get_records(anomaly_query)
    
    if anomalies:
        alert = {
            'alert_type': 'SALARY_ANOMALY',
            'timestamp': datetime.now().isoformat(),
            'anomaly_count': len(anomalies),
            'details': [
                {
                    'employee': anom[0],
                    'company': anom[1], 
                    'increase_pct': float(anom[2]),
                    'increase_amount': float(anom[3]),
                    'date': anom[4].isoformat()
                } for anom in anomalies
            ]
        }
        
        context['task_instance'].xcom_push(key='salary_anomalies', value=alert)
        logging.warning(f"Detected {len(anomalies)} salary anomalies")
    
    return len(anomalies)

monitor_anomalies_task = PythonOperator(
    task_id='monitor_salary_anomalies',
    python_callable=monitor_salary_anomalies,
    dag=monitoring_dag
)