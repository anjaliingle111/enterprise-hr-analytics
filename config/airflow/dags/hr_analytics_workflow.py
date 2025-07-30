from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import psycopg2
import pandas as pd

default_args = {
    'owner': 'hr_analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hr_analytics_daily_workflow',
    default_args=default_args,
    description='Daily HR Analytics Processing',
    schedule_interval=timedelta(hours=6),
    catchup=False
)

def generate_performance_summary():
    """Generate daily performance summary"""
    try:
        conn = psycopg2.connect(
            host='enterprise-postgres', port=5432, 
            user='hr_user', password='hr_pass', database='hrdb'
        )
        
        query = """
        SELECT 
            c.company_name as department,
            COUNT(pr.review_id) as reviews_today,
            AVG(pr.overall_rating) as avg_rating,
            COUNT(CASE WHEN pr.overall_rating >= 4.0 THEN 1 END) as high_performers
        FROM performance_reviews pr
        JOIN employees e ON pr.employee_id = e.employee_id
        JOIN companies c ON e.company_id = c.company_id
        WHERE pr.created_at >= CURRENT_DATE - INTERVAL '1 day'
        GROUP BY c.company_name;
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"📊 Performance Summary Generated: {len(results)} departments analyzed")
        for dept, reviews, rating, performers in results:
            print(f"  • {dept}: {reviews} reviews, {rating:.2f} avg rating, {performers} high performers")
        
        conn.close()
        return {"status": "success", "departments": len(results)}
    except Exception as e:
        print(f"❌ Error: {e}")
        return {"status": "error", "message": str(e)}

def generate_engagement_report():
    """Generate employee engagement insights"""
    try:
        conn = psycopg2.connect(
            host='enterprise-postgres', port=5432,
            user='hr_user', password='hr_pass', database='hrdb'
        )
        
        query = """
        SELECT 
            AVG(engagement_score) as avg_engagement,
            AVG(satisfaction_score) as avg_satisfaction,
            COUNT(CASE WHEN likely_to_leave > 7 THEN 1 END) as flight_risk_count,
            COUNT(*) as total_responses
        FROM employee_surveys
        WHERE survey_date >= CURRENT_DATE - INTERVAL '30 days';
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            engagement, satisfaction, flight_risk, total = result
            print(f"💡 Engagement Report:")
            print(f"  • Average Engagement: {engagement:.1f}/10")
            print(f"  • Average Satisfaction: {satisfaction:.1f}/10") 
            print(f"  • Flight Risk Employees: {flight_risk}")
            print(f"  • Total Survey Responses: {total}")
        
        conn.close()
        return {"status": "success", "flight_risk": flight_risk}
    except Exception as e:
        print(f"❌ Error: {e}")
        return {"status": "error", "message": str(e)}

def analyze_compensation_trends():
    """Analyze compensation trends"""
    try:
        conn = psycopg2.connect(
            host='enterprise-postgres', port=5432,
            user='hr_user', password='hr_pass', database='hrdb'
        )
        
        query = """
        SELECT 
            c.company_name as department,
            COUNT(e.employee_id) as employee_count,
            AVG(e.current_salary) as avg_salary,
            MIN(e.current_salary) as min_salary,
            MAX(e.current_salary) as max_salary
        FROM employees e
        JOIN companies c ON e.company_id = c.company_id
        WHERE e.employment_status = 'active'
        GROUP BY c.company_name
        ORDER BY avg_salary DESC;
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"💰 Compensation Analysis:")
        for dept, count, avg_sal, min_sal, max_sal in results:
            print(f"  • {dept}: {count} employees, ${avg_sal:,.0f} avg salary")
        
        conn.close()
        return {"status": "success", "departments_analyzed": len(results)}
    except Exception as e:
        print(f"❌ Error: {e}")
        return {"status": "error", "message": str(e)}

# Define tasks
performance_task = PythonOperator(
    task_id='generate_performance_summary',
    python_callable=generate_performance_summary,
    dag=dag
)

engagement_task = PythonOperator(
    task_id='generate_engagement_report', 
    python_callable=generate_engagement_report,
    dag=dag
)

compensation_task = PythonOperator(
    task_id='analyze_compensation_trends',
    python_callable=analyze_compensation_trends,
    dag=dag
)

# Set task dependencies
performance_task >> engagement_task >> compensation_task
