from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2

default_args = {
    'owner': 'hr_analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 25),
    'email_on_failure': False,
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
    try:
        conn = psycopg2.connect(
            host='enterprise-postgres', port=5432, 
            user='hr_user', password='hr_pass', database='hrdb'
        )
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT 
            c.company_name as department,
            COUNT(pr.review_id) as reviews,
            AVG(pr.overall_rating) as avg_rating
        FROM performance_reviews pr
        JOIN employees e ON pr.employee_id = e.employee_id
        JOIN companies c ON e.company_id = c.company_id
        GROUP BY c.company_name;
        """)
        
        results = cursor.fetchall()
        print(f"📊 Performance Summary: {len(results)} departments analyzed")
        
        for dept, reviews, rating in results:
            print(f"  • {dept}: {reviews} reviews, {rating:.2f} avg rating")
        
        conn.close()
        return {"status": "success"}
    except Exception as e:
        print(f"❌ Error: {e}")
        return {"status": "error"}

def generate_engagement_report():
    try:
        conn = psycopg2.connect(
            host='enterprise-postgres', port=5432,
            user='hr_user', password='hr_pass', database='hrdb'
        )
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT 
            AVG(engagement_score) as avg_engagement,
            COUNT(CASE WHEN likely_to_leave > 7 THEN 1 END) as flight_risk
        FROM employee_surveys;
        """)
        
        result = cursor.fetchone()
        if result:
            engagement, flight_risk = result
            print(f"💡 Engagement: {engagement:.1f}/10, Flight Risk: {flight_risk} employees")
        
        conn.close()
        return {"status": "success"}
    except Exception as e:
        print(f"❌ Error: {e}")
        return {"status": "error"}

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

# Set dependencies
performance_task >> engagement_task
