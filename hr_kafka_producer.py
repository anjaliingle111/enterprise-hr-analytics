from kafka import KafkaProducer
import psycopg2
import json
import time
from datetime import datetime

def hr_kafka_producer():
    # Connect to Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='hrdb',
        user='hr_user', 
        password='hr_pass'
    )
    
    print('🚀 HR KAFKA PRODUCER STARTED')
    print('📡 Sending real-time HR data to Kafka topics')
    print('=' * 50)
    
    cycle = 1
    while True:
        cursor = conn.cursor()
        
        print(f'\n📊 KAFKA CYCLE #{cycle} - {datetime.now().strftime("%H:%M:%S")}')
        
        # Update some employee salaries
        cursor.execute("""
            UPDATE employees 
            SET current_salary = current_salary * (1.02 + RANDOM() * 0.06),
                updated_at = NOW()
            WHERE employee_id IN (
                SELECT employee_id FROM employees 
                ORDER BY RANDOM() LIMIT 3
            )
            RETURNING employee_id, first_name, last_name, current_salary, updated_at
        """)
        
        updates = cursor.fetchall()
        
        # Send updates to Kafka
        for emp_id, first, last, salary, updated in updates:
            salary_event = {
                'event_type': 'salary_update',
                'employee_id': emp_id,
                'employee_name': f'{first} {last}',
                'new_salary': float(salary),
                'timestamp': updated.isoformat(),
                'cycle': cycle
            }
            
            # Send to Kafka topic
            producer.send('hr-salary-updates', value=salary_event)
            print(f'📡 SENT TO KAFKA: {first} {last} - ${salary:,.0f}')
        
        # Send business metrics to another topic
        cursor.execute("""
            SELECT COUNT(*), AVG(current_salary)::INT, SUM(current_salary)::BIGINT
            FROM employees WHERE employment_status = 'active'
        """)
        
        total, avg_sal, payroll = cursor.fetchone()
        
        metrics_event = {
            'event_type': 'business_metrics',
            'total_employees': total,
            'average_salary': avg_sal,
            'total_payroll': int(payroll),
            'timestamp': datetime.now().isoformat(),
            'cycle': cycle
        }
        
        producer.send('hr-employee-events', value=metrics_event)
        print(f'📊 SENT METRICS: {total:,} employees, ${avg_sal:,} avg, ${payroll:,} payroll')
        
        conn.commit()
        cursor.close()
        
        # Ensure all messages are sent
        producer.flush()
        
        print(f'✅ Cycle #{cycle} - Messages sent to Kafka successfully!')
        cycle += 1
        time.sleep(20)

if __name__ == '__main__':
    try:
        hr_kafka_producer()
    except KeyboardInterrupt:
        print('\n🛑 Kafka producer stopped')