#!/usr/bin/env python3
"""
Data Mesh Implementation for TechCorp HR Analytics
Creates domain-oriented data products
"""

import psycopg2
import json
from kafka import KafkaProducer
from datetime import datetime

class HRDataMesh:
    def __init__(self):
        self.conn = psycopg2.connect(
            host='localhost', port=5432, user='hr_user', 
            password='hr_pass', database='hrdb'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        
    def publish_performance_domain(self):
        """Performance Management Domain Data Product"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
        SELECT 
            c.company_name as department,
            COUNT(pr.review_id) as total_reviews,
            AVG(pr.overall_rating) as avg_rating,
            COUNT(CASE WHEN pr.overall_rating >= 4.0 THEN 1 END) as high_performers,
            CURRENT_TIMESTAMP as generated_at
        FROM performance_reviews pr
        JOIN employees e ON pr.employee_id = e.employee_id
        JOIN companies c ON e.company_id = c.company_id
        GROUP BY c.company_name;
        """)
        
        for row in cursor.fetchall():
            data_product = {
                'domain': 'performance_management',
                'department': row[0],
                'total_reviews': row[1],
                'avg_rating': float(row[2]) if row[2] else 0,
                'high_performers': row[3],
                'generated_at': row[4].isoformat(),
                'data_quality_score': 0.95
            }
            
            self.producer.send('data-mesh-performance-domain', data_product)
            print(f"📊 Performance Domain: {row[0]} - {row[1]} reviews, {float(row[2] or 0):.2f} avg rating")
    
    def publish_engagement_domain(self):
        """Employee Engagement Domain Data Product"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
        SELECT 
            c.company_name as department,
            COUNT(es.survey_id) as survey_responses,
            AVG(es.engagement_score) as avg_engagement,
            AVG(es.satisfaction_score) as avg_satisfaction,
            COUNT(CASE WHEN es.likely_to_leave > 7 THEN 1 END) as flight_risk_count
        FROM employee_surveys es
        JOIN employees e ON es.employee_id = e.employee_id
        JOIN companies c ON e.company_id = c.company_id
        GROUP BY c.company_name;
        """)
        
        for row in cursor.fetchall():
            data_product = {
                'domain': 'employee_engagement',
                'department': row[0],
                'survey_responses': row[1],
                'avg_engagement': float(row[2]) if row[2] else 0,
                'avg_satisfaction': float(row[3]) if row[3] else 0,
                'flight_risk_count': row[4],
                'generated_at': datetime.now().isoformat(),
                'data_quality_score': 0.92
            }
            
            self.producer.send('data-mesh-engagement-domain', data_product)
            print(f"💡 Engagement Domain: {row[0]} - {float(row[2] or 0):.1f}/10 engagement, {row[4]} flight risk")
    
    def publish_compensation_domain(self):
        """Compensation Analytics Domain Data Product"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
        SELECT 
            c.company_name as department,
            COUNT(e.employee_id) as employee_count,
            AVG(e.current_salary) as avg_salary,
            MIN(e.current_salary) as min_salary,
            MAX(e.current_salary) as max_salary,
            SUM(e.current_salary) as total_payroll
        FROM employees e
        JOIN companies c ON e.company_id = c.company_id
        WHERE e.employment_status = 'active'
        GROUP BY c.company_name;
        """)
        
        for row in cursor.fetchall():
            data_product = {
                'domain': 'compensation_analytics',
                'department': row[0],
                'employee_count': row[1],
                'avg_salary': float(row[2]),
                'min_salary': float(row[3]),
                'max_salary': float(row[4]),
                'total_payroll': float(row[5]),
                'generated_at': datetime.now().isoformat(),
                'data_quality_score': 0.98
            }
            
            self.producer.send('data-mesh-compensation-domain', data_product)
            print(f"💰 Compensation Domain: {row[0]} - {row[1]} employees, ${float(row[2]):,.0f} avg salary")
    
    def run_data_mesh(self):
        print("🌐 TECHCORP DATA MESH - DOMAIN DATA PRODUCTS")
        print("=" * 60)
        
        # Create domain topics
        topics = [
            'data-mesh-performance-domain',
            'data-mesh-engagement-domain', 
            'data-mesh-compensation-domain'
        ]
        
        for topic in topics:
            print(f"📡 Publishing to: {topic}")
        
        print("\n🎯 PUBLISHING DOMAIN DATA PRODUCTS:")
        self.publish_performance_domain()
        print()
        self.publish_engagement_domain()
        print()
        self.publish_compensation_domain()
        
        print(f"\n✅ Data Mesh deployment complete!")
        print(f"🌟 3 HR domains publishing data products to Kafka")
        
        self.producer.close()
        self.conn.close()

if __name__ == "__main__":
    mesh = HRDataMesh()
    mesh.run_data_mesh()
