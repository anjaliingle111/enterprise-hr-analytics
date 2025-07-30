#!/usr/bin/env python3
"""
TechCorp Solutions Inc - Power BI API Connector
Enterprise HR Analytics REST API for real-time Power BI integration
"""

from flask import Flask, jsonify
import psycopg2
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'hr_user',
    'password': 'hr_pass',
    'database': 'hrdb'
}

def get_db_connection():
    """Get database connection with error handling"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

@app.route('/')
def home():
    """API status and available endpoints"""
    return jsonify({
        "message": "TechCorp Solutions Inc - HR Analytics API",
        "status": "running",
        "company": "TechCorp Solutions Inc",
        "employees": 1000,
        "departments": 5,
        "last_updated": datetime.now().isoformat(),
        "endpoints": [
            {"path": "/api/techcorp/departments", "description": "Department overview with headcount and salaries"},
            {"path": "/api/techcorp/performance", "description": "Performance reviews and ratings by department"},
            {"path": "/api/techcorp/engagement", "description": "Employee engagement and satisfaction metrics"},
            {"path": "/api/techcorp/company-kpis", "description": "Company KPIs in Power BI friendly format"}
        ]
    })

@app.route('/api/techcorp/departments')
def departments():
    """TechCorp department overview - headcount, salaries, budgets"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify([{"error": "Database connection failed"}])
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                c.company_name as department,
                c.parent_company,
                COUNT(e.employee_id) as employee_count,
                CAST(AVG(e.current_salary) AS INTEGER) as avg_salary,
                CAST(SUM(e.current_salary) AS INTEGER) as total_payroll,
                CAST(MIN(e.current_salary) AS INTEGER) as min_salary,
                CAST(MAX(e.current_salary) AS INTEGER) as max_salary,
                COUNT(CASE WHEN e.current_salary > 200000 THEN 1 END) as executives,
                COUNT(CASE WHEN e.current_salary < 75000 THEN 1 END) as entry_level
            FROM employees e
            JOIN companies c ON e.company_id = c.company_id
            WHERE e.employment_status = 'active'
            GROUP BY c.company_id, c.company_name, c.parent_company
            ORDER BY total_payroll DESC
        """)
        
        data = []
        for row in cursor.fetchall():
            data.append({
                'department': row[0],
                'parent_company': row[1],
                'employee_count': row[2],
                'avg_salary': row[3],
                'total_payroll': row[4],
                'min_salary': row[5],
                'max_salary': row[6],
                'executives': row[7],
                'entry_level': row[8]
            })
        
        conn.close()
        logger.info(f"Departments endpoint: Returned {len(data)} departments")
        return jsonify(data)
        
    except Exception as e:
        logger.error(f"Departments endpoint error: {e}")
        return jsonify([{"error": str(e), "endpoint": "departments"}])

@app.route('/api/techcorp/performance')
def performance():
    """Performance reviews and ratings analysis by department"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify([{"error": "Database connection failed"}])
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                c.company_name as department,
                COUNT(pr.review_id) as total_reviews,
                CAST(AVG(pr.overall_rating) AS DECIMAL(10,2)) as avg_overall_rating,
                COUNT(CASE WHEN pr.overall_rating >= 4.0 THEN 1 END) as high_performers,
                COUNT(CASE WHEN pr.overall_rating >= 3.0 AND pr.overall_rating < 4.0 THEN 1 END) as average_performers,
                COUNT(CASE WHEN pr.overall_rating < 3.0 THEN 1 END) as low_performers
            FROM performance_reviews pr
            JOIN employees e ON pr.employee_id = e.employee_id
            JOIN companies c ON e.company_id = c.company_id
            WHERE pr.review_status = 'completed'
            GROUP BY c.company_name
            ORDER BY avg_overall_rating DESC
        """)
        
        data = []
        for row in cursor.fetchall():
            total_reviews = row[1]
            high_perf_pct = (row[3] / total_reviews * 100) if total_reviews > 0 else 0
            
            data.append({
                'department': row[0],
                'total_reviews': total_reviews,
                'avg_overall_rating': float(row[2]) if row[2] is not None else 0,
                'high_performers': row[3],
                'average_performers': row[4],
                'low_performers': row[5],
                'high_performer_percentage': round(high_perf_pct, 1
