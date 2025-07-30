#!/usr/bin/env python3
"""
TechCorp Solutions Inc - Complete HR Analytics API
All endpoints for Power BI integration including individual employee records
"""

from flask import Flask, jsonify
import psycopg2
from datetime import datetime

app = Flask(__name__)

def get_db():
    return psycopg2.connect(host='localhost', port=5432, user='hr_user', password='hr_pass', database='hrdb')

@app.route('/')
def home():
    return jsonify({
        "message": "TechCorp Solutions Inc - Complete HR Analytics API",
        "status": "running",
        "company": "TechCorp Solutions Inc",
        "employees": 1000,
        "departments": 5,
        "last_updated": datetime.now().isoformat(),
        "endpoints": [
            {"path": "/api/techcorp/departments", "description": "Department overview with headcount and salaries"},
            {"path": "/api/techcorp/company-kpis", "description": "Company KPIs and summary metrics"},
            {"path": "/api/techcorp/performance", "description": "Performance reviews and ratings by department"},
            {"path": "/api/techcorp/engagement", "description": "Employee engagement and satisfaction metrics"},
            {"path": "/api/techcorp/skills", "description": "Skills gap analysis by department"},
            {"path": "/api/techcorp/leave", "description": "Leave utilization and PTO analytics"},
            {"path": "/api/techcorp/employees", "description": "Individual employee records (1,000 employees)"}
        ]
    })

@app.route('/api/techcorp/departments')
def departments():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.company_name, c.parent_company, COUNT(e.employee_id), 
                   CAST(AVG(e.current_salary) AS INTEGER), CAST(SUM(e.current_salary) AS INTEGER),
                   CAST(MIN(e.current_salary) AS INTEGER), CAST(MAX(e.current_salary) AS INTEGER),
                   COUNT(CASE WHEN e.current_salary > 200000 THEN 1 END),
                   COUNT(CASE WHEN e.current_salary < 75000 THEN 1 END)
            FROM employees e JOIN companies c ON e.company_id = c.company_id
            WHERE e.employment_status = 'active'
            GROUP BY c.company_id, c.company_name, c.parent_company
            ORDER BY SUM(e.current_salary) DESC
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
        return jsonify(data)
    except Exception as e:
        return jsonify([{"error": str(e), "endpoint": "departments"}])

@app.route('/api/techcorp/company-kpis')
def company_kpis():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(e.employee_id), CAST(SUM(e.current_salary)/1000000.0 AS DECIMAL(10,2)),
                   CAST(AVG(e.current_salary) AS INTEGER), COUNT(DISTINCT c.company_id),
                   COUNT(CASE WHEN e.current_salary > 200000 THEN 1 END),
                   COUNT(CASE WHEN e.current_salary > 150000 THEN 1 END),
                   COUNT(CASE WHEN e.current_salary < 75000 THEN 1 END)
            FROM employees e JOIN companies c ON e.company_id = c.company_id
            WHERE e.employment_status = 'active'
        """)
        
        row = cursor.fetchone()
        data = [
            {'kpi_name': 'Total Employees', 'kpi_value': row[0], 'category': 'Workforce'},
            {'kpi_name': 'Total Payroll (Millions)', 'kpi_value': float(row[1]), 'category': 'Financial'},
            {'kpi_name': 'Average Salary', 'kpi_value': row[2], 'category': 'Financial'},
            {'kpi_name': 'Departments', 'kpi_value': row[3], 'category': 'Structure'},
            {'kpi_name': 'Executives', 'kpi_value': row[4], 'category': 'Workforce'},
            {'kpi_name': 'Senior Staff', 'kpi_value': row[5], 'category': 'Workforce'},
            {'kpi_name': 'Entry Level', 'kpi_value': row[6], 'category': 'Workforce'}
        ]
        
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([{"error": str(e), "endpoint": "company-kpis"}])

@app.route('/api/techcorp/performance')
def performance():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.company_name, COUNT(pr.review_id), CAST(AVG(pr.overall_rating) AS DECIMAL(10,2)),
                   CAST(AVG(pr.goals_rating) AS DECIMAL(10,2)), CAST(AVG(pr.competencies_rating) AS DECIMAL(10,2)),
                   COUNT(CASE WHEN pr.overall_rating >= 4.0 THEN 1 END),
                   COUNT(CASE WHEN pr.overall_rating >= 3.0 AND pr.overall_rating < 4.0 THEN 1 END),
                   COUNT(CASE WHEN pr.overall_rating < 3.0 THEN 1 END)
            FROM performance_reviews pr 
            JOIN employees e ON pr.employee_id = e.employee_id
            JOIN companies c ON e.company_id = c.company_id
            GROUP BY c.company_name
            ORDER BY AVG(pr.overall_rating) DESC
        """)
        
        data = []
        for row in cursor.fetchall():
            total_reviews = row[1]
            high_perf_pct = (row[5] / total_reviews * 100) if total_reviews > 0 else 0
            
            data.append({
                'department': row[0],
                'total_reviews': total_reviews,
                'avg_overall_rating': float(row[2]) if row[2] else 0,
                'avg_goals_rating': float(row[3]) if row[3] else 0,
                'avg_competencies_rating': float(row[4]) if row[4] else 0,
                'high_performers': row[5],
                'average_performers': row[6],
                'low_performers': row[7],
                'high_performer_percentage': round(high_perf_pct, 1)
            })
        
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([{"error": str(e), "endpoint": "performance"}])

@app.route('/api/techcorp/engagement')
def engagement():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.company_name, COUNT(es.survey_id), CAST(AVG(es.engagement_score) AS DECIMAL(10,1)),
                   CAST(AVG(es.satisfaction_score) AS DECIMAL(10,1)), CAST(AVG(es.work_life_balance) AS DECIMAL(10,1)),
                   CAST(AVG(es.career_development) AS DECIMAL(10,1)), CAST(AVG(es.compensation_satisfaction) AS DECIMAL(10,1)),
                   COUNT(CASE WHEN es.likely_to_leave > 7 THEN 1 END),
                   COUNT(CASE WHEN es.would_recommend_company THEN 1 END)
            FROM employee_surveys es
            JOIN employees e ON es.employee_id = e.employee_id
            JOIN companies c ON e.company_id = c.company_id
            GROUP BY c.company_name
            ORDER BY AVG(es.engagement_score) DESC
        """)
        
        data = []
        for row in cursor.fetchall():
            survey_responses = row[1]
            recommend_rate = (row[8] / survey_responses * 100) if survey_responses > 0 else 0
            
            data.append({
                'department': row[0],
                'survey_responses': survey_responses,
                'avg_engagement': float(row[2]) if row[2] else 0,
                'avg_satisfaction': float(row[3]) if row[3] else 0,
                'avg_work_life_balance': float(row[4]) if row[4] else 0,
                'avg_career_development': float(row[5]) if row[5] else 0,
                'avg_compensation_satisfaction': float(row[6]) if row[6] else 0,
                'flight_risk_count': row[7],
                'would_recommend_count': row[8],
                'recommendation_rate': round(recommend_rate, 1)
            })
        
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([{"error": str(e), "endpoint": "engagement"}])

@app.route('/api/techcorp/skills')
def skills():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.company_name, sm.skill_category, COUNT(*), 
                   CAST(AVG(sm.proficiency_level) AS DECIMAL(10,1)),
                   CAST(AVG(sm.target_level) AS DECIMAL(10,1)),
                   CAST(AVG(sm.target_level - sm.proficiency_level) AS DECIMAL(10,1))
            FROM skills_matrix sm
            JOIN employees e ON sm.employee_id = e.employee_id
            JOIN companies c ON e.company_id = c.company_id
            GROUP BY c.company_name, sm.skill_category
            ORDER BY AVG(sm.target_level - sm.proficiency_level) DESC
        """)
        
        data = []
        for row in cursor.fetchall():
            skill_gap = float(row[5]) if row[5] else 0
            
            if skill_gap > 1.5:
                priority = 'High Priority'
            elif skill_gap > 0.8:
                priority = 'Medium Priority'
            else:
                priority = 'Low Priority'
            
            data.append({
                'department': row[0],
                'skill_category': row[1],
                'employees_assessed': row[2],
                'avg_current_level': float(row[3]) if row[3] else 0,
                'avg_target_level': float(row[4]) if row[4] else 0,
                'avg_skill_gap': skill_gap,
                'gap_priority': priority
            })
        
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([{"error": str(e), "endpoint": "skills"}])

@app.route('/api/techcorp/leave')
def leave():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.company_name, lb.leave_type, COUNT(*), 
                   CAST(AVG(lb.annual_allocation) AS DECIMAL(10,1)),
                   CAST(AVG(lb.used_days) AS DECIMAL(10,1)),
                   CAST(AVG(lb.remaining_days) AS DECIMAL(10,1))
            FROM leave_balances lb
            JOIN employees e ON lb.employee_id = e.employee_id
            JOIN companies c ON e.company_id = c.company_id
            WHERE lb.year = 2024
            GROUP BY c.company_name, lb.leave_type
            ORDER BY c.company_name, lb.leave_type
        """)
        
        data = []
        for row in cursor.fetchall():
            avg_allocation = float(row[3]) if row[3] else 0
            avg_used = float(row[4]) if row[4] else 0
            utilization_rate = (avg_used / avg_allocation * 100) if avg_allocation > 0 else 0
            
            data.append({
                'department': row[0],
                'leave_type': row[1],
                'employee_count': row[2],
                'avg_allocation': avg_allocation,
                'avg_used': avg_used,
                'avg_remaining': float(row[5]) if row[5] else 0,
                'utilization_rate': round(utilization_rate, 1)
            })
        
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([{"error": str(e), "endpoint": "leave"}])

@app.route('/api/techcorp/employees')
def employees():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                e.employee_id,
                e.first_name,
                e.last_name,
                e.email,
                c.company_name as department,
                COALESCE(jc.job_title, 'Staff') as job_title,
                e.current_salary,
                e.hire_date,
                e.employment_status,
                e.employment_type,
                EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.hire_date)) as years_tenure,
                CASE 
                    WHEN e.current_salary > 200000 THEN 'Executive'
                    WHEN e.current_salary > 150000 THEN 'Senior'
                    WHEN e.current_salary > 100000 THEN 'Mid-Level'
                    WHEN e.current_salary > 75000 THEN 'Junior+'
                    ELSE 'Entry Level'
                END as salary_band
            FROM employees e
            JOIN companies c ON e.company_id = c.company_id
            LEFT JOIN job_codes jc ON e.job_code_id = jc.job_code_id
            WHERE e.employment_status = 'active'
            ORDER BY e.current_salary DESC
        """)
        
        data = []
        for row in cursor.fetchall():
            data.append({
                'employee_id': row[0],
                'first_name': row[1],
                'last_name': row[2],
                'full_name': f"{row[1]} {row[2]}",
                'email': row[3],
                'department': row[4],
                'job_title': row[5],
                'current_salary': int(row[6]),
                'hire_date': str(row[7]) if row[7] else None,
                'employment_status': row[8],
                'employment_type': row[9],
                'years_tenure': int(row[10]) if row[10] else 0,
                'salary_band': row[11]
            })
        
        conn.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([{"error": str(e), "endpoint": "employees"}])

if __name__ == '__main__':
    print("🚀 TechCorp Solutions Inc - Complete HR Analytics API")
    print("=" * 70)
    print("📊 All 7 endpoints available for Power BI")
    print("💼 Company: TechCorp Solutions Inc")
    print("👥 Employees: 1,000 across 5 departments")
    print("💰 Annual Payroll: $138.3M")
    print("=" * 70)
    print("🌐 API Endpoints:")
    print("   • /api/techcorp/departments      - Department overview")
    print("   • /api/techcorp/company-kpis     - Company KPIs")
    print("   • /api/techcorp/performance      - Performance analytics")  
    print("   • /api/techcorp/engagement       - Employee engagement")
    print("   • /api/techcorp/skills           - Skills gap analysis")
    print("   • /api/techcorp/leave            - Leave utilization")
    print("   • /api/techcorp/employees        - Individual employee records")
    print("=" * 70)
    print("🚀 Starting complete API server on port 8000...")
    
    app.run(host='0.0.0.0', port=8000, debug=True)
