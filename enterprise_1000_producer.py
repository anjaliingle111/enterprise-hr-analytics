import json
import time
import random
from datetime import datetime
import psycopg2
from kafka import KafkaProducer

class Enterprise1000StreamingPlatform:
    def __init__(self):
        print("🏢 INITIALIZING ENTERPRISE 1000-EMPLOYEE STREAMING PLATFORM")
        print("💰 Lambda Architecture + CDC + Data Mesh")
        print("🚀 Processing $150M+ Annual Payroll")
        print("="*80)
        
        # Database connection
        self.conn = psycopg2.connect(
            host='enterprise-postgres',
            database='hrdb',
            user='hr_user',
            password='hr_pass'
        )
        
        # Kafka producer (with fallback handling)
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['enterprise-kafka:9092'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                request_timeout_ms=5000,
                retry_backoff_ms=1000,
                max_block_ms=5000
            )
            self.kafka_enabled = True
            print("✅ Kafka: Connected successfully")
        except Exception as e:
            print(f"⚠️  Kafka: Connection failed ({e}) - Running in database-only mode")
            self.kafka_enabled = False
        
        self.cursor = self.conn.cursor()
        
        # Get workforce summary
        self.cursor.execute("""
            SELECT COUNT(*), 
                   ROUND(SUM(current_salary)/1000000.0, 1) as payroll_millions,
                   ROUND(AVG(current_salary), 0) as avg_salary
            FROM dim_employee WHERE is_current = true
        """)
        count, payroll, avg = self.cursor.fetchone()
        print(f"✅ PostgreSQL: Connected ({count:,} employees, ${payroll}M payroll)")
        print("✅ Enterprise Platform: Ready for streaming")
        
    def get_enterprise_sample(self, sample_size=20):
        """Get diverse sample across companies and salary ranges"""
        self.cursor.execute("""
            WITH ranked_employees AS (
                SELECT de.employee_key, de.first_name, de.last_name, 
                       de.current_salary, c.company_name, jc.job_title, 
                       d.department_name,
                       ROW_NUMBER() OVER (PARTITION BY de.company_id 
                                         ORDER BY RANDOM()) as rn
                FROM dim_employee de
                JOIN companies c ON de.company_id = c.company_id
                JOIN job_codes jc ON de.job_code_id = jc.job_code_id
                JOIN dim_department d ON de.department_id = d.department_id
                WHERE de.is_current = true
            )
            SELECT employee_key, first_name, last_name, current_salary, 
                   company_name, job_title, department_name
            FROM ranked_employees 
            WHERE rn <= 4  -- 4 from each company = 20 total
            ORDER BY current_salary DESC
        """)
        return self.cursor.fetchall()
    
    def generate_executive_updates(self, employees):
        """Generate realistic compensation updates"""
        updates = []
        total_increase = 0
        
        for emp_key, fname, lname, salary, company, title, dept in employees:
            # Market-driven increase (3-15% based on salary level)
            if float(salary) > 250000:  # Executive level
                increase_pct = random.uniform(0.08, 0.15)
            elif float(salary) > 150000:  # Senior level  
                increase_pct = random.uniform(0.06, 0.12)
            else:  # Mid-level
                increase_pct = random.uniform(0.03, 0.09)
            
            salary_float = float(salary)
            new_salary = int(salary_float * (1 + increase_pct))
            increase = new_salary - salary_float
            total_increase += increase
            
            update = {
                'event_type': 'executive_compensation_update',
                'employee_key': emp_key,
                'employee_name': f"{fname} {lname}",
                'company': company,
                'job_title': title,
                'department': dept,
                'previous_salary': salary_float,
                'new_salary': new_salary,
                'increase_amount': increase,
                'increase_percentage': increase_pct,
                'timestamp': datetime.now().isoformat(),
                'market_trend': 'aggressive' if increase_pct > 0.10 else 'steady'
            }
            updates.append(update)
        
        return updates, total_increase
    
    def generate_enterprise_intelligence(self, updates, total_increase):
        """Generate business intelligence summary"""
        companies_affected = len(set(u['company'] for u in updates))
        avg_increase = total_increase / len(updates) if updates else 0
        high_impact_updates = [u for u in updates if u['increase_percentage'] > 0.10]
        
        intelligence = {
            'event_type': 'enterprise_business_intelligence',
            'cycle_timestamp': datetime.now().isoformat(),
            'employees_updated': len(updates),
            'companies_affected': companies_affected,
            'total_increase_amount': total_increase,
            'average_increase': avg_increase,
            'projected_annual_impact': total_increase * 12,
            'high_impact_updates': len(high_impact_updates),
            'market_trend_classification': 'aggressive_growth' if avg_increase > 15000 else 'steady_growth',
            'budget_impact_percentage': (total_increase / 150000000) * 100,  # Against $150M payroll
            'strategic_recommendation': self.get_strategic_recommendation(avg_increase, total_increase)
        }
        
        return intelligence
    
    def get_strategic_recommendation(self, avg_increase, total_increase):
        """Generate strategic recommendations"""
        if total_increase > 200000:
            return "Consider budget reallocation - high compensation pressure detected"
        elif avg_increase > 20000:
            return "Monitor competitor activity - above-market increases observed"
        else:
            return "Compensation trends within acceptable range - continue monitoring"
    
    def generate_competitive_intelligence(self):
        """Simulate competitive market intelligence"""
        competitors = ['Google', 'Meta', 'Microsoft', 'Amazon', 'Apple', 'Netflix', 'Uber', 'Airbnb']
        actions = ['salary_increase', 'equity_program', 'hiring_spree', 'layoffs', 'office_expansion']
        
        competitor_moves = []
        for _ in range(random.randint(2, 4)):
            competitor_moves.append({
                'competitor': random.choice(competitors),
                'action': random.choice(actions),
                'impact_percentage': random.uniform(5.0, 20.0),
                'confidence_score': random.uniform(0.7, 0.95),
                'timestamp': datetime.now().isoformat()
            })
        
        return {
            'event_type': 'competitive_market_intelligence',
            'market_moves': competitor_moves,
            'analysis_timestamp': datetime.now().isoformat(),
            'market_volatility': 'high' if len(competitor_moves) > 3 else 'moderate'
        }
    
    def stream_enterprise_cycle(self, cycle_num):
        """Execute one complete enterprise streaming cycle"""
        print(f"\n📊 ENTERPRISE STREAMING CYCLE #{cycle_num}")
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 80)
        
        # Get enterprise sample
        employees = self.get_enterprise_sample(20)
        
        # Generate updates
        updates, total_increase = self.generate_executive_updates(employees[:10])
        
        print(f"💼 EXECUTIVE COMPENSATION UPDATES ({len(updates)} executives):")
        for update in updates[:5]:  # Show top 5
            print(f"👤 {update['employee_name']} | {update['company']}")
            print(f"   📋 {update['job_title']} | {update['department']}")
            print(f"   💰 ${update['previous_salary']:,.0f} → ${update['new_salary']:,} (+{update['increase_percentage']:.1%})")
            print(f"   📊 Budget Impact: {(update['increase_amount']/150000000)*100:.3f}%")
            print()
        
        # Send to Kafka if available
        if self.kafka_enabled:
            try:
                for update in updates:
                    self.producer.send('hr-executive-updates', value=update)
                print(f"✅ Sent {len(updates)} executive updates to Kafka")
            except Exception as e:
                print(f"⚠️  Kafka send failed: {e}")
        
        # Generate and display business intelligence
        intelligence = self.generate_enterprise_intelligence(updates, total_increase)
        print(f"📈 ENTERPRISE BUSINESS INTELLIGENCE:")
        print(f"   💵 Total Salary Increases: ${intelligence['total_increase_amount']:,.0f}")
        print(f"   📊 Average Increase: ${intelligence['average_increase']:,.0f}")
        print(f"   🚀 Projected Annual Impact: ${intelligence['projected_annual_impact']:,.0f}")
        print(f"   💼 Market Trend: {intelligence['market_trend_classification'].replace('_', ' ').title()}")
        print(f"   📋 Strategic Rec: {intelligence['strategic_recommendation']}")
        
        if self.kafka_enabled:
            try:
                self.producer.send('hr-enterprise-intelligence', value=intelligence)
                print(f"✅ Sent enterprise intelligence to Kafka")
            except Exception as e:
                print(f"⚠️  Intelligence send failed: {e}")
        
        # Generate competitive intelligence
        competitive = self.generate_competitive_intelligence()
        print(f"\n🔍 COMPETITIVE MARKET INTELLIGENCE:")
        for move in competitive['market_moves']:
            print(f"   🏢 {move['competitor']}: {move['action'].replace('_', ' ')} ({move['impact_percentage']:.1f}% impact)")
        
        if self.kafka_enabled:
            try:
                self.producer.send('hr-competitive-intelligence', value=competitive)
                print(f"✅ Sent competitive intelligence to Kafka")
                self.producer.flush()
            except Exception as e:
                print(f"⚠️  Competitive send failed: {e}")
    
    def run_streaming_platform(self):
        """Main streaming loop"""
        print(f"\n🚀 STARTING ENTERPRISE STREAMING PLATFORM")
        print(f"🔄 Processing 1,000+ employees across 5 companies")
        print(f"💰 Managing $150M+ annual payroll")
        print(f"📡 Streaming to {'Kafka topics' if self.kafka_enabled else 'database-only mode'}")
        print("="*80)
        
        cycle = 1
        try:
            while True:
                self.stream_enterprise_cycle(cycle)
                cycle += 1
                print(f"\n⏳ Next cycle in 45 seconds...")
                time.sleep(45)
        except KeyboardInterrupt:
            print(f"\n🛑 Enterprise streaming platform stopped")
            print(f"📊 Completed {cycle-1} streaming cycles")
        except Exception as e:
            print(f"❌ Platform error: {e}")
        finally:
            if hasattr(self, 'conn'):
                self.conn.close()
            if self.kafka_enabled and hasattr(self, 'producer'):
                self.producer.close()

if __name__ == '__main__':
    platform = Enterprise1000StreamingPlatform()
    platform.run_streaming_platform()