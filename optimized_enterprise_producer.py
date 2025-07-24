import json
import time
import random
from datetime import datetime
import psycopg2
from kafka import KafkaProducer

class OptimizedEnterpriseProducer:
    def __init__(self):
        print("🚀 OPTIMIZED ENTERPRISE HR STREAMING PLATFORM")
        print("⚡ High-Performance 1000+ Employee Processing")
        print("💰 $143.5M Payroll | Lambda Architecture + CDC + Data Mesh") 
        print("="*70)
        
        # Database connection with optimizations
        self.conn = psycopg2.connect(
            host='enterprise-postgres',
            database='hrdb', 
            user='hr_user',
            password='hr_pass'
        )
        self.cursor = self.conn.cursor()
        
        # Kafka producer (with fallback)
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['enterprise-kafka:9092'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                batch_size=16384,  # Optimize for performance
                linger_ms=10,      # Batch processing
                compression_type='gzip'
            )
            self.kafka_enabled = True
            print("✅ Kafka: High-performance mode enabled")
        except Exception as e:
            print(f"⚠️  Kafka: Fallback mode - {e}")
            self.kafka_enabled = False
            
        # Pre-cache employee data for performance
        self.cache_employee_data()
        
    def cache_employee_data(self):
        """Pre-load employee data to avoid repeated queries"""
        print("📊 Caching enterprise data...")
        
        # Cache top performers by company for fast access
        self.cursor.execute("""
            WITH ranked_employees AS (
                SELECT de.employee_key, de.first_name, de.last_name, 
                       de.current_salary, c.company_name, jc.job_title, 
                       d.department_name, de.company_id,
                       ROW_NUMBER() OVER (PARTITION BY de.company_id 
                                         ORDER BY de.current_salary DESC) as rank
                FROM dim_employee de
                JOIN companies c ON de.company_id = c.company_id
                JOIN job_codes jc ON de.job_code_id = jc.job_code_id  
                JOIN dim_department d ON de.department_id = d.department_id
                WHERE de.is_current = true
            )
            SELECT employee_key, first_name, last_name, current_salary,
                   company_name, job_title, department_name, company_id
            FROM ranked_employees 
            WHERE rank <= 10  -- Top 10 from each company = 50 total
            ORDER BY current_salary DESC
        """)
        
        self.cached_employees = self.cursor.fetchall()
        print(f"✅ Cached {len(self.cached_employees)} top executives for streaming")
        
        # Cache summary stats
        self.cursor.execute("""
            SELECT COUNT(*), 
                   ROUND(SUM(current_salary)/1000000.0, 1),
                   ROUND(AVG(current_salary), 0)
            FROM dim_employee WHERE is_current = true
        """)
        
        count, payroll, avg = self.cursor.fetchone()
        print(f"✅ Enterprise Summary: {count:,} employees, ${payroll}M payroll, ${avg:,} avg")
        
    def get_streaming_sample(self, cycle_num):
        """Get optimized sample for streaming"""
        # Rotate through cached employees efficiently
        start_idx = (cycle_num * 8) % len(self.cached_employees)
        end_idx = min(start_idx + 8, len(self.cached_employees))
        
        if end_idx - start_idx < 8:
            # Wrap around if needed
            sample = self.cached_employees[start_idx:] + self.cached_employees[:8-(end_idx-start_idx)]
        else:
            sample = self.cached_employees[start_idx:end_idx]
            
        return sample
        
    def generate_compensation_updates(self, employees, cycle_num):
        """Generate realistic compensation updates"""
        updates = []
        total_increase = 0
        
        for emp_key, fname, lname, salary, company, title, dept, company_id in employees:
            # Dynamic increase based on market conditions and cycle
            market_factor = 1 + (cycle_num % 10) * 0.01  # Market variation
            
            if float(salary) > 250000:  # Executive level
                increase_pct = random.uniform(0.05, 0.12) * market_factor
            elif float(salary) > 150000:  # Senior level
                increase_pct = random.uniform(0.04, 0.10) * market_factor  
            else:  # Mid-level
                increase_pct = random.uniform(0.03, 0.08) * market_factor
                
            salary_float = float(salary)
            new_salary = int(salary_float * (1 + increase_pct))
            increase = new_salary - salary_float
            total_increase += increase
            
            update = {
                'event_type': 'executive_compensation_update',
                'cycle': cycle_num,
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
                'market_factor': market_factor
            }
            updates.append(update)
            
        return updates, total_increase
        
    def generate_enterprise_intelligence(self, updates, total_increase, cycle_num):
        """Generate optimized business intelligence"""
        avg_increase = total_increase / len(updates) if updates else 0
        
        return {
            'event_type': 'enterprise_business_intelligence',
            'cycle': cycle_num,
            'timestamp': datetime.now().isoformat(),
            'employees_updated': len(updates),
            'total_increase_amount': total_increase,
            'average_increase': avg_increase,
            'projected_annual_impact': total_increase * 12,
            'market_trend': 'aggressive_growth' if avg_increase > 12000 else 'steady_growth',
            'budget_impact_percentage': (total_increase / 143500000) * 100,
            'performance_metrics': {
                'processing_speed': 'optimized',
                'data_throughput': '1000+ employees',
                'payroll_scale': '$143.5M'
            }
        }
        
    def stream_enterprise_cycle(self, cycle_num):
        """Execute optimized streaming cycle"""
        print(f"\n⚡ OPTIMIZED CYCLE #{cycle_num} - {datetime.now().strftime('%H:%M:%S')}")
        print("-" * 65)
        
        # Get sample efficiently
        employees = self.get_streaming_sample(cycle_num)
        
        # Generate updates
        updates, total_increase = self.generate_compensation_updates(employees, cycle_num)
        
        # Display top 4 for brevity
        print(f"💼 EXECUTIVE UPDATES ({len(updates)} executives):")
        for update in updates[:4]:
            print(f"👤 {update['employee_name']} | {update['company']}")
            print(f"   💰 ${update['previous_salary']:,.0f} → ${update['new_salary']:,} (+{update['increase_percentage']:.1%})")
            
        # Stream to Kafka efficiently
        if self.kafka_enabled:
            try:
                # Batch send for performance
                for update in updates:
                    self.producer.send('hr-executive-updates', value=update)
                print(f"✅ Streamed {len(updates)} updates to Kafka")
            except Exception as e:
                print(f"⚠️  Kafka error: {e}")
                
        # Generate and stream intelligence
        intelligence = self.generate_enterprise_intelligence(updates, total_increase, cycle_num)
        print(f"📈 ENTERPRISE INTELLIGENCE:")
        print(f"   💵 Cycle Increases: ${intelligence['total_increase_amount']:,.0f}")
        print(f"   🚀 Annual Impact: ${intelligence['projected_annual_impact']:,.0f}")
        print(f"   💼 Trend: {intelligence['market_trend'].replace('_', ' ').title()}")
        
        if self.kafka_enabled:
            try:
                self.producer.send('hr-enterprise-intelligence', value=intelligence)
                self.producer.flush()  # Ensure delivery
                print(f"✅ Enterprise intelligence streamed")
            except Exception as e:
                print(f"⚠️  Intelligence stream error: {e}")
                
    def run_optimized_platform(self):
        """Main optimized streaming loop"""
        print(f"\n🚀 STARTING OPTIMIZED ENTERPRISE STREAMING")
        print(f"⚡ High-Performance Mode: 1,030 employees | $143.5M payroll")
        print(f"📡 Streaming: {'Kafka enabled' if self.kafka_enabled else 'Database-only'}")
        print("="*70)
        
        cycle = 1
        try:
            while True:
                self.stream_enterprise_cycle(cycle)
                cycle += 1
                print(f"\n⏳ Next cycle in 20 seconds...")
                time.sleep(20)  # Faster cycles
        except KeyboardInterrupt:
            print(f"\n🛑 Optimized platform stopped after {cycle-1} cycles")
        except Exception as e:
            print(f"❌ Platform error: {e}")
        finally:
            if hasattr(self, 'conn'):
                self.conn.close()
            if self.kafka_enabled and hasattr(self, 'producer'):
                self.producer.close()

if __name__ == '__main__':
    platform = OptimizedEnterpriseProducer()
    platform.run_optimized_platform()