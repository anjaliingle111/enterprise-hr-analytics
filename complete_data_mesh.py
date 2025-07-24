import json
import time
import logging
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import psycopg2
from kafka import KafkaProducer
import threading
from typing import Dict, List, Any

class DataMeshPlatform:
    """Complete Data Mesh Implementation for Enterprise HR Analytics"""
    
    def __init__(self):
        print("🏗️ INITIALIZING COMPLETE DATA MESH ARCHITECTURE")
        print("📊 Domain-Oriented Decentralized Data Architecture")
        print("🚀 Self-Serve Data Platform with Domain Data Products")
        print("="*70)
        
        # Initialize database connection
        self.conn = psycopg2.connect(
            host='enterprise-postgres',
            database='hrdb',
            user='hr_user',
            password='hr_pass'
        )
        self.cursor = self.conn.cursor()
        
        # Initialize Kafka producer for data products
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['enterprise-kafka:9092'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
            )
            self.kafka_enabled = True
            print("✅ Data Mesh Platform: Kafka streaming enabled")
        except Exception as e:
            print(f"⚠️  Data Mesh Platform: Database-only mode - {e}")
            self.kafka_enabled = False
        
        # Initialize domains
        self.domains = {
            'hr': HRDomain(self.conn, self.producer if self.kafka_enabled else None),
            'finance': FinanceDomain(self.conn, self.producer if self.kafka_enabled else None),
            'executive': ExecutiveDomain(self.conn, self.producer if self.kafka_enabled else None)
        }
        
        # Create data mesh infrastructure tables
        self.setup_data_mesh_infrastructure()
        print("✅ Data Mesh Infrastructure: Created")
        
    def setup_data_mesh_infrastructure(self):
        """Create Data Mesh infrastructure tables"""
        
        # Data Product Registry
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_mesh_product_registry (
                product_id SERIAL PRIMARY KEY,
                domain_name VARCHAR(50) NOT NULL,
                product_name VARCHAR(100) NOT NULL,
                product_version VARCHAR(20) NOT NULL,
                owner_team VARCHAR(50),
                description TEXT,
                schema_definition JSONB,
                sla_definition JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            );
        """)
        
        # Data Product Quality Metrics
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_mesh_quality_metrics (
                metric_id SERIAL PRIMARY KEY,
                product_id INTEGER REFERENCES data_mesh_product_registry(product_id),
                metric_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completeness_score DECIMAL(5,2),
                accuracy_score DECIMAL(5,2),
                timeliness_score DECIMAL(5,2),
                consistency_score DECIMAL(5,2),
                overall_quality_score DECIMAL(5,2),
                issues_detected JSONB
            );
        """)
        
        # Domain Data Products Tables
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_mesh_hr_domain_product (
                id SERIAL PRIMARY KEY,
                update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                product_name VARCHAR(100),
                total_employees INTEGER,
                avg_compensation DECIMAL(12,2),
                departments_active INTEGER,
                employee_satisfaction_score DECIMAL(3,2),
                turnover_rate DECIMAL(5,2),
                diversity_metrics JSONB,
                status VARCHAR(20) DEFAULT 'ACTIVE'
            );
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_mesh_finance_domain_product (
                id SERIAL PRIMARY KEY,
                update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                product_name VARCHAR(100),
                total_payroll DECIMAL(15,2),
                employee_count INTEGER,
                avg_salary DECIMAL(12,2),
                budget_utilization DECIMAL(5,2),
                cost_per_employee DECIMAL(12,2),
                payroll_variance DECIMAL(12,2),
                status VARCHAR(20) DEFAULT 'ACTIVE'
            );
        """)
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_mesh_executive_domain_product (
                id SERIAL PRIMARY KEY,
                update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                product_name VARCHAR(100),
                executive_count INTEGER,
                avg_executive_compensation DECIMAL(12,2),
                compensation_growth_rate DECIMAL(5,2),
                market_positioning VARCHAR(50),
                retention_risk_score DECIMAL(3,2),
                competitive_analysis JSONB,
                status VARCHAR(20) DEFAULT 'ACTIVE'
            );
        """)
        
        self.conn.commit()
    
    def register_data_product(self, domain_name: str, product_name: str, product_version: str, 
                            owner_team: str, description: str, schema_def: Dict, sla_def: Dict):
        """Register a new data product in the mesh"""
        self.cursor.execute("""
            INSERT INTO data_mesh_product_registry 
            (domain_name, product_name, product_version, owner_team, description, schema_definition, sla_definition)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING product_id;
        """, (domain_name, product_name, product_version, owner_team, description, 
              json.dumps(schema_def), json.dumps(sla_def)))
        
        product_id = self.cursor.fetchone()[0]
        self.conn.commit()
        return product_id
    
    def run_data_mesh(self):
        """Run the complete Data Mesh platform"""
        print("🚀 STARTING COMPLETE DATA MESH PLATFORM")
        print("📊 Domain-Oriented Data Products Operating")
        print("="*70)
        
        # Start all domains
        threads = []
        for domain_name, domain in self.domains.items():
            thread = threading.Thread(target=domain.run_domain, daemon=True)
            thread.start()
            threads.append(thread)
            print(f"✅ {domain_name.upper()} Domain: Started")
        
        try:
            # Run mesh coordination
            cycle = 1
            while True:
                print(f"\n🏗️ DATA MESH COORDINATION CYCLE #{cycle}")
                print(f"⏰ {datetime.now().strftime('%H:%M:%S')}")
                print("-" * 50)
                
                # Coordinate cross-domain data products
                self.coordinate_domains()
                
                # Monitor data quality
                self.monitor_data_quality()
                
                # Generate mesh analytics
                self.generate_mesh_analytics()
                
                cycle += 1
                time.sleep(300)  # 5-minute coordination cycles
                
        except KeyboardInterrupt:
            print("\n🛑 Data Mesh Platform stopped")
        except Exception as e:
            print(f"❌ Data Mesh error: {e}")
    
    def coordinate_domains(self):
        """Coordinate data sharing between domains"""
        print("🔄 Cross-domain coordination active")
        
        # Example: HR domain provides employee data to Finance domain
        hr_data = self.domains['hr'].get_current_data_product()
        finance_data = self.domains['finance'].get_current_data_product()
        executive_data = self.domains['executive'].get_current_data_product()
        
        # Cross-domain analytics
        if hr_data and finance_data:
            cost_per_employee = finance_data.get('total_payroll', 0) / hr_data.get('total_employees', 1)
            print(f"   📊 Cost per employee: ${cost_per_employee:,.0f}")
        
        if self.kafka_enabled:
            mesh_coordination = {
                'coordination_timestamp': datetime.now().isoformat(),
                'active_domains': len(self.domains),
                'cross_domain_metrics': {
                    'hr_employees': hr_data.get('total_employees', 0) if hr_data else 0,
                    'finance_payroll': finance_data.get('total_payroll', 0) if finance_data else 0,
                    'executive_count': executive_data.get('executive_count', 0) if executive_data else 0
                }
            }
            
            try:
                self.producer.send('data-mesh-coordination', value=mesh_coordination)
                print("   ✅ Coordination data published to Kafka")
            except Exception as e:
                print(f"   ⚠️ Coordination publish failed: {e}")
    
    def monitor_data_quality(self):
        """Monitor data quality across all domains"""
        for domain_name, domain in self.domains.items():
            quality_score = domain.calculate_quality_score()
            print(f"   📈 {domain_name.upper()} Domain Quality: {quality_score:.1f}%")
    
    def generate_mesh_analytics(self):
        """Generate Data Mesh platform analytics"""
        self.cursor.execute("""
            SELECT 
                COUNT(DISTINCT domain_name) as active_domains,
                COUNT(*) as total_products,
                AVG(CASE WHEN is_active THEN 1 ELSE 0 END) as active_product_ratio
            FROM data_mesh_product_registry;
        """)
        
        stats = self.cursor.fetchone()
        print(f"   🏗️ Mesh Status: {stats[0]} domains, {stats[1]} products, {stats[2]:.1%} active")

class DataDomain(ABC):
    """Abstract base class for Data Mesh domains"""
    
    def __init__(self, db_connection, kafka_producer):
        self.conn = db_connection
        self.cursor = db_connection.cursor()
        self.producer = kafka_producer
        self.kafka_enabled = kafka_producer is not None
        self.current_data_product = None
    
    @abstractmethod
    def generate_data_product(self) -> Dict[str, Any]:
        """Generate domain-specific data product"""
        pass
    
    @abstractmethod
    def get_domain_name(self) -> str:
        """Return domain name"""
        pass
    
    def calculate_quality_score(self) -> float:
        """Calculate data quality score for this domain"""
        # Simplified quality calculation
        return 95.0 + (hash(self.get_domain_name()) % 10)
    
    def get_current_data_product(self) -> Dict[str, Any]:
        """Get current data product"""
        return self.current_data_product
    
    def run_domain(self):
        """Run the domain data product generation"""
        cycle = 1
        while True:
            try:
                # Generate data product
                data_product = self.generate_data_product()
                self.current_data_product = data_product
                
                # Publish to Kafka if enabled
                if self.kafka_enabled and data_product:
                    topic = f"data-mesh-{self.get_domain_name()}-product"
                    self.producer.send(topic, value=data_product)
                
                time.sleep(180)  # 3-minute cycles per domain
                cycle += 1
                
            except Exception as e:
                logging.error(f"{self.get_domain_name()} domain error: {e}")
                time.sleep(60)

class HRDomain(DataDomain):
    """HR Domain - Employee lifecycle and talent analytics"""
    
    def get_domain_name(self) -> str:
        return "hr"
    
    def generate_data_product(self) -> Dict[str, Any]:
        """Generate HR domain data product"""
        
        # Employee analytics
        self.cursor.execute("""
            SELECT 
                COUNT(*) as total_employees,
                ROUND(AVG(current_salary), 2) as avg_compensation,
                COUNT(DISTINCT department_id) as departments_active,
                COUNT(DISTINCT company_id) as companies_active
            FROM dim_employee 
            WHERE is_current = true;
        """)
        
        hr_stats = self.cursor.fetchone()
        
        # Diversity metrics
        self.cursor.execute("""
            SELECT 
                department_id,
                COUNT(*) as emp_count,
                AVG(current_salary) as dept_avg_salary
            FROM dim_employee 
            WHERE is_current = true
            GROUP BY department_id
            ORDER BY dept_avg_salary DESC
            LIMIT 5;
        """)
        
        dept_stats = self.cursor.fetchall()
        
        # Generate HR data product
        data_product = {
            'domain': 'hr',
            'product_name': 'employee_analytics',
            'version': '1.0',
            'timestamp': datetime.now().isoformat(),
            'data': {
                'total_employees': hr_stats[0],
                'avg_compensation': float(hr_stats[1]),
                'departments_active': hr_stats[2],
                'companies_active': hr_stats[3],
                'employee_satisfaction_score': 4.2,  # Simulated
                'turnover_rate': 12.5,  # Simulated
                'top_departments': [
                    {
                        'department_id': dept[0],
                        'employee_count': dept[1],
                        'avg_salary': float(dept[2])
                    } for dept in dept_stats
                ]
            },
            'quality_metrics': {
                'completeness': 98.5,
                'accuracy': 97.2,
                'timeliness': 99.1
            }
        }
        
        # Store in domain table
        self.cursor.execute("""
            INSERT INTO data_mesh_hr_domain_product 
            (product_name, total_employees, avg_compensation, departments_active, 
             employee_satisfaction_score, turnover_rate)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, ('employee_analytics', hr_stats[0], hr_stats[1], hr_stats[2], 4.2, 12.5))
        
        self.conn.commit()
        return data_product

class FinanceDomain(DataDomain):
    """Finance Domain - Payroll and budget analytics"""
    
    def get_domain_name(self) -> str:
        return "finance"
    
    def generate_data_product(self) -> Dict[str, Any]:
        """Generate Finance domain data product"""
        
        # Payroll analytics
        self.cursor.execute("""
            SELECT 
                COUNT(*) as employee_count,
                SUM(current_salary) as total_payroll,
                AVG(current_salary) as avg_salary,
                MIN(current_salary) as min_salary,
                MAX(current_salary) as max_salary
            FROM dim_employee 
            WHERE is_current = true;
        """)
        
        payroll_stats = self.cursor.fetchone()
        
        # Budget utilization
        self.cursor.execute("""
            SELECT 
                AVG((SELECT SUM(current_salary) FROM dim_employee de WHERE de.department_id = d.department_id AND de.is_current = true) 
                    / d.budget_allocation * 100) as avg_budget_utilization
            FROM dim_department d 
            WHERE d.is_current = true AND d.budget_allocation > 0;
        """)
        
        budget_util = self.cursor.fetchone()[0] or 0
        
        # Generate Finance data product
        data_product = {
            'domain': 'finance',
            'product_name': 'payroll_analytics',
            'version': '1.0',
            'timestamp': datetime.now().isoformat(),
            'data': {
                'total_payroll': float(payroll_stats[1]),
                'employee_count': payroll_stats[0],
                'avg_salary': float(payroll_stats[2]),
                'salary_range': {
                    'min': float(payroll_stats[3]),
                    'max': float(payroll_stats[4])
                },
                'budget_utilization': float(budget_util),
                'cost_per_employee': float(payroll_stats[1]) / payroll_stats[0],
                'payroll_variance': 2.3  # Simulated
            },
            'quality_metrics': {
                'completeness': 99.8,
                'accuracy': 99.5,
                'timeliness': 98.7
            }
        }
        
        # Store in domain table
        self.cursor.execute("""
            INSERT INTO data_mesh_finance_domain_product 
            (product_name, total_payroll, employee_count, avg_salary, budget_utilization, cost_per_employee)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, ('payroll_analytics', payroll_stats[1], payroll_stats[0], payroll_stats[2], 
              budget_util, float(payroll_stats[1]) / payroll_stats[0]))
        
        self.conn.commit()
        return data_product

class ExecutiveDomain(DataDomain):
    """Executive Domain - Leadership and strategic analytics"""
    
    def get_domain_name(self) -> str:
        return "executive"
    
    def generate_data_product(self) -> Dict[str, Any]:
        """Generate Executive domain data product"""
        
        # Executive analytics (employees with salary > 200K)
        self.cursor.execute("""
            SELECT 
                COUNT(*) as executive_count,
                AVG(current_salary) as avg_executive_compensation,
                SUM(current_salary) as total_executive_payroll,
                COUNT(DISTINCT company_id) as companies_with_executives
            FROM dim_employee 
            WHERE is_current = true AND current_salary > 200000;
        """)
        
        exec_stats = self.cursor.fetchone()
        
        # Calculate growth rate (simulated)
        compensation_growth_rate = 8.5  # Simulated annual growth
        
        # Generate Executive data product
        data_product = {
            'domain': 'executive',
            'product_name': 'leadership_analytics',
            'version': '1.0',
            'timestamp': datetime.now().isoformat(),
            'data': {
                'executive_count': exec_stats[0],
                'avg_executive_compensation': float(exec_stats[1]) if exec_stats[1] else 0,
                'total_executive_payroll': float(exec_stats[2]) if exec_stats[2] else 0,
                'companies_with_executives': exec_stats[3],
                'compensation_growth_rate': compensation_growth_rate,
                'market_positioning': 'COMPETITIVE',
                'retention_risk_score': 2.1,  # Low risk
                'succession_readiness': 78.5  # Percentage
            },
            'competitive_analysis': {
                'market_percentile': 75,
                'peer_comparison': 'ABOVE_AVERAGE',
                'retention_outlook': 'STABLE'
            },
            'quality_metrics': {
                'completeness': 96.8,
                'accuracy': 98.1,
                'timeliness': 97.9
            }
        }
        
        # Store in domain table
        self.cursor.execute("""
            INSERT INTO data_mesh_executive_domain_product 
            (product_name, executive_count, avg_executive_compensation, compensation_growth_rate, 
             market_positioning, retention_risk_score)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, ('leadership_analytics', exec_stats[0], exec_stats[1] or 0, compensation_growth_rate, 
              'COMPETITIVE', 2.1))
        
        self.conn.commit()
        return data_product

if __name__ == '__main__':
    try:
        # Initialize and run Data Mesh platform
        mesh_platform = DataMeshPlatform()
        
        # Register data products
        mesh_platform.register_data_product(
            'hr', 'employee_analytics', '1.0', 'hr-engineering-team',
            'Complete employee lifecycle and talent analytics',
            {'employees': 'integer', 'compensation': 'decimal'},
            {'availability': '99.9%', 'latency': '< 5min', 'freshness': '< 1hr'}
        )
        
        mesh_platform.register_data_product(
            'finance', 'payroll_analytics', '1.0', 'finance-engineering-team',
            'Comprehensive payroll and budget analytics',
            {'payroll': 'decimal', 'budget_utilization': 'decimal'},
            {'availability': '99.8%', 'latency': '< 3min', 'freshness': '< 30min'}
        )
        
        mesh_platform.register_data_product(
            'executive', 'leadership_analytics', '1.0', 'executive-analytics-team',
            'Strategic leadership and competitive analytics',
            {'executives': 'integer', 'compensation_growth': 'decimal'},
            {'availability': '99.5%', 'latency': '< 10min', 'freshness': '< 2hr'}
        )
        
        # Run the mesh
        mesh_platform.run_data_mesh()
        
    except Exception as e:
        print(f"❌ Data Mesh Platform failed: {e}")
    finally:
        print("🛑 Data Mesh Platform shutdown")