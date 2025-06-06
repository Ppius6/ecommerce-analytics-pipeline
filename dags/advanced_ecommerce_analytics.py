from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'advanced_ecommerce_analytics',
    default_args=default_args,
    description='Advanced E-commerce Analytics Pipeline',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['ecommerce', 'analytics', 'advanced'],
    max_active_runs=1
)

def create_analytics_schema(**context):
    """Create analytics schema in PostgreSQL"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Create analytics schema
    pg_hook.run("CREATE SCHEMA IF NOT EXISTS analytics;")
    pg_hook.run("GRANT ALL PRIVILEGES ON SCHEMA analytics TO airflow;")
    
    logger.info("✅ Analytics schema created successfully")
    return "Schema created"

def build_customer_analytics(**context):
    """Build comprehensive customer analytics table"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Fixed customer analytics SQL with proper date calculations
    customer_analytics_sql = """
    DROP TABLE IF EXISTS analytics.customer_metrics;
    
    CREATE TABLE analytics.customer_metrics AS
    WITH customer_orders AS (
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name as full_name,
            c.email,
            c.customer_segment,
            c.country,
            c.registration_date::date as registration_date,
            COUNT(o.order_id) as total_orders,
            COUNT(CASE WHEN o.status = 'completed' THEN 1 END) as completed_orders,
            COALESCE(SUM(CASE WHEN o.status = 'completed' THEN o.total_amount ELSE 0 END), 0) as total_revenue,
            ROUND(AVG(CASE WHEN o.status = 'completed' THEN o.total_amount END)::numeric, 2) as avg_order_value,
            MAX(o.order_date::date) as last_order_date,
            MIN(o.order_date::date) as first_order_date
        FROM staging.customers c
        LEFT JOIN staging.orders o ON c.customer_id = o.customer_id
        GROUP BY 1,2,3,4,5,6
    )
    SELECT 
        customer_id,
        full_name,
        email,
        customer_segment,
        country,
        registration_date,
        total_orders,
        completed_orders,
        ROUND(total_revenue::numeric, 2) as total_revenue,
        avg_order_value,
        last_order_date,
        first_order_date,
        
        CASE 
            WHEN total_orders = 0 THEN 'No Orders'
            WHEN total_orders = 1 THEN 'One-time Buyer'
            WHEN total_orders BETWEEN 2 AND 5 THEN 'Regular Customer'
            WHEN total_orders BETWEEN 6 AND 10 THEN 'Loyal Customer'
            ELSE 'VIP Customer'
        END as customer_tier,
        
        CASE 
            WHEN total_revenue = 0 THEN 'No Value'
            WHEN total_revenue < 100 THEN 'Low Value'
            WHEN total_revenue BETWEEN 100 AND 500 THEN 'Medium Value'
            WHEN total_revenue BETWEEN 500 AND 1000 THEN 'High Value'
            ELSE 'Premium Value'
        END as value_segment,
        
        CASE 
            WHEN last_order_date IS NULL THEN 'No Orders'
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'At Risk'
            WHEN last_order_date >= CURRENT_DATE - INTERVAL '180 days' THEN 'Dormant'
            ELSE 'Lost'
        END as customer_status,
        
        CASE 
            WHEN last_order_date IS NULL THEN NULL
            ELSE (CURRENT_DATE - last_order_date)::int
        END as days_since_last_order
        
    FROM customer_orders;
    """
    
    pg_hook.run(customer_analytics_sql)
    
    # Get row count
    result = pg_hook.get_first("SELECT COUNT(*) FROM analytics.customer_metrics")
    row_count = result[0]
    
    logger.info(f"✅ Built customer analytics table with {row_count:,} records")
    return f"Built {row_count} customer records"

def build_product_analytics(**context):
    """Build product performance analytics"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Fixed product analytics SQL with proper GROUP BY
    product_analytics_sql = """
    DROP TABLE IF EXISTS analytics.product_performance;
    
    CREATE TABLE analytics.product_performance AS
    WITH product_sales AS (
        SELECT 
            p.product_id,
            p.product_name,
            p.category,
            ROUND(p.price::numeric, 2) as price,
            ROUND(p.cost::numeric, 2) as cost,
            COUNT(DISTINCT oi.order_id) as total_orders,
            COALESCE(SUM(oi.quantity), 0) as total_quantity_sold,
            COALESCE(ROUND(SUM(oi.total_amount)::numeric, 2), 0) as total_revenue,
            ROUND(AVG(oi.unit_price)::numeric, 2) as avg_selling_price
        FROM staging.products p
        LEFT JOIN staging.order_items oi ON p.product_id = oi.product_id
        LEFT JOIN staging.orders o ON oi.order_id = o.order_id AND o.status = 'completed'
        GROUP BY p.product_id, p.product_name, p.category, p.price, p.cost
    )
    SELECT 
        product_id,
        product_name,
        category,
        price,
        cost,
        ROUND((price - cost)::numeric, 2) as profit_margin,
        total_orders,
        total_quantity_sold,
        total_revenue,
        avg_selling_price,
        
        CASE 
            WHEN total_quantity_sold = 0 THEN 'No Sales'
            WHEN total_quantity_sold < 10 THEN 'Low Performer'
            WHEN total_quantity_sold BETWEEN 10 AND 50 THEN 'Average Performer'
            WHEN total_quantity_sold BETWEEN 50 AND 100 THEN 'Good Performer'
            ELSE 'Top Performer'
        END as performance_category,
        
        CASE 
            WHEN price > 0 THEN ROUND(((price - cost) / price * 100)::numeric, 2)
            ELSE 0
        END as profit_margin_percentage
        
    FROM product_sales
    ORDER BY total_revenue DESC;
    """
    
    pg_hook.run(product_analytics_sql)
    
    result = pg_hook.get_first("SELECT COUNT(*) FROM analytics.product_performance")
    row_count = result[0]
    
    logger.info(f"✅ Built product analytics table with {row_count:,} records")
    return f"Built {row_count} product records"

def build_daily_sales_summary(**context):
    """Build daily sales summary for dashboards"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    daily_sales_sql = """
    DROP TABLE IF EXISTS analytics.daily_sales;
    
    CREATE TABLE analytics.daily_sales AS
    WITH daily_metrics AS (
        SELECT 
            o.order_date::date as sale_date,
            COUNT(DISTINCT o.order_id) as total_orders,
            ROUND(SUM(o.total_amount)::numeric, 2) as total_revenue,
            ROUND(AVG(o.total_amount)::numeric, 2) as avg_order_value,
            COUNT(DISTINCT o.customer_id) as unique_customers
        FROM staging.orders o
        WHERE o.status = 'completed'
        GROUP BY o.order_date::date
    ),
    top_category_by_day AS (
        SELECT 
            o.order_date::date as sale_date,
            p.category,
            SUM(oi.quantity) as category_quantity,
            ROW_NUMBER() OVER (PARTITION BY o.order_date::date ORDER BY SUM(oi.quantity) DESC) as rn
        FROM staging.orders o
        JOIN staging.order_items oi ON o.order_id = oi.order_id
        JOIN staging.products p ON oi.product_id = p.product_id
        WHERE o.status = 'completed'
        GROUP BY o.order_date::date, p.category
    )
    SELECT 
        dm.sale_date,
        dm.total_orders,
        dm.total_revenue,
        dm.avg_order_value,
        dm.unique_customers,
        COALESCE(tc.category, 'Unknown') as top_category
    FROM daily_metrics dm
    LEFT JOIN top_category_by_day tc ON dm.sale_date = tc.sale_date AND tc.rn = 1
    ORDER BY dm.sale_date DESC;
    """
    
    pg_hook.run(daily_sales_sql)
    
    result = pg_hook.get_first("SELECT COUNT(*) FROM analytics.daily_sales")
    row_count = result[0]
    
    logger.info(f"✅ Built daily sales summary with {row_count:,} days")
    return f"Built {row_count} daily records"

def run_data_quality_checks(**context):
    """Run comprehensive data quality checks"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    quality_checks = [
        {
            'name': 'Customer analytics row count',
            'query': 'SELECT COUNT(*) FROM analytics.customer_metrics',
            'expected': 'should be > 900',
            'check': lambda x: x > 900
        },
        {
            'name': 'Product analytics row count',
            'query': 'SELECT COUNT(*) FROM analytics.product_performance',
            'expected': 'should be > 190',
            'check': lambda x: x > 190
        },
        {
            'name': 'Daily sales row count',
            'query': 'SELECT COUNT(*) FROM analytics.daily_sales',
            'expected': 'should be > 0',
            'check': lambda x: x > 0
        },
        {
            'name': 'Customers with null names',
            'query': 'SELECT COUNT(*) FROM analytics.customer_metrics WHERE full_name IS NULL',
            'expected': 'should be = 0',
            'check': lambda x: x == 0
        },
        {
            'name': 'Products with negative revenue',
            'query': 'SELECT COUNT(*) FROM analytics.product_performance WHERE total_revenue < 0',
            'expected': 'should be = 0',
            'check': lambda x: x == 0
        }
    ]
    
    all_passed = True
    
    logger.info("🔍 Running data quality checks...")
    
    for check in quality_checks:
        result = pg_hook.get_first(check['query'])[0]
        passed = check['check'](result)
        
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"{status} {check['name']}: {result:,} ({check['expected']})")
        
        if not passed:
            all_passed = False
    
    if not all_passed:
        raise ValueError("❌ Data quality checks failed!")
    
    logger.info("🎉 All data quality checks passed!")
    return "All quality checks passed"

def generate_analytics_summary(**context):
    """Generate comprehensive analytics summary"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Customer segment analysis
        customer_summary = pg_hook.get_pandas_df("""
            SELECT 
                customer_tier,
                COUNT(*) as customers,
                ROUND(AVG(total_revenue)::numeric, 2) as avg_revenue,
                ROUND(SUM(total_revenue)::numeric, 2) as total_revenue
            FROM analytics.customer_metrics 
            GROUP BY customer_tier 
            ORDER BY total_revenue DESC
        """)
        
        # Product category performance
        product_summary = pg_hook.get_pandas_df("""
            SELECT 
                category,
                COUNT(*) as products,
                SUM(total_quantity_sold) as units_sold,
                ROUND(SUM(total_revenue)::numeric, 2) as revenue
            FROM analytics.product_performance 
            GROUP BY category 
            ORDER BY revenue DESC
            LIMIT 10
        """)
        
        # Customer status breakdown
        status_summary = pg_hook.get_pandas_df("""
            SELECT 
                customer_status,
                COUNT(*) as customers,
                ROUND(AVG(total_revenue)::numeric, 2) as avg_revenue
            FROM analytics.customer_metrics 
            GROUP BY customer_status 
            ORDER BY COUNT(*) DESC
        """)
        
        # Business metrics
        business_metrics = pg_hook.get_pandas_df("""
            SELECT 
                COUNT(*) as total_customers,
                COUNT(CASE WHEN customer_status = 'Active' THEN 1 END) as active_customers,
                ROUND(SUM(total_revenue)::numeric, 2) as total_revenue,
                ROUND(AVG(total_revenue)::numeric, 2) as avg_customer_value,
                ROUND(SUM(total_revenue) / NULLIF(SUM(total_orders), 0)::numeric, 2) as avg_order_value
            FROM analytics.customer_metrics
        """)
        
        logger.info("\n" + "="*60)
        logger.info("📊 E-COMMERCE ANALYTICS SUMMARY REPORT")
        logger.info("="*60)
        
        logger.info("\n🎯 BUSINESS METRICS:")
        for _, row in business_metrics.iterrows():
            logger.info(f"   Total Customers: {row['total_customers']:,}")
            logger.info(f"   Active Customers: {row['active_customers']:,}")
            logger.info(f"   Total Revenue: ${row['total_revenue']:,.2f}")
            logger.info(f"   Avg Customer Value: ${row['avg_customer_value']:,.2f}")
            logger.info(f"   Avg Order Value: ${row['avg_order_value']:,.2f}")
        
        logger.info("\n👥 CUSTOMER TIERS:")
        for _, row in customer_summary.iterrows():
            logger.info(f"   {row['customer_tier']}: {row['customers']:,} customers, ${row['total_revenue']:,.2f} revenue")
        
        logger.info("\n📊 CUSTOMER STATUS:")
        for _, row in status_summary.iterrows():
            logger.info(f"   {row['customer_status']}: {row['customers']:,} customers, ${row['avg_revenue']:,.2f} avg revenue")
        
        logger.info("\n📦 TOP PRODUCT CATEGORIES:")
        for _, row in product_summary.iterrows():
            logger.info(f"   {row['category']}: {row['products']} products, {row['units_sold']:,} units, ${row['revenue']:,.2f} revenue")
        
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        # Return basic summary even if pandas operations fail
        basic_counts = pg_hook.get_first("""
            SELECT 
                (SELECT COUNT(*) FROM analytics.customer_metrics) as customers,
                (SELECT COUNT(*) FROM analytics.product_performance) as products,
                (SELECT COUNT(*) FROM analytics.daily_sales) as days
        """)
        
        logger.info(f"✅ Analytics Summary: {basic_counts[0]} customers, {basic_counts[1]} products, {basic_counts[2]} days analyzed")
    
    return "Analytics summary generated successfully"

# Define tasks
create_schema_task = PythonOperator(
    task_id='create_analytics_schema',
    python_callable=create_analytics_schema,
    dag=dag
)

build_customer_analytics_task = PythonOperator(
    task_id='build_customer_analytics',
    python_callable=build_customer_analytics,
    dag=dag
)

build_product_analytics_task = PythonOperator(
    task_id='build_product_analytics', 
    python_callable=build_product_analytics,
    dag=dag
)

build_daily_sales_task = PythonOperator(
    task_id='build_daily_sales_summary',
    python_callable=build_daily_sales_summary,
    dag=dag
)

run_quality_checks_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

generate_summary_task = PythonOperator(
    task_id='generate_analytics_summary',
    python_callable=generate_analytics_summary,
    dag=dag
)

# Task dependencies
create_schema_task >> [build_customer_analytics_task, build_product_analytics_task, build_daily_sales_task]
[build_customer_analytics_task, build_product_analytics_task, build_daily_sales_task] >> run_quality_checks_task >> generate_summary_task
