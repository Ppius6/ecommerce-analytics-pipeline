from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ecommerce_data_pipeline',
    default_args=default_args,
    description='E-commerce Analytics Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ecommerce', 'analytics']
)

def load_csv_to_postgres(table_name, csv_file, **context):
    """Load CSV data into PostgreSQL staging table"""
    
    csv_path = f'/opt/airflow/data/{csv_file}'
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File {csv_path} not found!")
    
    df = pd.read_csv(csv_path)
    print(f"Loading {len(df)} rows from {csv_file}")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    df.to_sql(
        name=table_name,
        con=engine,
        schema='staging',
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"Successfully loaded {len(df):,} rows into staging.{table_name}")
    return f"Loaded {len(df)} rows"

def validate_data_quality(**context):
    """Run data quality checks"""
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    checks = [
        ('Customer count', 'SELECT COUNT(*) FROM staging.customers', lambda x: x > 900),
        ('Product count', 'SELECT COUNT(*) FROM staging.products', lambda x: x > 190),
        ('Order count', 'SELECT COUNT(*) FROM staging.orders', lambda x: x > 4000),
        ('Order items count', 'SELECT COUNT(*) FROM staging.order_items', lambda x: x > 10000),
        ('Null emails', 'SELECT COUNT(*) FROM staging.customers WHERE email IS NULL', lambda x: x == 0),
        ('Negative prices', 'SELECT COUNT(*) FROM staging.products WHERE price <= 0', lambda x: x == 0)
    ]
    
    all_passed = True
    
    for check_name, query, condition in checks:
        result = pg_hook.get_first(query)[0]
        passed = condition(result)
        
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {check_name} = {result:,}")
        
        if not passed:
            all_passed = False
    
    if not all_passed:
        raise ValueError("Data quality checks failed!")
    
    print("🎉 All data quality checks passed!")
    return "Quality checks passed"

def generate_summary(**context):
    """Generate business summary"""
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Fixed queries with proper PostgreSQL casting
    queries = {
        'Total Customers': 'SELECT COUNT(*) FROM staging.customers',
        'Total Products': 'SELECT COUNT(*) FROM staging.products',
        'Total Orders': 'SELECT COUNT(*) FROM staging.orders',
        'Total Revenue': 'SELECT ROUND(SUM(total_amount)::numeric, 2) FROM staging.orders WHERE status = \'completed\'',
        'Avg Order Value': 'SELECT ROUND(AVG(total_amount)::numeric, 2) FROM staging.orders WHERE status = \'completed\''
    }
    
    print("\n" + "="*50)
    print("📊 E-COMMERCE BUSINESS METRICS")
    print("="*50)
    
    for metric, query in queries.items():
        try:
            result = pg_hook.get_first(query)[0]
            if result is None:
                result = 0
            
            if 'Revenue' in metric or 'Avg' in metric:
                print(f"💰 {metric}: ${result:,.2f}")
            else:
                print(f"📈 {metric}: {result:,}")
        except Exception as e:
            print(f"❌ Error calculating {metric}: {e}")
            print(f"   Query: {query}")
    
    # Additional insights
    try:
        order_status = pg_hook.get_pandas_df("""
            SELECT 
                status,
                COUNT(*) as count,
                ROUND(SUM(total_amount)::numeric, 2) as revenue
            FROM staging.orders 
            GROUP BY status 
            ORDER BY count DESC
        """)
        
        print("\n📋 ORDER STATUS BREAKDOWN:")
        for _, row in order_status.iterrows():
            print(f"   {row['status'].title()}: {row['count']:,} orders, ${row['revenue']:,.2f}")
            
    except Exception as e:
        print(f"📊 Order status analysis: {e}")
    
    print("="*50)
    return "Summary completed successfully"

# Define tasks
load_customers = PythonOperator(
    task_id='load_customers',
    python_callable=load_csv_to_postgres,
    op_kwargs={'table_name': 'customers', 'csv_file': 'customers.csv'},
    dag=dag
)

load_products = PythonOperator(
    task_id='load_products',
    python_callable=load_csv_to_postgres,
    op_kwargs={'table_name': 'products', 'csv_file': 'products.csv'},
    dag=dag
)

load_orders = PythonOperator(
    task_id='load_orders',
    python_callable=load_csv_to_postgres,
    op_kwargs={'table_name': 'orders', 'csv_file': 'orders.csv'},
    dag=dag
)

load_order_items = PythonOperator(
    task_id='load_order_items',
    python_callable=load_csv_to_postgres,
    op_kwargs={'table_name': 'order_items', 'csv_file': 'order_items.csv'},
    dag=dag
)

validate_quality = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

generate_summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    dag=dag
)

# Task dependencies
[load_customers, load_products] >> load_orders >> load_order_items >> validate_quality >> generate_summary_task
