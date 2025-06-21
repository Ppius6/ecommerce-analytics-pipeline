# E-commerce Data Pipeline

## 🎯 Project Aim

Build a complete data pipeline that transforms raw e-commerce data into analytics-ready tables for business intelligence and reporting. The pipeline processes customer orders, product data, and generates business metrics like customer segmentation, product performance, and daily sales analytics.

## 🛠️ Tools Used

- **Docker & Docker Compose** - Containerized infrastructure
- **Apache Airflow** - Workflow orchestration and scheduling
- **PostgreSQL** - Staging database for raw data
- **ClickHouse** - Analytics database for transformed data
- **Python** - Data generation and processing scripts

## 🚀 How to Run

### Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available

### Setup & Run

```bash
# 1. Clone the repository
git clone <your-repo-url>
cd ecommerce-data-pipeline

# 2. Start all services
docker-compose up -d

# 3. Generate sample data
docker-compose exec airflow-scheduler python /opt/airflow/scripts/generate_data.py

# 4. Access the services
```

### Access Points

- **Airflow UI**: <http://localhost:8081> (admin/admin)
- **ClickHouse**: <http://localhost:8123> (default/no password)
- **PostgreSQL**: localhost:5432 (postgres/postgres)

### Pipeline Execution

1. Open Airflow UI at <http://localhost:8081>
2. Enable the `ecommerce_data_pipeline` DAG
3. Trigger the DAG manually or wait for scheduled run
4. Monitor pipeline progress in Airflow
5. Query analytics tables in ClickHouse for insights

### Stop Services

```bash
docker-compose down
```

## 📊 Output

The pipeline creates analytics-ready tables:

- `analytics.customer_metrics` - Customer segmentation and behavior
- `analytics.product_performance` - Product sales and profitability  
- `analytics.daily_sales` - Daily aggregated sales metrics
