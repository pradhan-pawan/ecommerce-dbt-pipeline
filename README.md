# 🛒 E-Commerce Analytics Pipeline
### DBT + Snowflake + Python

![DBT](https://img.shields.io/badge/dbt-1.11.7-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-AWS-blue)
![Python](https://img.shields.io/badge/Python-3.13-green)

## 🏗️ Architecture
```
Snowflake RAW Schema
        ↓
DBT Staging (Views)
stg_customers | stg_orders | stg_products
        ↓
DBT Gold (Tables)
gold_customer_summary | gold_product_performance | gold_sales_trends
```

## 📦 Tech Stack

| Tool | Purpose |
|------|---------|
| Snowflake (AWS) | Cloud Data Warehouse |
| DBT Core 1.11 | Transformations + Testing + Docs |
| Python + uv | Environment Management |
| GitHub | Version Control |

## 📁 Project Structure
```
ecommerce_dbt/
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── schema.yml
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   └── stg_products.sql
│   └── gold/
│       ├── gold_customer_summary.sql
│       ├── gold_product_performance.sql
│       └── gold_sales_trends.sql
├── snapshots/
│   └── customers_snapshot.sql
├── macros/
│   └── utils.sql
└── dbt_project.yml
```

## 🔄 Data Lineage
```
ecommerce_raw.customers → stg_customers → gold_customer_summary
ecommerce_raw.orders    → stg_orders    → gold_sales_trends
ecommerce_raw.products  → stg_products  → gold_product_performance
```

## ✅ Data Quality Tests

- 12 tests across all staging models
- unique, not_null, accepted_values constraints
- Run with: `dbt test`

## 🚀 How to Run
```bash
# Setup
uv venv .venv
.venv\Scripts\activate
uv pip install dbt-core dbt-snowflake

# Run all models
cd ecommerce_dbt
dbt run

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

## 📊 Gold Layer Models

| Model | Description | Rows |
|-------|-------------|------|
| gold_customer_summary | Customer spend, loyalty tier, cancel rate | 20 |
| gold_product_performance | Revenue by product and category | 20 |
| gold_sales_trends | Monthly MoM trends with window functions | 20 |

## 👤 Author
Pawan Pradhan — [GitHub](https://github.com/pradhan-pawan)