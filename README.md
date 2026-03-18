# 🛒 E-Commerce Data Pipeline & Dashboard

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791?style=flat&logo=postgresql&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-Public-E97627?style=flat&logo=tableau&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-2.0+-150458?style=flat&logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0+-red?style=flat)

A portfolio-ready, end-to-end data engineering project that extracts raw e-commerce data, transforms it through a structured ETL pipeline, loads it into a PostgreSQL star-schema data warehouse, and presents business insights through an interactive Tableau dashboard.

**[View Live Dashboard](https://public.tableau.com/views/Book1_17738389756430/Dashboard1)**

---

## Key Findings

Analysing **96,478 orders** worth **13.2M euros** in revenue from the Brazilian Olist e-commerce platform:

- Revenue grew consistently from late 2016 to mid-2018, with a sharp Black Friday spike in November 2017
- Health & Beauty is the top revenue category, followed by Watches & Gifts and Bed/Bath/Table
- Average delivery time is 12.6 days — states in the North (AM, RR, AP) experience the longest delays
- 59% of customers gave 5-star reviews — overall average review score is 4.16/5
- 8.1% of delivered orders were late — concentrated in remote northern states

---

## Architecture

```
Data Source  -->  ETL Pipeline  -->  Data Warehouse  -->  Dashboard
Kaggle Olist      extract.py        PostgreSQL 18        Tableau Public
8 CSV files       transform.py      Star Schema          4-page dashboard
~100K orders      load.py           7 views              Live & public
```

Data flow: CSV -> extract.py -> transform.py -> load.py -> staging schema -> SQL transformations -> warehouse schema -> Tableau

---

## Star Schema

```
                    dim_date
                       |
         dim_customer -+
                       |
                  fact_orders
                       |
          dim_product -+
                       |
           dim_seller -+
```

| Table | Rows | Description |
|---|---|---|
| fact_orders | 96,478 | Central fact table with all order metrics |
| dim_customer | 96,478 | Customer dimension with city/state |
| dim_product | 32,951 | Product dimension with translated categories |
| dim_seller | 3,095 | Seller dimension with location |
| dim_date | 1,461 | Date dimension (2016-2019) |

---

## Project Structure

```
ecommerce-pipeline/
├── src/
│   ├── extract.py          # Reads 8 CSVs, validates row counts, adds metadata
│   ├── transform.py        # Cleans and enriches all datasets (7 functions)
│   ├── load.py             # Loads to PostgreSQL via SQLAlchemy with upsert logic
│   └── pipeline.py         # Orchestrator: runs E->T->L with logging & --dry-run
├── sql/
│   ├── 01_create_schema.sql
│   ├── 02_create_dimensions.sql
│   ├── 03_create_facts.sql
│   ├── 04_load_dimensions.sql
│   ├── 05_load_facts.sql
│   └── 06_create_views.sql
├── data/
│   ├── raw/                # Source CSVs (gitignored)
│   └── processed/          # Exported CSVs for Tableau
├── tests/
│   └── test_pipeline.py    # pytest unit tests
├── notebooks/              # Jupyter EDA notebooks
├── .env.example            # Environment variable template
├── requirements.txt
└── README.md
```

---

## Setup & Reproduction

### Prerequisites

- Python 3.10+
- PostgreSQL 15+ (or Postgres.app on Mac)
- Kaggle account to download the dataset

### 1. Clone the repo

```bash
git clone https://github.com/ughitsashwin/ecommerce-pipeline.git
cd ecommerce-pipeline
```

### 2. Set up Python environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Download the dataset

Download the Brazilian E-Commerce Public Dataset by Olist from Kaggle (https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all CSV files in data/raw/.

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit .env and set your PostgreSQL connection:

```
DATABASE_URL=postgresql+psycopg2://your_username@localhost:5432/ecommerce
```

### 5. Create the database

```bash
psql -U postgres -c "CREATE DATABASE ecommerce;"
```

### 6. Run the SQL schema scripts

```bash
psql -U postgres -d ecommerce -f sql/01_create_schema.sql
psql -U postgres -d ecommerce -f sql/02_create_dimensions.sql
psql -U postgres -d ecommerce -f sql/03_create_facts.sql
```

### 7. Run the ETL pipeline

```bash
# Dry run (extract + transform only, no DB write)
python src/pipeline.py --dry-run

# Full run
python src/pipeline.py
```

### 8. Load the warehouse

```bash
psql -U postgres -d ecommerce -f sql/04_load_dimensions.sql
psql -U postgres -d ecommerce -f sql/05_load_facts.sql
psql -U postgres -d ecommerce -f sql/06_create_views.sql
```

### 9. Verify it worked

```bash
psql -U postgres -d ecommerce -c "SELECT * FROM warehouse.vw_executive_kpis;"
```

Expected output:
```
total_orders | total_revenue | avg_order_value | avg_delivery_days | avg_review_score | overall_late_pct
96478        | 13221498.11   | 137.04          | 12.6              | 4.16             | 8.1
```

---

## Running Tests

```bash
pytest tests/ -v
```

Tests cover transformation functions, null handling, date parsing, row count validation, and enrichment calculations.

---

## Dashboard

**View Live Dashboard: https://public.tableau.com/views/Book1_17738389756430/Dashboard1**

| Page | Charts | Insight |
|---|---|---|
| Monthly Revenue Trend | Line chart | Black Friday spike in Nov 2017 |
| Revenue by Category | Horizontal bar + color | Health & Beauty leads by revenue |
| Late Delivery by State | Bar + reference line | Northern states have highest delay rates |
| Review Score Distribution | Bar chart | 59% of orders receive 5-star reviews |

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.10+ | ETL pipeline |
| pandas | Data transformation |
| SQLAlchemy | Database ORM |
| PostgreSQL 18 | Data warehouse |
| Tableau Public | Dashboard & visualisation |
| pytest | Unit testing |
| python-dotenv | Environment config |

---

## Future Improvements

- Orchestration with Apache Airflow for automated daily runs
- Replace raw SQL transformations with dbt models
- Add Great Expectations for automated data validation
- GitHub Actions CI/CD to run tests on every push
- Dockerise the full stack for one-command reproducibility
- Merge in a second data source (weather or economic data)

---

## Dataset

Brazilian E-Commerce Public Dataset by Olist — released under CC BY-NC-SA 4.0 license.
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

Built as a portfolio project demonstrating end-to-end data engineering: pipeline design, warehouse modelling, and business intelligence.
