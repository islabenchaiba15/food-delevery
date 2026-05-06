# QuickEats — End-to-End Food Delivery SaaS Analytics Pipeline
### Data Engineering · Data Warehouse · Power BI Dashboards | 2020–2024

---

## 📋 Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture](#architecture)
3. [Business Problem](#business-problem)
4. [Methodology](#methodology)
5. [Skills](#skills)
6. [Results & Business Recommendations](#results--business-recommendations)
7. [Next Steps](#next-steps)

---

## 1. Executive Summary

QuickEats is a B2B2C SaaS food delivery platform serving restaurants, cloud kitchens, and food businesses across the United States. This project delivers a **production-grade, end-to-end analytics solution** — from raw data ingestion through a medallion data warehouse to executive-ready Power BI dashboards — covering **100,000 orders, 19,870 unique customers, and $15.02M in total revenue** across the 2020–2024 period.

The pipeline was built on **Apache Kafka → S3 Bucket → dbt → Data Warehouse**, orchestrated by **Apache Airflow**, and visualised in **Power BI** through two purpose-built dashboards: *Revenue & Operations* and *Subscription & SaaS Metrics*. The analysis uncovered a near-total MRR collapse driven by cross-tier restaurant churn, a critically concentrated revenue base in High Value customers, and a 35% order non-completion rate — translating into four prioritised business recommendations with quantified revenue impact.

---

## 2. Architecture

### Pipeline Overview

```
[Data Sources]  →  [Streaming]  →  [Storage]   →  [Data Warehouse]  →  [BI Layer]
  APIs / CSVs       Apache Kafka     S3 Bucket       Bronze / Silver        Power BI
  (10 tables)       (real-time)      (Data Storage)  / Gold (dbt)           Dashboards
```

> 📌 *Full architecture diagram available in `/assets/architecture.png`*

### Medallion Architecture

The warehouse follows a **three-layer medallion architecture**, managed entirely through **dbt** models and orchestrated by **Apache Airflow**:

| Layer | Name | Description |
|---|---|---|
| 🥉 Bronze | Raw | Raw ingested data landed in the **S3 Bucket** — no transformations, preserving source fidelity |
| 🥈 Silver | Cleaned | Deduplicated, type-cast, null-handled, and standardised data ready for modelling |
| 🥇 Gold | Business Ready | Aggregated, joined, and metric-enriched tables powering dashboards and reporting |

### Data Flow

- **Ingestion:** 10 source CSV tables (orders, payments, restaurants, users, customers, drivers, delivery, reviews, promotions, billing) are streamed via **Apache Kafka** and landed in an **S3 Bucket** as the raw storage layer, forming the Bronze tier of the medallion architecture
- **Transformation:** **dbt** reads directly from the **S3 Bucket** (Bronze layer) and applies cleaning, joins, and business logic transformations through Silver and Gold layers, producing a **Star Schema** optimised for analytical querying
- **Orchestration:** **Apache Airflow** schedules and monitors all pipeline DAGs — from ingestion through dbt model runs to dashboard refresh triggers
- **Visualisation:** **Power BI** connects to the Gold layer and surfaces two dashboards covering Revenue & Operations and Subscription & SaaS Metrics

---

## 3. Business Problem

### Context

QuickEats operates a dual-revenue model: **monthly SaaS subscription fees** from restaurant clients (across four plan tiers — Starter, Growth, Pro, Enterprise) and **per-order delivery commissions**. Without centralised, reliable reporting, the business had no visibility into why revenue was fluctuating, which customers or restaurant tiers were at risk, or where operational gaps were eroding margins.

### The Problem

| Dimension | Challenge |
|---|---|
| **Revenue** | Monthly gross revenue swings between –10% and +17% MoM with no clear explanation |
| **Subscriptions** | MRR declining from $0.45M (Q2 2021) toward near-zero by Q2 2025 — cause unknown |
| **Customers** | Revenue concentration risk across customer value segments was unquantified |
| **Operations** | Delivery completion rate and order cancellation patterns were unmonitored |
| **Churn** | Enterprise clients — the highest-paying tier — churning at the highest rate |

### Why It Matters

Without this analysis, the business risks losing its highest-value restaurant clients silently, continuing to invest in low-yield subscriber segments, and missing a $676K+ annual revenue opportunity sitting in improved delivery execution. The dashboards and pipeline built here give leadership a **single source of truth** for every revenue, subscription, and operational decision.

**Stakeholders impacted:** Executive leadership, Head of Revenue, Customer Success, Product, and Operations teams.

---

## 4. Methodology

### A. Data Engineering

#### Data Ingestion
- **Sources:** 10 structured CSV tables totalling over 343,000 records, covering the full platform data model — orders, payments, subscriptions, deliveries, users, and reviews
- **Transport:** Apache Kafka streams data from platform APIs and operational databases into the pipeline in real time
- **Storage:** Raw files are landed into an **S3 Bucket** (partitioned by source table and ingestion date), serving as the centralised raw storage layer and the entry point to the Bronze tier

#### Data Modelling (Medallion Architecture)

**Bronze → Raw Layer**
- Store all source records exactly as received — no filtering, no casting
- Preserves full audit trail for data lineage and debugging

**Silver → Cleaned Layer**
- Null handling across critical fields (order status, payment method, restaurant tier)
- Type standardisation: dates parsed, monetary fields cast to `DECIMAL(10,2)`, categorical fields normalised
- Deduplication using `ROW_NUMBER()` window functions on order and payment primary keys
- Incremental loading strategy to process only new/changed records per run

**Gold → Business Ready Layer**
- **Fact tables:** `fact_orders`, `fact_payments`, `fact_deliveries`, `fact_billing`
- **Dimension tables:** `dim_restaurants`, `dim_customers`, `dim_users`, `dim_promotions`, `dim_drivers`
- **Aggregated marts:** Monthly revenue summary, MRR by plan tier by quarter, customer value segment rollup, payment method distribution
- Star schema design optimised for Power BI's DirectQuery and Import modes

#### Data Quality & Consistency
- dbt **tests** applied at every layer: `not_null`, `unique`, `accepted_values`, and `relationships` across all primary and foreign keys
- Row count reconciliation checks between Bronze and Silver to catch ingestion gaps
- Airflow DAG alerts on pipeline failure with automated Slack notification hooks

---

### B. Data Analysis

#### KPI Definition

All KPIs were formally defined, documented, and implemented as consistent DAX measures across both dashboards:

| KPI | Formula | Dashboard |
|---|---|---|
| Total Revenue | Subscription Revenue + Gross Revenue | Both |
| Gross Revenue | Gross Bookings – Refunds – Discounts | Revenue & Ops |
| Net Revenue | Gross Revenue – Operating Costs | Revenue & Ops |
| MRR | SUM(active restaurant fees) per month | SaaS Metrics |
| Churn Rate | (Restaurants Lost ÷ Start Count) × 100 | SaaS Metrics |
| Delivery Rate | (Delivered Orders ÷ Total Orders) × 100 | Revenue & Ops |
| AOV | Gross Revenue ÷ Total Delivered Orders | Revenue & Ops |

#### Dashboard Design

**Dashboard 1 — Revenue & Operations**
Designed for the Head of Revenue and Operations leadership. Surfaces 10 headline KPI cards and six analytical charts covering monthly revenue trends, payment method distribution, customer value segmentation, order status breakdown, geographic revenue spread, and cancellation/refund time series.

**Dashboard 2 — Subscription & SaaS Metrics**
Designed for the CEO and Customer Success leadership. Tracks MRR over time, active restaurant counts by plan tier, churn rate by tier, subscription volume, and revenue per subscriber — the critical metrics for a SaaS business's health.

#### Exploratory Data Analysis
- Time-series decomposition of monthly revenue to separate trend, cyclicality, and promotional noise
- Pareto analysis of customer value segments confirming the 69%/~20% revenue concentration pattern
- Cohort-style analysis of restaurant churn by plan tier and subscription quarter
- Payment method cross-tabulation with AOV to test revenue-per-transaction hypothesis

---

## 5. Skills

### 🔧 Data Engineering

| Skill | Application |
|---|---|
| **SQL — Advanced** | CTEs, window functions (`ROW_NUMBER`, `LAG`, `LEAD`), conditional aggregations, incremental merge logic |
| **dbt** | Multi-layer model development (Bronze/Silver/Gold), dbt tests, documentation, incremental materialisation strategies |
| **Apache Kafka** | Real-time data streaming from platform APIs to the S3 Bucket storage layer |
| **Amazon S3 Bucket** | Raw data storage with date-based partitioning — serves as the Bronze layer entry point for all dbt transformations |
| **Apache Airflow** | DAG design, dependency management, pipeline scheduling, failure alerting |
| **Data Modelling** | Star Schema design — fact and dimension table architecture optimised for analytical workloads |
| **ETL / ELT** | Full ELT pipeline from API ingestion through dbt transformation to warehouse-ready Gold tables |
| **Data Quality** | dbt test suites, row-count reconciliation, null/duplicate detection across pipeline layers |

---

### 📊 Data Analysis

| Skill | Application |
|---|---|
| **Power BI** | Two-dashboard solution with 10+ KPI cards, dual-axis line charts, donut charts, bar charts, maps, and slicers |
| **DAX** | Custom measures for MoM % growth, churn rate, revenue per subscriber, delivery completion rate, refund rate |
| **KPI Framework Design** | End-to-end KPI definition, formula documentation, and dashboard implementation |
| **Business Analysis** | Translating ambiguous revenue trends into specific, actionable findings with quantified business impact |
| **Storytelling with Data** | Structuring insights around business questions, not just metrics — from "what happened" to "what to do" |
| **Customer Segmentation** | Pareto and cohort analysis on High/Medium/Low Value segments to map revenue concentration risk |

---

## 6. Results & Business Recommendations

### Key Findings

#### Revenue & Operations

| Finding | Detail |
|---|---|
| 💰 **$15.02M total revenue** generated over five years — 42% from SaaS subscriptions, 58% from order commissions | Platform is genuinely dual-revenue, reducing single-stream dependency |
| 📉 **35.1% of orders never completed delivery** | At AOV of $135.22, this represents the single largest operational gap on the platform |
| 💳 **52.36% of gross revenue flows through just two payment methods** | Credit Card (28.27%) + Debit Card (24.09%) — gateway concentration is a business risk |
| 👥 **~69% of gross revenue generated by High Value customers** | Pareto concentration — losing even 10–15% of this segment costs ~$600K–$900K in gross revenue |
| 🎯 **Net margin of only 21%** | $1.70M net on $8.67M gross — margin improvement is the platform's top strategic lever |

#### Subscription & SaaS Metrics

| Finding | Detail |
|---|---|
| 📉 **MRR collapsed from $0.45M → ~$0.02M** between Q2 2021 and Q2 2025 — an 95%+ decline | Driven entirely by restaurant churn across all four plan tiers simultaneously |
| 🏢 **Enterprise churn rate: 13.5%** — the highest of all tiers | Each lost Enterprise client costs $11,988/year. No dedicated Customer Success function exists |
| 🔢 **More subscribers ≠ more revenue** | Enterprise (1.36K subscribers) generates $1,353/subscriber vs. Starter (2.36K subscribers) at $364/subscriber |
| 📊 **Pro plan leads on both volume and revenue** | 2.51K subscribers, $2.06M revenue (32.41% share) — the most balanced tier by unit economics |

---

### ✅ Actionable Recommendations

**Priority 1 — Stop the MRR Bleed** *(Urgency: Immediate)*
Implement a restaurant health score that monitors order volume, login frequency, and contract renewal proximity across all plan tiers. Trigger automated outreach workflows for any account falling below health threshold. Focus first on Growth tier (highest absolute churn volume) and Enterprise (highest revenue per lost account).

**Priority 2 — Protect the High Value Customer Base** *(Urgency: Immediate)*
With ~69% of gross revenue concentrated in High Value customers, retention investment here has the highest ROI of any action available. Launch a priority support tier, personalised promotion engine, and loyalty rewards programme exclusively for this segment before allocating budget to new customer acquisition.

**Priority 3 — Improve Delivery Completion Rate** *(Urgency: Short-Term)*
The 64.9% delivery completion rate means 35,100 orders were placed but not fulfilled. A 5-percentage-point improvement at AOV $135.22 generates **~$676K in additional gross revenue annually at zero acquisition cost**. Investigate driver assignment failures, cancellation triggers, and order-to-dispatch time gaps in the delivery data.

**Priority 4 — Upsell Starter Subscribers** *(Urgency: Medium-Term)*
Starter subscribers generate $364/subscriber vs. $1,353 for Enterprise — a 3.7x gap in unit value. A structured upgrade pathway triggered at defined usage thresholds (order volume, menu size, time on platform) converting just 15% of the 2.36K Starter base to Growth tier would generate **~$235K in incremental annual subscription revenue**.

---

## 7. Next Steps

### Pipeline & Engineering
- [ ] **Real-time streaming completion:** Extend Kafka consumers to ingest order and delivery events with sub-minute latency, enabling live dashboard refresh instead of daily batch loads
- [ ] **Incremental dbt models at scale:** Migrate all Silver and Gold models to dbt incremental materialisation using `merge` strategy to reduce warehouse compute costs as data volume grows
- [ ] **Data contract enforcement:** Implement schema validation at the Kafka consumer layer to catch upstream data model changes before they propagate to the warehouse
- [ ] **Automated pipeline monitoring:** Build Airflow SLA miss alerts and a pipeline health dashboard tracking row counts, null rates, and test pass rates per run

### Analytics & BI
- [ ] **Restaurant health score model:** Build a composite churn risk score using order frequency, login recency, support ticket volume, and subscription payment status — surfaced as a Power BI alert layer for Customer Success
- [ ] **Forecasting:** Implement MRR and gross revenue 90-day forecasting using Prophet or ARIMA, embedded directly in the Power BI subscription dashboard
- [ ] **Cohort analysis dashboard:** Build a restaurant retention cohort view showing monthly survival rates by plan tier and signup quarter — essential for understanding where in the customer lifecycle churn is highest
- [ ] **Customer LTV modelling:** Develop a predictive Lifetime Value model for both restaurant subscribers and end consumers to prioritise acquisition and retention spend by expected value

### Data Quality & Governance
- [ ] **dbt documentation site:** Publish the full dbt docs site (`dbt docs generate`) as a living data dictionary accessible to all business stakeholders
- [ ] **PII masking layer:** Implement column-level masking in the Silver layer for customer PII fields (email, phone, address) to ensure GDPR and CCPA compliance in all Gold-layer analytics outputs
- [ ] **Source freshness monitoring:** Add dbt `source freshness` checks to alert on stale ingestion from any of the 10 source tables

---

## 🗂️ Repository Structure

```
quickeats-analytics/
│
├── dbt/
│   ├── models/
│   │   ├── bronze/          # Raw source staging models
│   │   ├── silver/          # Cleaned and typed intermediate models
│   │   └── gold/            # Business-ready fact/dim/mart models
│   ├── tests/               # dbt data quality tests
│   └── dbt_project.yml
│
├── airflow/
│   └── dags/                # Pipeline DAGs (ingestion, transformation, refresh)
│
├── powerbi/
│   ├── revenue_operations.pbix
│   └── saas_metrics.pbix
│
├── assets/
│   └── architecture.png     # Pipeline architecture diagram
│
├── reports/
│   └── QuickEats_Analysis_Report.pdf
│
└── README.md
```

---

## 🛠️ Tech Stack

| Category | Tools |
|---|---|
| **Streaming** | Apache Kafka |
| **Storage** | Amazon S3 Bucket |
| **Transformation** | dbt (Data Build Tool) |
| **Orchestration** | Apache Airflow |
| **Visualisation** | Microsoft Power BI |
| **Query Language** | SQL (Advanced) |
| **Metrics Layer** | DAX (Power BI) |
| **Architecture** | Medallion (Bronze / Silver / Gold) |

---

*QuickEats Analytics Engineering Project | Data Pipeline & Business Intelligence | 2020–2024*
