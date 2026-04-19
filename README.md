# 🚀 E-Commerce Data Engineering Pipeline (Azure + Databricks + Power BI)

## 📌 Project Overview

Built an **end-to-end data engineering pipeline** that ingests raw CSV data, processes it using **PySpark (Databricks)**, stores it in **Delta Lake**, and exposes insights through **Power BI dashboards**.

This project simulates a **real-world production pipeline** with multiple datasets and automated workflows.

---

## 🏗️ Architecture

```
Raw CSV (Landing Zone 1 - ADLS)
        ↓
Azure Data Factory (Ingestion)
        ↓
Parquet (Landing Zone 2 - to_processed_data)
        ↓
Databricks (PySpark)
        ↓
Bronze → Silver (Delta Tables)
        ↓
Gold Layer (Final Table)
        ↓
Databricks SQL
        ↓
Power BI Dashboard
```
## Architecture

<img width="738" height="552" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/a2b509e6-c0d5-45e8-98fa-997f02b7fe6d" />

---

## 📂 Data Layers (Medallion Architecture)

### 🥉 Bronze Layer
- Raw data stored in Delta format  
- Minimal transformation  
- Schema enforcement  

### 🥈 Silver Layer
- Data cleaning  
- Type casting  
- Null handling  
- Deduplication  

### 🥇 Gold Layer
- Aggregation + joins  
- Business-ready dataset  
- Final table: `ecom_one_big_table`  

---

## 📊 Datasets

- Users  
- Buyers  
- Sellers  
- Countries  

Each dataset has:
- Independent ADF pipeline  
- Separate Bronze & Silver tables  

---

## ⚙️ Pipelines (ADF)

### 🔹 4 Independent Pipelines

| Pipeline | Purpose |
|--------|--------|
| pipeline_users | Process users data |
| pipeline_buyers | Process buyers data |
| pipeline_sellers | Process sellers data |
| pipeline_countries | Process countries data |

Each pipeline performs:

```
CSV → Parquet → Databricks (Bronze → Silver) → Archive
```

---

### 🔹 Gold Pipeline

- Reads all Silver tables  
- Handles **data availability checks**  
- Performs joins + aggregation  
- Writes final Delta table  

---

## 🧠 Key Engineering Concepts Implemented

- Event-driven pipelines (ADF triggers)  
- Delta Lake schema handling (`overwriteSchema`)  
- File lifecycle management (staging → processed)  
- Data validation before processing  
- Handling schema mismatch issues  
- Aggregation before joins to avoid duplication  
- Modular pipeline design  

---

## 🛠️ Technologies Used

- Azure Data Factory  
- Azure Data Lake Storage Gen2  
- Azure Databricks  
- PySpark  
- Delta Lake  
- SQL (Databricks SQL)  
- Power BI  

---

## 📊 Dashboard (Power BI)

### 📌 Key Visuals

- KPI Cards (Total Buyers, Sellers, Engagement)  
- Country-wise engagement analysis  
- Buyer vs Seller comparison  
- Market opportunity detection  
- User activity trends  

---

## 📁 Power BI File

Download dashboard:

👉 [ecommerce_dashboard.pbix](https://github.com/BOSS6292/ecommerce-data-engineering-pipeline/blob/main/images/Power%20BI.pdf)

---

## 📊 Example SQL Queries

```sql
SELECT 
    Country,
    SUM(Users_productsSold + Buyers_Total + Sellers_Total) AS engagement_score
FROM ecom_gold_table
GROUP BY Country
ORDER BY engagement_score DESC;
```

```sql
SELECT 
    Country,
    SUM(Buyers_Total) / NULLIF(SUM(Sellers_Total), 0) AS ratio
FROM ecom_gold_table
GROUP BY Country;
```

---

## 💡 Business Insights

- Identified countries with **high demand but low supply**  
- Analyzed **buyer-seller imbalance**  
- Measured **user engagement trends**  
- Derived **market expansion opportunities**  

---

## 🚧 Challenges Solved

- Delta schema mismatch errors  
- Empty dataset handling  
- Incorrect data types (string → numeric in Power BI)  
- Duplicate data due to improper joins  
- Synchronizing multiple pipelines  

---

## 📈 Future Improvements

- Switch from `overwrite` → `append` (incremental loads)  
- Add orchestration using Airflow  
- Build real-time streaming pipeline  
- Add data quality checks  
- Integrate CI/CD  

---

## 🎯 Why This Project Stands Out

✔ End-to-end pipeline (ingestion → transformation → analytics)  
✔ Real-world cloud architecture  
✔ Business-focused analytics  
✔ Dashboard visualization  
✔ Handles real production issues  

---

## 🧠 What I Learned

- Designing scalable data pipelines  
- Working with Delta Lake and Spark  
- Handling real-world data issues  
- Building analytical dashboards  
- Connecting engineering with business insights  

---

## 👨‍💻 Author

**Tejas Bomble**  
Aspiring Data Engineer  

---

## ⭐ If you like this project

Give it a ⭐ and feel free to connect!
