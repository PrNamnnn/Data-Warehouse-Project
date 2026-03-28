# 📊 Data Warehouse Project

## 🏗 Architecture
This project follows a Medallion Architecture:

- **Bronze Layer** → Raw data ingestion
- **Silver Layer** → Cleaned and transformed data
- **Gold Layer** → Business-ready analytical models

---

## 🥇 Gold Layer Overview

The Gold Layer contains final analytical views:

### 1. dim_products
- Product-level information
- Includes category and cost details
- Only active products are considered

### 2. dim_customers
- Customer demographic data
- Combines CRM and ERP sources
- Standardized gender handling

### 3. fact_sales
- Core transactional table
- Linked with product and customer dimensions
- Contains sales metrics (amount, quantity, price)

---

## 🔗 Data Model
- `fact_sales` connects to:
  - `dim_products` via product_key
  - `dim_customers` via customer_key

---

## 🎯 Use Cases
- Sales analysis
- Customer segmentation
- Product performance tracking

---

## 🛠 Tech Stack
- SQL Server (T-SQL)
- Data Warehousing Concepts

---

## 👤 Author

**Naman Prabhakar**  
📧 naman.prabhakar2007@gmail.com
🔗 LinkedIn: www.linkedin.com/in/namanprabhakar
