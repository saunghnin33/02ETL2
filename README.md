# 02ETL2
---

# 📄 Monthly Sales Data ETL Script Documentation

This script performs an ETL (Extract, Transform, Load) operation for monthly sales data stored in Excel files. The data is cleaned, transformed, and loaded into a PostgreSQL data warehouse.

---

## 📦 Dependencies

```python
import pandas as pd
import psycopg2
from tqdm import tqdm
import os
from dotenv import load_dotenv
```

- **pandas**: For data manipulation and reading Excel files.
- **psycopg2**: To connect and interact with PostgreSQL database.
- **tqdm**: For progress tracking during file iteration.
- **os** and **dotenv**: For loading environment variables securely.

---

## 🌐 Environment Setup

```python
load_dotenv()
```

Loads database connection credentials from a `.env` file to maintain security and flexibility.

---

## 🗄️ Database Table Names

```python
order_table='monthlysales.f_order'
customer_table='monthlysales.d_customer'
location_table='monthlysales.d_location'
product_table='monthlysales.d_product'
```

Defines the schema and tables where data will be inserted.

---

## 🔐 Database Credentials

```python
host = os.getenv('HOST')
...
password = os.getenv("PASSWORD")
```

Retrieves database credentials from environment variables.

---

## 📂 Load Excel Files

```python
folder_path = "D://02ETL//monthly_sales_version2"
files = os.listdir(folder_path)
```

Lists all Excel files from the specified directory for processing.

---

## 🔄 File Processing Loop

```python
for file in tqdm(files):
```

Processes each file in the folder:

### 1. **Read Excel File**

```python
df = pd.read_excel(excel_file_path)
```

### 2. **Rename Columns**

```python
columns_to_rename = [...]
df.columns = columns_to_rename
```

Standardizes column names to maintain consistency.

### 3. **Extract Relevant Columns for Orders**

```python
required_column = [...]
dff = df[required_column].copy()
```

Extracts only the columns necessary for the `f_order` table.

### 4. **Convert Date Columns**

```python
dff['order_date'] = pd.to_datetime(...)
```

Ensures date columns are in datetime format.

---

## 🔗 Database Connection

```python
conn = psycopg2.connect(**db_config)
cur = conn.cursor()
```

Establishes a connection with the PostgreSQL database.

---

## 🧩 Insert Dimensions and Facts

### ✅ Insert into `d_customer`

```python
for i, row in customer_df.iterrows():
    ...
    ON CONFLICT (customer_id) DO NOTHING;
```

Ensures no duplicate customers are inserted.

---

### ✅ Insert into `d_product`

```python
for i, row in product_df.iterrows():
    ...
    ON CONFLICT (product_id) DO NOTHING;
```

---

### ✅ Insert into `d_location`

```python
for i, row in location_df.iterrows():
    ...
    ON CONFLICT (postal_code) DO NOTHING;
```

---

### ✅ Insert into `f_order`

```python
for i, row in dff.iterrows():
    ...
```

No conflict handling – assumes each order is unique.

---

## ✅ Final Steps

```python
conn.commit()
cur.close()
conn.close()
```

Commits the transaction and closes the database connection.

---

## 📌 Notes

- Assumes each file contains non-overlapping data.
- No error handling – consider adding try-except blocks for robustness.
- Avoids inserting duplicates using `ON CONFLICT DO NOTHING` for dimension tables.

---
