## Data-Wharehouse-Project

🗂️ Folder Structure
There are two main folders in the project:

1. datasets/
This folder contains two subfolders representing the data sources:

CRM/
customer_info.csv
product_info.csv
sales_details.csv

ERP/
Customer_info.csv
customer_location.csv
product_categories.csv

2. sql_files/
This folder contains the SQL transformation scripts:
bronze.sql → loads raw data from CSV files into staging tables.
silver_customer.sql, silver_product.sql → transform and clean the data.
gold.sql → joins the cleaned dimension tables with the fact table to build the final star schema.

🧾 Final Tables
The Gold layer includes a simplified star schema composed of:
dim_customers → Contains customer-related information.
dim_products → Contains product-related information.
fact_sales → Contains transactional sales data.

Relationships:
fact_sales.customer_key → joins with dim_customers.customer_key
fact_sales.product_key → joins with dim_products.product_key

📄 License
This project is for educational and non-commercial purposes only.
The dataset used in this project was originally shared by Baraa Khatib for training and learning purposes.

If you are the original creator and would like the data removed or credited differently, please feel free to reach out.

