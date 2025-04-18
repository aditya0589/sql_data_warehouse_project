# sql_data_warehouse_project
This project implements an SQL-based Data Warehouse ETL system to transform raw operational data into cleansed, enriched, and structured data using MySQL. The system prepares data from multiple domains such as customer info, product data, sales transactions, and ERP records for downstream analytics, dashboards, and data science workflows.

Project Architecture:

![image](https://github.com/user-attachments/assets/a49b6880-0c9b-4653-9a79-40771e48b622)

*** ETL Type ***
Extract: Data sourced from bronze schema (raw ingestion layer)

Transform: Cleanup, deduplication, standardization, and enrichment in bronze layer

Load: Final structured data loaded into gold schema

Domain	Bronze Table	Silver Table	Description
Customer Info	crm_customer_info	silver.crm_customer_info	Deduplicated and standardized customer profiles
Product Info	crm_prd_info	silver.crm_prd_info	Clean product dimension with category and product line logic
Sales Data	crm_sales_details	silver.crm_sales_details	Clean transactional fact table with corrected metrics
ERP Customer	erp_cust_a212	silver.erp_cust_a212	Cleansed demographic info
ERP Location	erp_loc_a101	silver.erp_loc_a101	Standardized country codes
Product Categories	erp_px_cat_g1v2	silver.erp_px_cat_g1v2	Clean hierarchical product category

*** Skills Demonstrated ***
SQL Window Functions (ROW_NUMBER(), LEAD())

ETL Pipeline Design

Data Standardization

Date Parsing & Error Handling

Data Warehouse Modeling


