/*
This script creates the views for the gold layer tables
applies all the business logic in the data
*/
-- customers view
CREATE VIEW gold.dim_customers AS
  SELECT 
  	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
  	ci.cst_id AS customer_id,
  	ci.cst_key AS customer_number,
  	ci.cst_firstname AS customer_firstname,
  	ci.cst_lastname AS customer_lastname,
  	ci.cst_marital_status AS marital_status,
  	CASE WHEN ci.cst_gndr IS NOT NULL THEN ci.cst_gndr
  		WHEN ca.gen IS NULL THEN ci.cst_gndr
          WHEN ca.gen != ci.cst_gndr THEN ci.cst_gndr
  		ELSE ca.gen
  	END customer_gender,
  	ci.cst_create_date AS create_date,
      ca.bdate AS customer_dob,
      lo.sntry AS customer_country
  FROM silver.crm_customer_info AS ci
  LEFT JOIN silver.erp_cust_a212 AS ca
  ON ci.cst_key = ca.cid
  LEFT JOIN silver.erp_loc_a101 AS lo
  ON ci.cst_key = lo.cid;


-- products view
CREATE VIEW gold.dim_products AS
  SELECT 
    ROW_NUMBER() OVER (ORDER BY prd_start_dt) AS product_num,
    prd_id AS product_id,
    prd_key AS product_key,
    prd_nm AS product_name,
    prd_cost AS product_cost,
    prd_line AS product_line,
    prd_start_dt AS start_date,
    cat_id AS category_id,
    cat.cat AS category,
    cat.subcat AS sub_category,
    cat.maintenance AS maintenance
  FROM silver.crm_prd_info AS pr
  LEFT JOIN silver.erp_px_cat_g1v2 AS cat
  ON pr.cat_id = cat.id
  WHERE prd_end_dt IS NULL;


-- sales view
CREATE VIEW gold.fact_sales AS
  SELECT 
    sd.sls_ord_num AS order_number,
    pr.product_num,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
  FROM silver.crm_sales_details AS sd
  LEFT JOIN dim_products AS pr
    ON sd.sls_prod_key = pr.product_key
  LEFT JOIN dim_customers AS cu
    ON sd.sls_cust_id = cu.customer_id



