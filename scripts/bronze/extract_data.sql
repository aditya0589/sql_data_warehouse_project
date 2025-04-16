/*
This script extracts the data from the csv files into the tables of the bronze layer
Since mysql doesnt natively support LOAD DATA LOCAL INPATH directly inside a stored procedure, we are using a sql script
*/


-- Enable LOCAL INFILE
SET GLOBAL local_infile = 1;
-- Use your schema
USE bronze;
SELECT "loading bronze layer" AS message;
-- Load crm_customer_info

SELECT "Loading CRM Tables" AS message;
TRUNCATE TABLE crm_customer_info;
LOAD DATA LOCAL INFILE 'datasets/cust_info.csv'
INTO TABLE crm_customer_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@cst_id, @cst_key, @cst_firstname, @cst_lastname, @cst_marital_status, @cst_gndr, @cst_create_date)
SET
    cst_id            = NULLIF(@cst_id, ''),
    cst_key           = NULLIF(@cst_key, ''),
    cst_firstname     = NULLIF(@cst_firstname, ''),
    cst_lastname      = NULLIF(@cst_lastname, ''),
    cst_marital_status= NULLIF(@cst_marital_status, ''),
    cst_gndr          = NULLIF(@cst_gndr, ''),
    cst_create_date   = STR_TO_DATE(NULLIF(@cst_create_date, ''), '%Y-%m-%d');

-- Load crm_prd_info
TRUNCATE TABLE crm_prd_info;
LOAD DATA LOCAL INFILE 'datasets/prd_info.csv'
INTO TABLE crm_prd_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@prd_id, @prd_key, @prd_nm, @prd_cost, @prd_line, @prd_start_dt, @prd_end_dt)
SET
    prd_id        = NULLIF(@prd_id, ''),
    prd_key       = NULLIF(@prd_key, ''),
    prd_nm        = NULLIF(@prd_nm, ''),
    prd_cost      = NULLIF(@prd_cost, ''),
    prd_line      = NULLIF(@prd_line, ''),
    prd_start_dt  = STR_TO_DATE(NULLIF(@prd_start_dt, ''), '%Y-%m-%d'),
    prd_end_dt    = STR_TO_DATE(NULLIF(@prd_end_dt, ''), '%Y-%m-%d');

-- Load crm_sales_details
TRUNCATE TABLE crm_sales_details;    
LOAD DATA LOCAL INFILE 'datasets/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@sls_ord_num, @sls_prod_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price)
SET
    sls_ord_num   = NULLIF(TRIM(@sls_ord_num), ''),
    sls_prod_key  = NULLIF(TRIM(@sls_prod_key), ''),
    sls_cust_id   = NULLIF(TRIM(@sls_cust_id), ''),
    sls_order_dt  = NULLIF(TRIM(@sls_order_dt), ''),
    sls_ship_dt   = NULLIF(TRIM(@sls_ship_dt), ''),
    sls_due_dt    = NULLIF(TRIM(@sls_due_dt), ''),
    sls_sales     = NULLIF(TRIM(@sls_sales), ''),
    sls_quantity  = NULLIF(TRIM(@sls_quantity), ''),
    sls_price     = NULLIF(TRIM(@sls_price), '');

SELECT "Loading ERP Tables" AS message;
-- Load erp_loc_a101
TRUNCATE TABLE erp_loc_a101;
LOAD DATA LOCAL INFILE 'datasets/LOC_A101.csv'
INTO TABLE erp_loc_a101
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@cid, @sntry)
SET
    cid   = NULLIF(TRIM(@cid), ''),
    sntry = NULLIF(TRIM(@sntry), '');

-- Load erp_cust_a212
TRUNCATE TABLE erp_cust_a212;
LOAD DATA LOCAL INFILE 'datasets/CUST_AZ12.csv'
INTO TABLE erp_cust_a212
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@cid, @bdate, @gen)
SET
    cid   = NULLIF(TRIM(@cid), ''),
    bdate = STR_TO_DATE(NULLIF(TRIM(@bdate), ''), '%Y-%m-%d'),
    gen   = NULLIF(TRIM(@gen), '');

-- Load erp_px_cat_g1v2
TRUNCATE TABLE erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE 'datasets/PX_CAT_G1V2.csv'
INTO TABLE erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@id, @cat, @subcat, @maintenance)
SET
    id          = NULLIF(TRIM(@id), ''),
    cat         = NULLIF(TRIM(@cat), ''),
    subcat      = NULLIF(TRIM(@subcat), ''),
    maintenance = NULLIF(TRIM(@maintenance), '');

