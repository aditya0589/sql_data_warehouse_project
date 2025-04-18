/*
This sql script creates a stored procedure for the silver layer of the data warehouse

Data from the bronze layer is extracted, cleaned and transformed. then loaded into the tables of the silver layer. 

to run this stored procedure : CALL silver.sp_transform_etl_silver();
*/


DELIMITER $$

CREATE PROCEDURE silver.sp_transform_etl_silver()
BEGIN
    -- Step 1: CRM Customer Info
    SELECT 'loading customer info' AS message;
    TRUNCATE TABLE silver.crm_customer_info;
    INSERT INTO silver.crm_customer_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
            WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
        END,
        CASE 
            WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
            WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
        END,
        cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM crm_customer_info
    ) t
    WHERE flag_last = 1;
    SELECT 'customer info complete' AS message;

    -- Step 2: Product Info
    SELECT 'loading product info' AS message;
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
        prd_id,
        cat_id,
        prd_key_test,
        prd_nm,
        prd_cost,
        prd_line,
        CAST(prd_start_dt AS DATE),
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
    FROM (
        SELECT 
            prd_id,
            prd_key,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key_test,
            prd_nm,
            IFNULL(prd_cost, 0) AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            END AS prd_line,
            prd_start_dt,
            prd_end_dt
        FROM bronze.crm_prd_info
    ) t;
SELECT 'loaded product info' AS message;
    -- Step 3: Sales Details
    SELECT 'sales details loading' AS message;
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prod_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prod_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
        END,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
        END,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
        END,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE ABS(sls_sales)
        END,
        sls_quantity,
        CASE 
            WHEN sls_price <= 0 OR sls_price IS NULL
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;
	SELECT 'sales details loaded' AS message;
    SELECT '-----------------------------------------' AS message;
    -- Step 4: ERP Customer Data
    SELECT 'erp customer details loading' AS message;
    TRUNCATE TABLE silver.erp_cust_a212;
    INSERT INTO silver.erp_cust_a212 (
        cid,
        bdate,
        gen
    )
    SELECT 
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) 
            ELSE cid 
        END,
        CASE 
            WHEN bdate > NOW() OR bdate < '1925-01-01' THEN NULL 
            ELSE bdate 
        END,
        CASE 
            WHEN TRIM(gen) = '' THEN NULL 
            WHEN UPPER(gen) = 'F' THEN 'Female'
            WHEN UPPER(gen) = 'M' THEN 'Male'
            WHEN gen = '0' THEN NULL
            ELSE gen
        END
    FROM bronze.erp_cust_a212;
	SELECT 'loaded erp customer details' AS message;
    -- Step 5: ERP Location Data
    SELECT 'loading erp location data' AS message;
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        cid,
        sntry
    )
    SELECT 
        REPLACE(cid, '-', ''),
        CASE 
            WHEN sntry = 'US' THEN 'United States'
            WHEN sntry = 'USA' THEN 'United States'
            WHEN sntry = 'DE' THEN 'Germany'
            ELSE sntry
        END
    FROM bronze.erp_loc_a101;
	SELECT 'loaded erp location data' AS message;
    -- Step 6: ERP Product Categories
    SELECT 'erp px category data loading' AS message;
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT 
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
    SELECT 'erp px category data loaded' AS message;
END$$

DELIMITER ;

