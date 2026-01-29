/* =====================================================
   TRANSFORM & LOAD – CRM Customer Info
   Source : Bronze.crm_cust_info
   Logic  :
      - Remove duplicates (keep latest record)
      - Trim text fields
      - Standardize marital status & gender
   ===================================================== */

-- Optional but recommended for re-run safety
-- TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info
(
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
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,

    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,

    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,

    cst_create_date
FROM
(
    SELECT *,
           ROW_NUMBER() OVER
           (
               PARTITION BY cst_id
               ORDER BY cst_create_date DESC
           ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;


/* =====================================================
   DATA QUALITY CHECKS – Silver.crm_prd_info
   Expectation: NO rows returned
   ===================================================== */


-- 1. Check for NULL or duplicated Product ID (Primary Key)
SELECT
    prd_id,
    COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1
    OR prd_id IS NULL;


-- 2. Check for unwanted leading/trailing spaces in product name
SELECT
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);


-- 3. Check for invalid product cost
-- Expectation: No negative or NULL values
SELECT
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0
   OR prd_cost IS NULL;


-- 4. Review standardized product line values
-- Expectation: Only known values (Mountain, Road, Touring, Other Sales, n/a)
SELECT DISTINCT
    prd_line
FROM silver.crm_prd_info;


-- 5. Check for invalid date ranges
-- End date should not be earlier than start date
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- 6. Final sanity check (optional – for manual review)
SELECT *
FROM silver.crm_prd_info;



/* =====================================================
   DATA QUALITY CHECKS – Silver.crm_sales_details
   PURPOSE :
     Validate dates, quantities, prices, and sales logic
   EXPECTATION :
     All queries should return NO rows
   ===================================================== */


--------------------------------------------------------
-- 1. Check for invalid Due Date values
--    - Zero or non-8-digit values
--    - Dates outside logical business range
--------------------------------------------------------
SELECT
    NULLIF(sls_due_dt, 0) AS sls_order_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <= 0
   OR LEN(sls_due_dt) != 8
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;


--------------------------------------------------------
-- 2. Check for invalid date sequence
--    - Order date should not be after ship or due date
--------------------------------------------------------
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;


--------------------------------------------------------
-- 3. Validate sales calculation logic
--    Compare original vs recalculated values
--------------------------------------------------------
SELECT DISTINCT
    sls_sales    AS old_sls_sales,
    sls_quantity,
    sls_price    AS old_sls_price,

    CASE
        WHEN sls_sales IS NULL
          OR sls_sales <= 0
          OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    CASE
        WHEN sls_price IS NULL
          OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY
    sls_sales,
    sls_quantity,
    sls_price;


--------------------------------------------------------
-- 4. Check for NULL critical business fields
--    (Order, Product, Customer)
--------------------------------------------------------
SELECT *
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL
   OR sls_prd_key IS NULL
   OR sls_cust_id IS NULL;


--------------------------------------------------------
-- 5. Check for duplicate order + product combinations
--    (Potential duplicate sales records)
--------------------------------------------------------
SELECT
    sls_ord_num,
    sls_prd_key,
    COUNT(*) AS duplicate_count
FROM silver.crm_sales_details
GROUP BY
    sls_ord_num,
    sls_prd_key
HAVING COUNT(*) > 1;


--------------------------------------------------------
-- 6. Check for unrealistic quantities or prices
--    (Business sanity check)
--------------------------------------------------------
SELECT *
FROM silver.crm_sales_details
WHERE sls_quantity > 100000
   OR sls_price > 1000000;


--------------------------------------------------------
-- 7. Final manual inspection (optional)
--------------------------------------------------------
SELECT *
FROM silver.crm_sales_details;


/* =====================================================
   Data Quality Checks – Silver Layer
   Table: Silver.erp_cust_az12
   Purpose: Validate cleaned customer data
   ===================================================== */

--------------------------------------------------------
-- 1️⃣ Birth Date validation
--    - Dates too old
--    - Future dates
--------------------------------------------------------
SELECT DISTINCT
    bdate
FROM Silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > CAST(GETDATE() AS DATE);



--------------------------------------------------------
-- 2️⃣ Gender domain check
--    - Ensure standardized values only
--------------------------------------------------------
SELECT DISTINCT
    gen
FROM Silver.erp_cust_az12;



--------------------------------------------------------
-- 3️⃣ NULL checks on key columns
--    - Customer ID should never be NULL
--------------------------------------------------------
SELECT *
FROM Silver.erp_cust_az12
WHERE cid IS NULL;



--------------------------------------------------------
-- 4️⃣ Duplicate Customer IDs
--    - One customer should appear once
--------------------------------------------------------
SELECT
    cid,
    COUNT(*) AS cnt
FROM Silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1;



--------------------------------------------------------
-- 5️⃣ Invalid or unexpected Gender values
--    - Defensive check (should not exist in Silver)
--------------------------------------------------------
SELECT *
FROM Silver.erp_cust_az12
WHERE gen NOT IN ('Male', 'Female', 'n/a')
   OR gen IS NULL;



--------------------------------------------------------
-- 6️⃣ Age sanity check
--    - Customers older than 100 years
--------------------------------------------------------
SELECT
    cid,
    bdate,
    DATEDIFF(YEAR, bdate, GETDATE()) AS calculated_age
FROM Silver.erp_cust_az12
WHERE bdate IS NOT NULL
  AND DATEDIFF(YEAR, bdate, GETDATE()) > 100;



--------------------------------------------------------
-- 7️⃣ Empty or whitespace-only Customer IDs
--------------------------------------------------------
SELECT *
FROM Silver.erp_cust_az12
WHERE LTRIM(RTRIM(cid)) = '';



/* =====================================================
   Data Quality Checks – Silver Layer
   Source Table: Bronze.erp_loc_a101
   Purpose: Validate location data before loading
   ===================================================== */

--------------------------------------------------------
-- 1️⃣ Check for NULL or empty Customer IDs
--------------------------------------------------------
SELECT *
FROM Bronze.erp_loc_a101
WHERE cid IS NULL
   OR LTRIM(RTRIM(cid)) = '';



--------------------------------------------------------
-- 2️⃣ Check for unwanted characters in Customer ID
--    Expectation: No '-' after cleaning
--------------------------------------------------------
SELECT DISTINCT
    cid
FROM Silver.erp_loc_a101
WHERE cid LIKE '%-%';



--------------------------------------------------------
-- 3️⃣ Country code validation (raw values)
--    Review all distinct country inputs
--------------------------------------------------------
SELECT DISTINCT
    cntry
FROM Silver.erp_loc_a101;



--------------------------------------------------------
-- 4️⃣ Empty or NULL country values
--------------------------------------------------------
SELECT *
FROM Bronze.erp_loc_a101
WHERE cntry IS NULL
   OR TRIM(cntry) = '';
----CHECK ilver.erp_loc_a101 
SELECT *
FROM Silver.erp_loc_a101

