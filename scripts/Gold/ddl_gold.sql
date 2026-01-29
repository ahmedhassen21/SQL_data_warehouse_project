/* =========================================================
   View Name   : gold.fact_sales
   Layer       : Gold
   Purpose     : Sales Fact Table
                 - Store transactional sales data
                 - Link sales with product and customer dimensions
   Grain       : One row per order line
   Notes       :
     - Uses CRM sales details as the fact source
     - Joins with product & customer dimensions using business keys
========================================================= */

CREATE VIEW gold.fact_sales AS
SELECT 
    -- Order identifier
    sd.sls_ord_num   AS order_number,

    -- Dimension keys
    pr.product_key,
    cu.customer_key,

    -- Date attributes
    sd.sls_order_dt  AS order_date,
    sd.sls_ship_dt   AS shipping_date,
    sd.sls_due_dt    AS due_date,

    -- Measures
    sd.sls_sales     AS sales_amount,
    sd.sls_quantity  AS quantity,
    sd.sls_price     AS price

FROM Silver.crm_sales_details sd

-- Join product dimension
LEFT JOIN Gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number

-- Join customer dimension
LEFT JOIN Gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;


/* =========================================================
   View Name   : gold.dim_customers
   Layer       : Gold
   Purpose     : Customer Dimension
                 - Combine CRM customer info
                 - Enrich with ERP demographic & location data
                 - Generate surrogate customer_key
   Notes       :
     - CRM is the master source for gender when available
     - ERP data used as fallback
========================================================= */

CREATE VIEW gold.dim_customers AS 
SELECT 
    -- Surrogate key generated for the dimension
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,

    -- Business identifiers
    ci.cst_id          AS customer_id,
    ci.cst_key         AS customer_number,

    -- Customer name attributes
    ci.cst_firstname   AS first_name,
    ci.cst_lastname    AS last_name,

    -- Location attribute
    la.cntry           AS country,

    -- Marital status from CRM
    ci.cst_marital_status AS marital_status,

    -- Gender logic:
    -- CRM is the master source
    -- Fallback to ERP if CRM value is 'n/a'
    CASE 
        WHEN ci.cst_gndr != 'n/a' 
            THEN ci.cst_gndr   -- CRM IS THE MASTER OF GENDER INFO
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    -- Demographic attribute from ERP
    ca.bdate AS birthdate,

    -- Record creation date from CRM
    ci.cst_create_date AS create_date

FROM Silver.crm_cust_info ci

-- Join ERP customer demographic data
LEFT JOIN Silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid

-- Join ERP location data
LEFT JOIN Silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

/* =========================================================
   View Name   : gold.dim_products
   Layer       : Gold
   Purpose     : Product Dimension
                 - Store current active products only
                 - Enrich product data with category attributes
   Logic       :
     - Generate surrogate product key
     - Filter out historical (SCD Type 2) records
   Notes       :
     - prd_end_dt IS NULL → current active product
     - Historical records remain in Silver layer
========================================================= */

CREATE VIEW gold.dim_products AS
SELECT 
    -- Surrogate product key
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

    -- Business identifiers
    pn.prd_id   AS product_id,
    pn.prd_key  AS product_number,

    -- Descriptive attributes
    pn.prd_nm   AS product_name,

    -- Category attributes
    pn.cat_id   AS category_id,
    pc.cat      AS category,
    pc.subcat   AS subcategory,
    pc.maintenance,

    -- Product details
    pn.prd_cost     AS product_cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date		

FROM Silver.crm_prd_info pn

-- Join product category lookup
LEFT JOIN Silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id

-- Filter out all historical data (SCD Type 2)
WHERE pn.prd_end_dt IS NULL;






