/* =========================================================
   PROJECT    : Data Warehouse - Silver Layer
   AUTHOR     : Ahmed Hassan
   PURPOSE    : Create Silver Tables EXACTLY as Source
   LAYER      : Silver
   WARNING    : Tables will be DROPPED and recreated
                NO transformations are applied in Silver
   ========================================================= */

USE DataWarehouse;
GO

/* =========================================================
   TABLE: silver.crm_cust_info
   SOURCE: CRM System
   NOTE  : Data stored AS-IS from source (no cleaning)
   ========================================================= */
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
BEGIN
    DROP TABLE silver.crm_cust_info;
END
GO

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,   -- Stored as INT from source system
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   TABLE: silver.crm_prd_info
   SOURCE: CRM System
   ========================================================= */
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
BEGIN
    DROP TABLE silver.crm_prd_info;
END
GO

CREATE TABLE silver.crm_prd_info (
    prd_id      INT,
    cat_id      NVARCHAR(50), 
    prd_key     NVARCHAR(50),
    prd_nm      NVARCHAR(50),
    prd_cost    INT,
    prd_line    NVARCHAR(50),
    prd_start_dt   DATE,  -- Stored as INT from source system
    prd_end_dt  DATE,   -- Stored as INT from source system
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   TABLE: silver.crm_sales_details
   SOURCE: CRM System
   ========================================================= */
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
BEGIN
    DROP TABLE silver.crm_sales_details;
END
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  DATE,  -- Stored as INT from source system
    sls_ship_dt   DATE,  -- Stored as INT from source system
    sls_due_dt    DATE,  -- Stored as INT from source system
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   TABLE: silver.erp_loc_a101
   SOURCE: ERP System
   ========================================================= */
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
BEGIN
    DROP TABLE silver.erp_loc_a101;
END
GO

CREATE TABLE silver.erp_loc_a101 (
    cid     NVARCHAR(50),
    cntry   NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   TABLE: silver.erp_cust_az12
   SOURCE: ERP System
   ========================================================= */
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
BEGIN
    DROP TABLE silver.erp_cust_az12;
END
GO

CREATE TABLE silver.erp_cust_az12 (
    cid     NVARCHAR(50),
    bdate   DATE,  -- Stored as INT from source system
    gen     NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   TABLE: silver.erp_px_cat_g1v2
   SOURCE: ERP System
   ========================================================= */
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
BEGIN
    DROP TABLE silver.erp_px_cat_g1v2;
END
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

/* =========================================================
   ✅ SILVER LAYER CREATED SUCCESSFULLY
   RULE:
   - Store data EXACTLY as received from source systems
   - NO data type changes
   - NO business logic
   - Transformations belong to GOLD layer
   ========================================================= */
