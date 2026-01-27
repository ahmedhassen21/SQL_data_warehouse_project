/* =========================================================
   PROJECT    : Data Warehouse - Bronze Layer
   AUTHOR     : Ahmed Hassan
   PURPOSE    : Create Bronze (Raw) Tables EXACTLY as Source
   LAYER      : Bronze
   WARNING    : Tables will be DROPPED and recreated
                NO transformations are applied in Bronze
   ========================================================= */

USE DataWarehouse;
GO

/* =========================================================
   TABLE: bronze.crm_cust_info
   SOURCE: CRM System
   NOTE  : Data stored AS-IS from source (no cleaning)
   ========================================================= */
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.crm_cust_info;
END
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE   -- Stored as INT from source system
);
GO

/* =========================================================
   TABLE: bronze.crm_prd_info
   SOURCE: CRM System
   ========================================================= */
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.crm_prd_info;
END
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id      INT,
    prd_key     NVARCHAR(50),
    prd_nm      NVARCHAR(50),
    prd_cost    INT,
    prd_line    NVARCHAR(50),
    prd_start   DATETIME,  -- Stored as INT from source system
    prd_end_dt  DATETIME   -- Stored as INT from source system
);
GO

/* =========================================================
   TABLE: bronze.crm_sales_details
   SOURCE: CRM System
   ========================================================= */
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.crm_sales_details;
END
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  INT,  -- Stored as INT from source system
    sls_ship_dt   INT,  -- Stored as INT from source system
    sls_due_dt    INT,  -- Stored as INT from source system
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT
);
GO

/* =========================================================
   TABLE: bronze.erp_loc_a101
   SOURCE: ERP System
   ========================================================= */
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.erp_loc_a101;
END
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid     NVARCHAR(50),
    cntry   NVARCHAR(50)
);
GO

/* =========================================================
   TABLE: bronze.erp_cust_az12
   SOURCE: ERP System
   ========================================================= */
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.erp_cust_az12;
END
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid     NVARCHAR(50),
    bdate   DATE,  -- Stored as INT from source system
    gen     NVARCHAR(50)
);
GO

/* =========================================================
   TABLE: bronze.erp_px_cat_g1v2
   SOURCE: ERP System
   ========================================================= */
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
BEGIN
    DROP TABLE bronze.erp_px_cat_g1v2;
END
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO

/* =========================================================
   ✅ BRONZE LAYER CREATED SUCCESSFULLY
   RULE:
   - Store data EXACTLY as received from source systems
   - NO data type changes
   - NO business logic
   - Transformations belong to SILVER layer
   ========================================================= */
