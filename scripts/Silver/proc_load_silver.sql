/* =====================================================
   PROCEDURE : silver.load_silver
   PROJECT   : Data Warehouse
   LAYER     : Silver
   PURPOSE   :
      - Transform & load data from Bronze to Silver
      - Apply data cleansing & standardization
      - Ensure re-runnability (TRUNCATE + INSERT)
      - Log load duration per table & per batch
   WARNING   :
      - This procedure TRUNCATES Silver tables
      - No source (Bronze) data is modified
   ===================================================== */

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @batch_start_time DATETIME,
        @batch_end_time   DATETIME,
        @table_start_time DATETIME,
        @table_end_time   DATETIME;

    BEGIN TRY

        /* ===============================
           Batch Start
           =============================== */
        SET @batch_start_time = GETDATE();
        PRINT '========================================';
        PRINT '🚀 Silver Layer Load Started';
        PRINT 'Start Time: ' + CAST(@batch_start_time AS NVARCHAR);
        PRINT '========================================';


        /* =====================================================
           Load: Silver.crm_cust_info
           ===================================================== */
        SET @table_start_time = GETDATE();

        TRUNCATE TABLE silver.crm_cust_info;

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
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM
        (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @table_end_time = GETDATE();
        PRINT '✔ crm_cust_info loaded successfully';
        PRINT 'Duration (sec): ' + CAST(DATEDIFF(SECOND,@table_start_time,@table_end_time) AS NVARCHAR);
        PRINT '----------------------------------------';


        /* =====================================================
           Load: Silver.crm_prd_info
           ===================================================== */
        SET @table_start_time = GETDATE();

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info
        (
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
            REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                LEAD(prd_start_dt)
                OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
            AS DATE)
        FROM bronze.crm_prd_info;

        SET @table_end_time = GETDATE();
        PRINT '✔ crm_prd_info loaded successfully';
        PRINT 'Duration (sec): ' + CAST(DATEDIFF(SECOND,@table_start_time,@table_end_time) AS NVARCHAR);
        PRINT '----------------------------------------';


        /* =====================================================
           Load: Silver.crm_sales_details
           ===================================================== */
        SET @table_start_time = GETDATE();

        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details
        (
            sls_ord_num,
            sls_prd_key,
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
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,
            CASE
                WHEN sls_sales IS NULL
                  OR sls_sales <= 0
                  OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity,0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @table_end_time = GETDATE();
        PRINT '✔ crm_sales_details loaded successfully';
        PRINT 'Duration (sec): ' + CAST(DATEDIFF(SECOND,@table_start_time,@table_end_time) AS NVARCHAR);
        PRINT '----------------------------------------';


        /* =====================================================
           Load: Silver.erp_cust_az12
           ===================================================== */
        SET @table_start_time = GETDATE();

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END,
            CASE
                WHEN bdate IS NULL THEN NULL
                WHEN bdate > CAST(GETDATE() AS DATE) THEN NULL
                WHEN bdate < '1900-01-01' THEN NULL
                ELSE bdate
            END,
            CASE
                WHEN UPPER(LTRIM(RTRIM(gen))) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(LTRIM(RTRIM(gen))) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @table_end_time = GETDATE();
        PRINT '✔ erp_cust_az12 loaded successfully';
        PRINT 'Duration (sec): ' + CAST(DATEDIFF(SECOND,@table_start_time,@table_end_time) AS NVARCHAR);
        PRINT '----------------------------------------';


        /* =====================================================
           Load: Silver.erp_loc_a101
           ===================================================== */
        SET @table_start_time = GETDATE();

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid,cntry)
        SELECT
            REPLACE(cid,'-',''),
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        SET @table_end_time = GETDATE();
        PRINT '✔ erp_loc_a101 loaded successfully';
        PRINT 'Duration (sec): ' + CAST(DATEDIFF(SECOND,@table_start_time,@table_end_time) AS NVARCHAR);
        PRINT '----------------------------------------';


        /* =====================================================
           Load: Silver.erp_px_cat_g1v2
           ===================================================== */
        SET @table_start_time = GETDATE();

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
        SELECT id,cat,subcat,maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @table_end_time = GETDATE();
        PRINT '✔ erp_px_cat_g1v2 loaded successfully';
        PRINT 'Duration (sec): ' + CAST(DATEDIFF(SECOND,@table_start_time,@table_end_time) AS NVARCHAR);
        PRINT '----------------------------------------';


        /* ===============================
           Batch End
           =============================== */
        SET @batch_end_time = GETDATE();

        PRINT '✅ Silver Layer Load Completed Successfully';
        PRINT 'Batch Duration (sec): ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR);
        PRINT 'End Time: ' + CAST(@batch_end_time AS NVARCHAR);
        PRINT '========================================';

    END TRY
    BEGIN CATCH
        PRINT '========================================';
        PRINT '❌ ERROR DURING SILVER LOAD';
        PRINT 'Message : ' + ERROR_MESSAGE();
        PRINT 'Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Line    : ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '========================================';
        THROW;
    END CATCH
END;
GO
EXEC silver.load_silver
