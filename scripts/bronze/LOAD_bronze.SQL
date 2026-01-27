/* =========================================================
   PROCEDURE : bronze.load_bronze
   PROJECT   : Data Warehouse
   PURPOSE   : Load RAW data into Bronze layer tables
   LAYER     : Bronze
   WARNING   :
      - This procedure TRUNCATES Bronze tables
      - Then reloads data from CSV source files
      - NO transformations are applied here
   ========================================================= */

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time   DATETIME;

    BEGIN TRY

    /* =====================================================
       Load CRM - Customer Info
       ===================================================== */
    PRINT '==============================================';
    PRINT 'Starting load: crm_cust_info';

    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_cust_info;

    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\CM\Desktop\SQL course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();

    PRINT 'crm_cust_info loaded successfully';
    PRINT 'Load Duration: ' 
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
          + ' seconds';


    /* =====================================================
       Load CRM - Product Info
       ===================================================== */
    PRINT '----------------------------------------------';
    PRINT 'Starting load: crm_prd_info';

    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_prd_info;

    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\CM\Desktop\SQL course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();

    PRINT 'crm_prd_info loaded successfully';
    PRINT 'Load Duration: ' 
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
          + ' seconds';


    /* =====================================================
       Load CRM - Sales Details
       ===================================================== */
    PRINT '----------------------------------------------';
    PRINT 'Starting load: crm_sales_details';

    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_sales_details;

    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\CM\Desktop\SQL course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();

    PRINT 'crm_sales_details loaded successfully';
    PRINT 'Load Duration: ' 
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
          + ' seconds';


    /* =====================================================
       Load ERP - Customer AZ12
       ===================================================== */
    PRINT '----------------------------------------------';
    PRINT 'Starting load: erp_cust_az12';

    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_cust_az12;

    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\CM\Desktop\SQL course\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();

    PRINT 'erp_cust_az12 loaded successfully';
    PRINT 'Load Duration: ' 
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
          + ' seconds';


    /* =====================================================
       Load ERP - Location A101
       ===================================================== */
    PRINT '----------------------------------------------';
    PRINT 'Starting load: erp_loc_a101';

    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_loc_a101;

    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\CM\Desktop\SQL course\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();

    PRINT 'erp_loc_a101 loaded successfully';
    PRINT 'Load Duration: ' 
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
          + ' seconds';


    /* =====================================================
       Load ERP - Product Category
       ===================================================== */
    PRINT '----------------------------------------------';
    PRINT 'Starting load: erp_px_cat_g1v2';

    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\CM\Desktop\SQL course\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();

    PRINT 'erp_px_cat_g1v2 loaded successfully';
    PRINT 'Load Duration: ' 
          + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
          + ' seconds';


    /* =====================================================
       Procedure completed successfully
       ===================================================== */
    PRINT '==============================================';
    PRINT '✅ Bronze layer loading completed successfully';
    PRINT 'Completion time: ' 
          + CONVERT(VARCHAR(19), GETDATE(), 120);

    END TRY
    BEGIN CATCH

        PRINT '==============================================';
        PRINT '❌ ERROR OCCURRED DURING BRONZE LOADING';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Line    : ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '==============================================';

    END CATCH
END;
GO

EXEC bronze.load_bronze;
