/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


   
    
    CREATE OR ALTER PROCEDURE bronze.load_bronze AS
    BEGIN
        DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME ,@batch_end_time DATETIME;
        BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT ' =================================  '  ;
        PRINT ' LOADING BRONZE LAYER' ;
        PRINT' =================================== ' ;

        PRINT ' ------------------------------------';
        PRINT  ' LOADING CRM TABLES ' ;
        PRINT' ------------------------------------- ' ;

        SET @start_time = GETDATE();
        PRINT ' >> Truncating table : bronze.crm_cust_info  ';
        TRUNCATE TABLE  bronze.crm_cust_info ;

        PRINT ' >> Inserting data into Table:bronze.crm_cust_info ' ;
        BULK INSERT bronze.crm_cust_info
        FROM  'C:\SQL_DATAWAREHOUSE_PROJECT\datasets\source_crm\cust_info.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
        );

        SELECT COUNT(*) crm_cust_info FROM  bronze.crm_cust_info;

        SET @end_time = GETDATE();

        PRINT ' >> Load Duration: ' 
    + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
    + ' seconds';

         PRINT'--------------------------------';
       
       
        SET @start_time = GETDATE();
        PRINT ' >> Truncating table : bronze.crm_prd_info '  ;
        TRUNCATE TABLE  bronze.crm_prd_info;

        PRINT ' >> Inserting data into Table:bronze.crm_prd_info ';
        BULK INSERT bronze.crm_prd_info
        FROM  'C:\SQL_DATAWAREHOUSE_PROJECT\datasets\source_crm\prd_info.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
        );
        SELECT COUNT(*) crm_prd_info FROM  bronze.crm_prd_info;

        SET @end_time = GETDATE();
       PRINT ' >> Load Duration: ' 
    + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
    + ' seconds';

        PRINT'--------------------------------------------------------';
       

        
        --SET @start_time = GETDATE();
        --PRINT ' >> Truncating table : bronze.crm_sales_details_info ' ;
        --TRUNCATE TABLE  bronze.crm_sales_details_info ;

        --PRINT ' >> Inserting data into Table:bronze.crm_sales_details_info ' ;
        --BULK INSERT  bronze.crm_sales_details_info
        --FROM  'C:\SQL_DATAWAREHOUSE_PROJECT\datasets\source_crm\sales_details.csv'
        --WITH (
        --        FIRSTROW = 2,
        --        FIELDTERMINATOR = ',',
        --        TABLOCK
        --);
        --SELECT COUNT(*) crm_sales_details_info  FROM  bronze.crm_sales_details_info ;

        --SET @end_time = GETDATE();
       --PRINT ' >> Load Duration: '  + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10))  + ' seconds';

        PRINT'--------------------------------------------------------';
       

        
        PRINT'--------------------------------------------';
        PRINT' LOADING ERP TABLES ' ;
            PRINT'--------------------------------------------';

        
        SET @start_time = GETDATE();
        PRINT ' >> Truncating table : bronze.erp_cust_az12_info  ' ;
        TRUNCATE TABLE  bronze.erp_cust_az12_info;

        PRINT ' >> Inserting data into Table: bronze.erp_cust_az12_info ' ;
        BULK INSERT bronze.erp_cust_az12_info
        FROM  'C:\SQL_DATAWAREHOUSE_PROJECT\datasets\source_erp\CUST_AZ12.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
        );
        SELECT COUNT(*) COUNT_CUST_AZ12
        FROM bronze.erp_cust_az12_info ;

          SET @end_time = GETDATE();
        PRINT ' >> Load Duration: ' 
    + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
    + ' seconds';

        PRINT'--------------------------------------------------------';
       
       
        SET @start_time = GETDATE();
        PRINT'>> Truncating table :bronze.erp_loc_a101_info ' ;
        TRUNCATE TABLE  bronze.erp_loc_a101_info;

        PRINT ' >> Inserting data into Table: bronze.erp_loc_a101_info ' ;
        BULK INSERT bronze.erp_loc_a101_info
        FROM  'C:\SQL_DATAWAREHOUSE_PROJECT\datasets\source_erp\LOC_A101.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
        );

        SELECT COUNT(*) COUNT_LOC_A101
        FROM bronze.erp_loc_a101_info ;

          SET @end_time = GETDATE();
      PRINT ' >> Load Duration: ' 
    + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
    + ' seconds';

        PRINT'--------------------------------------------------------';
       
        
        SET @start_time = GETDATE();
        PRINT ' >> Truncating table :  bronze.erp_px_cat_g1v2_info ' ;
        TRUNCATE TABLE  bronze.erp_px_cat_g1v2_info;

        PRINT ' >> Inserting data into Table: bronze.erp_px_cat_g1v2_info ' ;
        BULK INSERT bronze.erp_px_cat_g1v2_info
        FROM  'C:\SQL_DATAWAREHOUSE_PROJECT\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
        );

        SELECT COUNT(*) COUNT_PX_CAT_G1V2
        FROM bronze.erp_px_cat_g1v2_info;

          SET @end_time = GETDATE();
       PRINT ' >> Load Duration: ' 
    + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) 
    + ' seconds';

    SET @batch_end_time = GETDATE();
    PRINT'==============================================='
    PRINT'LOADING BRONZE LAYER IS COMPLETED'
    PRINT' Total Load Duration:' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS VARCHAR(10)) + 'seconds';
    PRINT'================================================'

        PRINT'>>--------------------------------------------------------';
       
        END TRY
        BEGIN CATCH 
        PRINT'ERROR OCCURED DURING BRONZE LAYER'
        PRINT 'ERROR MESSAGE: ' + CAST(ERROR_MESSAGE() AS NVARCHAR(4000));
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
        PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR(10));

        END CATCH
          
    END;
    GO
