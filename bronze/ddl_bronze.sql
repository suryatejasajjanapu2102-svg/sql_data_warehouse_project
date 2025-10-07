
--/*
--=============================================================
--Create Database and Schemas
--=============================================================
--Script Purpose:
--    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
--    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
--    within the database: 'bronze', 'silver', and 'gold'.
	
--WARNING:
--    Running this script will drop the entire 'DataWarehouse' database if it exists. 
--    All data in the database will be permanently deleted. Proceed with caution 
--    and ensure you have proper backups before running this script.
--*/

--USE master;
--GO

---- Drop and recreate the 'DataWarehouse' database
--IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
--BEGIN
--    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
--    DROP DATABASE DataWarehouse;
--END;
--GO

---- Create the 'DataWarehouse' database
--CREATE DATABASE DataWarehouse;
--GO

--USE DataWarehouse;
--GO

---- Create Schemas
--CREATE SCHEMA bronze;
--GO

--CREATE SCHEMA silver;
--GO

--CREATE SCHEMA gold;
--GO

/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

GO

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE

);
GO
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
    GO
    CREATE TABLE bronze.crm_prd_info (

     prd_id INT,
     prd_key NVARCHAR(50),
     prd_nm NVARCHAR(50),
     prd_cost INT,
     prd_line NVARCHAR(50),
     prd_start_dt DATE,
     prd_end_dt DATE

);
GO
IF OBJECT_ID('bronze.crm_sales_details_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details_info;
    GO
    CREATE TABLE  bronze.crm_sales_details_info(
    sls_ord_num NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
    
    );
    GO
IF OBJECT_ID('bronze.erp_cust_az12_info', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12_info;
    GO
    CREATE TABLE  bronze.erp_cust_az12_info(
    cid  NVARCHAR(50),
    bdate DATE,
    gen  NVARCHAR(50)
   
    );
    GO
IF OBJECT_ID('bronze.erp_loc_a101_info', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101_info;
    GO

    CREATE TABLE bronze.erp_loc_a101_info(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
    );
   GO
IF OBJECT_ID('bronze.erp_px_cat_g1v2_info', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2_info;
    GO

    CREATE TABLE bronze.erp_px_cat_g1v2_info (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
    );

   
    
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


    EXEC bronze.load_bronze



