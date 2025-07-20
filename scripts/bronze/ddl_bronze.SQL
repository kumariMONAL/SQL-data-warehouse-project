/*
==============================================================================
DDL Script : Create Bronze  Tables
==============================================================================
Script Purpose :
  This script creates  tables in the 'bronze' schema ,dropping  existing tables
  if they already exist.
  Run this  script tore_define the DDL structure of 'bronze' Tables
===============================================================================
*/



CREATE DATABASE bronze.DataWarehouse;
use DataWarehouse;
--==========================================================================
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

--============================================================================
IF OBJECT_ID ('bronze.crm_cust_info' , 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
cst_id NVARCHAR(MAX),
cst_key NVARCHAR(MAX),
cst_firstname NVARCHAR(MAX),
cst_lastname NVARCHAR(MAX),
cst_marital_status NVARCHAR(MAX),
cst_gndr NVARCHAR(MAX),
cst_create_date NVARCHAR(MAX)
);

--=======================================================================
IF OBJECT_ID ('bronze.crm_prd_info' , 'U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
prd_id NVARCHAR(MAX),
prd_key NVARCHAR(MAX),
prd_nm NVARCHAR(MAX),
prd_cost NVARCHAR(MAX),
prd_line NVARCHAR(MAX),
prd_start_dt NVARCHAR(MAX),
prd_end_dt NVARCHAR(MAX) 
);

--===========================================================================
IF OBJECT_ID ('bronze.crm_sales_details' , 'U') IS NOT NULL
DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
sls_ord_num NVARCHAR(MAX) ,
sls_prd_key NVARCHAR(MAX) ,
sls_cust_id NVARCHAR(MAX) ,
sls_order_dt NVARCHAR(MAX) ,
sls_ship_dt NVARCHAR(MAX) ,
sls_due_dt NVARCHAR(MAX) ,
sls_sales NVARCHAR(MAX) ,
sls_quantity NVARCHAR(MAX) ,
sls_price NVARCHAR(MAX) 
);

--=====================================================================================
IF OBJECT_ID ('bronze.erp_CUST_AZ12' , 'U') IS NOT NULL
DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12 (
CID NVARCHAR(MAX) , 
BDATE NVARCHAR(MAX) ,
GEN NVARCHAR(50) 
);

--========================================================================================
IF OBJECT_ID ('bronze.erp_LOC_A101' , 'U') IS NOT NULL
DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101 (
CID  NVARCHAR(MAX),
CNTRY  NVARCHAR(50)
);

--=========================================================================================

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2' , 'U') IS NOT NULL
DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2 (
ID  NVARCHAR(MAX),
CAT  NVARCHAR(MAX),
SUBCAT  NVARCHAR(MAX),
MAINTENANCE  NVARCHAR(MAX)
);
--==========================================================================================
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    SET @batch_start_time = GETDATE();
BEGIN TRY
PRINT '=================================================================================';
PRINT 'Loading bronze layer';
PRINT '=================================================================================';
PRINT '---------------------------------------------------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '---------------------------------------------------------------------------------';
SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.crm_cust_info';

TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'  --- EXTR GAP SHOILD NOT BE THERE INSIDER INVERTED COMMAS OTHERWISE OUR SERVER WILL FAIL TO FIND THE PATH.
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT ' >> Load Duaration:' + CAST(DATEDIFF(second,@start_time ,@end_time) AS NVARCHAR);
SELECT * FROM bronze.crm_cust_info;

SELECT COUNT(*) FROM bronze.crm_cust_info;

SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.crm_prd_info';
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT ' >> Load Duaration:' + CAST(DATEDIFF(second,@start_time ,@end_time) AS NVARCHAR);

SELECT * FROM bronze.crm_prd_info;

SELECT COUNT(*) FROM bronze.crm_prd_info;


SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.crm_sales_details';
TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT ' >> Load Duaration:' + CAST(DATEDIFF(second,@start_time ,@end_time) AS NVARCHAR);
SELECT * FROM bronze.crm_sales_details;

SELECT COUNT(*) FROM bronze.crm_sales_details;


SET @start_time = GETDATE();
PRINT '---------------------------------------------------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '---------------------------------------------------------------------------------';
PRINT '>> Truncating Table: bronze.erp_LOC_A101';
SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_LOC_A101;
BULK INSERT bronze.erp_LOC_A101
FROM 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT ' >> Load Duaration:' + CAST(DATEDIFF(second,@start_time ,@end_time) AS NVARCHAR);
SELECT * FROM bronze.erp_LOC_A101;

SELECT COUNT(*) FROM bronze.erp_LOC_A101;


SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.erp_CUST_AZ12';
TRUNCATE TABLE bronze.erp_CUST_AZ12;
BULK INSERT bronze.erp_CUST_AZ12
FROM 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT ' >> Load Duaration:' + CAST(DATEDIFF(second,@start_time ,@end_time) AS NVARCHAR);
SELECT * FROM bronze.erp_CUST_AZ12;

SELECT COUNT(*) FROM bronze.erp_CUST_AZ12;

SET @start_time = GETDATE();
PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2';
TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
BULK INSERT bronze.erp_PX_CAT_G1V2
FROM 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
SET @end_time = GETDATE();
PRINT ' >> Load Duaration:' + CAST(DATEDIFF(second,@start_time ,@end_time) AS NVARCHAR);
PRINT '>> ----------------------';
SELECT * FROM bronze.erp_PX_CAT_G1V2;

SELECT COUNT(*) FROM bronze.erp_PX_CAT_G1V2;
SET @batch_end_time = GETDATE();
PRINT 'Loading Bronze Layer is Completed';
PRINT '  -Total LoadDuration :' + CAST(DATEDIFF(second,@batch_start_time, @batch_end_time) AS NVARCHAR); 

END TRY
BEGIN CATCH
PRINT '=====================================================================';
PRINT' ERROR OCCURED DURING LOADING BRONZE LAYER';
PRINT 'Error Message' + ERROR_MESSAGE();
PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
PRINT '=====================================================================';


END CATCH
END;
EXEC bronze.load_bronze;

