/*
====================================================================================================================================================================================
DDL Script : Create Silver Tables
====================================================================================================================================================================================
Script Purpose :
  This script helps in cleaning the table by removing unwanted space,abbrivations, updated the facts ,etc in order to increase the readibility, usability and efficiency of the table
=====================================================================================================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver 
AS
BEGIN
    Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
         SET @batch_start_time=GETDATE();
         PRINT '==========================================================================================';
         PRINT 'Loading Silver Layer';
         PRINT'===========================================================================================';
         PRINT '------------------------------------------------------------------------------------------';
         PRINT 'Loading CRM Tables';
         PRINT '------------------------------------------------------------------------------------------';

        ----------------------------------------------------------------------------------------------------
        -- 1. crm_cust_info
        ----------------------------------------------------------------------------------------------------
        SET @start_time=GETDATE();
        PRINT '>> Truncating Table : silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
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
            TRIM(cst_lastname) AS cst_lastname,
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE 
                WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
                WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM
        bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
      )t
      WHERE flag_last =1;
      SET @end_time=GETDATE();
      PRINT'>>Load Duration:' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'secondS';
      PRINT '>>-------------';
        -------------------------------------------------------------------------------------------------
        -- 2. crm_prd_info
        -------------------------------------------------------------------------------------------------
        SET @start_time =GETDATE()
        PRINT '>> Truncating Table : silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales' 
                WHEN 'T' THEN 'Touring' 
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE),
            CAST(
                DATEADD(DAY, -1, 
                    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
                ) AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
        PRINT '>> -------------';


        ---------------------------------------------------------------------------------------------------------
        -- 3. crm_sales_details
        ---------------------------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
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
            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE TRY_CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
            END AS sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price 
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
        PRINT '>> -------------';


        ---------------------------------------------------------------------------------------------------------
        -- 4. erp_CUST_AZ12
        ---------------------------------------------------------------------------------------------------------
         SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.erp_CUST_AZ12';
        TRUNCATE TABLE silver.erp_CUST_AZ12;

        PRINT '>> Inserting Data Into: silver.erp_CUST_AZ12';
        INSERT INTO silver.erp_CUST_AZ12 (cid, bdate, gen)
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_CUST_AZ12;
         SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
        PRINT '>> -------------';

		PRINT '----------------------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------------------------------------------------------';


        --------------------------------------------------------------------------------------------------
        -- 5. erp_LOC_A101
        --------------------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.erp_LOC_A101';
        TRUNCATE TABLE silver.erp_LOC_A101;

        PRINT '>> Inserting Data Into: silver.erp_LOC_A101';
        INSERT INTO silver.erp_LOC_A101 (cid, cntry)
        SELECT 
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN cntry IS NULL OR cntry = '' THEN 'n/a'
                WHEN cntry IN ('US', 'USA') THEN 'United States'
                WHEN cntry = 'DE' THEN 'Germany'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_LOC_A101;
         SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
        PRINT '>> -------------';

        ------------------------------------------------------------------------------------------------------------
        -- 6. erp_PX_CAT_G1V2
        ------------------------------------------------------------------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.erp_PX_CAT_G1V2';
        TRUNCATE TABLE silver.erp_PX_CAT_G1V2;

        PRINT '>> Inserting Data Into: silver.erp_PX_CAT_G1V2';
        INSERT INTO silver.erp_PX_CAT_G1V2 (ID, CAT, SUBCAT, MAINTENANCE)
        SELECT 
            ID,
            CAT,
            SUBCAT,
            MAINTENANCE
        FROM bronze.erp_PX_CAT_G1V2;
        SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) +'seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '===================================================================================================================';
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) +'seconds';
		PRINT '===================================================================================================================';
		
	END TRY
	BEGIN CATCH
		PRINT '=================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=================================================';
	END CATCH

END;
GO
EXEC silver.load_silver;
