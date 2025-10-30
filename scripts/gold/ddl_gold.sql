/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
DROP VIEW IF EXISTS gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id                           AS customer_id,
    ci.cst_firstname                    AS firstname,
    ci.cst_lastname                     AS lastname,
    co.CNTRY                            AS country,
    ca.BDATE                            AS birthdate,
    ci.cst_create_date                  AS create_date,
    ci.cst_marital_status               AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr   --crm_cust_info is the supreme sourceof info
        ELSE COALESCE(ca.gen, 'n/a')
    END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_CUST_AZ12 ca
    ON ci.cst_key = ca.CID
LEFT JOIN silver.erp_LOC_A101 co
    ON ci.cst_key = co.CID;
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
DROP view if exists gold.dim_products;
GO
CREATE VIEW gold.dim_products AS 
SELECT
    ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
    pn.prd_id                                              AS product_id,
    pn.prd_nm                                              AS product_nm,
    pn.cat_id                                              AS category_id,
    pn.prd_line                                            AS product_line,
    pc.MAINTENANCE,
    pc.CAT                                                 AS category,
    pc.SUBCAT                                              AS subcategory,
    pn.prd_start_dt                                        AS startdate
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_PX_CAT_G1V2  pc
        ON pn.cat_id=pc.ID;
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
DROP VIEW IF EXISTS gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT
  sd.sls_ord_num         AS order_number,
  pr.product_key,
  cu.customer_key,
  sd.sls_quantity,
  sd.sls_price,
  sd.sls_order_dt       AS order_date,
  sd.sls_ship_dt        AS ship_date,
  sd.sls_due_dt         AS due_date
  FROM silver.crm_sales_details sd
  left join gold.dim_products pr
      ON sd.sls_prd_key = pr.product_key
  left join gold.dim_customers cu
      ON sd.sls_cust_id=cu.customer_id;
