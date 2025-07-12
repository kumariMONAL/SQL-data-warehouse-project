/* 
===================================================================================
Create Database and Schemas 
===================================================================================

Script Purpose:
This script is creating a new database 'DataWarehouse'.But before this it deletes the older DataWarehouse database. Then it creates the scehmas inside 
this new database :'bronze','silver', and 'gold'

WARNING:
Running this script will also delete all the files and schemas of the 'DataWarehouse' database if we  will drop it inorder to create the new DataWarehouse.
So be very careful that if this older database contains important files and scehmas one must create the backup for safty purpose.
*/

USE master;
GO
  -- Drop and recreate the 'DataWarehouse' database
  IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'DataWarehouse')
  BEGIN
       ALTER DATABASE DataWarehouse SET SINGLR_USER WITH ROLLBACK IMMEDIATE;
      DROP DATABSE DataWarehouse;
END;
GO
  --Create  the 'DataWarehouse' Database
CREATE  DATABASE DataWarehouse;
USE DataWarehouse;
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE  SCHEMA gold;
GO
