/*
========================================
  Database Initialization Script
  Project: Data Warehouse
========================================
*/

-- Step 1: Create Database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
END
GO

-- Step 2: Use Database
USE DataWarehouse;
GO

-- Step 3: Create Schemas

-- Bronze Layer (Raw Data)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

-- Silver Layer (Cleaned & Transformed Data)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

-- Gold Layer (Business-Level Aggregations)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO

/*
========================================
  End of Script
========================================
*/
