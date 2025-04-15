/*
The following script is used to create a database and create three schemas within the database. 
The script creates a database called as DataWarehouse if it does not already exists
then it creates three schemas bronze, silver and gold (in accordance with the medallic data engineering model) within the database

Note: if the database DataWarehouse already exists, then it is not again created, so make sure such a database does not exist or is empty. 

creating using mysql. 
*/




SHOW DATABASES;

CREATE DATABASE IF NOT EXISTS DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;


