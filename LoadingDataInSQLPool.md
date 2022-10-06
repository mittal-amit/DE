**4. Loading/Copy data in the SQL Pool with new user (with some loading permission)**

-- On System Database > master
```
CREATE LOGIN user_load WITH PASSWORD = 'Azure@123'
```

-- On newpool > new query

--Creatign user on login and assigning the permissions
```
CREATE USER user_load FOR LOGIN user_load;
GRANT ADMINISTER DATABASE BULK OPERATIONS TO user_load;
GRANT CREATE TABLE TO user_load;
GRANT ALTER ON SCHEMA::dbo TO user_load;
```
--Create workload group and workload classifier
```
CREATE WORKLOAD GROUP DataLoads
with (
	MIN_PERCENTAGE_RESOURCE = 100
	,CAP_PERCENTAGE_RESOURCE = 100
	,REQUEST_MIN_RESOURCE_GRANT_PERCENT = 100
	);
```
```
CREATE WORKLOAD CLASSIFIER [ELTLogin]
with (
	WORKLOAD_GROUP = 'DataLoads'
	,MEMBERNAME = 'user_load'
	);
```
```
DROP EXTERNAL TABLE [logdata]
```
```
CREATE EXTERNAL TABLE [logdata]
(
    [Id] [int] ,
    [Correlationid] [VARCHAR](200) ,
    [Operationname] [VARCHAR](200) ,
    [Status] [VARCHAR](100) ,
    [Eventcategory] [VARCHAR](100) ,
    [Level] [VARCHAR](100) ,
    [Time] [DATETIME] ,
    [Subscription] [VARCHAR](200) ,
    [Eventinitiatedby] [VARCHAR](1000) ,
    [Resourcetype] [VARCHAR](1000) ,
    [Resourcegroup] [VARCHAR](1000) )
WITH (
    LOCATION = 'raw/parquet/',
    DATA_SOURCE = log_data,
    FILE_FORMAT = parquetfile
)
```
--Grant the required privileges to the new user
```
GRANT INSERT ON logdata TO user_load;
GRANT SELECT ON logdata TO user_load;
```



-- New log in as the new user
--The FIRSTROW option helps to ensure the first header row is not 
```
SELECT * FROM [logdata]
```
-- Change access level to BLOBL (anonymous read access for blobs only)
```
COPY INTO logdata FROM 'https://datalake303.blob.core.windows.net/data/raw/Log.csv'
WITH
(
FIRSTROW=2
)
```

**-- Parquet file**
```
DELETE FROM [logdata] 
```
```
SELECT * FROM [logdata]
```
```
COPY INTO logdata FROM 'https://datalake303.blob.core.windows.net/data/raw/parquet/*.parquet'
WITH
(
FILE_TYPE='PARQUET',
CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='__________'
)
```
