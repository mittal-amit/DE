**-- Lab- Using SQL Pool - External Tables - Parquet**

-- DB isn't required as dedicated SQL Pool in place

```CREATE MASTER KEY ENCRYPTION BY PASSWORD = '_______';```

-- Here we are using the Storage account key for authorization
```
CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
WITH 
	IDENTITY='appdatalake2000',
	SECRET = '_______'
```
-- In thr SQL Pool, we can use HADOOP Driver to mention/access the source
```
CREATE EXTERNAL DATA SOURCE log_data
WITH (  LOCATION = 'abfss://data@datalake303.dfs.core.windows.net',
        CREDENTIAL = AzureStorageCredential,
		TYPE = HADOOP
        ) 
```
-- If you made a mistake with the table, you can drop the table and recreate it again
```
DROP EXTERNAL TABLE [logdata]
```
-- Here we are mentioning the file format as Parquet
```
CREATE EXTERNAL FILE FORMAT parquetfile WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
	);
```
-- Notice that the column names don't contain spaces
-- When Azure Daya Factory was used to generate these files, the column names could not have spaces
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
-- Select all the records
```
SELECT count(*) FROM [logdata]
```
--Count of each operation name
```
SELECT [Operationname], COUNT([Operationname]) as [Operation Count] FROM [logdata] GROUP BY [Operationname] ORDER BY [Operation Count]
```
