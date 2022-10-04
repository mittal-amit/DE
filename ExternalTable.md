**-- Lab- Using External tables which is majorly used for ad-hoc analysis and the data is present in the Azure Data Lake Storage**

**There are basically three things required:**

**1. Authroization of Data Storage A/c**

**2. Format of external file (Parquet, CSV)**

**3. Create External Table**

-- First we need to create a database in the serverless pool (will access log.csv file)

```CREATE DATABASE [appdb]```

-- Here we are creating a database master key
-- This key will be used to protect the Shared Access Signature which is specified in the next step
-- Ensure to switch the context to the new database first

```CREATE MASTER KEY ENCRYPTION BY PASSWORD = '______';```

-- Here we are using the Shared Access Signature to authorize the use of the Azure Data Lake Storage account
```
CREATE DATABASE SCOPED CREDENTIAL SasToken
WITH IDENTITY='SHARED ACCESS SIGNATURE'
, SECRET = '_______'
```
-- This defines the source of the data
```
CREATE EXTERNAL DATA SOURCE log_data
WITH (  LOCATION = 'https://____.blob.core.windows.net/data',
        CREDENTIAL = SasToken
        ) 
```
/* This creates an External File Format object that defines the external data that can be
present in Hadoop, Azure Blob Storage or Azure Data Lake Store

Here with FIRST_ROW,  we are saying please skip the first row because this contains header information
*/
```
CREATE EXTERNAL FILE FORMAT TextFileFormat WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        FIRST_ROW = 2))
```
-- Here we define the external table
```
CREATE EXTERNAL TABLE [logdata]
(
    [Id] [int] NULL,
    [Correlationid] [VARCHAR](200) Null,
    [Operationname] [VARCHAR](200) Null,
    [Status] [VARCHAR](100) Null,
    [Eventcategory] [VARCHAR](100) Null,
    [Level] [VARCHAR](100) Null,
    [Time] [DATETIME] Null,
    [Subscription] [VARCHAR](200) Null,
    [Eventinitiatedby] [VARCHAR](1000) Null,
    [Resourcetype] [VARCHAR](200) Null,
    [Resourcegroup] [VARCHAR](200) Null)
WITH (
    LOCATION = '/raw/Log.csv',
    DATA_SOURCE = log_data,
    FILE_FORMAT = TextFileFormat
)
```
-- If you made a mistake with the table, you can drop the table and recreate it again

```DROP EXTERNAL TABLE [logdata]```

-- Drop an external file format  
```DROP EXTERNAL FILE FORMAT TextFileFormat```

-- Select all the records

```SELECT * FROM [logdata]```

--Count of each operation name

```SELECT [Operationname], COUNT([Operationname]) as [Operation Count] FROM [logdata] GROUP BY [Operationname] ORDER BY [Operation Count]```
