# Databricks notebook source

1) How does Lakehouse replace the dependency on using Data lakes and Data warehouses in a Data and Analytics solution?
- All the above

2) You are currently working on storing data you received from different customer surveys, this data is highly unstructured and changes over time,  why Lakehouse is a better choice compared to a Data warehouse?
- Lakehouse supports schema enforcement and evolution, traditional data warehouses lack schema evolution

3) Which of the following locations hosts the driver and worker nodes of a Databricks-managed cluster?
- Data plane

4) You have written a notebook to generate a summary data set for reporting, Notebook was scheduled using the job cluster, but you realized it takes an average of 8 minutes to start the cluster, what feature can be used to start the cluster in a timely fashion?
- Use the Databricks cluster pools feature to reduce the startup time

5) Which of the following statement is true about Databricks repos?
- Databricks repos allow you to comment and commit code changes and push them to a remote branch

6) Which of the statement is correct about the cluster pools?
- Cluster pools allow you to save time when starting a new cluster

7) Once a cluster is deleted, below additional actions need to performed by the administrator
- No action needs to be performed. All resorces are automatically removed

8) How does a Delta Lake differ from a traditional data lake?
- Delta Lake is an open storage format like parquet with additional capabilities that can provide reliability, security, and performance

9) How VACCUM and OPTIMIZE commands can be used to manage the DELTA lake?
- OPTIMIZE command can be used to compact small parquet files, and the VACCUM command can be used to delete parquet files that are marked for deletion/unused

10) Which of the below commands can be used to drop a DELTA table?
- DROP TABLE table_name

11) Delete records from the transactions Delta table where transactionDate is greater than current timestamp?
- DELETE FROM transactions where transactionDate > current_timestamp()

12) Identify one of the below statements that can query a delta table in PySpark Dataframe API
- Spark.read.table('table_name')

13) The default threshold of VACUUM is 7 days, internal audit team asked to certain tables to maintain at least 365 days as part of compliance requirement, which of the below setting is needed to implement
- ALTER TABLE table_name set TBLPROPERTIES (delat.deletedFileRetentionDuration = 'interval 365 days')

14) Which of the following commands can be used to query a delta table?
- Both A and B

15) Below table temp_data has one column called raw contains JSON data that records temperature for every four hours in the day for the city of Chicago, you are asked to calculate the maximum temperature that was ever recorded for 12:00 PM hour across all the days.  Parse the JSON data and use the necessary array function to calculate the max temp.
Table: temp_date
Column: raw
Datatype: string
Expected output: 58
- SELECT array_max(from_json(raw:chicago[*].temp[3], 'array<int>'))
  FROM temp_data

16) Which of the following SQL statements can be used to update a transactions table, to set a flag on the table from Y to N
- UPDATE transactions SET active_flag = 'N' WHERE active_flag = 'Y'

17) Below sample input data contains two columns, one cartId also known as session id, and the second column is called items, every time a customer makes a change to the cart this is stored as an array in the table, the Marketing team asked you to create a unique list of item’s that were ever added to the cart by each customer, fill in blanks by choosing the appropriate array function so the query produces below expected result as shown below.
Schema: cartId INT, items Array<INT>
Sample Data
SELECT cartId, ___ (___(items)) as items
FROM carts GROUP BY cartId
Expected result:
cartId              items
1                 [1,100,200,300,250]

- ARRAY_UNION, COLLECT_SET

18) You were asked to identify number of times a temperature sensor exceed threshold temperature (100.00) by each device, each row contains 5 readings collected every 5 minutes, fill in the blank with the appropriate functions.
Schema: deviceId INT, deviceTemp ARRAY<double>, dateTimeCollected TIMESTAMP
SELECT deviceId, __ (__ (__(deviceTemp], i -> i > 100.00)))
FROM devices
GROUP BY deviceId

-SUM, SIZE, FILTER

19) You are currently looking at a table that contains data from an e-commerce platform, each row contains a list of items(Item number) that were present in the cart, when the customer makes a change to the cart the entire information is saved as a separate list and appended to an existing list for the duration of the customer session, to identify all the items customer bought you have to make a unique list of items, you were asked to create a unique item’s list that was added to the cart by the user, fill in the blanks of below query by choosing the appropriate higher-order function?
Note: See below sample data and expected output.
Schema: cartId INT, items Array<INT>
Fill in the blanks: 
SELECT cartId, _(_(items)) FROM carts

- ARRAY_DISTINCT, FLATTEN

20) You are working on IOT data where each device has 5 reading in an array collected in Celsius, you were asked to covert each individual reading from Celsius to Fahrenheit, fill in the blank with an appropriate function that can be used in this scenario.
Schema: deviceId INT, deviceTemp ARRAY<double>
SELECT deviceId, __(deviceTempC,i-> (i * 9/5) + 32) as deviceTempF
FROM sensors

- TRANSFORM

21) Which of the following array functions takes input column return unique list of values in an array?
- COLLECT_SET

22) You are looking to process the data based on two variables, one to check if the department is supply chain or check if process flag is set to True
- if department == 'supply chain' or process:
    
23) What is the output of below function when executed with input parameters 1, 3 :
def check_input(x,y):
    if x < y:
        x= x+1
        if x>y:
            x= x+1
            if x <y:
            x = x+1
    return x

- 2

24) Which of the following python statements can be used to replace the schema name and table name in the query?
- table_name = 'sales'
  query = f'select * from {schema_name}.{table_name}'

25) you are currently working on creating a spark stream process to read and write in for a one-time micro batch, and also rewrite the existing target table, fill in the blanks to complete the below command sucesfully.
spark.table("source_table")
.writeStream
.option("____", “dbfs:/location/silver")
.outputMode("____")
.trigger(Once=____)
.table("target_table")

- checkpointlocation, complete, True

26) You were asked to write python code to stop all running streams, which of the following command can be used to get a list of all active streams currently running so we can stop them, fill in the blank.
for s in _______________:
  s.stop()

- spark.streams.active

27) At the end of the inventory process a file gets uploaded to the cloud object storage, you are asked to build a process to ingest data which of the following method can be used to ingest the data incrementally, schema of the file is expected to change overtime ingestion process should be able to handle these changes automatically. Below is the auto loader to command to load the data, fill in the blanks for successful execution of below code.
spark.readStream
.format("cloudfiles")
.option("_______",”csv)
.option("_______", ‘dbfs:/location/checkpoint/’)
.load(data_source)
.writeStream
.option("_______",’ dbfs:/location/checkpoint/’)
.option("_______", "true")
.table(table_name))

- cloudfiles.format, cloudfiles.schemalocation, checkpointlocation, mergeSchema

28) Which of the following scenarios is the best fit for AUTO LOADER?
- Efficiently process new data incrementally from cloud object storage

29) You are asked to setup an AUTO LOADER to process the incoming data, this data arrives in JSON format and get dropped into cloud object storage and you are required to process the data as soon as it arrives in cloud storage, which of the following statements is correct
- AUTO LOADER can support file notification method so it can process data as it arrives

30) What is the main difference between the bronze layer and silver layer in a medallion architecture?
- Bronze is raw copy of ingested data, silver contains data with production schema and optimized for ELT/ETL throughput

31) What is the main difference between the silver layer and the gold layer in medalion architecture?
- Gold may contain aggregated data

32) What is the main difference between the silver layer and gold layer in medallion architecture?
- Silver optimized to perform ETL, Gold is optimized query performance

33) A dataset has been defined using Delta Live Tables and includes an expectations clause: CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01')
What is the expected behavior when a batch of data containing data that violates these constraints is processed?
- Records that violate the expectation are added to the target dataset and recorded as invalid in the event log

34) A dataset has been defined using Delta Live Tables and includes an expectations clause: CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION DROP ROW
What is the expected behavior when a batch of data containing data that violates these constraints is processed?
- Records that violate the expectation are dropped from the target dataset and recorded as invalid in the event log.

35) You are asked to debug a databricks job that is taking too long to run on Sunday’s, what are the steps you are going to take to identify the step that is taking longer to run?
- Under Workflow UI and jobs select job you want to monitor and select the run, notebook activity can be viewed.

36) Your colleague was walking you through how a job was setup, but you noticed a warning message that said, “Jobs running on all-purpose cluster are considered all purpose compute", the colleague was not sure why he was getting the warning message, how do you best explain this warning message?
- All-purpose clusters are more expensive than the job clusters

37) Your team has hundreds of jobs running but it is difficult to track cost of each job run, you are asked to provide a recommendation on how to monitor and track cost across various workloads
- Use Tags, during job creation so cost can be easily tracked

38) The sales team has asked the Data engineering team to develop a dashboard that shows sales performance for all stores, but the sales team would like to use the dashboard but would like to select individual store location, which of the following approaches Data Engineering team can use to build this functionality into the dashboard.
- Use query Parameters which then allow user to choose any location

39) You are working on a dashboard that takes a long time to load in the browser, due to the fact that each visualization contains a lot of data to populate, which of the following approaches can be taken to address this issue?
- Use Databricks SQL Query filter to limit the amount of data in each visualization

40) One of the queries in the Databricks SQL Dashboard takes a long time to refresh, which of the below steps can be taken to identify the root cause of this issue?
- Use Query History, to view queries and select query, and check the query profile to see time spent in each step

41) A SQL Dashboard was built for the supply chain team to monitor the inventory and product orders, but all of the timestamps displayed on the dashboards are showing in UTC format, so they requested to change the time zone to the location of New York. How would you approach resolving this issue?
- Under SQL Admin Console, set the SQL configuration parameter time zone to America/New_York

42) Which of the following technique can be used to implement fine-grained access control to rows and columns of the Delta table based on the user's access?
- Use dynamic view functions

43) Unity catalog helps you manage the below resources in Databricks at account level
- All of the above

44) John Smith is a newly joined team member in the Marketing team who currently has access read access to sales tables but does not have access to delete rows from the table, which of the following commands help you accomplish this?
- GRANT MODIFY ON TABLE table_name TO john.smith@marketing.com

45) Kevin is the owner of both the sales table and regional_sales_vw view which uses the sales table as the underlying source for the data, and Kevin is looking to grant select privilege on the view regional_sales_vw to one of newly joined team members Steven. Which of the following is a true statement?
-  Kevin can grant access to the view, because he is the owner of the view and the underlying table

