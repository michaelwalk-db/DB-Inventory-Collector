#These imports are definitely used
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import uuid
from datetime import datetime

#these may not used (yet)
from os import getgrouplist
from functools import reduce
import requests
import json, math
import concurrent.futures
import time


class InventoryCollector:
    GRANT_EXECUTION_ID_PREFIX = "grants-"
    OBJECT_EXECUTION_ID_PREFIX = "objects-"

    def __init__(self, spark, inventory_catalog, inventory_database):
        """
        Initialize the InventoryCollector class with the required parameters.

        Args:
            spark (SparkSession): The SparkSession instance used to execute Spark SQL queries.
            inventory_catalog (str): The name of the catalog where the inventory tables will be stored. Must exist.
            inventory_database (str): The name of the database where the inventory tables will be stored. Will be created if not exist.
        """
        self.spark = spark
        self.inventory_catalog = inventory_catalog
        self.inventory_database = inventory_database
        
        
        if self.inventory_catalog == 'hive_metastore':
            # Assume we've not using UC yet, OR already issued USE CATALOG hive_metastore
            self.inventory_catdb = f"`{self.inventory_database}`"
            self.inventory_destHms = True
        else:
            self.inventory_catdb = f"`{self.inventory_catalog}`.`{self.inventory_database}`"
            self.inventory_destHms = False

    def initialize(self):
        print(f'Will save results to: {self.inventory_catdb}. {("Saving to HMS" if self.inventory_destHms else "Saving to UC Catalog")}')
        self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.inventory_catdb}')
        self.spark.sql(f'CREATE TABLE IF NOT EXISTS {self.inventory_catdb}.grant_statements (Principal STRING, ActionType STRING, ObjectType STRING, ObjectKey STRING, inventory_execution_id STRING, execution_time TIMESTAMP, source_database STRING, grant_statement STRING)')
        self.spark.sql(f'CREATE TABLE IF NOT EXISTS {self.inventory_catdb}.db_objects (source_database STRING, table STRING, objectType STRING, location STRING, viewText STRING, errMsg STRING, inventory_execution_id STRING, execution_time TIMESTAMP)')
    
    def resetAllData(self):
        print(f"Dropping inventory database: {self.inventory_catdb}")
        self.spark.sql(f'DROP DATABASE IF EXISTS {self.inventory_catdb} CASCADE')
        self.initialize()
        
    def scan_database_objects(self, database_name):
        execution_id = self.OBJECT_EXECUTION_ID_PREFIX + str(uuid.uuid4())
        execution_time = current_timestamp()
        start_time_python = datetime.now()
        print(f"Running DB Inventory for {database_name} with exec_id {execution_id} at time {start_time_python}")

        objTableSchema = StructType([
            StructField("source_database", StringType(), True),
            StructField("table", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("location", StringType(), True),
            StructField("viewText", StringType(), True),
            StructField("errMsg", StringType(), True)
        ])
        object_rows = set()

        tables = self.spark.sql(f"SHOW TABLES IN {database_name}").filter(col("isTemporary") == False).collect()
        print(f"{database_name} has {len(tables)} objects")

        for table in tables:
            table_name = table.tableName
            try:
                desc_result = self.spark.sql(f"DESCRIBE TABLE EXTENDED {database_name}.{table_name};")

                object_type = desc_result.filter('col_name = "Type"').select("data_type").collect()[-1].data_type

                object_location_raw = desc_result.filter('col_name = "Location"').select("data_type").collect()
                object_location = object_location_raw[-1].data_type if len(object_location_raw) > 0 else "n/a"

                view_text = desc_result.filter('col_name = "View Text"').select("data_type").collect()[-1].data_type if object_type == 'VIEW' else "n/a"

            except Exception as e2:
                print(f" TABLE: {database_name}.{table_name} -- ERROR RETRIEVING DETAILS:\nERROR MSG: {str(e2)}")
                object_rows.add(Row(database=database_name, table=table_name, objectType="ERROR", location=None, viewText=None, errMsg=str(e2)))
                continue

            print(f" TABLE: {database_name}.{table_name} -- TYPE: {object_type}")
            object_rows.add(Row(database=database_name, table=table_name, objectType=object_type, location=object_location, viewText=view_text, errMsg=None))

        object_df = self.spark.createDataFrame(object_rows, objTableSchema)
        object_df = object_df.withColumn("inventory_execution_id", lit(execution_id)).withColumn("execution_time", execution_time)

        end_time = datetime.now()
        elapsed = end_time - start_time_python

        print(f"Finished inventory of database {database_name} execution_id: {execution_id}. End time {end_time}. Elapsed: {elapsed}")
        
        object_df.write.mode("append").saveAsTable(f"{self.inventory_catdb}.db_objects")

        return (execution_id, object_df)
   
    def get_result_table(self, data_type="grants", nameOnly = False):
        """
        Get the result table based on the data_type parameter.

        Args:
            data_type (str): The type of data to retrieve, either "grants" or "objects". Defaults to "grants".

        Returns:
            DataFrame: The corresponding result table.
        """
        data_type_to_table = {
            "grants": f"{self.inventory_catdb}.grant_statements",
            "objects": f"{self.inventory_catdb}.db_objects"
        }

        if data_type not in data_type_to_table.keys():
            raise ValueError(f"Invalid data_type. Must be one of {list(data_type_to_table.keys())}.")
        tableName = data_type_to_table[data_type]
        
        if nameOnly:
            return tableName
        else:
            return self.spark.table(tableName)
        
    def get_results_by_execution_id(self, execution_id, data_type=None):
        """
        Get the results associated with an input execution ID.

        Args:
            execution_id (str): The inventory execution ID to search for.
            data_type (str, optional): The type of data to retrieve, either "grants" or "objects". Defaults to None.

        Returns:
            DataFrame: The results for the specified execution ID.
        """
        if data_type is None:
            if execution_id.startswith(self.GRANT_EXECUTION_ID_PREFIX):
                data_type = "grants"
            elif execution_id.startswith(self.OBJECT_EXECUTION_ID_PREFIX):
                data_type = "objects"
            else:
                raise ValueError(f"Unable to determine data_type from execution_id: {execution_id}. Please provide a valid data_type parameter.")

        data_df = self.get_result_table(data_type)
        return data_df.filter(data_df["inventory_execution_id"] == execution_id)

    def get_last_execution(self, data_type="grants", database_name=None):
        table_name = self.get_result_table(data_type, nameOnly=True)
        
        if database_name:
            where_clause = f"WHERE source_database = '{database_name}'"
        else:
            where_clause = ""
    
        latest_execution_id = self.spark.sql(f"""
            SELECT inventory_execution_id
            FROM (
                SELECT inventory_execution_id, execution_time,
                    ROW_NUMBER() OVER (ORDER BY execution_time DESC) as row_num
                FROM {table_name}
                {where_clause}
            ) ranked_data
            WHERE row_num = 1
        """).collect()[0][0]
        
        return latest_execution_id

    def get_all_latest_executions(self, data_type="grants"):
        table_name = self.get_result_table(data_type, nameOnly=True)

        # Group by source_database and return the last execution id for every source_database
        latest_execution_df = self.spark.sql(f"""
            SELECT DISTINCT source_database, inventory_execution_id, execution_time
            FROM (
                SELECT source_database, inventory_execution_id, execution_time,
                    ROW_NUMBER() OVER (PARTITION BY source_database ORDER BY execution_time DESC) as row_num
                    FROM {table_name}
            ) ranked_data
            WHERE row_num = 1
        """)
        
        return latest_execution_df.withColumn("data_type", lit(data_type))
        
    def get_last_results(self, *args):
        exec_id = self.get_last_execution(*args)
        return self.get_results_by_execution_id(exec_id)

    def get_execution_history(self, data_type=None):
        if data_type is None:
            # Get execution history for both grants and objects, and union the results
            grants_history = self.get_execution_history(data_type="grants")
            objects_history = self.get_execution_history(data_type="objects")
            execution_history = grants_history.union(objects_history)
        else:
            # Execute the SQL query for the specified data_type and return the result
            table_name = self.get_result_table(data_type, nameOnly=True)
            execution_history = self.spark.sql(f"""
                SELECT DISTINCT inventory_execution_id, execution_time, source_database
                FROM {table_name}
                ORDER BY execution_time DESC
            """)
    
        return execution_history.withColumn('data_type', lit(data_type))

    def get_database_inventory_summary(self):
        # Get the latest grant executions for each database
        latest_grant_executions = self.get_all_latest_executions(data_type="grants")
        latest_grant_executions = latest_grant_executions.withColumnRenamed("source_database", "database")
        latest_grant_executions = latest_grant_executions.withColumnRenamed("inventory_execution_id", "grant_last_execution_id")
        latest_grant_executions = latest_grant_executions.withColumnRenamed("execution_time", "grant_last_execution_time")
        latest_grant_executions = latest_grant_executions.drop("data_type")

        # Get the latest object inventory executions for each database
        latest_object_executions = self.get_all_latest_executions(data_type="objects")
        latest_object_executions = latest_object_executions.withColumnRenamed("source_database", "database")
        latest_object_executions = latest_object_executions.withColumnRenamed("inventory_execution_id", "object_last_execution_id")
        latest_object_executions = latest_object_executions.withColumnRenamed("execution_time", "object_last_execution_time")
        latest_object_executions = latest_object_executions.drop("data_type")

        # Join the grant and object inventory executions
        summary_df = latest_grant_executions.join(latest_object_executions, on="database", how="outer")
    
        # # Fill in missing values
        # summary_df = summary_df.fillna({"grant_last_execution_id": "", "grant_last_execution_time": "", 
        #                                 "object_last_execution_id": "", "object_last_execution_time": ""})
    
        # Add object counts for each database
        object_counts = self.spark.table(f"{self.inventory_catdb}.db_objects").groupBy("source_database", "objectType").count()
        object_counts = object_counts.groupBy("source_database").pivot("objectType").agg(first("count"))
        object_counts = object_counts.withColumnRenamed("source_database", "database")
        object_counts = object_counts.fillna(0)
        summary_df = summary_df.join(object_counts, on="database", how="outer")
    
        # Add grant count for each database
        grant_count = self.spark.table(f"{self.inventory_catdb}.grant_statements").groupBy("source_database").count()
        grant_count = grant_count.withColumnRenamed("source_database", "database")
        grant_count = grant_count.withColumnRenamed("count", "grant_count")
        grant_count = grant_count.fillna(0)
        summary_df = summary_df.join(grant_count, on="database", how="outer")
    
    
        # Return the result
        return summary_df



    def scan_database_grants(self, database_name):
        # Create a random execution ID and get the current timestamp
        execution_id = self.GRANT_EXECUTION_ID_PREFIX + str(uuid.uuid4())
        execution_time = current_timestamp()
        start_time_python = datetime.now()
        print(f"Start inventory of database {database_name}. Creating inventory_execution_id: {execution_id} and execution_time: {execution_time}")
        processed_acl = set()
    
        # Set the current database
        self.spark.sql(f"USE {database_name}")

        quotedDbName = f'`{database_name}`'
        # append the database df to the list
        databaseGrants = ( self.spark.sql(f"SHOW GRANT ON DATABASE {database_name}")
                                    .filter(col("ObjectType")=="DATABASE")
                                    .withColumn("ObjectKey", lit(quotedDbName))
                                    .withColumn("ObjectType", lit("DATABASE"))
                                    # .filter(col("ActionType")!="OWN")
                            )
        processed_acl.update(databaseGrants.collect())

        # Get the list of tables and views
        tables_and_views = self.spark.sql(f"SHOW TABLES in {database_name}").filter(col("isTemporary") == False).collect()
    
        # Iterate over tables and views
        for table_view in tables_and_views:
            table_name = table_view["tableName"]
            table_fullname = f'`{database_name}`.`{table_view.tableName}`'
        
            tableGrants = (self.spark.sql(f"SHOW GRANT ON TABLE {table_name}")
                            .filter(col("ObjectType")=="TABLE")
                            .withColumn("ObjectKey", lit(table_fullname))
                            .withColumn("ObjectType", lit("TABLE"))
                            .collect()
                        )
            # tableGrants Schema:
            # Principal, ActionType, ObjectType, ObjectKey

            # Add to combined list
            processed_acl.update(tableGrants)
        
            
        # If there are no grants in the database, return early
        if len(processed_acl) == 0:
            end_time = datetime.now()
            elapsed = end_time - start_time_python
            print(f"{end_time} - Finished inventory of database {database_name}. No grants found. execution_id: {execution_id}. Elapsed: {elapsed}")
            return (execution_id, None)

            
        # Convert the processed ACL entries into a DataFrame
        all_acl = (self.spark.createDataFrame(processed_acl, ["Principal", "ActionType", "ObjectType", "ObjectKey"]).distinct()
                    .withColumn("inventory_execution_id", lit(execution_id))
                    .withColumn("execution_time", lit(execution_time))
                    .withColumn("source_database", lit(database_name)))

        # Build GRANT statements for each row in the all_acl DataFrame
        all_acl = all_acl.withColumn(
            "grant_statement",
            concat(
                lit("GRANT "),
                all_acl["ActionType"],
                lit(" ON "),
                all_acl["ObjectType"], 
                lit(" "),
                all_acl["ObjectKey"], 
                lit(' TO `'),
                all_acl["Principal"],
                lit('`'),
            )
        )

        end_time = datetime.now()
        elapsed = end_time - start_time_python

        print(f"{end_time} - Finished inventory of database {database_name}. {len(processed_acl)} acl items found. execution_id: {execution_id}. Elapsed: {elapsed}")

        # Append the GRANT statements to the grant_statements table
        all_acl.write.mode("append").saveAsTable(f"{self.inventory_catdb}.grant_statements")
        print(f"{datetime.now()} - Finished writing {len(processed_acl)} results for database {database_name}. {execution_id}.")
        return (execution_id, all_acl)
        
    def scan_all_databases(self):
        # Get the list of databases
        databases = self.spark.sql("SHOW DATABASES").select("databaseName").collect()
        
        # Iterate over databases
        for db in databases:
            database_name = db["databaseName"]
            print(f"Starting inventory of database {database_name}")
            
            # Scan database objects
            (object_exec_id, object_df) = self.scan_database_objects(database_name)
            print(f"Finished scanning objects for {database_name}. Execution ID: {object_exec_id}")
            
            # Scan database grants
            (grant_exec_id, grant_df) = self.scan_database_grants(database_name)
            print(f"Finished scanning grants for {database_name}. Execution ID: {grant_exec_id}")
        
        print("Finished scanning all databases")
        
    def scan_all_databases(self, rescan=False):
    
        if not rescan:
            print("First, scanning existing progress")
            # Generate exclusion list for grant_statements
            grant_db_list = self.get_result_table('grants').select("source_database").distinct().collect()
            grant_db_list = [row["source_database"] for row in grant_db_list]

            # Generate exclusion list for db_objects
            object_db_list = self.get_result_table('objects').select("source_database").distinct().collect()
            object_db_list = [row["source_database"] for row in object_db_list]
        else:
            grant_db_list = []
            object_db_list = []

        # Get a list of all databases
        all_databases = [db["databaseName"] for db in self.spark.sql("SHOW DATABASES").select("databaseName").collect()]
        
        for database_name in all_databases:
            # Skip databases that have already been scanned and have data in the inventory
            if not rescan and database_name in grant_db_list and database_name in object_db_list:
                print(f"Skipping database {database_name} as it has already been scanned and has data in the inventory.")
                continue

            # Scan database objects
            if database_name not in object_db_list:
                (object_exec_id, object_df) = self.scan_database_objects(database_name)
                print(f"Finished scanning objects for {database_name}. Execution ID: {object_exec_id}")

            # Scan database grants
            if database_name not in grant_db_list:
                (grant_exec_id, grant_df) = self.scan_database_grants(database_name)
                print(f"Finished scanning grants for {database_name}. Execution ID: {grant_exec_id}")        
        
        print("Finished scanning all databases")

#End of class