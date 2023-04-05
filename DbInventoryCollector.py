# Start of DbInventoryCollector.py
# Manually copy this entire file into a single databricks notebook cell

# These imports are used
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, FloatType
import uuid
from datetime import datetime, timedelta

# Unused imports but saving for future reference
from functools import reduce
import concurrent.futures
import time

# Inventory Collector Class -- Holds all functions in this file for an easy import statement
class InventoryCollector:
    @staticmethod
    def CreateWidgets(dbutils, spark, reset = False):
        if reset:
            dbutils.widgets.removeAll()

        allCatalogs = ['hive_metastore']
        try:
            catQueryResult = [r.catalog for r in spark.sql('show CATALOGS').collect()]
            if len(catQueryResult) == 1 and catQueryResult[0] == spark_catalog:
                print("WARNING: Not connected to a Unity Catalog Cluster.")
            allCatalogs = allCatalogs + [r.catalog for r in spark.sql('show CATALOGS').collect()]
        except Exception as e:
            print("Unexpected error retrieving catalog listing.")
            print(e)

        dbutils.widgets.dropdown("Scan_Catalog", 'hive_metastore', allCatalogs, "Scan Catalog")

        # print("\n*********************************\nCatalog list:")
        # print('\n'.join(allCatalogs))

        selectedCatalog = dbutils.widgets.get("Scan_Catalog")

        try:
            spark.sql(f'use catalog {selectedCatalog};')
        except Exception as e:
            if selectedCatalog == "hive_metastore":
                pass
            else:
                raise e

        scanCatalogDbList = [row.databaseName for row in spark.sql('show databases').select('databaseName').collect()]
        if len('hmsDatabaseList') > 1024:
            print('Warning! More than 1024 HMS databases. Picker widget will only display first 1024')
        dbutils.widgets.dropdown("Scan_Database", scanCatalogDbList[0], scanCatalogDbList[0:1023], "Scan Database")
        dbutils.widgets.text("Inventory_Catalog",   "hive_metastore", "Write Catalog for Inventory")
        dbutils.widgets.text("Inventory_Database",   "databricks_inventory", "Write Database for Inventory")
        dbutils.widgets.text("Migration_Catalog", "ChangeMeToDest", "Migration Destination Catalog")

        print(f"Database list for Catalog {selectedCatalog}:\n")
        print('\n'.join(scanCatalogDbList))


    OBJECT_EXECUTION_ID_PREFIX = "objects-"
    GRANT_EXECUTION_ID_PREFIX = "grants-"
    FUNCTION_EXECUTION_ID_PREFIX = "func_meta-"

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
            self.inventory_catdb = f"`{self.inventory_database}`" #Will have to rely on tryUse Catalog to ensure this works
            self.inventory_destHms = True
        else:
            self.inventory_catdb = f"`{self.inventory_catalog}`.`{self.inventory_database}`"
            self.inventory_destHms = False

    def setCatalog(self, catalogName):
        if catalogName == 'hive_metastore':
            try:
                self.spark.sql(f'USE CATALOG {catalogName};')
            except Exception as e:
                pass #Probably just not on UC enabled cluster, so just ignore
        else:
            #No try, this is important
            self.spark.sql(f'USE CATALOG {catalogName};')
            
    def setStorageCatalog(self):
        self.setCatalog(self.inventory_catalog)

    def initialize(self):
        print(f'Will save results to: {self.inventory_catdb}. {("Saving to HMS" if self.inventory_destHms else "Saving to UC Catalog")}')
        self.setStorageCatalog()
        self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.inventory_catdb}')

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.inventory_catdb}.grant_statements (
                Principal STRING,
                ActionType STRING,
                ObjectType STRING,
                ObjectKey STRING,
                inventory_execution_id STRING,
                execution_time TIMESTAMP,
                source_catalog STRING,
                source_database STRING,
                grant_statement STRING
            )
        """)

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.inventory_catdb}.db_objects (
                source_catalog STRING,
                source_database STRING,
                table STRING,
                objectType STRING,
                location STRING,
                viewText STRING,
                errMsg STRING,
                inventory_execution_id STRING,
                execution_time TIMESTAMP
            )
        """)

        # Create execution_history table if it does not exist
        create_exec_hist_query = f"""
            CREATE TABLE IF NOT EXISTS {self.inventory_catdb}.execution_history (
                inventory_execution_id STRING,
                execution_time TIMESTAMP,
                source_catalog STRING,
                source_database STRING,
                data_type STRING,
                time_elapsed FLOAT,
                records_written INT
            )
        """
        self.spark.sql(create_exec_hist_query)
    
    def migrate_storage_add_catalog(self):
        self.spark.sql(f"""
            ALTER TABLE {self.inventory_catdb}.grant_statements
            ADD COLUMN source_catalog STRING
        """)

        self.spark.sql(f"""
            UPDATE {self.inventory_catdb}.grant_statements
            SET source_catalog = 'hive_metastore'
            WHERE source_catalog IS NULL
        """)

        self.spark.sql(f"""
            ALTER TABLE {self.inventory_catdb}.db_objects
            ADD COLUMN source_catalog STRING 
        """)

        self.spark.sql(f"""
            UPDATE {self.inventory_catdb}.db_objects
            SET source_catalog = 'hive_metastore'
            WHERE source_catalog IS NULL
        """)


        self.spark.sql(f"""
            ALTER TABLE {self.inventory_catdb}.execution_history
            ADD COLUMN source_catalog STRING 
        """)

        self.spark.sql(f"""
            UPDATE {self.inventory_catdb}.execution_history
            SET source_catalog = 'hive_metastore'
            WHERE source_catalog IS NULL
        """)

    
    def resetAllData(self):
        print(f"Dropping inventory database: {self.inventory_catdb}")
        self.spark.sql(f'DROP DATABASE IF EXISTS {self.inventory_catdb} CASCADE')
        self.initialize()
    
    def write_execution_record(self, execution_id, execution_time, catalog_name, database_name, data_type, records_written):
        # Define the schema for the execution_history DataFrame
        schema = StructType([
            StructField("inventory_execution_id", StringType(), True),
            StructField("execution_time", TimestampType(), True),
            StructField("source_catalog", StringType(), True),
            StructField("source_database", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("time_elapsed", FloatType(), True),
            StructField("records_written", IntegerType(), True)
        ])

        #success print
        end_time = datetime.now()
        time_elapsed = (end_time - execution_time).total_seconds()
        
        # Create a DataFrame with the provided data and schema
        execution_record = self.spark.createDataFrame(
            [(execution_id, execution_time, catalog_name, database_name, data_type, time_elapsed, records_written)],
            schema=schema
        )
        self.setStorageCatalog()
        execution_record.write.mode("append").saveAsTable(f"{self.inventory_catdb}.execution_history")
        print(f"{end_time} - Finished {data_type.upper()} inventory of database {catalog_name}.{database_name} execution_id: {execution_id}. Elapsed: {time_elapsed}s")

        
    def scan_database_objects(self, scanCatalog, database_name):
        execution_id = self.OBJECT_EXECUTION_ID_PREFIX + str(uuid.uuid4())
        execution_time = datetime.now()
        print(f"{execution_time} - Start OBJECT Inventory for {database_name} with exec_id {execution_id}")

        objTableSchema = StructType([
            StructField("source_catalog", StringType(), True),
            StructField("source_database", StringType(), True),
            StructField("table", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("location", StringType(), True),
            StructField("viewText", StringType(), True),
            StructField("errMsg", StringType(), True)
        ])
        object_rows = set()

        self.setCatalog(scanCatalog)
        
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
                object_rows.add(Row(source_catalog=scanCatalog, database=database_name, table=table_name, objectType="ERROR", location=None, viewText=None, errMsg=str(e2)))
                continue

            print(f" TABLE: {database_name}.{table_name} -- TYPE: {object_type}")
            object_rows.add(Row(source_catalog=scanCatalog, database=database_name, table=table_name, objectType=object_type, location=object_location, viewText=view_text, errMsg=None))

        #Create object DF and write to db_objects table
        if len(object_rows) > 0:
            object_df = self.spark.createDataFrame(object_rows, objTableSchema)
            object_df = object_df.withColumn("inventory_execution_id", lit(execution_id)).withColumn("execution_time", lit(execution_time))        
            self.setStorageCatalog()
            object_df.write.mode("append").saveAsTable(f"{self.inventory_catdb}.db_objects")
        else:
            object_df = None

        #Record finished (this also prints)
        self.write_execution_record(execution_id, execution_time, scanCatalog, database_name, "objects", len(object_rows))
        
        return (execution_id, object_df)
    
    def scan_database_grants(self, scanCatalog, database_name):
        # Create a random execution ID and get the current timestamp
        execution_id = self.GRANT_EXECUTION_ID_PREFIX + str(uuid.uuid4())
        execution_time = datetime.now()
        print(f"{execution_time} - Start 'grants' inventory of database {database_name}. Creating inventory_execution_id: {execution_id}")
        processed_acl = set()
    
        # Set the current database
        self.setCatalog(scanCatalog)
        self.spark.sql(f"USE {database_name}")

        quotedDbName = f'`{database_name}`'
        # append the database df to the list
        databaseGrants = ( self.spark.sql(f"SHOW GRANT ON DATABASE {database_name}")
                                    .filter('ObjectType == "DATABASE" OR ObjectType == "SCHEMA"')
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
        numGrants = len(processed_acl)
        if numGrants > 0:
            # Convert the processed ACL entries into a DataFrame
            acl_df = (self.spark.createDataFrame(processed_acl, ["Principal", "ActionType", "ObjectType", "ObjectKey"]).distinct()
                        .withColumn("inventory_execution_id", lit(execution_id))
                        .withColumn("execution_time", lit(execution_time))
                        .withColumn("source_database", lit(database_name))
                        .withColumn("source_catalog", lit(scanCatalog)))

            # Build GRANT statements for each row in the all_acl DataFrame
            acl_df = acl_df.withColumn(
                "grant_statement",
                concat(
                    lit("GRANT "),
                    acl_df["ActionType"],
                    lit(" ON "),
                    acl_df["ObjectType"], 
                    lit(" "),
                    acl_df["ObjectKey"], 
                    lit(' TO `'),
                    acl_df["Principal"],
                    lit('`'),
                )
            )

            self.setStorageCatalog()
            acl_df.write.mode("append").saveAsTable(f"{self.inventory_catdb}.grant_statements")
            print(f"{datetime.now()} - Finished writing {len(processed_acl)} results for database {database_name}. {execution_id}.")
        else:
            acl_df = None

        #Record finished (this also prints)
        self.write_execution_record(execution_id, execution_time, scanCatalog, database_name, "grants", numGrants)
        return (execution_id, acl_df)

    def scan_catalog_functions(self, scan_catalog):
        def list_functions(catalogName, databaseName):
            functions = self.spark.sql(f"SHOW FUNCTIONS IN `{catalogName}`.`{databaseName}`").filter(col("function").startswith(f"{catalogName}.{databaseName}"))
            return functions

        def describe_function_extended(functionNameFull):
            result = self.spark.sql(f"DESCRIBE FUNCTION EXTENDED {functionNameFull}")
            result_str = "\n".join([row["function_desc"] for row in result.collect()])
            return result_str

        execution_id = self.FUNCTION_EXECUTION_ID_PREFIX + str(uuid.uuid4())
        execution_time = datetime.now()
        print(f"{execution_time} - Start 'functions' inventory of catalog {scan_catalog}. Creating inventory_execution_id: {execution_id}")

        # Create an empty DataFrame to store the results
        schema = "source_catalog STRING, source_database STRING, function_name_full STRING, function_desc STRING"
        results_df = self.spark.createDataFrame([], schema)

        self.setCatalog(scan_catalog)
        for db in self.spark.sql(f"SHOW DATABASES").collect():
            funcs = list_functions(scan_catalog, db.databaseName).collect()
            funcCount = len(funcs)
            if funcCount > 0: 
                print(f"{db.databaseName} has {funcCount} functions")
                for func in funcs:
                    function_name = func["function"]
                    describe_result = describe_function_extended(function_name)
                    row = (scan_catalog, db.databaseName, function_name, describe_result)
                    print(function_name)
                    print(describe_result)

                    # Add the result to the results DataFrame
                    results_df = results_df.union(self.spark.createDataFrame([row], schema))

        # Save the results to a Delta table
        results_df = results_df.withColumn("inventory_execution_id", lit(execution_id)).withColumn("execution_time", lit(execution_time))
        self.setStorageCatalog()
        results_df.write.format("delta").mode("append").saveAsTable(f"{self.inventory_catdb}.db_functions")
        return results_df

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
            self.setStorageCatalog()
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

    def get_last_execution_id(self, data_type="grants", catalog_name = "hive_metastore", database_name=None):
        table_name = self.get_result_table(data_type, nameOnly=True)
        where_clause = f"WHERE source_catalog = '{catalog_name}'"
        if database_name:
            where_clause = where_clause + f" AND source_database = '{database_name}'"
    
        self.setStorageCatalog()
        try:
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
        except IndexError:
            latest_execution_id = None
        
        return latest_execution_id

    def get_all_latest_executions(self, data_type="grants", whichCatalog = 'hive_metastore'):
        table_name = self.get_result_table(data_type, nameOnly=True)
        self.setStorageCatalog()
        # Group by source_database and return the last execution id for every source_database
        latest_execution_df = self.spark.sql(f"""
            SELECT DISTINCT source_database, inventory_execution_id, execution_time
            FROM (
                SELECT source_database, inventory_execution_id, execution_time,
                    ROW_NUMBER() OVER (PARTITION BY source_database ORDER BY execution_time DESC) as row_num
                    FROM {table_name}
                    WHERE source_catalog = '{whichCatalog}'
            ) ranked_data
            WHERE row_num = 1
        """)
        
        return latest_execution_df.withColumn("data_type", lit(data_type))
        
    def get_last_results(self, *args):
        exec_id = self.get_last_execution_id(*args)
        if exec_id is None: return None
        return self.get_results_by_execution_id(exec_id)

    def get_execution_history(self, data_type=None):
        self.setStorageCatalog()
        if data_type is None:
            # Get execution history for both grants and objects, and union the results
            grants_history = self.get_execution_history(data_type="grants")
            objects_history = self.get_execution_history(data_type="objects")
            execution_history = grants_history.union(objects_history)
        else:
            # Execute the SQL query for the specified data_type and return the result
            table_name = self.get_result_table(data_type, nameOnly=True)
            execution_history = self.spark.sql(f"""
                SELECT DISTINCT inventory_execution_id, execution_time, source_catalog, source_database
                FROM {table_name}
                ORDER BY execution_time DESC
            """).withColumn('data_type', lit(data_type))
    
        return execution_history

    def get_database_inventory_summary(self, whichCatalog):
        # Get the latest grant executions for each database
        latest_grant_executions = self.get_all_latest_executions(data_type="grants", whichCatalog=whichCatalog)
        latest_grant_executions = latest_grant_executions.withColumnRenamed("source_database", "database")
        latest_grant_executions = latest_grant_executions.withColumnRenamed("inventory_execution_id", "grant_last_execution_id")
        latest_grant_executions = latest_grant_executions.withColumnRenamed("execution_time", "grant_last_execution_time")
        latest_grant_executions = latest_grant_executions.drop("data_type")

        # Get the latest object inventory executions for each database
        latest_object_executions = self.get_all_latest_executions(data_type="objects", whichCatalog=whichCatalog)
        latest_object_executions = latest_object_executions.withColumnRenamed("source_database", "database")
        latest_object_executions = latest_object_executions.withColumnRenamed("inventory_execution_id", "object_last_execution_id")
        latest_object_executions = latest_object_executions.withColumnRenamed("execution_time", "object_last_execution_time")
        latest_object_executions = latest_object_executions.drop("data_type")

        # Join the grant and object inventory executions
        summary_df = latest_grant_executions.join(latest_object_executions, on="database", how="outer")
    
        # Don't fill in missing values, keep as NULL
        # summary_df = summary_df.fillna({"grant_last_execution_id": "", "grant_last_execution_time": "", 
        #                                 "object_last_execution_id": "", "object_last_execution_time": ""})
    
        # Add object counts for each database
        object_counts = self.spark.table(f"{self.inventory_catdb}.db_objects").filter(col("source_catalog") == lit(whichCatalog)).groupBy("inventory_execution_id", "source_database", "objectType").count()
        object_counts = object_counts.withColumnRenamed("inventory_execution_id", "object_last_execution_id")
        object_counts = object_counts.groupBy("object_last_execution_id").pivot("objectType").agg(first("count"))
        object_counts = object_counts.fillna(0)
        summary_df = summary_df.join(object_counts, on=["object_last_execution_id"], how="left")
    
        # Add grant count for each database
        grant_counts = self.spark.table(f"{self.inventory_catdb}.grant_statements").filter(col("source_catalog") == lit(whichCatalog)).groupBy("inventory_execution_id", "source_database").count()
        grant_counts = grant_counts.withColumnRenamed("source_database", "database") # Needed?
        grant_counts = grant_counts.withColumnRenamed("count", "grant_count")
        grant_counts = grant_counts.withColumnRenamed("inventory_execution_id", "grant_last_execution_id")
        grant_counts = grant_counts.fillna(0)
        summary_df = summary_df.join(grant_counts, on=["grant_last_execution_id", "database"], how="left")
    
        # Return the result
        return summary_df

        
    def scan_all_databases(self, scanCatalog, rescan=False, scanObjects = True, scanGrants = True, databaseScanList = None):
        if not scanObjects and not scanGrants:
            print("WARNING: Neither scanObjects nor scanGrants requested. Doing nothing.")
            return
        
        grant_db_list = []
        object_db_list = []
        
        if not rescan:
            print("First, scanning existing progress")
            self.setStorageCatalog()
            # Generate exclusion list for db_objects
            if scanObjects:
                object_db_list = self.get_result_table('objects').select("source_database").distinct().collect()
                object_db_list = [row["source_database"] for row in object_db_list]

            # Generate exclusion list for grant_statements
            if scanGrants:
                grant_db_list = self.get_result_table('grants').select("source_database").distinct().collect()
                grant_db_list = [row["source_database"] for row in grant_db_list]
            

        # Get a list of databases to scan
        self.setCatalog(scanCatalog)
        scan_databases = [db["databaseName"] for db in self.spark.sql("SHOW DATABASES").select("databaseName").collect()]
        if databaseScanList is not None:
            scan_databases = [db for db in scan_databases if db in databaseScanList]
            print(f"databaseFilterList list present. Will only scan: {scan_databases}")

        for database_name in scan_databases:
            # Skip databases that have already been scanned and have data in the inventory
            if not rescan and database_name in grant_db_list and database_name in object_db_list:
                print(f"Skipping database {database_name} as it has already been scanned and has data in the inventory.")
                continue

            # Scan database objects
            if scanObjects and database_name not in object_db_list:
                (object_exec_id, object_df) = self.scan_database_objects(scanCatalog, database_name)
                print(f"Finished scanning objects for {database_name}. Execution ID: {object_exec_id}")

            # Scan database grants
            if scanGrants and database_name not in grant_db_list:
                (grant_exec_id, grant_df) = self.scan_database_grants(scanCatalog, database_name)
                print(f"Finished scanning grants for {database_name}. Execution ID: {grant_exec_id}")        
        
        print("Finished scanning all databases")

    def _generate_ddl_external(self, df: DataFrame, destCatalog: str) -> DataFrame:
        # Create a new column with the desired SQL DDL statements
        df = df.withColumn(
            "sql_ddl",
            concat_ws("",
                lit("-- Upgrade EXTERNAL table: hive_metastore."),
                concat_ws(".",
                    df["source_database"],
                    df["table"],
                    lit(f" to {destCatalog}"), 
                    df["source_database"],
                    df["table"]
                ),
                lit(f"\nCREATE TABLE IF NOT EXISTS {destCatalog}."), 
                df["source_database"], lit("."), df["table"],
                lit(" LIKE hive_metastore."),
                df["source_database"], lit("."), df["table"],
                lit(" COPY LOCATION;\nALTER TABLE hive_metastore."),
                df["source_database"], lit("."), df["table"],
                lit(f" SET TBLPROPERTIES ('upgraded_to' = '{destCatalog}."), 
                df["source_database"], lit("."), df["table"],
                lit("');")
            )
        )
        
        return df
    
    def _generate_ddl_managed(self, df: DataFrame, destCatalog: str) -> DataFrame:
        # Create a new column with the desired SQL DDL statements
        df = df.withColumn(
            "sql_ddl",
            concat_ws("",
                lit("-- Copy MANAGED table: hive_metastore."),
                concat_ws(".",
                    df["source_database"],
                    df["table"],
                    lit(f" to {destCatalog}"), 
                    df["source_database"],
                    df["table"]
                ),
                lit(f"\nCREATE TABLE IF NOT EXISTS {destCatalog}."), 
                df["source_database"], lit("."), df["table"],
                lit(" DEEP CLONE hive_metastore."),
                df["source_database"], lit("."), df["table"],
                lit(";")
            )
        )
        
        return df
    
    def _generate_ddl_view(self, df: DataFrame, destCatalog: str) -> DataFrame:
        # Create a new column with the desired SQL DDL statements
        df = df.withColumn(
            "sql_ddl",
            concat_ws("",
                lit("-- Recreate VIEW: hive_metastore."),
                concat_ws(".",
                    df["source_database"],
                    df["table"],
                    lit(f" to {destCatalog}"), 
                    df["source_database"],
                    df["table"]
                ),
                lit(f"\nUSE CATALOG {destCatalog};"),
                lit(f"\nUSE DATABASE "),
                df["source_database"],
                lit(f";\nCREATE VIEW  IF NOT EXISTS {destCatalog}."), 
                df["source_database"], lit("."), df["table"],
                lit(" AS\n"),
                df["viewText"],
                lit(";")
            )
        )
        
        return df
    
    # TODO: Add call out for external locations referenced
    # https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html#manage-external-locations&language-sql

    def generate_migration_object_sql(self, inventoryDf: DataFrame, destCatalog: str, includeManaged = True) -> DataFrame:
        def collectSql(df, joinSep: str = '\n'):
            sqlList = [r.sql_ddl for r in df.select('sql_ddl').collect()]
            return (len(sqlList), joinSep.join(sqlList))

        print(f"Generating Object DDL to migrate from catalog hive_metastore to {destCatalog}")
        if includeManaged:
            print("Attention: Managed tables are INCLUDED. This will COPY the data.")
        else:
            print("Attention: Managed tables are EXCLUDED. View creation may fail.")

        # Filter input DataFrame by objectType and generate DDL for each type        
        createDbCommands = set()

        # First, complain about error objects we're not generating a DDL for
        error_df = inventoryDf.filter(inventoryDf["objectType"] == "ERROR")
        error_messages = [] #for output
        for row in error_df.collect():
            source_db = row["source_database"]
            table_name = row["table"]
            try:
                error_message = row["errMsg"].strip()
                if error_message:
                    first_line_error = error_message.split('\n')[0]
                else:
                    first_line_error = "Error message is empty or not available"
            except Exception as e:
                first_line_error = "Exception retrieving message"
            error_messages.append(f"-- ERROR: Error retrieving information for object in the source database '{source_db}' and table '{table_name}'.\n--        Error Message (1st line): {first_line_error}")            

        #Generate DDLs for non-error object types
        df_external = self._generate_ddl_external(inventoryDf.filter(inventoryDf["objectType"] == "EXTERNAL"), destCatalog)
        if includeManaged:
            df_managed = self._generate_ddl_managed(inventoryDf.filter(inventoryDf["objectType"] == "MANAGED"), destCatalog)
        df_view = self._generate_ddl_view(inventoryDf.filter(inventoryDf["objectType"] == "VIEW"), destCatalog)

        #Generate extra commands just for database creation by scanning generated DDL
        createDbCommands.update([f"CREATE DATABASE IF NOT EXISTS {r.source_database};" 
                            for r in df_external.select('source_database').distinct().collect()])
        if includeManaged:
            createDbCommands.update([f"CREATE DATABASE IF NOT EXISTS {r.source_database};" 
                                for r in df_managed.select('source_database').distinct().collect()])
        createDbCommands.update([f"CREATE DATABASE IF NOT EXISTS {r.source_database};" 
                            for r in df_view.select('source_database').distinct().collect()])
        
        allCreateDbSql = "\n".join(createDbCommands)
        
        #Collect all SQL results here so we have the counts.
        (externalCount, externalSql) = collectSql(df_external, "\n\n")
        
        if includeManaged:
            (managedCount, managedSql) = collectSql(df_managed, "\n\n")
        else:
            (managedCount, managedSql) = (0, "")
        
        (viewCount, viewSql) = collectSql(df_view, "\n\n")
        errorCount = len(error_messages)

        #Start creating main block of SQL
        combinedSql = f"""-- Databricks Unity Catalog Migration DDL Generation
-- Automatically generated using DbInventoryCollector on {datetime.now()}
-- Generation Counts:
-- Databases: {len(createDbCommands)}
-- External Tables: {externalCount}
-- Managed Tables: {managedCount}
-- Views: {viewCount}
-- Errors: {errorCount} (Number of tables DDL generation skipped due to inventory error)
"""
        if errorCount > 0:
            combinedSql = combinedSql + "-- ERROR! Some tables were unable to have DDL generated due to erros during the inventory step. Error Listing:\n" + "\n".join(error_messages)

        combinedSql = combinedSql + f"\n--\n-- Set destination catalog\n--\nUSE CATALOG {destCatalog};\n"

        combinedSql = combinedSql + "\n--\n-- Create Destination Databases\n--\n" + allCreateDbSql
        
        (externalCount, externalSql) = collectSql(df_external, "\n\n")
        if externalCount > 0:
            combinedSql = combinedSql + f"\n\n--\n-- Create {externalCount} EXTERNAL Tables.\n--\n" + externalSql
        else:
            combinedSql = combinedSql + "\n\n--\n-- There are zero EXTERNAL Tables \n--"
        
        if includeManaged:
            if managedCount > 0:
                combinedSql = combinedSql + f"\n\n--\n-- Create {managedCount} MANAGED Tables.\n--\n" + managedSql
            else:
                combinedSql = combinedSql + "\n\n--\n-- There are zero MANAGED Tables \n--"

        if viewCount > 0:
            combinedSql = combinedSql + f"\n\n--\n-- Create {viewCount} VIEWs.\n--\n" + viewSql
        else:
            combinedSql = combinedSql + "\n\n--\n-- There are zero VIEWs \n--"

        return combinedSql

    # UC Priviledge Docs: https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html
    # UC Priviledge listing:
    #   Direct Catalog Priviledges: ALL PRIVILEGES, CREATE SCHEMA, USE CATALOG
    #   Cascading Catalog Priviledges: CREATE FUNCTION, CREATE TABLE, USE SCHEMA, EXECUTE, MODIFY, SELECT
    #   Direct Schema Priviledges: ALL PRIVILEGES, CREATE FUNCTION, CREATE TABLE, USE SCHEMA
    #   Cascading Schema Priviledges: EXECUTE, MODIFY, SELECT
    #   Table Priviledges: ALL PRIVILEGES, SELECT, MODIFY
    #   View Priviledges: ALL PRIVILEGES, SELECT
    #   Function Priviledges: ALL PRIVILEGES, EXECUTE
    #   External Location: ALL PRIVILEGES, CREATE EXTERNAL TABLE, READ FILES, WRITE FILES, CREATE MANAGED STORAGE

    # HIVE:
    # Docs: https://docs.databricks.com/sql/language-manual/sql-ref-privileges-hms.html
    # SELECT: gives read access to an object. Doubles as execute
    # CREATE: gives ability to create an object (for example, a table in a schema).
    # MODIFY: gives ability to add, delete, and modify data to or from an object.
    # USAGE: does not give any abilities, but is an additional requirement to perform any action on a schema object.
    # READ_METADATA: gives ability to view an object and its metadata.
    # CREATE_NAMED_FUNCTION: gives ability to create a named UDF in an existing catalog or schema.
    # MODIFY_CLASSPATH: gives ability to add files to the Spark class path.
    # ALL PRIVILEGES: gives all privileges (is translated into all the above privileges).

    #This dict takes the above comment's worth of explanations into a map between hive priviledge and unity catalog priviledge
    hive_to_uc_privilege_map = {
        ("FUNCTION", "SELECT"): "EXECUTE",
        ("TABLE", "SELECT"): "SELECT",
        ("TABLE", "MODIFY"): "MODIFY",
        ("TABLE", "READ_METADATA"): "IGNORE",
        ("TABLE", "DENIED_SELECT"): "IGNORE",
        ("TABLE", "OWN"): "ALTER",
        ("DATABASE", "USAGE"): "USE_SCHEMA",
        ("DATABASE", "CREATE"): "CREATE_TABLE",
        ("DATABASE", "CREATE_NAMED_FUNCTION"): "CREATE_FUNCTION",
        ("DATABASE", "SELECT"): "SELECT",
        ("DATABASE", "MODIFY"): "SELECT",
        ("DATABASE", "OWN"): "ALTER",
        ("DATABASE", "READ_METADATA"): "IGNORE",
    }

    def generate_migration_grant_sql(self, df: DataFrame, dest_catalog: str) -> str:
        if df is None:
            return('-- The input dataframe is None. (This probably means there are no grants to migrate)')
        
        dfCol = df.collect()
        if dfCol is None or len(dfCol) == 0:
            return('-- There are no non-inherited grants to migrate')

        migrated_grants = []
        unique_principals = set()
        unique_principal_schema_pairs = set()
        for row in dfCol:
            principal = row['Principal']
            action_type = row['ActionType']
            object_type = row['ObjectType']
            object_key = row['ObjectKey']
            source_database = row['source_database']

            unique_principals.add(principal)
            if object_type == "TABLE":
                unique_principal_schema_pairs.add((principal, source_database))

            new_action = self.hive_to_uc_privilege_map.get((object_type, action_type), None)

            if new_action is None or new_action == "IGNORE":
                print(f"Note: Skipping migration of GRANT action {action_type} on type {object_type} to {principal} -- not used in UC")
                continue
            elif new_action == "ALTER":
                #https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/ownership.html
                #ALTER <SECURABLE_TYPE> <SECURABLE_NAME> OWNER TO <PRINCIPAL>;
                statement = f"ALTER {object_type} {object_key} OWNER TO `{principal}`;"
            else:
                #https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html
                statement = f"GRANT {new_action} ON {object_type} {object_key} TO `{principal}`;"

            migrated_grants.append(statement)

        all_grants = [f'USE CATALOG {dest_catalog};']
        # Generate GRANT USE CATALOG statements for unique principals
        for principal in unique_principals:
            all_grants.append(f"GRANT USE CATALOG ON CATALOG {dest_catalog} TO `{principal}`;")

        # Generate GRANT USE SCHEMA statements for unique principal/schema pairs
        for principal, schema in unique_principal_schema_pairs:
            all_grants.append(f"GRANT USE SCHEMA ON SCHEMA {schema} TO `{principal}`;")

        all_grants = all_grants + migrated_grants
        return "\n".join(all_grants)
    # End generate grant migration DDL

    
    def generate_migration_ddl(self, sourceCatalog, sourceDatabase, destCatalog, forceRescan = False):
        #Pull object Data & Generate
        objectDF = None if forceRescan else self.get_last_results('objects', sourceCatalog, sourceDatabase)
        if objectDF is None:
            _, objectDF = self.scan_database_objects(sourceCatalog, sourceDatabase)

        if objectDF is not None:
            ddl_object = self.generate_migration_object_sql(objectDF, destCatalog)
        else:
            print("WARNING: No Objects to generate DDL for")
            ddl_object is None

        #Pull Grant Data & Generate
        grantDF = None if forceRescan else self.get_last_results('grants', sourceCatalog, sourceDatabase)
        if grantDF is None:
            _, grantDF = self.scan_database_grants(sourceCatalog, sourceDatabase)

        if grantDF is not None:
            print(f"Generating Grant DDL to migrate from {sourceCatalog}.{sourceDatabase} to {destCatalog}.{sourceDatabase}")
            ddl_grants = self.generate_migration_grant_sql(grantDF, destCatalog)
        else:
            print("WARNING: No Grants to generate DDL for")
            ddl_grants is None

        #Convert to arrays for easier execution / printing
        ddl_object = self.split_sql_to_list(ddl_object)
        ddl_grants = self.split_sql_to_list(ddl_grants)

        return (ddl_object, ddl_grants)

    @staticmethod
    def split_sql_to_list(sqlList):
        if sqlList is None:
            return None
        return [s.strip() for s in sqlList.split(';')]

    @staticmethod
    def strip_sql_comments(sqlStatement):
        return '\n'.join([x.strip() for x in sqlStatement.split('\n') if not x.strip().startswith('--') and x.strip()])

    def execute_sql_list(self, sql_list, echo=True):
        for statementRaw in sql_list:
            if statementRaw == '': continue
            statementClean = self.strip_sql_comments(statementRaw)
            if statementClean == '': continue           
            if echo: print(f"Executing SQL:\n{statementClean}\n")
            try:
                self.spark.sql(statementClean)
            except Exception as e:
                print(e)
#End of class