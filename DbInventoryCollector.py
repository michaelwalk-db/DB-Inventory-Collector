#These imports are definitely used
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, FloatType
import uuid
from datetime import datetime, timedelta

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

    def tryUseCatalog(self):
        try:
            self.spark.sql(f'USE CATALOG {self.inventory_catalog};')
        except Exception as e:
            pass #Probably just not on UC enabled cluster, so just ignore

    def initialize(self):
        print(f'Will save results to: {self.inventory_catdb}. {("Saving to HMS" if self.inventory_destHms else "Saving to UC Catalog")}')
        self.tryUseCatalog()
        self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.inventory_catdb}')
        self.spark.sql(f'CREATE TABLE IF NOT EXISTS {self.inventory_catdb}.grant_statements (Principal STRING, ActionType STRING, ObjectType STRING, ObjectKey STRING, inventory_execution_id STRING, execution_time TIMESTAMP, source_database STRING, grant_statement STRING)')
        self.spark.sql(f'CREATE TABLE IF NOT EXISTS {self.inventory_catdb}.db_objects (source_database STRING, table STRING, objectType STRING, location STRING, viewText STRING, errMsg STRING, inventory_execution_id STRING, execution_time TIMESTAMP)')

        # Create execution_history table if it does not exist
        create_exec_hist_query = f"""
            CREATE TABLE IF NOT EXISTS {self.inventory_catdb}.execution_history (
                inventory_execution_id STRING,
                execution_time TIMESTAMP,
                source_database STRING,
                data_type STRING,
                time_elapsed FLOAT,
                records_written INT
            )
        """
        self.spark.sql(create_exec_hist_query)
    
    def resetAllData(self):
        print(f"Dropping inventory database: {self.inventory_catdb}")
        self.spark.sql(f'DROP DATABASE IF EXISTS {self.inventory_catdb} CASCADE')
        self.initialize()
    
    def write_execution_record(self, execution_id, execution_time, database_name, data_type, records_written):
        # Define the schema for the execution_history DataFrame
        schema = StructType([
            StructField("inventory_execution_id", StringType(), True),
            StructField("execution_time", TimestampType(), True),
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
            [(execution_id, execution_time, database_name, data_type, time_elapsed, records_written)],
            schema=schema
        )

        execution_record.write.mode("append").saveAsTable(f"{self.inventory_catdb}.execution_history")
        print(f"{end_time} - Finished {data_type.upper()} inventory of database {database_name} execution_id: {execution_id}. Elapsed: {time_elapsed}s")

        
    def scan_database_objects(self, database_name):
        execution_id = self.OBJECT_EXECUTION_ID_PREFIX + str(uuid.uuid4())
        execution_time = datetime.now()
        print(f"{execution_time} - Start OBJECT Inventory for {database_name} with exec_id {execution_id}")

        objTableSchema = StructType([
            StructField("source_database", StringType(), True),
            StructField("table", StringType(), True),
            StructField("objectType", StringType(), True),
            StructField("location", StringType(), True),
            StructField("viewText", StringType(), True),
            StructField("errMsg", StringType(), True)
        ])
        object_rows = set()

        self.tryUseCatalog()
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

        #Create object DF and write to db_objects table
        if len(object_rows) > 0:
            object_df = self.spark.createDataFrame(object_rows, objTableSchema)
            object_df = object_df.withColumn("inventory_execution_id", lit(execution_id)).withColumn("execution_time", lit(execution_time))        
            object_df.write.mode("append").saveAsTable(f"{self.inventory_catdb}.db_objects")
        else:
            object_df = None

        #Record finished (this also prints)
        self.write_execution_record(execution_id, execution_time, database_name, "objects", len(object_rows))
        
        return (execution_id, object_df)
    
    def scan_database_grants(self, database_name):
        # Create a random execution ID and get the current timestamp
        execution_id = self.GRANT_EXECUTION_ID_PREFIX + str(uuid.uuid4())
        execution_time = datetime.now()
        print(f"{execution_time} - Start 'grants' inventory of database {database_name}. Creating inventory_execution_id: {execution_id}")
        processed_acl = set()
    
        # Set the current database
        self.tryUseCatalog()
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
        numGrants = len(processed_acl)
        if numGrants > 0:
            # Convert the processed ACL entries into a DataFrame
            acl_df = (self.spark.createDataFrame(processed_acl, ["Principal", "ActionType", "ObjectType", "ObjectKey"]).distinct()
                        .withColumn("inventory_execution_id", lit(execution_id))
                        .withColumn("execution_time", lit(execution_time))
                        .withColumn("source_database", lit(database_name)))

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

            acl_df.write.mode("append").saveAsTable(f"{self.inventory_catdb}.grant_statements")
            print(f"{datetime.now()} - Finished writing {len(processed_acl)} results for database {database_name}. {execution_id}.")
        else:
            acl_df = None

        #Record finished (this also prints)
        self.write_execution_record(execution_id, execution_time, database_name, "grants", numGrants)
        return (execution_id, acl_df)

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
            self.tryUseCatalog()
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

    def get_last_execution_id(self, data_type="grants", database_name=None):
        table_name = self.get_result_table(data_type, nameOnly=True)
        
        if database_name:
            where_clause = f"WHERE source_database = '{database_name}'"
        else:
            where_clause = ""
    
        self.tryUseCatalog()
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

    def get_all_latest_executions(self, data_type="grants"):
        table_name = self.get_result_table(data_type, nameOnly=True)
        self.tryUseCatalog()
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
        exec_id = self.get_last_execution_id(*args)
        if exec_id is None: return None
        return self.get_results_by_execution_id(exec_id)

    def get_execution_history(self, data_type=None):
        self.tryUseCatalog()
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
            """).withColumn('data_type', lit(data_type))
    
        return execution_history

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
    
        # Don't fill in missing values, keep as NULL
        # summary_df = summary_df.fillna({"grant_last_execution_id": "", "grant_last_execution_time": "", 
        #                                 "object_last_execution_id": "", "object_last_execution_time": ""})
    
        # Add object counts for each database
        object_counts = self.spark.table(f"{self.inventory_catdb}.db_objects").groupBy("inventory_execution_id", "source_database", "objectType").count()
        object_counts = object_counts.withColumnRenamed("inventory_execution_id", "object_last_execution_id")
        object_counts = object_counts.withColumnRenamed("source_database", "database") # Needed?
        object_counts = object_counts.groupBy("object_last_execution_id").pivot("objectType").agg(first("count"))
        object_counts = object_counts.fillna(0)
        summary_df = summary_df.join(object_counts, on="object_last_execution_id", how="left")
    
        # Add grant count for each database
        grant_counts = self.spark.table(f"{self.inventory_catdb}.grant_statements").groupBy("inventory_execution_id", "source_database").count()
        grant_counts = grant_counts.withColumnRenamed("source_database", "database") # Needed?
        grant_counts = grant_counts.withColumnRenamed("count", "grant_count")
        grant_counts = grant_counts.withColumnRenamed("inventory_execution_id", "grant_last_execution_id")
        grant_counts = grant_counts.fillna(0)
        summary_df = summary_df.join(grant_counts, on="grant_last_execution_id", how="left")
    
        # Return the result
        return summary_df

        
    def scan_all_databases(self, rescan=False):
        self.tryUseCatalog()

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
                lit(f"\nCREATE TABLE {destCatalog}."), 
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
                lit(f"\nCREATE TABLE {destCatalog}."), 
                df["source_database"], lit("."), df["table"],
                lit(" CLONE hive_metastore."),
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
                lit(f";\nCREATE VIEW {destCatalog}."), 
                df["source_database"], lit("."), df["table"],
                lit(" AS\n"),
                df["viewText"],
                lit(";")
            )
        )
        
        return df
    
    def generate_migration_object_sql(self, inventoryDf: DataFrame, destCatalog: str) -> DataFrame:
        def collectSql(df, joinSep: str = '\n'):
            sqlList = [r.sql_ddl for r in df.select('sql_ddl').collect()]
            return joinSep.join(sqlList)

        # Filter input DataFrame by objectType and generate DDL for each type
        df_external = self._generate_ddl_external(inventoryDf.filter(inventoryDf["objectType"] == "EXTERNAL"), destCatalog)
        df_managed = self._generate_ddl_managed(inventoryDf.filter(inventoryDf["objectType"] == "MANAGED"), destCatalog)
        df_view = self._generate_ddl_view(inventoryDf.filter(inventoryDf["objectType"] == "VIEW"), destCatalog)
        
        createDbCommands = set()
        createDbCommands.update([f"CREATE DATABASE IF NOT EXISTS {r.source_database};" 
                            for r in df_external.select('source_database').distinct().collect()])
        createDbCommands.update([f"CREATE DATABASE IF NOT EXISTS {r.source_database};" 
                            for r in df_managed.select('source_database').distinct().collect()])
        createDbCommands.update([f"CREATE DATABASE IF NOT EXISTS {r.source_database};" 
                            for r in df_view.select('source_database').distinct().collect()])
        
        allCreateDbSql = "\n".join(createDbCommands)
        
        combinedSql = f"""-- Databricks Unity Catalog Migration DDL Generation
-- Automatically generated using DbInventoryCollector on {datetime.now()}
-- Should create {len(createDbCommands)} databases

-- First: set destination catalog
"""
        combinedSql = combinedSql + f"USE CATALOG {destCatalog};\n"

        combinedSql = combinedSql + "\n--\n-- Create Destination Databases\n--\n" + allCreateDbSql
        
        combinedSql = combinedSql + "\n\n--\n-- Create EXTERNAL Tables first.\n--\n" + collectSql(df_external, "\n\n")
        combinedSql = combinedSql + "\n\n--\n-- Create MANAGED tables next\n--\n" + collectSql(df_managed, "\n\n")
        combinedSql = combinedSql + "\n\n--\n-- Create VIEWs next\n--\n" + collectSql(df_view, "\n\n")
        
        return combinedSql

    hive_to_uc_privilege_map = {
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

            new_action = hive_to_uc_privilege_map.get((object_type, action_type), None)

            if new_action is None or new_action == "IGNORE":
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

#End of class