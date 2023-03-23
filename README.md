# DB-Inventory-Collector
## Databricks Database Inventory and Access Control Scanner
Collects grants from tables and views.

This Python script provides a class InventoryCollector that scans a Spark environment for database objects and access control grants. It stores the results in two tables: db_objects and grant_statements.

##  Testing Note:
Note: this has only been tested for performing the inventory on the hive_metastore. Performing inventory on a different UC catalog may have different behaviour.


# Usage
To use the DbInventoryCollector, first instantiate the class with 3 arguments:
* **spark** -- the spark object
* **inventory_catalog** -- What catalog to use to save inventory results to. Must already exist.
* **inventory_database** -- What database to save inventory results to. Will be created.
```
scanner = InventoryCollector(spark, 'INVENTORY_CATALOG', 'INVENTORY_DATABASE')
```

## Scanning all databases
To scan all databases in the Spark environment:

```
scanner.scan_all_databases()
```
## To rescan all databases, ignoring previous results:
```
scanner.scan_all_databases(rescan=True)
```
## Scanning a specific database
To scan a specific database for objects and grants:
```
database_name = "example_database"

# Scan database objects
(object_exec_id, object_df) = scanner.scan_database_objects(database_name)

# Scan database grants
(grant_exec_id, grant_df) = scanner.scan_database_grants(database_name)
```

## View summarized results
Lists one row per database with summarized counts of scan results.
```
summary_df = scanner.get_database_inventory_summary()
display(summary_df)
```

# Data Storage
All data is stored in append-only delta tables. This means if you re-run multiple times, there will be multiple scan results. Results are indexed by a `data_type` and a `execution_id`. The `data_type`s are 'object' and 'grant' and are stored in two different tables.

## db_objects
This table stores the inventory of database objects. Each row represents an object (table or view) in a specific database.

|Column Name                 | Data Type     |              Description
|----------------------------|---------------|------------------------------------
|objectType                  | string        |  The type of the database object (table or view).
|objectKey                   | string        |  The full name of the database object, including database name and table/view name.
|database                    | string        |  The name of the database the object belongs to.
|tableName                   | string        |  The name of the table or view.
|isTemporary                 | boolean       |  Indicates if the table or view is temporary.
|inventory_execution_id      | string        |  A unique identifier for the inventory execution.
|execution_time              | timestamp     |  The time when the inventory execution took place.
|source_database             | string        |  The name of the source database where the object is located.


## grant_statements
This table stores the access control grant statements. Each row represents a grant statement for a specific database object or the database itself.

| Column Name               | Data Type   | Description
|--------------------------|--------------|------------------------------------
| Principal                 | string      | The principal (user or role) to which the grant applies.
| ActionType                | string      | The action type (e.g., SELECT, INSERT, UPDATE , DELETE) granted by the statement.
| ObjectType                | string      | The type of the object (table, view, or database ) the grant applies to.
| ObjectKey                 | string      | The full name of the object (including database name and table/view name) or the database name the grant applies to.
| inventory_execution_id    | string      | A unique identifier for the inventory execution.
| execution_time            | timestamp   | The time when the inventory execution took place.
| source_database           | string      | The name of the source database where the grant is located.
| grant_statement           | string      | The grant statement represented as a string .


# Additional Methods
The DbInventoryAccessControlScanner class also provides methods to retrieve execution histories and summaries, as well as to filter results by execution ID.

* `get_results_by_execution_id(execution_id, data_type=None)`
* `get_last_execution(data_type="grants", database_name=None)`
* `get_all_latest_executions(data_type="grants")`
* `get_last_results(*args)`
* `get_execution_history(data_type=None)`
* `get_database_inventory_summary()`


