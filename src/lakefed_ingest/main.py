from databricks.connect.session import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.dbutils import DBUtils
from typing import Optional
from pathlib import Path
from jsonschema import validate
import textwrap
import pprint
import pyspark.sql.functions as F
from pyspark.sql import Window
import math
import json
import os
import re

# Get SparkSession
# https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate() # type: ignore

spark = get_spark()
dbutils = DBUtils(spark)

def get_sql_ddl(catalog:str, schema:str, table:str, partition_col:str, file_path:str) -> str:
    """Get SQL DDL to create target table
    
    Placeholders in DDL text are replaced so that object identifiers
    don't need to be hard coded, and therefore managed in multiple
    places.
    
    String replacement solution source
    https://stackoverflow.com/a/6117124
    
    Args:
        file_path (str): Path of config file relative to project root (config/ddl_create_lakefed_tgt.txt)
    
    Returns:
        str: SQL DDL statement
    """

    root_dir = Path(__file__).resolve().parents[2]
    file_path_full = os.path.join(root_dir, file_path)

    with open(file_path_full) as f:
        sql_ddl = f.read()
    
    # Define string replacements here
    rep = {'{catalog}': catalog, '{schema}': schema, '{table}': table, '{partition_col}': partition_col}
    
    # Perform string replacement
    rep = dict((re.escape(k), v) for k, v in rep.items()) 
    pattern = re.compile("|".join(rep.keys()))
    sql_ddl = pattern.sub(lambda m: rep[re.escape(m.group(0))], sql_ddl)
    
    return sql_ddl

def get_partition_boundaries(catalog:str, schema:str, table:str, partition_col:str) -> tuple[int, int]:
    """Get partition boundaries (Min and max values for partition column)
    
    Args:
        catalog (str): Catalog name
        schema (str): Schema name
        table (str): Table name
        partition_col (str): Column used to partition the table
    
    Returns:
        tuple[int, int]: Partition boundaries
    """

    partition_boundaries_qry = f"""\
        select
          min({partition_col}) as lower_bound,
          max({partition_col}) as upper_bound
        from {catalog}.{schema}.{table}
    """
    print(textwrap.dedent(partition_boundaries_qry))
    
    partition_boundaries_df = spark.sql(partition_boundaries_qry)
    lower_bound = partition_boundaries_df.collect()[0]["lower_bound"]
    upper_bound = partition_boundaries_df.collect()[0]["upper_bound"]
    
    return lower_bound, upper_bound

def get_jdbc_config(file_path:str) -> dict:
    """Get JDBC config from json file and return as dict

    JDBC pushdown is intended to be used only when Lakehouse
    Federation doesn't support functions needed to get the
    size of a table
    
    Args:
        file_path (str): Path of config file relative to project root (config/postgresql_jdbc.json)
    
    Returns:
        dict: JDBC config
    """

    root_dir = Path(__file__).resolve().parents[2]
    file_path_full = os.path.join(root_dir, file_path)

    with open(file_path_full) as f:
        config = json.load(f)
    
    # Validate config schema
    config_schema = {
        "type" : "object",
        "properties" : {
            "host" : {"type" : "string"},
            "port" : {"type" : "string"},
            "database" : {"type" : "string"},
            "secret_scope" : {"type" : "string"},
            "secret_key_user" : {"type" : "string"},
            "secret_key_pwd" : {"type" : "string"},
        },
    }
    
    validate(instance=config, schema=config_schema)
    
    print('JDBC config:')
    pprint.pprint(config)

    return config

def get_table_size_sqlserver(catalog:str, schema:str, table:str) -> int:
    """Get SQL Server table size
    
    Args:
        catalog (str): Catalog name
        schema (str): Schema name
        table (str): Table name
    
    Returns:
        int: Size of SQL Server table in MB
    """
    
    table_size_qry = f"""\
        SELECT
          CAST(ROUND((SUM(a.total_pages) * 8) / 1024, 2) AS NUMERIC(36, 2)) AS Total_MB
        FROM
          sys.tables t
          JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
          JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
          JOIN sys.allocation_units a ON p.partition_id = a.container_id
          LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE
          s.Name = '{schema}'
          AND t.name = '{table}'
          AND t.is_ms_shipped = 0
          AND i.object_id > 255
        GROUP BY
          t.Name, s.Name, p.Rows
    """
    print(f'Query used to get table size:\n{textwrap.dedent(table_size_qry)}')
    
    spark.sql(f'use catalog {catalog}')
    table_size_mb = spark.sql(table_size_qry).collect()[0][0]
    
    return table_size_mb

def get_table_size_postgresql(catalog:str, schema:str, table:str, jdbc_config_file:Optional[str]=None) -> int:
    """Get PostgreSQL table size

    Getting a table's size in PostgreSQL requires either using a JDBC pushdown query,
    or creating a view in the source database that can then be queried using Lakehouse Federation.
    This is because Lakehouse Federation doesn't currently support PostgreSQL object size functions
    such as pg_table_size()

    To use JDBC pushdown, add a config file that matches the schema of config/postgresql_jdbc.json.
    Then provide the path to the config file when calling this function.
    https://docs.databricks.com/en/connect/external-systems/postgresql.html

    To use Lakehouse Federation, create the view below in the PostgreSQL database and don't
    use the jdbc_config_file argument.
    
    create or replace view public.vw_pg_table_size
     as
     select
      table_schema,
      table_name,
      pg_table_size(quote_ident(table_name)),
      pg_size_pretty(pg_table_size(quote_ident(table_name))) as pg_table_size_pretty
    from information_schema.tables
    where table_schema not in ('pg_catalog', 'information_schema')
    and table_type = 'BASE TABLE';
    
    Args:
        schema (str): Schema name
        table (str): Table name
        jdbc_config_file (str): Path of config file relative to project root (config/postgresql_jdbc.json)
    
    Returns:
        int: Size of PostgreSQL table in MB
    """
    
    table_size_mb = 0

    if jdbc_config_file:
        print('Using JDBC pushdown to get table size')
        config = get_jdbc_config(jdbc_config_file)
        
        # Get database user credentials from secrets
        # https://docs.databricks.com/en/security/secrets/index.html
        user = dbutils.secrets.get(scope=config['secret_scope'], key=config['secret_key_user'])
        password = dbutils.secrets.get(scope=config['secret_scope'], key=config['secret_key_pwd'])
        
        # Query to get table size will be pushed down to PostgreSQL database
        table_size_qry = f"(select pg_relation_size('{schema}.{table}') as size_in_bytes) as size_in_bytes"
        
        print(f'Query used to get table size:\n{table_size_qry}')
        
        table_size_in_bytes = (
            spark.read.format("postgresql")
            .option("dbtable", table_size_qry)
            .option("host", config['host'])
            .option("port", config['port'])
            .option("database", config['database'])
            .option("user", user)
            .option("password", password)
            .load()
        ).collect()[0]['size_in_bytes']
    else:
        print('Querying PostgreSQL view to get table size')
        table_size_qry = f"""\
            select pg_table_size
            from public.vw_pg_table_size
            where table_schema = '{schema}'
            and table_name = '{table}'
        """
        
        print(f'Query used to get table size:\n{textwrap.dedent(table_size_qry)}')
        
        spark.sql(f'use catalog {catalog}')
        table_size_mb = spark.sql(table_size_qry).collect()[0]['size_in_bytes']
    
    table_size_mb = math.ceil(table_size_in_bytes / 1024 / 1024)
    
    return table_size_mb

def get_table_size_delta(catalog:str, schema:str, table:str) -> int:
    """Get Delta Lake table size

    A Delta source is intended only for testing
    
    Args:
        catalog (str): Catalog name
        schema (str): Schema name
        table (str): Table name
    
    Returns:
        int: Size of Delta table in MB
    """
    
    table_size_in_bytes = spark.sql(f'desc detail {catalog}.{schema}.{table}').collect()[0]['sizeInBytes']
    table_size_mb = table_size_in_bytes / 1024 / 1024
    print(f'Table size MB: {table_size_mb}')
    return table_size_mb

def get_partition_list(partition_col:str, lower_bound:int, upper_bound:int, num_partitions:int) -> list[dict]:
    """Generate list of queries based on partitioning schematic
    
    Function is derived from the JDBC partitioning code in Spark:
    
    Given a partitioning schematic (a column of integral type, a number of
    partitions, and upper and lower bounds on the column's value), generate
    WHERE clauses for each partition so that each row in the table appears
    exactly once.  The parameters minValue and maxValue are advisory in that
    incorrect values may cause the partitioning to be poor, but no data
    will fail to be represented.
    
    Null value predicate is added to the first partition where clause to include
    the rows with null value for the partitions column.
    
    Spark source code - https://github.com/apache/spark/blob/7bbcbb84c266b6ff418cd2c3361aa7350299d0ae/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JDBCRelation.scala#L129
    
    Args:
        partition_col (str): Column used to generate partition queries. Using a clustered index column is recommended.
        lower_bound (str): Lowest partition column value
        upper_bound (str): Higest partition column value
        num_partitions (str): Number of partition queries
    
    Returns:
        list[dict]: List of dicts containing lower_bound, upper_bound, and src_query
    """

    partition_list = []
    stride = int(upper_bound / num_partitions - lower_bound / num_partitions)
    
    i = 0
    currentValue = lower_bound
    while (i < num_partitions):
        lBoundValue = str(currentValue)
        lBound = f'{partition_col} >= {lBoundValue}' if i != 0 else None
        currentValue += stride
        uBoundValue = str(currentValue)
        uBound = f'{partition_col} < {uBoundValue}' if i != num_partitions - 1 else None
        if uBound == None:
            whereClause = lBound
        elif lBound == None:
            whereClause = f'{uBound} or {partition_col} is null'
        else:
            whereClause = f'{lBound} and {uBound}'
        partition_list.append({'lower_bound' : int(lBoundValue), 'upper_bound' : int(uBoundValue) - 1, 'where_clause' : whereClause})
        i = i + 1
        
    return partition_list

def get_partition_df(partition_list:list[dict], num_partitions:int, batch_size:int = 500) -> DataFrame:
    """Create dataframe from list of partitions

    Partitions are defined by a where clause that selects a range of data.
    A batch_id is assigned to each partition. This provides a way of
    dividing the full partition table into N size batches.
    
    Batching is used to avoid exceeding the 48 KiB limit of taskValues.
    https://docs.databricks.com/en/jobs/share-task-context.html
    
    Args:
        partition_list (list[dict]): List of dictionaries containing partition ranges
        num_partitions (int): Number of partitions is used to determing the number of batches
        batch_size (int): Batch size. Defaults to 500.
    """
    
    # Calculate number of batches and round up
    num_batches = math.ceil(num_partitions / batch_size)
    print(f'Number of batches: {num_batches}')
    
    partition_df = spark.createDataFrame(
        partition_list, # type: ignore
        schema="lower_bound int, upper_bound int, where_clause string, batch_id int"
    )
    window = Window.orderBy("lower_bound")
    partition_df = partition_df.withColumn("batch_id", F.ntile(num_batches).over(window))

    return partition_df