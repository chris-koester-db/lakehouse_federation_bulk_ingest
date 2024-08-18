from databricks.connect.session import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import textwrap
import pyspark.sql.functions as F
import math
import json
import os
from pathlib import Path

# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate() # type: ignore

spark = get_spark()
dbutils = DBUtils(spark)

def _get_partition_boundaries(catalog, schema, table, partition_col) -> tuple[int, int]:
    """Get partition boundaries (Min and max values for partition column)
    
    Args:
        catalog (str): Catalog name
        schema (str): Schema name
        table (str): Table name
        partition_col (str): Column used to partition the table
    
    Returns:
        tuple[int, int]: Partition boundaries
    """

    partition_boundaries_qry = f"""select
    min({partition_col}) as lower_bound,
    max({partition_col}) as upper_bound
    from {catalog}.{schema}.{table}"""
    
    partition_boundaries_df = spark.sql(partition_boundaries_qry)
    lower_bound = partition_boundaries_df.collect()[0]["lower_bound"]
    upper_bound = partition_boundaries_df.collect()[0]["upper_bound"]
    
    return lower_bound, upper_bound

def get_table_size_sqlserver(catalog, schema, table) -> int:
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
    print(f'Table size MB: {table_size_mb}')
    return table_size_mb

def get_table_size_postgresql(schema, table, config_file_path:str='config/postgresql_jdbc.json') -> int:
    """Get PostgreSQL table size

    Uses JDBC because Lakehouse Federation doesn't support table size functions
    https://docs.databricks.com/en/connect/external-systems/postgresql.html
    
    Args:
        schema (str): Schema name
        table (str): Table name
        config_file_path (str): Path of config file relative to project root
    
    Returns:
        int: Size of PostgreSQL table in MB
    """
    
    # Read config file and deserialize to a dict
    root_dir = Path(__file__).resolve().parents[2]
    file_path = os.path.join(root_dir, config_file_path)

    with open(file_path) as f:
        config = json.load(f)

    print(f'PostgreSQL connection config:\n{config}')
    
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

    table_size_mb = table_size_in_bytes / 1024 / 1024
    print(f'Table size MB: {table_size_mb}')
    return table_size_mb

def get_table_size_delta(catalog, schema, table) -> int:
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

def get_partition_spec(src_type, catalog, schema, table, partition_col, partition_size_mb) -> dict:
    """Get partition specification for source table

    The partition specification is used to generate a list of partitions
    
    Args:
        src_type (str): Source type
        catalog (str): Catalog name
        schema (str): Schema name
        table (str): Table name
        partition_col (str): Column used to partition the table
        partition_size_mb (int): Partition size in MB
    
    Returns:
        dict: Partition specification for source table (Upper / lower bound and number of partitions)
    """
    
    lower_bound, upper_bound = _get_partition_boundaries(catalog, schema, table, partition_col)
    
    table_size_mb = 0

    if src_type == 'sqlserver':
        table_size_mb = get_table_size_sqlserver(catalog, schema, table)
    elif src_type == 'postgresql':
        table_size_mb = get_table_size_postgresql(schema, table)
    elif src_type == 'delta':
        table_size_mb = get_table_size_delta(catalog, schema, table)
    else:
        raise ValueError(f'Unsupported src_type: {src_type}')

    # Get number of partitions. Minimum is 2.
    num_partitions = int(table_size_mb / partition_size_mb)
    num_partitions = max(num_partitions, 2)

    partition_spec = {
        'lower_bound': lower_bound,
        'upper_bound': upper_bound,
        'num_partitions': num_partitions
    }
    
    print(f'Partition spec:\n{partition_spec}')
    return partition_spec

def get_partition_list(partition_column:str, lower_bound:int, upper_bound:int, num_partitions:int) -> list[dict]:
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
        partition_column (str): Column used to generate partition queries. Using a primary key column is recommended.
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
        lBound = f'{partition_column} >= {lBoundValue}' if i != 0 else None
        currentValue += stride
        uBoundValue = str(currentValue)
        uBound = f'{partition_column} < {uBoundValue}' if i != num_partitions - 1 else None
        if uBound == None:
            whereClause = lBound
        elif lBound == None:
            whereClause = f'{uBound} or {partition_column} is null'
        else:
            whereClause = f'{lBound} and {uBound}'
        partition_list.append({'lower_bound' : int(lBoundValue), 'upper_bound' : int(uBoundValue) - 1, 'where_clause' : whereClause})
        i = i + 1
        
    return partition_list

def partition_list_to_table(partition_list:list[dict], tbl_name:str, num_partitions:int, batch_size:int = 500) -> None:
    """Write partition list to table

    Partitions are defined by a where clause that selects a range of data.
    A batch_id is assigned to each partition. This provides a way of
    dividing the full partition table into N size batches.
    
    Batching is used to avoid exceeding the 48 KiB limit of taskValues.
    https://docs.databricks.com/en/jobs/share-task-context.html
    
    Args:
        partition_list (list[dict]): List of dictionaries containing partition ranges
        tbl_name (str): Table name
        num_partitions (int): Number of partitions is used to determing the number of batches
        batch_size (int): Batch size. Defaults to 500.
    """
    
    # Calculate number of batches and round up
    num_batches = math.ceil(num_partitions / batch_size)
    print(f'num_batches: {num_batches}')
    
    df = spark.createDataFrame(partition_list) # type: ignore
    df = df.withColumn("batch_id", F.expr(f"ntile({num_batches}) over (order by lower_bound) as batch_id"))
    df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(tbl_name)