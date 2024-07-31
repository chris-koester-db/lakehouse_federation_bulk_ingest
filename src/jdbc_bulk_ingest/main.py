from databricks.connect.session import DatabricksSession
from pyspark.sql import SparkSession

# Create a new Databricks Connect session. If this fails,
# check that you have configured Databricks Connect correctly.
# See https://docs.databricks.com/dev-tools/databricks-connect.html.
def get_spark() -> SparkSession:
    try:
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate() # type: ignore

spark = get_spark()

def generate_partition_queries(catalog, schema, table, partitionColumn, lowerBound:int, upperBound:int, numPartitions:int):
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
        catalog (str): Catalog name
        schema (str): Schema name
        table (str): Table name
        partitionColumn (str): Column used to generate partition queries. Using a primary key column is recommended.
        lowerBound (str): Lowest partition column value
        upperBound (str): Higest partition column value
        numPartitions (str): Number of partition queries
    
    Returns:
        list[dict]]: List of dicts containing lower_bound, upper_bound, and src_query
    """

    queryList = []
    stride = int(upperBound / numPartitions - lowerBound / numPartitions)
    
    i = 0
    currentValue = lowerBound
    while (i < numPartitions):
        lBoundValue = str(currentValue)
        lBound = f'{partitionColumn} >= {lBoundValue}' if i != 0 else None
        currentValue += stride
        uBoundValue = str(currentValue)
        uBound = f'{partitionColumn} < {uBoundValue}' if i != numPartitions - 1 else None
        if uBound == None:
            whereClause = lBound
        elif lBound == None:
            whereClause = f'{uBound} or {partitionColumn} is null'
        else:
            whereClause = f'{lBound} and {uBound}'
        queryList.append({'lower_bound' : int(lBoundValue), 'upper_bound' : int(uBoundValue) - 1, 'src_query' : f'select * from {catalog}.{schema}.{table} where {whereClause}'})
        i = i + 1
        
    return queryList