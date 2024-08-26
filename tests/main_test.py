from pyspark.testing import assertDataFrameEqual
from jsonschema import exceptions
import pytest
import textwrap
from lakefed_ingest.main import *

# Partition list is scoped to module for multiple tests
partition_list_expected = [
    {'lower_bound': 1, 'upper_bound': 199, 'where_clause': 'customer_id < 200 or customer_id is null'},
    {'lower_bound': 200, 'upper_bound': 398, 'where_clause': 'customer_id >= 200 and customer_id < 399'},
    {'lower_bound': 399, 'upper_bound': 597, 'where_clause': 'customer_id >= 399 and customer_id < 598'},
    {'lower_bound': 598, 'upper_bound': 796, 'where_clause': 'customer_id >= 598 and customer_id < 797'},
    {'lower_bound': 797, 'upper_bound': 995, 'where_clause': 'customer_id >= 797'}
]

def test_partition_list() -> None:
    
    partition_list = get_partition_list(
        partition_col='customer_id',
        lower_bound=1,
        upper_bound=1000,
        num_partitions=5
    )
    
    assert partition_list == partition_list_expected

def test_get_partition_df() -> None:

    partition_list_w_batch_expected = [
        {'lower_bound': 1, 'upper_bound': 199, 'where_clause': 'customer_id < 200 or customer_id is null', 'batch_id': 1},
        {'lower_bound': 200, 'upper_bound': 398, 'where_clause': 'customer_id >= 200 and customer_id < 399', 'batch_id': 1},
        {'lower_bound': 399, 'upper_bound': 597, 'where_clause': 'customer_id >= 399 and customer_id < 598', 'batch_id': 2},
        {'lower_bound': 598, 'upper_bound': 796, 'where_clause': 'customer_id >= 598 and customer_id < 797', 'batch_id': 2},
        {'lower_bound': 797, 'upper_bound': 995, 'where_clause': 'customer_id >= 797', 'batch_id': 3}
    ]

    partition_df_expected = spark.createDataFrame(
        partition_list_w_batch_expected, # type: ignore
        schema="lower_bound int, upper_bound int, where_clause string, batch_id int"
    )

    partition_df = get_partition_df(partition_list=partition_list_expected, num_partitions=5, batch_size=2)

    assertDataFrameEqual(partition_df, partition_df_expected)

def test_get_jdbc_config_fails() -> None:
    """Test jdbc config with incorrect schema
    
    Incorrect schema should result in jsonschema.exceptions.ValidationError being raised
    """

    with pytest.raises(exceptions.ValidationError):
        get_jdbc_config(file_path='tests/jdbc_config_bad_schema.json')

def test_get_sql_ddl():
    expected_sql_ddl = f"""\
        create or replace table main.lakefed.lakefed_tgt (
          customer_id BIGINT,
          name STRING,
          alias STRING)
          CLUSTER BY (customer_id)
    """

    expected_sql_ddl = textwrap.dedent(expected_sql_ddl)

    sql_ddl = get_sql_ddl(
        catalog='main',
        schema='lakefed',
        table='lakefed_tgt',
        partition_col='customer_id',
        file_path='tests/ddl_create_lakefed_tgt.txt',
    )

    assert sql_ddl == expected_sql_ddl
