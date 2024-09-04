from pyspark.testing import assertDataFrameEqual
from jsonschema import exceptions
import pytest
import textwrap
from lakefed_ingest.main import *

# Partition list is scoped to module for multiple tests
partition_list_expected = [
    {'id': 0, 'where_clause': 'customer_id < 200 or customer_id is null'},
    {'id': 1, 'where_clause': 'customer_id >= 200 and customer_id < 399'},
    {'id': 2, 'where_clause': 'customer_id >= 399 and customer_id < 598'},
    {'id': 3, 'where_clause': 'customer_id >= 598 and customer_id < 797'},
    {'id': 4, 'where_clause': 'customer_id >= 797'}
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
        {'id': 0, 'where_clause': 'customer_id < 200 or customer_id is null', 'batch_id': 1},
        {'id': 1, 'where_clause': 'customer_id >= 200 and customer_id < 399', 'batch_id': 1},
        {'id': 2, 'where_clause': 'customer_id >= 399 and customer_id < 598', 'batch_id': 2},
        {'id': 3, 'where_clause': 'customer_id >= 598 and customer_id < 797', 'batch_id': 2},
        {'id': 4, 'where_clause': 'customer_id >= 797', 'batch_id': 3}
    ]

    partition_df_expected = spark.createDataFrame(
        partition_list_w_batch_expected, # type: ignore
        schema="id int, where_clause string, batch_id int"
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

@pytest.mark.parametrize(
    "bound_input,expected",
    [
        (42, 42),
        (datetime(2022, 12, 28, 23, 55, 59, 342380), 1672271759),
        (datetime(2023, 3, 21), 1679356800),
        pytest.param("42", 42, marks=pytest.mark.xfail),
    ],
)
def test_get_internal_bound_value(bound_input, expected):
    assert get_internal_bound_value(bound_input) == expected

@pytest.mark.parametrize(
    "bound_input,bound_orig,expected",
    [
        (42, 42, '42'),
        (1672271759, datetime(2022, 12, 28, 23, 55, 59, 342380), '2022-12-28 23:55:59'),
        (1679356800, datetime(2023, 3, 21).date(), '2023-03-21'),
        pytest.param(42, "42", 42, marks=pytest.mark.xfail),
    ],
)
def test_bound_value_to_str(bound_input, bound_orig, expected):
    assert bound_value_to_str(bound_input, bound_orig) == expected