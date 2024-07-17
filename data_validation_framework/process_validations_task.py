from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.operations import *
from data_validation_framework.common import get_dataframe
import yaml


spark = SparkSession.builder.appName("data validation framework") \
                            .config("spark.dynamicAllocation.enabled", "true") \
                            .config("spark.sql.shuffle.partitions", "600") \
                            .config("spark.default.parallelism", "600") \
                            .config("spark.driver.memory", "9g") \
                            .config("spark.executor.memory", "20g") \
                            .enableHiveSupport().getOrCreate()


def generate_table_comparison_details(table1, table2, filter_condition='', metric_validation_active=False, dimension_columns='', dim_metrics_columns='', ignored_columns='', metric_result_table='', materialization=None):
    """
    Process the count, row and metric(optional) data validation for the single set of table taking the below inputs from user function.
    Params:
    - table1 (str): first table name.
    - table2 (str): second table name.
    - filter (str): filter condition to b applied.
    - metric_validation_active (bool): metric_validation_active flag.
    - dimension_cols (str): Specify the dimension or slice columns(common in both tables) you wish to include in the validation query using this parameter.
    - dim_metrics_cols (str): columns (common in both tables) to be converted as metric columns (perform count agg) in the validation query. 
    - ignored_cols (ste): columns that you wish to ignore/exclude from the metrics of integral type.
    - materialization (str): Type of table string.
    Returns:
    - df (dataframe): The output dataframe contain the validation details for each input table provided in yaml.
    """
    data = compare_two_tables(spark, table1, table2, filter_condition, metric_validation_active, dimension_columns, dim_metrics_columns, ignored_columns, metric_result_table, materialization)
    df = get_dataframe(spark, data)
    return df


def generate_multiple_table_comparison_details(path):
    """
    Process the count, row and metric(optional) data validation taking the input yaml from user.
    Params:
    - path (dict): Dictionary containing the configuration for tables to be compared including table names, filter conditions, etc.
    Returns:
    - df (dataframe): The output dataframe contains the validation details for each input table provided in yaml.
    """

    config = yaml.safe_load(open(path, 'r'))
    input_rows = config['rows']
    result_rows = []
    for row in input_rows:
        table1 = row['table1']
        table2 = row['table2']
        query_filter = row.get('filter_condition', '')
        is_validation_active = row.get('is_validation_active', True)
        metric_validation_active = row.get('metric_validation_active', False)
        dimension_columns =  row.get('dimension_columns', '')
        dim_metrics_columns =  row.get('dim_metrics_columns', '')
        ignored_columns =  row.get('ignored_columns', '')
        metric_result_table = row.get('metric_result_table', '')
        materialization = row.get('materialization', '')
        
        if is_validation_active:
            result_rows = compare_two_tables(spark, table1, table2, query_filter, metric_validation_active, dimension_columns, dim_metrics_columns, ignored_columns, metric_result_table, materialization, result_rows)
        else:
            print(f"- Validation process skipped for the {table1} and {table2} due to 'False' or invalid value provided for 'is_validation_active' parameter.\n")

    df = get_dataframe(spark, result_rows)
    return df
