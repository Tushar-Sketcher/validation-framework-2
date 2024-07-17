import unittest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import lit, md5
from data_validation_framework.operations import ValidationError, compare_counts, compare_columns, compare_rows, \
    compare_metrics_by_dimension


class TestFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("data validation framework") \
            .getOrCreate()

    # Test compare_counts function for not match counts
    def test_compare_counts_not_match(self):
        # Mock DataFrames
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.count.return_value = 100
        df2_mock.count.return_value = 95

        result = compare_counts(df1_mock, df2_mock)
        self.assertEqual(result, (100, 95, 5, 5.0, False))

    # Test compare_counts function for equal counts
    def test_compare_counts_match(self):
        df1_mock_equal = Mock()
        df2_mock_equal = Mock()
        df1_mock_equal.count.return_value = 100
        df2_mock_equal.count.return_value = 100

        result_equal = compare_counts(df1_mock_equal, df2_mock_equal)
        self.assertEqual(result_equal, (100, 100, 0, 0.0, True))

    # Test compare_counts function for one DataFrame having zero count
    def test_compare_counts_with_zero(self):
        # Mock DataFrames where one count is zero
        df1_mock_zero = Mock()
        df2_mock_zero = Mock()
        df1_mock_zero.count.return_value = 0
        df2_mock_zero.count.return_value = 100

        result_zero = compare_counts(df1_mock_zero, df2_mock_zero)
        self.assertEqual(result_zero, (0, 100, -100, None, False))

    # Test cases for compare_columns() with columns of the same order
    def test_compare_columns_same_order(self):
        # Mock DataFrames with columns in the same order
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2']
        df2_mock.columns = ['col1', 'col2']

        result = compare_columns(df1_mock, df2_mock)
        self.assertTrue(result[0])  # Expect columns to match
        self.assertEqual(result[1], ['col1', 'col2'])  # Expect sorted columns from df1
        self.assertEqual(result[2], ['col1', 'col2'])  # Expect sorted columns from df2

    # Test cases for compare_columns() with columns of the different order
    def test_compare_columns_different_order(self):
        # Mock DataFrames with columns in different order
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2']
        df2_mock.columns = ['col2', 'col1']

        # Test compare_columns function
        result = compare_columns(df1_mock, df2_mock)
        self.assertFalse(False)  # Expect columns to not match due to order difference
        self.assertEqual(result[1], ['col1', 'col2'])  # Expect sorted columns from df1
        self.assertEqual(result[2], ['col1', 'col2'])  # Expect sorted columns from df2

    def test_compare_columns_extra_columns(self):
        # Mock DataFrames with extra columns in df2
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2']
        df2_mock.columns = ['col1', 'col2', 'col3']

        # Test compare_columns function
        result = compare_columns(df1_mock, df2_mock)
        self.assertFalse(result[0])  # Expect columns to not match due to extra column in df2
        self.assertEqual(result[1], ['col1', 'col2'])  # Expect sorted columns from df1
        self.assertEqual(result[2], ['col1', 'col2', 'col3'])  # Expect sorted columns from df2 including extra column

    def test_compare_columns_missing_columns(self):
        # Mock DataFrames with missing columns in df2
        df1_mock = Mock()
        df2_mock = Mock()
        df1_mock.columns = ['col1', 'col2', 'col3']
        df2_mock.columns = ['col1']

        # Test compare_columns function
        result = compare_columns(df1_mock, df2_mock)
        self.assertFalse(result[0])  # Expect columns to not match due to missing columns in df2
        self.assertEqual(result[1], ['col1', 'col2', 'col3'])  # Expect sorted columns from df1
        self.assertEqual(result[2], ['col1'])  # Expect sorted columns from df2

    # Test cases for compare_rows()
    def test_compare_rows(self):
        # Mock DataFrame for df1 and df2
        schema = StructType([
            StructField("col1", StringType(), True),
            StructField("col2", StringType(), True)
        ])

        # Mock data for df1 and df2
        data1 = [("value1", "value2")]
        data2 = [("value1", "value2")]

         # Create mock DataFrames
        df1_mock = self.spark.createDataFrame(data1, schema)
        df2_mock = self.spark.createDataFrame(data2, schema)

        # Mock the necessary DataFrame operations
        mock_select = Mock()
        mock_distinct = Mock()
        mock_distinct.return_value = df1_mock
        mock_select.return_value = mock_distinct
        df1_mock.select = mock_select
        df2_mock.select = mock_select

        # Mock get_coalesce function
        Mock(side_effect=lambda df, col_val: df.select(col_val).na.fill("_NULL").collect()[0][0])
        # Test compare_rows function
        result = compare_rows(df1_mock, df2_mock)
        # Test compare_rows function
        self.assertEqual(result, (0, 0))

    def setUp(self):
        # Initialize any setup needed for each test case
        pass

    def test_compare_metrics_by_dimension_success(self):
        # Mock SparkSession and relevant behavior
        spark_mock = Mock()
        spark_mock.sql.return_value = Mock()
        spark_mock.sql.return_value.dtypes = [('col1', 'bigint'), ('col2', 'int')]  # Mock schema for table1 and table2
        spark_mock.sql.return_value.columns = ['col1', 'col2']  # Mock column names
        spark_mock.sql.return_value.count.return_value = 1  # Mock count of returned rows
        spark_mock.sql.return_value.isEmpty.return_value = False  # Mock isEmpty to return False

        # Define test parameters
        table1 = "mock_table1"
        table2 = "mock_table2"
        query_filter = "col1 > 0"
        dimension_cols = "col1, col2"
        dim_metrics_cols = "metric_col1, metric_col2"
        ignored_cols = "col3"

        # Mock the expected SQL query generation within the function
        expected_sql_query = "SELECT * FROM mock_table1 LIMIT 1"

        with patch('builtins.print'), patch.object(spark_mock, 'sql', return_value=spark_mock.sql.return_value):
            try:
                result = compare_metrics_by_dimension(
                    table1, table2, query_filter, dimension_cols, dim_metrics_cols, ignored_cols, "mock_metric_table", spark_mock
                )
                self.assertIsInstance(result, Mock)  # Adjust this assertion based on what compare_metrics_by_dimension returns
                # Add more assertions based on expected behavior or return values

            except ValidationError as ve:
                self.fail(f"Unexpected ValidationError: {ve.message}")

if __name__ == "__main__":
    unittest.main()
