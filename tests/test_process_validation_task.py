import unittest
from unittest.mock import MagicMock, PropertyMock, patch
import yaml
from pyspark.sql import DataFrame
# Importing functions to be tested
from data_validation_framework.operations import compare_two_tables
from data_validation_framework.common import get_dataframe
from data_validation_framework.process_validations_task import (
    generate_table_comparison_details,
    generate_multiple_table_comparison_details
)

class TestDataValidation(unittest.TestCase):

    def setUp(self):
        # Patch necessary functions or objects here
        self.mock_compare_two_tables = patch('data_validation_framework.operations.compare_two_tables').start()
        self.mock_get_dataframe = patch('data_validation_framework.common.get_dataframe').start()
        self.mock_spark_session = patch('data_validation_framework.operations.SparkSession').start()

        self.mock_spark = self.mock_spark_session.return_value
        self.mock_sql = MagicMock()
        type(self.mock_sql).dtypes = PropertyMock(return_value=[('col1', 'bigint'), ('col2', 'int')])
        self.mock_sql.columns = ['col1', 'col2']
        self.mock_sql.count.return_value = 1
        self.mock_spark.sql.return_value = self.mock_sql     

    def tearDown(self):
        # Clean up patches
        patch.stopall()

    def test_generate_table_comparison_details_empty_tables(self):
        # Mock return values
        self.mock_compare_two_tables.return_value = []
        self.mock_spark_session.reset_mock()  # Reset mock calls for clean slate

        # Call the function
        result = generate_table_comparison_details(
            'table1', 'table2', '', False, '', '', ''
        )

        # Assertions
        self.assertIsInstance(result, DataFrame)  # Check if result is a DataFrame

    def test_generate_table_comparison_details_metric_validation_active(self):
        # Mock return values with metrics
        mock_metrics_result = [{'table': 'table1', 'count': 100, 'metrics': {'metric1': 10, 'metric2': 20}}]
        self.mock_compare_two_tables.return_value = mock_metrics_result
        self.mock_spark_session.reset_mock()  # Reset mock calls for clean slate

        # Call the function
        result = generate_table_comparison_details(
            'table1', 'table2', '', True, '', '', ''
        )

        # Assertions
        self.assertIsInstance(result, DataFrame)  # Check if result is a DataFrame

    def test_generate_table_comparison_details_exception_handling(self):
        # Mock exception in compare_two_tables
        self.mock_compare_two_tables.side_effect = Exception("Validation error")
        mock_spark = MagicMock()
        mock_sql = MagicMock()
        type(mock_sql).dtypes = PropertyMock(return_value=[('col1', 'bigint'), ('col2', 'int')])
        mock_sql.columns = ['col1', 'col2']
        mock_sql.count.return_value = 1
        mock_spark.sql.return_value = mock_sql
        self.mock_spark_session.return_value = mock_spark

        # Call the function
        result = generate_table_comparison_details(
            'table1', 'table2', '', True, '', '', ''
        )

        # Assertions
        self.assertIsInstance(result, DataFrame)  # Assuming exception handling returns a string

    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    def test_generate_multiple_table_comparison_details_invalid_yaml(self, mock_open):
        with self.assertRaises(FileNotFoundError):
            generate_multiple_table_comparison_details('dummy_path.yaml')

if __name__ == "__main__":
    unittest.main()
