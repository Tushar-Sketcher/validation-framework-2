import unittest
from unittest.mock import patch, MagicMock, mock_open
from data_validation_framework.operations import compare_two_tables
from data_validation_framework.common import get_dataframe
from data_validation_framework.process_validations_task import (
    generate_table_comparison_details,
    generate_multiple_table_comparison_details
)


# Mocking the spark session object
class MockSparkSession:
    def __init__(self):
        pass

@patch('data_validation_framework.operations.compare_two_tables')
@patch('data_validation_framework.common.get_dataframe')
class TestDataValidation(unittest.TestCase):

    def test_generate_table_comparison_details_success(self, mock_get_dataframe, mock_compare_two_tables):
        # Mock successful return value for compare_two_tables and get_dataframe
        mock_compare_two_tables.return_value = [{'table': 'table1', 'count': 100}]
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Call the function with mock arguments
        result = generate_table_comparison_details(
            MagicMock(spec=MockSparkSession),mock_compare_two_tables,mock_get_dataframe,'table1','table2','',True,'','','',True,None
        )

        # Assertions
        mock_compare_two_tables.assert_called_once_with(
            MagicMock(spec=MockSparkSession),[],'table1','table2','',True,'','','',True,None
        )
        mock_get_dataframe.assert_called_once()
        self.assertIsInstance(result, MagicMock)

    def test_generate_table_comparison_details_validation_inactive(self, mock_get_dataframe, mock_compare_two_tables):
        # Mock return value for compare_two_tables and get_dataframe
        mock_compare_two_tables.return_value = [{'table': 'table1', 'count': 100}]
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Call the function with validation_inactive = False
        result = generate_table_comparison_details(
            MagicMock(spec=MockSparkSession),mock_compare_two_tables,mock_get_dataframe,'table1','table2','',False,'','','',True,None
        )

        # Assertions
        mock_compare_two_tables.assert_not_called()  # Ensure compare_two_tables is not called
        mock_get_dataframe.assert_called_once()
        self.assertIsInstance(result, MagicMock)

    def test_generate_table_comparison_details_exception_handling(self, mock_get_dataframe, mock_compare_two_tables):
        # Mock exception in compare_two_tables
        mock_compare_two_tables.side_effect = Exception("Validation error")
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Call the function with mock arguments
        result = generate_table_comparison_details(
            MagicMock(spec=MockSparkSession),mock_compare_two_tables,mock_get_dataframe,'table1','table2','',True,'','','',True,None
        )

        # Assertions
        mock_compare_two_tables.assert_called_once()
        mock_get_dataframe.assert_called_once()
        self.assertIsInstance(result, MagicMock)

    def test_generate_multiple_table_comparison_details(self, mock_get_dataframe, mock_compare_two_tables):
        # Mock return value for compare_two_tables and get_dataframe
        mock_compare_two_tables.return_value = [{'table': 'tableA', 'count': 200}, {'table': 'tableB', 'count': 300}]
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Mock yaml content and path
        yaml_content = """
        rows:
          - table1: tableA
            table2: tableB
            filter_condition: ''
            is_validation_active: true
            metric_validation_active: false
            dimenssion_columns: ''
            dim_metrics_columns: ''
            ignored_columns: ''
            materialization: null
        """
        with patch('builtins.open', mock_open(read_data=yaml_content)):
            result = generate_multiple_table_comparison_details('dummy_path.yaml')

        # Assertions
        mock_compare_two_tables.assert_any_call(
            MagicMock(spec=MockSparkSession),[],'tableA','tableB','',True,'','','',True,None
        )
        mock_get_dataframe.assert_called_once()
        self.assertIsInstance(result, MagicMock)  # Adjust to match actual return type

    def test_generate_multiple_table_comparison_details_invalid_yaml(self, mock_get_dataframe, mock_compare_two_tables):
        # Mock return value for compare_two_tables and get_dataframe
        mock_compare_two_tables.return_value = [{'table': 'tableA', 'count': 200}, {'table': 'tableB', 'count': 300}]
        mock_get_dataframe.return_value = MagicMock(spec=MockSparkSession)

        # Mock invalid yaml content and path
        invalid_yaml_content = "invalid yaml"
        with patch('builtins.open', mock_open(read_data=invalid_yaml_content)):
            result = generate_multiple_table_comparison_details('dummy_path.yaml')

        # Assertions
        mock_compare_two_tables.assert_not_called()  # Ensure compare_two_tables is not called
        mock_get_dataframe.assert_not_called()  # Ensure get_dataframe is not called
        self.assertIsNone(result)  # Adjust to match actual behavior

if __name__ == "__main__":
    unittest.main()
