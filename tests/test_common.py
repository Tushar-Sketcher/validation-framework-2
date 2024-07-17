import unittest
from unittest.mock import Mock
from pyspark.sql import SparkSession
from data_validation_framework.common import get_query, get_partition_details

class TestFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize SparkSession for testing
        cls.spark = SparkSession.builder \
            .appName("data validation framework") \
            .getOrCreate()
        cls.spark.sql("""CREATE TABLE IF NOT EXISTS table1 (
                        col1 INT)
                        PARTITIONED BY (partition_date STRING)""")
        cls.spark.sql("CREATE OR REPLACE TEMP VIEW table2 AS SELECT * FROM VALUES (1, 'A') AS t(col1, col2)")

    def test_get_query(self):
        # Test get_query function with different scenarios
        self.assertEqual(get_query("table1", ""), "SELECT /*+ REPARTITION(200) */ * FROM table1")
        self.assertEqual(get_query("table2", "col1 = 'value'"), "SELECT /*+ REPARTITION(200) */ * FROM table2 WHERE col1 = 'value'")
        self.assertEqual(get_query("table3", None), "SELECT /*+ REPARTITION(200) */ * FROM table3")

    def test_get_partition_details(self):
        # Test get_partition_details function
        result_status, result_cols = get_partition_details(self.spark, "table1")
        self.assertEqual(result_status, "Partitioned")
        self.assertEqual(result_cols, ["partition_date"])

    def test_get_partition_details_non_partitioned(self):
        # Test get_partition_details function
        result_status, result_cols = get_partition_details(self.spark, "table2")
        self.assertEqual(result_status, "Non-partitioned")
        self.assertEqual(result_cols, [])

if __name__ == '__main__':
    unittest.main()
