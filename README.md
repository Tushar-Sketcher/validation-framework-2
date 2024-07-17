[[_TOC_]]

# Mops data-validation-framework library
## Summary
The '**data-validation-framework**' is python library designed to facilitate data validation for Hive tables specified in the input paramaters. It provides functionalities for comparing count data, row data and metric data between 2 tables(table1 and table2).

## Library Installation:
```bash
pip install data-validation-framework --index-url https://artifactory.zgtools.net/artifactory/api/pypi/pypi-local/simple --verbose --extra-index-url https://pypi.org/simple/

```

## Library Fuctions:
The data-validation-framework library includes the following key functions:
- **generate_multiple_table_comparison_details()**: Compares data between multiple tables based on input parameters provided in a YAML file. A dataframe output is generated containing comparison details for tables added in Yaml file.
- **generate_table_comparison_details()**: Compares data between two tables based on user-provided input parameters. A dataframe output is generated containing comparison details.


## How To use library functions:
- The framework consist of 2 library functions generate_multiple_table_comparison_details() and generate_table_comparison_details().
### generate_multiple_table_comparison_details():

- **generate_multiple_table_comparison_details(*path: string = ‘YAML File Path String’*)**

#### Purpose:
This is util fuction takes yaml file path as input paramter. In the YAML configuration user need to specify paramaters under '**rows**' for the set of tables they want to validate. It processes a Count Comparison, Row(data) Comparison and Metric validation based on the input YAML configuration provided by the user. This function accepts a YAML file path as a string input from the user. It then processes validations for input tables based on the parameters provided by the user within the YAML file. Refer this section for YAML [input parameter details](https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework/-/blob/feature/validation_framework/README.md#input-parameters-details-in-yaml-file). 

This functions return a dataframe output containing the validation details. 

Within above function we are primarily processing four functions in order to do overall Count Comparison, Row(data) Comparison and Metric validation.
- For Count validation we are processing *compare_counts()*. This function compares and returns the row count between two tables based applying the filter(if added) and difference between them.

- For Row data validation we are processing: *compare_columns()* and *compare_rows()*. The *compare_columns()* checks if the columns of two tables are the same, if those are same then execute *compare_rows()* to perform row-level(row data) validation between two tables using our MD5 hashing approach. This function return the data difference count details between 2 tables.

- Metric validation we are processing *compare_metrics_by_dimension()* function which returns a seperate dataframe containg the metric validation details and return a dataframe output. Also the output is stored as table default to the '*sandbox_marketing.mart_mops_tests.{table2_name}_metric_validation*'. The table name can be changed using the parameter '**metric_result_table**' specifying the table name in *catalog.schema.table_name* format. 
- **Note**: The metric validation function is processed only when **metric_validation_active** is set to **True** 

#### Input Parameters Details in YAML File:
Below are the parameters that user needs to provide in the yaml. Alternatively, users can create their own YAML file **adhering strictly to below input parameters names** and pass that YAML file path string to the function like below  
```bash
  generate_multiple_table_comparison_details(path=’/Workspace/path/to/your/yamlfile/yaml.yml’)
```

Below parameters should be specified under ‘**rows**’ section(do not change param’s names) in the yaml. (Examples file: [tables.yml](https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#files/1760665646812858) )
1. **is_validation_active** (Bool: Required): 
    - True or False boolean value. The default value is set to True but iff set False by user then the function will skip the overall validation process for its tables.
    - Users can set it to False in yaml if they do not wish to validate specific tables or already checked tables during validation. 
2. **table1** (str): Required. Name of the first Hive or Databricks table in the format catalog.schema.table_name.
3. **table2** (str): Required. Name of the second Hive or Databricks table in the format catalog.schema.table_name.
4. **filter_condition**: (str): Optional. Filter condition to be applied to both table1 and table2. Use SQL syntax (e.g., "column1 >= date'2024-01-01'"). Default is an empty string ("").
5. **materialization** (str: optional): Type of table. Add a string e.g. incremental/full_load.
6. **metric_validation_status**: (bool): Optional. Whether metric validation should be performed. Default is False.
7. **dimension_columns** (str: optional): Optional. Specify dimension or slice columns (common in both tables) for validation query. Provide as a string, separated by commas if multiple (e.g., "column1, column2"). Default is an empty string ("").
    - For single column use format:  ‘column1’ 
    - For multiple column, list them as: ‘column1, column2, column3, column4’
8. **dim_metric_columns** (str): Optional. Specify dimension columns to be converted into metric columns (perform count aggregation) in the validation query. Provide as a string, separated by commas if multiple (e.g., "column1, column2"). Default is an empty string ("").
    - For single column eg:  ‘column_name’ 
    - For multiple column e.g: ‘column1, column2, column3, column4’
9. **ignored_columns** (str: optional): Provide the list of columns that you wish to ignore/exclude from the metrics of integral type e.g ‘zuid’, ‘account_id’, ‘campaign_id’, ‘process_year’ etc. Default value is set to empty string (“ ”)
    - For single column eg:  ‘column_name’ 
    - For multiple column e.g: ‘column1, column2, column3, column4’

#### Example YAML configuration:

```bash
rows:
  - is_validation_active: True
    table1: 'hive_metastore.mart_mops.real_time_touring_agent'
    table2: 'sandbox_marketing.mart_mops_test.real_time_touring_agent_managed'
    filter_condition: "p_data_date >= date'2024-05-16'"
    materialization: 'incremental'
    metric_validation_active: True
    ignored_columns: 'mls_id, team_lead'
    dimension_columns: 'p_data_date'
    dim_metrics_columns: 'team_lead, mls_id'
    metric_result_table: 'sandbox_marketing.mart_mops_test.test2'
```

#### Example Usage:
Execute below steps or you can refer the this [data validation framework testing notebook](https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#notebook/3501886217257531/command/2038113280157046) for reference. Perform below commands in the databricks notebook.

1. Install Package:
```bash
!pip install data-validation-framework --index-url https://artifactory.zgtools.net/artifactory/api/pypi/pypi-local/simple --verbose --extra-index-url https://pypi.org/simple/
```
2. Import library-functions and pyspark functions:
```bash
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from data_validation_framework.process_validation_task import generate_multiple_table_comparison_details
```
3. Input YAML path:
Create a yaml file (like tables.yml https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#files/176066564681285) somewhere in databricks workspace or in the git repo (If you wish to create this as a task in workflows) with the above-mentioned required input parameters for the tables you are comparing and define path variable in notebook providing the yaml file path.
```bash
path = '/Workspace/Users/v-tushara@zillowgroup.com/tables.yml'
```

4. Execute generate_multiple_table_comparison_details(path):
Pass the ‘path’ variable to the functions generate_multiple_table_comparison_details() and execute. Once executed it will process validation and create a dataframe containing details such as table1 count, table2 count, count difference between table1 and table2, difference percentage, count match status, table1_subtract_cnt(count of data rows in table1 but not present in table2. The .subtract() action in pyspark), table2_subtract_cnt (count of data rows in table2 but not present in table1. The .subtract() action in pyspark), table1_datamatch_st (Whether table1 data matched with table2 True or False), table2_datamatch_st (Whether table2 data matched with table2, True or False ) and partition column details. You can view the output df by .show() or display() method.

```bash
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.process_validation_task import  generate_multiple_table_comparison_details

path = '/Workspace/Users/v-tushara@zillowgroup.com/tables.yml'

df = generate_multiple_table_comparison_details(path)
display(df)
```

### generate_table_comparison_details():

- **generate_table_comparison_details(*table1: string = '', table2: string = '', filter_condition: string = '', metric_validation_active: bool = False, dimension_columns: string = '', dim_metrics_columns: string = '', ignored_columns: string = '', metric_result_table: string = '', materialization: string or None = None*)**

#### Purpose:
This function compares data between two tables based on the input parameters provided by the user. It performs count comparison, row-level data validation, and optionally, metric validation based on the conditions specified.

#### Input Parameters:
1. **table1** (str): Required. Name of the first Hive or Databricks table in the format catalog.schema.table_name.
2. **table2** (str): Required. Name of the second Hive or Databricks table in the format catalog.schema.table_name.
3. **filter_condition** (str): Optional. Filter condition to be applied to both table1 and table2. Use SQL syntax (e.g., "column1 >= date'2024-01-01'"). Default is an empty string ("").
4. **metric_validation_active** (bool): Optional. Whether metric validation should be performed. Default is False.
5. **dimension_columns** (str): Optional. Specify dimension or slice columns (common in both tables) for validation query. Provide as a string, separated by commas if multiple (e.g., "column1, column2"). Default is an empty string ("").
6. **dim_metrics_columns** (str): Optional. Specify dimension columns to be converted into metric columns (perform count aggregation) in the validation query. Provide as a string, separated by commas if multiple (e.g., "column1, column2"). Default is an empty string ("").
7. **ignored_columns** (str): Optional. Specify columns to be ignored/excluded from metric calculations. Provide as a string, separated by commas if multiple (e.g., "column1, column2"). Default is an empty string ("").
8. **metric_result_table** (str): Optional. Name of the table where metric validation results should be stored. Default is "".
9. **materialization** (str or None*): Optional. Type of table materialization. Provide as a string (e.g., "incremental", "full_load"), or None if not applicable. Default is None.  

#### Example Usage

```bash
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from data_validation_framework.process_validation_task import generate_table_comparison_details

table1 = 'hive_metastore.database.table1'
table2 = 'hive_metastore.database.table2'
filter_condition = "column1 >= date'2024-01-01'"
metric_validation_active = True
dimension_columns = 'column2'
dim_metrics_columns = 'column3, column4'
ignored_columns = 'column5, column6'
metric_result_table = 'sandbox_marketing.metrics_validation_results'
materialization = 'incremental'

df = generate_table_comparison_details(
    table1=table1,
    table2=table2,
    filter_condition=filter_condition,
    metric_validation_active=metric_validation_active,
    dimension_columns=dimension_columns,
    dim_metrics_columns=dim_metrics_columns,
    ignored_columns=ignored_columns,
    metric_result_table=metric_result_table,
    materialization=materialization
)
Note
display(df)
```

### Notes:
- Ensure that table1 and table2 have consistent schemas before performing data validation. Schema mismatches may cause exceptions during execution.
- The metric_validation_active parameter determines whether metric validation should be executed. It is recommended to set this to True only when there is a data mismatch between table1 and table2 for count and row data comparison.
- Specify dimension_columns, dim_metrics_columns, and ignored_columns as needed to tailor the validation process to your specific requirements.

### Example Validation Notebook():
Refer to this example notebook showcasing how to use the generate_multiple_table_comparison_details() function, leveraging the input tables specified in the [tables.yml](https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#files/1760665646812858)

notebook: https://zg-marketing-lab.cloud.databricks.com/?o=3559368047888194#notebook/3501886217257531/command/2038113280157046


## Development Guide

### Getting Started and Running Tests Locally:
To begin development and run tests locally follow these steps using Ubuntu terminal in WSL:
1. **Clone Repository**: Clone the repository and navigate into the project directory. If you are working on a specific branch, switch to it using below commands:
```bash
git clone https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework.git

cd data-validation-framework

git checkout <enter your branch-name>
```
2. **Install Poetry**: Ensure poetry is installed on your local machine. If not, install it using command:
```bash
pip3 install poetry  

# Verify the poetry installation running below command
poetry --version
```
3. **Install Dependencies using Poetry**: Once poetry installation done, install the required dependencies listed in [pyproject.toml](https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework/-/blob/main/pyproject.toml#L16-21) using Poetry(Navigate to your project directory):
```bash
cd data-validation-framework # Skip this if already in root directory

poetry install
```
4. **Write Tests**: If necessary, create or modify test scripts in the 'tests' folder. Ensure that tests adhere to the framework's conventions. Here's an example of a test case using unittest. Skip this if tests are already added.
```bash
# tests/test_mymodule.py
import unittest
from mymodule import my_function

class TestMyModule(unittest.TestCase):

    def test_my_function(self):
        self.assertEqual(my_function(2, 3), 5)
        self.assertEqual(my_function(0, 0), 0)
        self.assertEqual(my_function(-1, 1), 0)

if __name__ == '__main__':
    unittest.main()
```
 - Make sure to update dependencies in pyproject.toml new dependencies are added during test modifications, and repeat **Step 3** to install them.

5. **Running Tests using Poetry**: Once dependencies are installed successfully, run tests (from root directory data-validation-framwork) using the following command:
```bash
poetry run python3 -m unittest discover -s tests -v 
```
- If you encounter below error in your tests related to Java, ensure that Java is installed and **JAVA_HOME** path is set correctly. Refer to [How to install Java and set JAVA_HOME on Ubuntu](https://linuxize.com/post/install-java-on-ubuntu-20-04/).
```bash
pyspark.errors.exceptions.base.PySparkRuntimeError: [JAVA_GATEWAY_EXITED] Java gateway process exited before sending its port number.
```

### Making Changes and Deployment

1. **Modify Code and Dependencies**: Implement your changes, including updating external dependencies if necessary. Update dependencies (If any) in [pyproject.toml](https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework/-/blob/main/pyproject.toml#L16-21) to install dependecies.

2. **Publish and Deployment**: Push your changes, trigger the CICD pipeline to publish using 'poetry' and check deployed version on [artifactory](https://artifactory.zgtools.net/ui/packages/pypi:%2F%2Fdata-validation-framework?name=data-validation-framework&type=packages).


## Use library with Databrick's Notebook task and Python Wheel task:

The **data-validation-framework** library can be utilized in the databricks workflows using Notebook Task and Python Task. We can install the library in the databricks cluster and use any of these tasks as per below-mentioned steps.

Prerequisites:
  - Go to the *Libraries* section inside the Databricks Cluster.  
  - Install ‘data-validation-framework’ python library selecting *PyPI* Library source 
  - Provide this *Index URL: https://artifactory.zgtools.net/artifactory/api/pypi/zg-pypi/simple* when installing and make sure the library is installed successfully in the cluster.

### Notebook Task(recommended):
  1. Create a notebook, add the code for library execution as mentioned in the step [Execute generate_multiple_table_comparison_details(path)](https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework/-/blob/feature/validation_framework/README.md#4-execute-generate_multiple_table_comparison_detailspath) and replace the path variable value with your yaml file path created as per the [configurations](https://gitlab.zgtools.net/analytics/data-engineering/big-data/marketing-de-databricks/data-validation-framework/-/blob/feature/validation_framework/USAGE.md#input-parameters-details-in-yaml-file) for your tables.
  2. Add **data-validation-framework** library dependency providing the *index url* in the task’s configuration in your databricks workflow and execute the task.


### Python Wheel Task:
- To execute the validation process using the python wheel task, we have created this ‘get_data_validations_wheel_task’ entry point.
- This entry point function accepts takes Yaml file path string as an argument and processes the validation execution.
- Use this entry point python wheel task and package name **data_validation_framework** to execute.
    1. Add **get_data_validations_wheel_task** in the ***Entry Point*** section and **data_validation_framework** in ***Package Name*** section in the python wheel task
    2. Provide the yaml file path variable in the ***Parameter*** section for the entry point function selecting using 'Keyword argument' and execute the task.

### Sample Workflow
Refer this sample workflow created. Using the library with Python wheel and Notebook task:
[Validation Framework Utility Test Example](https://zg-marketing-lab.cloud.databricks.com/jobs/144060734374961/tasks?o=3559368047888194) 
