from pyspark.sql.types import *
from pyspark.sql.functions import *
from spark_etl_job.Spark_etl import *

class sparkETLtestcases(unittest.TestCase):

    def setUp(self):
        """Start Spark, define config and path to test data """

        self.test_data_path = 'testing/test_data/'

    def tearDown(self):
        pass

    def test_json_parser(self):
        variable = self.test_data_path + 'customers.json'
        return json_parser(variable)

    def test_csv_parser(self):
        variable = self.test_data_path + 'customers.csv'
        return  json_parser(variable)


if __name__ == '__main__':
    unittest.main()


