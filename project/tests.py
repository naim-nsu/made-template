import unittest
import sqlite3
import pandas as pd

from project.config.config_var import *
from pipeline import DataPipeline

class Test(unittest.TestCase):
    def test_data_pipeline(self):
        
        #check if the db is already exits, if so remove it first
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        
        #check if the db is removed successfully
        self.assertFalse(os.path.exists(DB_PATH))
        #check the source file is exitst
        self.assertTrue(os.path.exists(SOURCE_INFO_PATH))
        
        #run the data pipeline
        data_pipeline = DataPipeline()
        data_pipeline.run_pipeline()
        
        #check if the data pipeline created the db
        self.assertTrue(os.path.exists(DB_PATH))
        
        #connect db and check the rows of the table are as expected
        conn = sqlite3.connect(DB_PATH)
        #print(DB_PATH)
        expected_row_counts = {
            'weather_data': 156,
            'insurance_claim_data': 624
        }
        
        for table_name, expected_row_count in expected_row_counts.items():
            db_rows = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            self.assertEqual(len(db_rows), expected_row_count)
        
        conn.close()

if __name__ == "__main__":
    unittest.main()