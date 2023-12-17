import json, sys
import pandas as pd
from typing import Dict, Union
from config.config_var import *

class DataPipeline:
    def __init__(self, source_info_path: str) -> None:
        self.source_info_path = source_info_path
        self.source_info = None
    
    def on_extract(self) ->  Dict:
        extracted_data = dict()
        for source in self.source_info["data_sources"]:
            if source["source_name"] not in extracted_data:
                extracted_data[source["source_name"]] = list()
                
            data_urls = source["data_urls"]
            sep=source["data_separator"]
            skiprows=source["skip_rows"] 
            skipfooter=source["skip_footer"]
            
            for url in data_urls:
                url = url.format(MY_USERNAME=GENESIS_USERNAME, MY_PASSWORD=GENESIS_PASSWORD)
                df = pd.read_csv(url, sep=sep, skiprows=skiprows, skipfooter=skipfooter)
                extracted_data[source["source_name"]].append(df)
        return extracted_data
    

    def on_transform(self, extracted_data: Dict) -> pd.DataFrame:
    

        return self.transformer.transformed_data

    def on_load(self, transformed_data: pd.DataFrame) -> None:
       
        self.loader.transformed_data = transformed_data
        self.loader.load()
    
    def _load_json(self, file_path: str) -> Union[Dict, None]:
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            print(f"Succeed: JSON loaded successfully")
            return data
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"Error: Failed to decode JSON data in file '{file_path}'.")
            sys.exit(1)
        except Exception as e:
            print(f"Error: An unexpected error occurred. {str(e)}")
            sys.exit(1)
            
    def run_pipeline(self) -> None:
        self.source_info = self._load_json(self.source_info_path)
        extracted_data = self.on_extract()
        # transformed_data = self.on_transform(extracted_data)
        # self.on_load(transformed_data)

if __name__ == "__main__":
    data_pipeline = DataPipeline(SOURCE_INFO_PATH)
    data_pipeline.run_pipeline()