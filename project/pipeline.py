import json, sys, calendar, sqlite3
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
                print(url)
                df = pd.read_csv(url, sep=sep, skiprows=skiprows, skipfooter=skipfooter, engine='python')
                extracted_data[source["source_name"]].append(df)
                
            print(f"Succeed: Data from {source['source_name'] } data source extracted successfully")
        return extracted_data
    

    def on_transform(self, extracted_data: Dict) -> pd.DataFrame:
        transformed_data = dict()
        for source in extracted_data:
            if source == "DWD":
                #concat all the dataframes into a single df
                df = pd.concat([df for df in extracted_data[source]], axis=0, ignore_index=True)
                
                #removed unnamed column
                df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                
                #rename header
                df = df.rename(columns={"Jahr": "Year", "Monat": "Month", "Deutschland": "Germany"})
                #keep data from 2010 to 2022
                df = df.loc[(df['Year'] >= 2010) & (df['Year'] <= 2022)]
                #change month value int to month name
                df['Month'] = df['Month'].replace(
                                            [num for num in range(1, 13)],
                                            [month for month in calendar.month_name[1:]])
                #creating new column Date with year and month
                df.insert(loc=2, column="Date", value=df['Month'] + "-" + df['Year'].astype(str))
                
                transformed_data[source] = df
                
            elif source == "Genesis":
                #concat all the dataframes into a single df
                df = pd.concat([df for df in extracted_data[source]], axis=0, ignore_index=True)
                
                #rename header
                df.columns = ["Year", "Month", "Category_Type", "Category_Name", "Total_Claim"]
                
                #remove invalid and unwanted rows
                df = df[df['Total_Claim'].apply(lambda x: str(x).isdigit())]
                df = df[df['Category_Type'].apply(lambda x: x in ["WZ08-G","WZ08-H","WZ08-I","WZ08-Q",])]
                
                #creating new column Date with year and month
                df.insert(loc=2, column="Date", value=df['Month'] + "-" + df['Year'].astype(str))
                
                transformed_data[source] = df
            
            print(f"Succeed: Data from {source} data source transformed successfully")
                
        return transformed_data

    def on_load(self, transformed_data: Dict) -> None:
        try:
            # connect to the database
            conn = sqlite3.connect(DB_PATH)
            print(f"Succeed: Database created successfully")

            # insert data into the database
            for source in transformed_data:
                if source == "DWD":
                    table_name = "weather_data"
                    dtype = {}
                elif source == "Genesis":
                    table_name = "insurance_claim_data"
                    dtype={
                        "Year": 'int',
                        "Month": 'text',
                        "Category_Type": 'text',
                        "Category_NAME": 'text',
                        "Total_Claim": 'int'
                    }
                
                transformed_data[source].to_sql(table_name, conn, if_exists='replace', index=False, dtype=dtype)
                print(f"Succeed: Data from {source} data source inserted into the database successfully")
            
            # close the connection
            conn.close()
        except sqlite3.Error as e:
            print(f"Error: An error occurred during table population: {str(e)}")
            sys.exit(1)
        
    
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
        transformed_data = self.on_transform(extracted_data)
        self.on_load(transformed_data)

if __name__ == "__main__":
    data_pipeline = DataPipeline(SOURCE_INFO_PATH)
    data_pipeline.run_pipeline()