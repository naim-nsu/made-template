import os, sys
import sqlite3
import shutil
import zipfile
import urllib.request
import pandas as pd
from typing import Dict, List, Any

class Datapipeline:
    def __init__(self, info: Dict) -> None:
        self.info = info

    def on_extract(self) -> str:
        try:
            #download the zip file
            zip_file_path = os.path.join(os.getcwd(), self.info['zip_file_name'])
            self._download_file(self.info['url'], zip_file_path)

            #unzip file
            extracted_file_path = os.path.join(os.getcwd(), self.info['zip_file_name'].split(".")[0])
            self._unzip_file(zip_file_path, extracted_file_path)

            #define data path
            data_path = os.path.join(extracted_file_path, self.info['csv_file_name'])
            
            # move the data.csv to root folder
            data_path = os.path.join(extracted_file_path, self.info["csv_file_name"])
            final_data_path = os.path.join(os.getcwd(), self.info["csv_file_name"])
            os.replace(data_path, final_data_path)

            #remove zip file
            self._delete_file(zip_file_path)

            # remove the extracted directory
            self._remove_directory(extracted_file_path)

            return final_data_path

        except Exception as e:
            print(f'Error: An error occured during Extraction: {str(e)}')
            sys.exit(1)

    def on_transform(self, data_path) -> pd.DataFrame:
        try:
            #define columns to keep
            columns = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"]

            #read csv info pandas dataframe
            df = self._read_csv(file_path=data_path, sep=';', decimal=',', usecols=columns, index_col=False)


            # rename columns
            df = df.rename(columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"})

            # transform temperatures in Celsius to Fahrenheit
            df[["Temperatur", "Batterietemperatur"]] = df[["Temperatur", "Batterietemperatur"]].apply(lambda x: round((x * 9/5) + 32, 2))

            # data validation - Geraet and Monat
            df = df[(df["Geraet"] > 0) & (df["Monat"] > 0)]

            #remove data file
            self._delete_file(data_path)


            return df
        
        except Exception as e:
            print(f'Error: An error occured during Transformation: {str(e)}')
            sys.exit(1)
        
    def on_load(self, data: pd.DataFrame ) -> None:
        try:
            # connect to the database
            conn = sqlite3.connect(self.info["db_name"])
            print(f"Succeed: Database created successfully")

            # insert data into the database
            data.to_sql(self.info["table_name"], conn, if_exists='replace', index=False)
            print(f"Succeed: Data inserted into the database successfully")
            
            # close the connection
            conn.close()
        except sqlite3.Error as e:
            print(f"Error: An error occurred during Data Loading: {str(e)}")
            sys.exit(1)


    def run_pipeline(self) -> None:
        data_path = self.on_extract()
        transformed_data = self.on_transform(data_path)
        self.on_load(transformed_data)

    # Helper functions
    def _download_file(self, url: str, output_path: str) -> None:
        try:
            urllib.request.urlretrieve(url, output_path)
            print(f"Succeed: Data downloaded successfully and saved as {output_path.split(os.sep)[-1]}")
        except urllib.error.URLError as e:
            print(f"Error: Failed to download data from URL. {str(e)}")
            sys.exit(1)
        except Exception as e:
            print(f"Error: An unexpected error occurred. {str(e)}")
            sys.exit(1)

    def _unzip_file(self, zip_path: str, extract_path: str) -> None:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            print("Succeed: File successfully extracted")
        except zipfile.BadZipFile:
            print("Error: Invalid zip file")
            sys.exit(1)
        except FileNotFoundError:
            print("Error: File or directory not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error: An error occurred- {str(e)}")
            sys.exit(1)
    
    def _read_csv(self, file_path: str, sep: str = ",", decimal='.',
                   header: int = 0,
                   usecols: List = None,
                   index_col: Any = None,
                   encoding: str = 'utf-8') -> pd.DataFrame:
        try:
            data_df = pd.read_csv(file_path, sep=sep, decimal=decimal, header=header,
                                   usecols=usecols, index_col=index_col, encoding=encoding)
            print(f"Succeed: '{file_path.split(os.sep)[-1]}' is successfully loaded")
            return data_df
        except FileNotFoundError:
            print(f"Error: File not found- '{file_path}'")
            sys.exit(1)
        except Exception as e:
            print(f"Error: Failed reading the file- {str(e)}")
            sys.exit(1)
    
    def _delete_file(self, file_path: str) -> None:
        try:
            os.remove(file_path)
            print(f"Succeed: File '{file_path.split(os.sep)[-1]}' deleted successfully")
        except OSError as e:
            print(f"Error: Issue occurred while deleting the file: {str(e)}")
            sys.exit(1)
        except Exception as e:
            print(f"Error: Failed deleting the file- {str(e)}")
            sys.exit(1)
    
    def _remove_directory(self, directory_path: str):
        try:
            shutil.rmtree(directory_path)
            print(f"Succeed: {directory_path} directory successfully removed")
        except FileNotFoundError:
            print("Error: Directory not found")
        except PermissionError:
            print("Error: Permission denied")
        except Exception as e:
            print(f"Error: An error occurred- {str(e)}")

if __name__ == '__main__':
    INFO = {
        'url': 'https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip',
        'zip_file_name':'mowesta-dataset-20221107.zip',
        'csv_file_name': 'data.csv',
        'db_name': 'temperatures.sqlite',
        'table_name': 'temperatures'
    }
    pipeline = Datapipeline(INFO)
    pipeline.run_pipeline()