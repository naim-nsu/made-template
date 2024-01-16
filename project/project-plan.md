# Project Plan

## Title

<!-- Give your project a short title. -->

**Analyzing the Impact of Weather Conditions on Insurance Claims in Germany**

## Main Question

<!-- Think about one main question you want to answer based on the data. -->

Is there any relationship between weather conditions and insurance claims in Germany?

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->

Weather significantly affects our daily lives and has a notable impact on various aspects, including insurance claims. Germany's diverse climate provides an intriguing topic for exploring how weather conditions relate to insurance claims. This study aims to analyze this relationship, investigating how different weather patterns might influence the frequency and severity of insurance claims in Germany. Understanding these connections could offer valuable insights for insurers and policymakers in managing risks associated with weather-related claims in the country.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

In this study, two open-source datasets have been utilized. The first dataset is sourced from the German Weather Service (Deutscher Wetterdienst - [DWD](https://opendata.dwd.de/)), which offers comprehensive weather-related data covering Germany and its individual states. The second dataset originates from [GENESIS](https://www-genesis.destatis.de/genesis/online/data?operation=sprachwechsel&language=en), providing detailed information on insurance claims across various categories within Germany.

### Datasource 1: Weather Datasource

- Source: [DWD](https://opendata.dwd.de/)
- Metadata URL: https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/
- Data URL: https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_01.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_02.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_03.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_04.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_05.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_06.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_07.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_08.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_09.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_10.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_11.txt
  https://opendata.dwd.de/climate_environment/CDC/regional_averages_DE/monthly/air_temperature_mean/regional_averages_tm_12.txt
- Data Type: CSV
- License Type: OpenData License

### Datasource 2: Insurance claims Datasource

- Source: [GENESIS](https://www-genesis.destatis.de/genesis/online/data?operation=sprachwechsel&language=en)
- Metadata URL: https://www-genesis.destatis.de/genesis//online?operation=table&code=52411-0020&bypass=true&levelindex=0&levelid=1699387440984#abreadcrumb
- Data URL: https://www-genesis.destatis.de/genesisWS/rest/2020/data/tablefile?username=MY_USERNAME&password=MY_PASSWORD&name=52411-0020&area=all&compress=false&transpose=false&startyear=2018&endyear=2022&language=en
- Data Type: CSV
- Authentioncation: Required
- License Type: OpenData License

## Project Structure

```bash
project/
├── config/
│   ├── __init__.py
│   ├── config_var.py                   #  Main configuration file
│   ├── config_var.example.py		    # Dummy configuration file to duplicate
│   └── source_info.json				# Data sources file
├── data/
│   ├── fau_made_project_ws23.sqlite	# Sqlite Database
├── main.py							    # Main entry point to run the pipeline
├── pipeline.py							# Data Pipeline
├── pipeline.sh							# Bash script of running pipeline
├── project-plan.md						# Project Plan
├── report.ipynb						# Notebook of final project report
├── tests.py							# Unit testing file
└── tests.sh							# Bash script for running tests
```

**Important files of the project and their roles:**

- `project/config/config_var.py`: It contain all the configuration variables including **GENESIS_USERNAME** and **GENESIS_PASSWORD**, which should be given to extract data from [GENESIS](https://www-genesis.destatis.de/genesis/online/data?operation=sprachwechsel&language=en).
- `project/main.py`: It will first look wheather the `project/config/config_var.py` file exists, if not, it will create the `project/config/config_var.py` file from `project/config/config_var.example.py`. After that, it will run an automated data pipeline that creates an SQLite database named `fau_made_project_ws23.sqlite` that contains two tables representing two open data sources of the project.
- `project/tests.sh`: A bash script that will execute the unit testing for the project by calling the `project/tests.py` Python script.
- `project/report.ipynb`: This Jupyter notebook functions as the conclusive report for the project, offering a thorough exploration of various aspects and discoveries. The report primarily delves into the correlation between weather conditions and insurance claims in Germany, based on the data in `fau_made_project_ws23.sqlite`. See the [report](project/report.ipynb).

**Continuous Integration Pipeline using GitHub Action:** <br>

A Continuous Integration (CI) pipeline has been set up through a GitHub action specified in [.github/workflows/ci-test.yml](.github/workflows/ci-test.yml). This pipeline is activated whenever modifications are made to the `project/` directory and pushed to the GitHub repository or when a pull request is initiated and merged into the main branch. The `ci-test.yml` workflow runs the `project/tests.sh` test script.

## Project Setup

1. Clone this git repository

```bash
git clone https://github.com/naim-nsu/made-template.git
```

2. Install [Python](https://www.python.org/). Then create a virtual environment inside the repo and activate it.

```bash
python -m venv <env_name>
source <env_name>/bin/activate
```

3. Download and install the required Python packages for the project.

```bash
pip install -r pandas
```

4. Give the credentials of [GENESIS](https://www-genesis.destatis.de/genesis/online/data?operation=sprachwechsel&language=en) portal in `project/config/config_var.py`. please note that `project/config/config_var.py` can be either manually created by renaming the `project/config/config_var.example.py` or it will be automatically created after running the `main.py`.

```bash
GENESIS_USERNAME = os.environ.get("GENESIS_USERNAME") or ''        #replace '' with your genesis username
GENESIS_PASSWORD = os.environ.get("GENESIS_PASSWORD") or ''        #replace '' with your genesis password
```

5. To run the project, go to the `project/` directory and run the `main.py` script. It will run the whole ETL pipeline and generate an SQLite database named `fau_made_project_ws23.sqlite` that contains two tables, `weather_data` and `insurance_claim_data`, representing two open data sources of the project.

```bash
cd project/
python main.py
```

6. To run the test script which will execute the component and system-level testing for the project, run the following command.

```bash
chmod +x tests.sh
sh tests.sh
```

7. Finally, run and explore the `project/report.ipynb` project notebook, and also feel free to modify it.
