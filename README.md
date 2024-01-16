# Analyzing the Impact of Weather Conditions on Insurance Claims in Germany

This project examines how **weather conditions in Germany affect insurance claims**, using two open-source datasets. The first dataset is from the German Weather Service (Deutscher Wetterdienst - [DWD](https://opendata.dwd.de/)), providing comprehensive weather data for Germany and its states. The second dataset is from [GENESIS](https://www-genesis.destatis.de/genesis/online/data?operation=sprachwechsel&language=en), offering detailed information on insurance claims in various categories across Germany. Refer to the [project plan](/project/project-plan.md) for more details.

## Project Structure

```bash
project/
├── config/
│   ├── __init__.py
│   ├── config_var.py			# Main configuration file
│   ├── config_var.example.py		# Dummy configuration file to duplicate
│   └── source_info.json		# Data sources file
├── data/
│   ├── fau_made_project_ws23.sqlite	# Sqlite Database
├── main.py				# Main entry point to run the pipeline
├── pipeline.py				# Data Pipeline
├── pipeline.sh				# Bash script of running pipeline
├── project-plan.md			# Project Plan
├── report.ipynb			# Notebook of final project report
├── tests.py				# Unit testing file
└── tests.sh				# Bash script for running tests
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
