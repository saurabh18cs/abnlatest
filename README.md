# EternalTeleSales Fran van Seb Group

## Description

This application is designed for a small company called **EternalTeleSales Fran van Seb Group** that specializes in telemarketing for various areas and wants to gain insights from their employees' data.

## Features

- **Data Integration**: The application can join multiple datasets together. In this case, it's joining `dataset_one.csv`, `dataset_two.csv`, and `dataset_three.csv`.

- **Data Filtering**: The application can filter data based on certain criteria. For example, it can filter data for employees working in the IT department.

- **Data Sorting**: The application can sort data based on certain fields. For example, it can sort data by the sales amount.

- **Data Limiting**: The application can limit the number of records in the output. For example, it can limit the output to the top 100 records.

- **Data Extraction**: The application can extract specific information from the data. For example, it can extract addresses and zip codes.

- **Data Aggregation**: The application can perform aggregations on the data. For example, it can calculate the total sales amount and the percentage of successful calls per department.

- **Data Export**: The application can export the processed data to CSV files.

- **Performance Analysis**: The application can identify the top performers in each department based on the percentage of successful calls and the sales amount.

## Installation

To install and run the application, follow these steps:

1. **Generate a Wheel Distribution**: A wheel distribution is a built distribution format for Python packages. You can generate a wheel distribution for your package using the `bdist_wheel` command:

    ```bash
    python setup.py bdist_wheel
    ```

2. **Install the Wheel Distribution**: You can install the wheel distribution using pip:

    ```bash
    pip install dist/abnlatest-0.0.1-py3-none-any.whl
    ```

## Usage

To use the application, follow these instructions from python terminal:

```bash
from abnlatest import job
python -m abnlatest.job "--fpath_1" "resources/latest/dataset_one.csv" "--fpath_2" "resources/latest/dataset_two.csv" "--fpath_3" "resources/latest/dataset_three.csv"