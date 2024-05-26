import os
import sys
import argparse
from typing import Dict, List, Tuple
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from pyspark.sql import (
    SparkSession, 
    DataFrame, 
    Column,    
    functions as F,
    Window
)


__author__ = "Saurabh Gupta"
APP_NAME = "ABNLatest"


"""
    Setup global logger with rotating file strategy
"""
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = RotatingFileHandler('job.log', maxBytes=2000, backupCount=2)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)


import argparse

def parser():
    """
    Parse command line arguments and return the parsed arguments.

    Returns:
        argparse.Namespace: The parsed command line arguments.
    """
    logger.info("Argument parser start")

    parser = argparse.ArgumentParser()
    parser.add_argument('--fpath_1', type=str, required=True,
                        help='Path to first file')
    parser.add_argument('--fpath_2', type=str, required=True,
                        help='Path to second file')
    parser.add_argument('--fpath_3', type=str, required=True,
                        help='Path to third file')
    args = parser.parse_args()

    check_fpaths(args)
    logger.info("Parsed filepaths checked")
    logger.info("Argument parser end")

    return args


import os

def check_fpaths(args):
    """
    Checks if the parsed filepaths exist and throws an exception otherwise.

    :param args: Parsed arguments <class 'argparse.Namespace'>.
    :type args: argparse.Namespace
    :raises Exception: If the filepaths are invalid.
    """

    current_path = os.getcwd()

    fpath_1 = os.path.join(current_path, args.fpath_1)
    if not os.path.exists(fpath_1):
        raise Exception('Invalid path to first file')

    fpath_2 = os.path.join(current_path, args.fpath_2)
    if not os.path.exists(fpath_2):
        raise Exception('Invalid path to second file')

    fpath_3 = os.path.join(current_path, args.fpath_3)
    if not os.path.exists(fpath_3):
        raise Exception('Invalid path to third file')


def extract(spark, args) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Reads CSV files from arguments and creates Spark DataFrames.

    :param spark: The SparkSession object used for reading CSV files.
    :type spark: pyspark.sql.session.SparkSession

    :param args: The parsed arguments containing file paths.
    :type args: argparse.Namespace

    :return: Three DataFrames extracted from the CSV files.
    :rtype: Tuple[pyspark.sql.dataframe.DataFrame, pyspark.sql.dataframe.DataFrame, pyspark.sql.dataframe.DataFrame]

    :raises: None
    """
    logger.info('Extraction start')

    df1_emp_calls = spark.read.csv(args.fpath_1, header=True)
    logger.info('DataFrame_1 extracted from CSV')

    df2_emp_personal = spark.read.csv(args.fpath_2, header=True)
    logger.info('DataFrame_2 extracted from CSV')

    df3_emp_sales = spark.read.csv(args.fpath_3, header=True)
    logger.info('DataFrame_3 extracted from CSV')

    logger.info('Extraction end')

    return df1_emp_calls, df2_emp_personal, df3_emp_sales


def transform(df1_emp_calls: DataFrame, df2_emp_personal: DataFrame, df3_emp_sales: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    Transforms the given DataFrames by joining them, renaming columns, and filtering based on countries.

    :param df1_emp_calls: The first DataFrame to be joined.
    :type df1_emp_calls: DataFrame
    :param df2_emp_personal: The second DataFrame to be joined.
    :type df2_emp_personal: DataFrame
    :param df3_emp_sales: The third DataFrame to be joined.
    :type df3_emp_sales: DataFrame
    :return: A tuple of transformed DataFrames: df_it_data, df_marketing_data, df_department_breakdown, df_best_performers.
    :rtype: Tuple[DataFrame, DataFrame, DataFrame, DataFrame]
    """
    df = df1_emp_calls.join(df2_emp_personal, on='id', how='inner')
    logger.info("DataFrames joined")
    df_it_data = transform_it_data(df, 'IT')
    df_marketing_data = transform_marketing_data(df)
    df_department_breakdown = transform_department_breakdown(df)
    df_best_performers = transform_best_performers(df)
    return df_it_data, df_marketing_data, df_department_breakdown, df_best_performers

def transform_it_data(df: DataFrame, area: str) -> DataFrame:
    """
    Transforms the input DataFrame by filtering for IT data, ordering by sales amount in descending order,
    and limiting the result to 100 rows.

    :param df: The input DataFrame containing the data to be transformed.
    :type df: DataFrame

    :return: The transformed DataFrame containing IT data, ordered by sales amount in descending order,
             and limited to 100 rows.
    :rtype: DataFrame
    """
    logger.info('Transformation start - IT Data')
    df_it_data = filter(df, df.area, area)
    df_it_data = df_it_data.orderBy(df_it_data.sales_amount.desc()).limit(100)
    logger.info('Transformation end - IT Data')
    return df_it_data

def transform_marketing_data(df) -> DataFrame:
    """
    Transforms the marketing data by filtering, extracting zip code, and cleaning the address column.

    :param df: The input DataFrame containing the marketing data.
    :type df: DataFrame
    :return: The transformed DataFrame with the address and zip code columns.
    :rtype: DataFrame
    """
    logger.info('Transformation start - Marketing Address Information')

    df_marketing_data = filter(df, df.area, 'Marketing')
    df_marketing_data = df_marketing_data.withColumn('zip_code', F.regexp_extract(df_marketing_data['address'], r'\b\d{4}\s\w{2}\b', 0))
    # remove zip code from address column
    df_marketing_data = df_marketing_data.withColumn('address', F.regexp_replace(df_marketing_data['address'], r'\b\d{4}\s\w{2}\b', ''))
    df_marketing_data = df_marketing_data.withColumn('address', F.regexp_replace(df_marketing_data['address'], '"', ''))
    df_marketing_data = df_marketing_data.withColumn('address', F.regexp_replace(df_marketing_data['address'], '\s*,\s*', ''))
    df_marketing_data = df_marketing_data.select('address', 'zip_code')

    logger.info('Transformation end - Marketing Address Information')
    return df_marketing_data

def transform_department_breakdown(df) -> DataFrame:
    """
    Transforms the input DataFrame by calculating department breakdown statistics.

    Args:
        df (DataFrame): The input DataFrame containing the data.

    Returns:
        DataFrame: The transformed DataFrame with department breakdown statistics.

    """
    logger.info('Transformation start - Department Breakdown')

    df_department_breakdown = df.groupBy('area').agg(F.sum('sales_amount').alias('total_sales_amount'), F.sum('calls_successful').alias('total_calls_successful'), F.sum('calls_made').alias('total_calls_made'))
    df_department_breakdown = df_department_breakdown.withColumn('percentage_successful', (df_department_breakdown['total_calls_successful'] / df_department_breakdown['total_calls_made']) * 100)
    df_department_breakdown = df_department_breakdown.withColumn('percentage_successful', F.round(df_department_breakdown['percentage_successful'], 2))
    df_department_breakdown = df_department_breakdown.withColumn('percentage_successful', F.concat(df_department_breakdown['percentage_successful'], F.lit('%')))
    df_department_breakdown = df_department_breakdown.withColumn('total_sales_amount', F.round(df_department_breakdown['total_sales_amount'], 2))
    df_department_breakdown = df_department_breakdown.withColumn('total_calls_successful', F.round(df_department_breakdown['total_calls_successful'], 2))
    df_department_breakdown = df_department_breakdown.withColumn('total_calls_made', F.round(df_department_breakdown['total_calls_made'], 2))

    logger.info('Transformation end - Department Breakdown')
    return df_department_breakdown

def transform_best_performers(df) -> DataFrame:
    """
    Transforms the input DataFrame to identify the best performers based on successful call percentage.

    Args:
        df (DataFrame): The input DataFrame containing employee data.

    Returns:
        DataFrame: The transformed DataFrame containing the top 3 best performers with a successful call percentage
                   greater than 75% for each department.

    Raises:
        None
    """
    logger.info('Bonus Transformation start - best performers')

    # Calculate the percentage of successful calls for each employee
    df = df.withColumn('percentage_successful_calls', F.round(F.col('calls_successful') / F.col('calls_made'), 2))

    # Define a window partitioned by the 'area' (department), and ordered by the percentage of successful calls
    window = Window.partitionBy('area').orderBy(F.desc('percentage_successful_calls'))

    # Rank the employees within each window, and filter to keep only the top 3
    df_best_performers = df.withColumn('rank', F.rank().over(window)).filter(F.col('rank') <= 3)

    # Filter to keep only the employees with a percentage of successful calls greater than 75%
    df_best_performers = df_best_performers.filter(
        F.round(F.col('percentage_successful_calls').cast('float')*100, 2) > 0.75
    )

    logger.info('Bonus Transformation end - best performers')
    return df_best_performers


def rename(df: DataFrame, col_names: Dict) -> DataFrame:
    """ 
    Renames DataFrame columns with provided names

    :param df: DataFrame object representing the input DataFrame.
               It should be of type `pyspark.sql.dataframe.DataFrame`.
    :param col_names: Dictionary containing the current column names as keys
                      and the new column names as values.
                      It should be of type `dict`.
    :returns: DataFrame object with renamed columns.
              It is of type `pyspark.sql.dataframe.DataFrame`.
    :raises: This function does not raise any exceptions.
    """
    for col in col_names.keys():
        df = df.withColumnRenamed(col, col_names[col]) 
    
    return df


def filter(df: DataFrame, col_object: Column, values: List) -> DataFrame:
    """
    Filters a DataFrame based on the values in a specific column.

    Args:
        df (DataFrame): The input DataFrame to filter.
        col_object (Column): The column object representing the column to filter on.
        values (List): The list of values to filter on.

    Returns:
        DataFrame: The filtered DataFrame.

    Example:
        >>> df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['id', 'value'])
        >>> col_object = df['value']
        >>> values = ['a', 'b']
        >>> filtered_df = filter(df, col_object, values)
        >>> filtered_df.show()
        +---+-----+
        | id|value|
        +---+-----+
        |  1|    a|
        |  2|    b|
        +---+-----+
    """
    return df.filter(col_object.isin(values))


def save(df_it_data: DataFrame, df_marketing_data: DataFrame, df_department_breakdown: DataFrame, df_best_performers: DataFrame) -> None:
    """
    Save the DataFrame as a CSV file in the 'client_data' directory.

    Args:
        df_it_data (DataFrame): The DataFrame containing IT data.
        df_marketing_data (DataFrame): The DataFrame containing marketing data.
        df_department_breakdown (DataFrame): The DataFrame containing department breakdown data.
        df_best_performers (DataFrame): The DataFrame containing best performers data.

    Returns:
        None
    """

    logger.info("Load start")

    current_path = os.getcwd()
    dir_dict = {'it_data': 'it_data.csv', 'marketing_address_info': 'marketing_data.csv', 'department_breakdown': 'department_breakdown.csv', 'best_performers': 'best_performers.csv'}

    for new_dir in dir_dict.keys():
        new_path = os.path.join(current_path+'\output', new_dir)

        if not os.path.exists(new_path):
            os.mkdir(new_path)

        if new_dir == 'it_data':
            df_it_data.toPandas().to_csv(os.path.join(new_path, dir_dict[new_dir]), 
                            header='True', index=False)
        elif new_dir == 'marketing_address_info':
            df_marketing_data.toPandas().to_csv(os.path.join(new_path, dir_dict[new_dir]), 
                            header='True', index=False)
        elif new_dir == 'department_breakdown':
            df_department_breakdown.toPandas().to_csv(os.path.join(new_path, dir_dict[new_dir]), 
                            header='True', index=False)
        elif new_dir == 'best_performers':
            df_best_performers.toPandas().to_csv(os.path.join(new_path, dir_dict[new_dir]), 
                            header='True', index=False)
        logger.info('DataFrame written to CSV')
        
        logger.info("Load end")


def main():
    """
    Entry point of the program.

    This function performs the following steps:
    1. Parses command line arguments using the `parser` function.
    2. Creates a Spark session.
    3. Extracts data from the specified sources using the `extract` function.
    4. Transforms the extracted data using the `transform` function.
    5. Displays the transformed data using the `show` method.
    6. Saves the transformed data using the `save` function.

    :return: None
    :rtype: None
    """
    logger.info("Demo start")

    # Parsed arguments
    try:
        args = parser()
    except Exception as e:
        print(e)
        logger.error(e)
        return

    # Create Spark session
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    logger.info("Spark session created")

    # EXTRACT
    df1_emp_calls, df2_emp_personal, df3_emp_sales = extract(spark, args)

    # TRANSFORM
    df_it_data, df_marketing_data, df_department_breakdown, df_best_performers = transform(df1_emp_calls, df2_emp_personal, df3_emp_sales)

    # LOAD
    save(df_it_data, df_marketing_data, df_department_breakdown, df_best_performers)

    logger.info("Demo end")


if __name__ == "__main__":
    main()