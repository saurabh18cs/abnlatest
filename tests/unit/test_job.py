import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from abnlatest.job import extract
from unittest.mock import patch
from abnlatest.job import transform_it_data


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.getOrCreate()


class MockArgs:
    def __init__(self, fpath_1, fpath_2, fpath_3):
        self.fpath_1 = fpath_1
        self.fpath_2 = fpath_2
        self.fpath_3 = fpath_3


@pytest.fixture(scope="module")
def test_emp_calls_df(spark):
    test_data = [
        (1, "area1", 54, 54),
        (2, "area2", 64, 54),
        (3, "area3", 74, 65),
        (4, "area4", 84, 65),
    ]
    return spark.createDataFrame(
        test_data, ["id", "area", "calls_made", "calls_successful"]
    )


@pytest.fixture(scope="module")
def test_emp_personal_df(spark):
    test_data = [
        (1, "name1", "address1", 1234),
        (2, "name2", "address2", 12345),
        (3, "name3", "address3", 123456),
        (4, "name4", "address4", 1234567),
    ]
    return spark.createDataFrame(test_data, ["id", "name", "address", "sales_amount"])


@pytest.fixture(scope="module")
def test_emp_sales_df(spark):
    test_data = [
        (1, 5, "company1", "recipient1", 30, "country1", "product1", 1),
        (2, 6, "company2", "recipient2", 35, "country2", "product2", 2),
        (3, 7, "company3", "recipient3", 40, "country3", "product3", 3),
        (4, 8, "company4", "recipient4", 45, "country4", "product4", 4),
    ]
    return spark.createDataFrame(
        test_data,
        [
            "id",
            "caller_id",
            "company",
            "recipient",
            "age",
            "country",
            "product_sold",
            "quantity",
        ],
    )


@patch.object(SparkSession, "read")
def test_extract(
    mock_read, spark, test_emp_sales_df, test_emp_personal_df, test_emp_calls_df
):
    # Mock the read.csv calls to return the mock DataFrames
    mock_read.csv.side_effect = [
        test_emp_calls_df,
        test_emp_personal_df,
        test_emp_sales_df,
    ]

    # Create a mock args object
    args = MockArgs("path/to/file1.csv", "path/to/file2.csv", "path/to/file3.csv")

    # Call the extract method
    extracted_df1, extracted_df2, extracted_df3 = extract(spark, args)

    expected_data_emp_calls = spark.createDataFrame(
        [
            (1, "area1", 54, 54),
            (2, "area2", 64, 54),
            (3, "area3", 74, 65),
            (4, "area4", 84, 65),
        ],
        ["id", "area", "calls_made", "calls_successful"],
    )
    expected_data_emp_personal = spark.createDataFrame(
        [
            (1, "name1", "address1", 1234),
            (2, "name2", "address2", 12345),
            (3, "name3", "address3", 123456),
            (4, "name4", "address4", 1234567),
        ],
        ["id", "name", "address", "sales_amount"],
    )
    expected_data_emp_sales = spark.createDataFrame(
        [
            (1, 5, "company1", "recipient1", 30, "country1", "product1", 1),
            (2, 6, "company2", "recipient2", 35, "country2", "product2", 2),
            (3, 7, "company3", "recipient3", 40, "country3", "product3", 3),
            (4, 8, "company4", "recipient4", 45, "country4", "product4", 4),
        ],
        [
            "id",
            "caller_id",
            "company",
            "recipient",
            "age",
            "country",
            "product_sold",
            "quantity",
        ],
    )
    # Assert the extracted DataFrames are equal to the mock DataFrames
    assert_df_equality(expected_data_emp_calls, extracted_df1)
    assert_df_equality(extracted_df2, expected_data_emp_personal)
    assert_df_equality(extracted_df3, expected_data_emp_sales)


def test_transform_it_data(spark, test_emp_calls_df, test_emp_personal_df):
    expected_data = [(4, "area4", 84, 65, "name4", "address4", 1234567)]
    expected_df = spark.createDataFrame(
        expected_data,
        [
            "id",
            "area",
            "calls_made",
            "calls_successful",
            "name",
            "address",
            "sales_amount",
        ],
    )
    df = test_emp_calls_df.join(test_emp_personal_df, on="id", how="inner")
    transformed_df = transform_it_data(df, "area4")
    assert_df_equality(transformed_df, expected_df)
