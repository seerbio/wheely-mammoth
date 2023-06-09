import pandas as pd
import numpy as np
import pytest
from pyspark.sql.types import ArrayType, StructType, StructField, DoubleType
from pyspark.sql.functions import col

from wheely.mammoth.spectra import PeaklistType
from wheely.mammoth.spectra.utils import *

# Test data
mz_values = [100.0, 101.0, 102.0]
intensity_values = [0.5, 0.8, 0.6]
mz_series = pd.Series(mz_values)
intensity_series = pd.Series(intensity_values)


def test_lists_to_peaklist(spark_session):
    schema = StructType(
        [
            StructField("mz", ArrayType(DoubleType()), nullable=False),
            StructField("intensity", ArrayType(DoubleType()), nullable=False),
        ]
    )

    df = spark_session.createDataFrame(
        pd.DataFrame(
            [
                [mz_values, intensity_values],
            ]
        ),
        schema=schema,
    )

    result = df.select(lists_to_peaklist(col("mz"), col("intensity")))

    expected_result = list(zip(mz_values, intensity_values))
    np.testing.assert_array_equal(
        result.toPandas().iloc[0, 0], expected_result
    )

    # Test that parsing the resulting column with other utils works

    to_lists = result.select(
        # Must spread this tuple in a select()
        *peaklist_to_lists(result.columns[0])
    ).toPandas()

    print(to_lists)

    assert len(to_lists.columns) == 2
    assert len(to_lists) == mock_peaklist.count()
    assert to_lists.iloc[0, 0] == mz_values
    assert to_lists.iloc[0, 1] == intensity_values

    to_pairs = result.select(
        peaklist_to_pairs(mock_peaklist.columns[0])
    ).toPandas()

    print(to_pairs)

    def _item(i):
        return lambda l: l[i]

    assert len(to_pairs.columns) == 1
    assert len(to_pairs) == len(mz_values)
    np.testing.assert_array_equal(
        to_pairs.iloc[:, 0].apply(_item(0)), mz_values
    )
    np.testing.assert_array_equal(
        to_pairs.iloc[:, 0].apply(_item(1)), intensity_values
    )


def test_rows_to_peaklist(spark_session):
    schema = StructType(
        [
            StructField("mz", DoubleType(), nullable=False),
            StructField("intensity", DoubleType(), nullable=False),
        ]
    )

    df = spark_session.createDataFrame(
        pd.DataFrame(list(zip(mz_values, intensity_values))), schema=schema
    )

    # Apply the Pandas UDF
    udf_result = df.groupby().agg(rows_to_peaklist("mz", "intensity"))

    # Convert the result back to a regular Pandas DataFrame for easier comparison
    result_df = udf_result.toPandas()

    expected_result = [(100, 0.5), (101, 0.8), (102, 0.6)]
    np.testing.assert_array_equal(result_df.iloc[0, 0], expected_result)


def test_pairs_to_peaklist(spark_session):
    schema = StructType(
        [
            StructField("pair", ArrayType(DoubleType()), nullable=False),
        ]
    )

    data = pd.DataFrame(
        [{"pair": [mz, i]} for mz, i in zip(mz_values, intensity_values)]
    )

    print(data)

    df = spark_session.createDataFrame(data, schema=schema)

    # Apply the Pandas UDF
    udf_result = df.groupby().agg(pairs_to_peaklist("pair"))

    # Convert the result back to a regular Pandas DataFrame for easier comparison
    result_df = udf_result.toPandas()

    print(result_df)

    expected_result = [(100, 0.5), (101, 0.8), (102, 0.6)]
    np.testing.assert_array_equal(result_df.iloc[0, 0], expected_result)


@pytest.fixture
def mock_peaklist(spark_session):
    """
    A mock dataframe of a single row, with a single column containing a peaklist.
    """
    schema = StructType([StructField("peaklist", PeaklistType)])

    return spark_session.createDataFrame(
        pd.DataFrame(
            [
                [list(zip(mz_values, intensity_values))],
            ]
        ),
        schema=schema,
    )


def test_peaklist_to_lists(mock_peaklist):
    result = mock_peaklist.select(
        # Must spread this tuple in a select()
        *peaklist_to_lists(mock_peaklist.columns[0])
    ).toPandas()

    print(result)

    assert len(result.columns) == 2
    assert len(result) == mock_peaklist.count()
    assert result.iloc[0, 0] == mz_values
    assert result.iloc[0, 1] == intensity_values


def test_peaklist_to_pairs(mock_peaklist):
    result = mock_peaklist.select(
        peaklist_to_pairs(mock_peaklist.columns[0])
    ).toPandas()

    print(result)

    def _item(i):
        return lambda l: l[i]

    assert len(result.columns) == 1
    assert len(result) == len(mz_values)
    np.testing.assert_array_equal(result.iloc[:, 0].apply(_item(0)), mz_values)
    np.testing.assert_array_equal(
        result.iloc[:, 0].apply(_item(1)), intensity_values
    )
