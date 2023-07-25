"""Fixtures that are used in multiple tests"""
import logging
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from wheely.mammoth import PsmDataset


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = (
        SparkSession.builder.master("local[2]")
        # .config('spark.jars.packages', 'com.databricks:spark-avro_2.11:3.0.1')
        .appName("pytest-pyspark-local-testing")
        # .enableHiveSupport()
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark


@pytest.fixture
def basic_crux_df():
    """A simple crux-like dataframe"""
    df = pd.DataFrame(
        [
            ["f1", 1, 10, 0.7, "APPLE", "target", 0.7, "p1", "APPLE"],
            ["f1", 2, 20, 0.4, "ANANAB", "decoy", 0.1, "p2", "BANANA"],
            ["f1", 3, 30, 0.1, "CHERRY", "target", 0.2, "p3", "CHERRY"],
            ["f1", 4, 40, 0.55, "DURIAN", "target", 0.8, "p4", "DURIAN"],
            ["f1", 5, 50, 0.25, "EGGPLANT", "target", 0.25, "p5", "EGGPLANT"],
            ["f1", 1, 10, 0.6, "FIG", "target", 0.6, "p6", "FIG"],
            ["f1", 2, 20, 0.2, "GARPE", "decoy", 0.2, "p7", "GRAPE"],
            ["f1", 3, 30, 0.7, "HEONDEYW", "decoy", 0.4, "p8", "HONEYDEW"],
            ["f1", 4, 40, 0.56, "ICE", "target", 0.56, "p9", "ICE"],
            ["f1", 5, 50, 0.3, "JMA", "decoy", 0.3, "p10", "JAM"],
        ],
        columns=[
            "file",
            "scan",
            "spectrum precursor m/z",
            "combined p-value",
            "sequence",
            "target/decoy",
            "x",
            "protein id",
            "original target sequence",
        ],
    )
    return df


@pytest.fixture
def basic_crux_spark_df(spark_session, basic_crux_df):
    """A simple Spark dataframe of PSMs"""
    df = basic_crux_df
    df["target"] = df["target/decoy"].replace({"target": True, "decoy": False})
    df = df.drop(columns="target/decoy")

    return spark_session.createDataFrame(df)


@pytest.fixture
def simple_psms(basic_crux_spark_df):
    return PsmDataset(
        psms=basic_crux_spark_df,
        target_column="target",
        spectrum_columns=["scan", "spectrum precursor m/z"],
        score_columns=["combined p-value", "x"],
        peptide_column="sequence",
        protein_column="protein id",
        protein_delim=",",
    )


@pytest.fixture(
    params=[
        ("encyclopedia_v1",),
        ("encyclopedia_v2",),
    ]
)
def real_encyclopedia_features(request):
    """Return the path of a PSM table from EncyclopeDIA"""

    (folder,) = request.param

    return Path(
        f"data/{folder}/2017dec27_overlap_dia_6b_rep1_604to616.dia.features.txt"
    )


# TODO: Add v2 ELIB
@pytest.fixture(
    params=[
        ("encyclopedia_v1",),
    ]
)
def real_encyclopedia_elib(request):
    """Return the path of an ELIB from EncyclopeDIA"""

    (folder,) = request.param

    return Path(
        f"data/{folder}/2017dec27_overlap_dia_6b_rep1_604to616.dia.elib"
    )
