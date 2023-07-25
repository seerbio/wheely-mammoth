"""
Test EncyclopeDIA ELIB/spectrum parsing
"""

import os

import numpy as np
import pandas as pd
import pytest

from pyspark.sql import functions as fns

from wheely.mammoth.parsers import read_encyclopedia_features
from wheely.mammoth.spectra.parsers.encyclopedia import *


@pytest.fixture
def first_psmid(real_encyclopedia_features, spark_session):
    """
    Not very useful, as the string might change and then we won't know the answers!
    """
    psms = read_encyclopedia_features(
        real_encyclopedia_features, spark_session
    )

    return psms.data.select("id").limit(1).toPandas().iloc[0, 0]


@pytest.fixture
def psmid():
    """
    Returns hard-coded value expected from `first_psmid`
    """
    return (
        "2017dec27_overlap_dia_6b_rep1_604to616.dia:1800.4603:DAPVGEEEAPAK+2"
    )


def test_get_peptide_for_psmid(psmid):
    assert get_peptide_for_psmid(psmid) == {
        "sequence": "DAPVGEEEAPAK",
        "charge": 2,
    }


@pytest.fixture(
    params=[
        ("id", lambda f: f),
        ("abs", os.path.abspath),
        ("uri", lambda f: f"file:{f}"),
        ("abs_uri", lambda f: f"file:{os.path.abspath(f)}"),
    ],
)
def loc_transform(request):
    return request.param[1]


@pytest.fixture
def elib_location(real_encyclopedia_elib, loc_transform):
    return loc_transform(real_encyclopedia_elib)


def test_read_elib_pandas(elib_location):
    df = read_encyclopedia_elib_pandas(elib_location)

    assert len(df) > 0

    for col in ["PeptideModSeq", "PrecursorCharge", "MassArray"]:
        assert col in df.columns


def test_read_elib_spark(spark_session, elib_location):
    ds = read_encyclopedia_elib(elib_location, spark=spark_session)

    assert ds.data.count() > 0

    for col in ["PeptideModSeq", "PrecursorCharge", "peaklist"]:
        assert col in ds.data.columns

    for col in ds.columns:
        assert col in map(
            str, ds.data.columns
        ), f"Did not find annotated column {col} in DataFrame! (columns={ds.data.columns})"

    # Spot-check peaklist

    pkl = (
        ds.data.select(
            ds.peaklists, fns.size(ds.peaklists).alias("peaklist_len")
        )
        .limit(1)
        .toPandas()
    )

    assert len(pkl.iloc[0, 0]) == pkl.iloc[0, 1]

    for pk in pkl.iloc[0, 0]:
        assert len(pk) >= 2, f"Not enough values for peak {pk}"


def test_read_elib_entries(
    spark_session,
    real_encyclopedia_features,
):
    psms = read_encyclopedia_features(
        real_encyclopedia_features, spark_session
    )

    # Make an arbitrary subset and cache it
    psms = psms.with_data(psms.data.sample(0.5, seed=0).cache())

    ds = read_encyclopedia_entries(
        psms,
        elib_loc=str(real_encyclopedia_features).replace(
            ".features.txt", ".elib"
        ),
    )

    # Note: not all PSMs have entries
    assert ds.data.count() <= psms.data.count()

    for col in ds.columns:
        assert (
            col in ds.data.columns
        ), f"Did not find annotated column {col} in DataFrame! (columns={ds.data.columns})"

    # Spot-check peaklist

    pkl = (
        ds.data.select(
            ds.peaklists, fns.size(ds.peaklists).alias("peaklist_len")
        )
        .limit(1)
        .toPandas()
    )

    assert len(pkl.iloc[0, 0]) == pkl.iloc[0, 1]

    for pk in pkl.iloc[0, 0]:
        # We expect 3 values per peak (mz, rt, corr)
        assert len(pk) == 3, f"Wrong number of values for peak {pk}"


def test_compute_elib_loc(spark_session):
    test_data = spark_session.createDataFrame(
        [
            ("/path/to/file.dia.features.txt",),
            ("/path/to/another.dia.features.txt",),
        ],
        schema="file_loc string",
    )

    result = test_data.select(
        compute_elib_loc(
            "file_loc",
        )
    ).toPandas()

    np.testing.assert_array_equal(
        result.iloc[:, 0].values,
        [
            "/path/to/file.dia.elib",
            "/path/to/another.dia.elib",
        ],
    )


def test_compute_elib_loc_real(
    spark_session,
    real_encyclopedia_features,
):
    """
    Test that `compute_elib_loc()` works as expected on default EncyclopeDIA feature data with
    default arguments.
    """
    ds = read_encyclopedia_features(real_encyclopedia_features)

    result = (
        ds.data.limit(16)
        .select("filename")
        .withColumn("elib_loc", compute_elib_loc())
        .toPandas()
    )

    # with pd.option_context("display.max_colwidth", None):
    #     print(result)

    assert all(
        os.path.samefile(
            v,
            str(real_encyclopedia_features).replace(".features.txt", ".elib"),
        )
        for v in result["elib_loc"].values
    )
