"""
Test EncyclopeDIA ELIB/spectrum parsing
"""

import os

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


@pytest.fixture
def real_encyclopedia_elib_abs(real_encyclopedia_elib):
    return os.path.abspath(real_encyclopedia_elib)


@pytest.fixture
def real_encyclopedia_elib_uri(real_encyclopedia_elib):
    return f"file:{real_encyclopedia_elib}"


@pytest.fixture
def real_encyclopedia_elib_abs_uri(real_encyclopedia_elib_abs):
    return f"file:{real_encyclopedia_elib_abs}"


@pytest.fixture(
    params=[
        "real_encyclopedia_elib",  # relative
        "real_encyclopedia_elib_abs",
        "real_encyclopedia_elib_uri",  # relative
        "real_encyclopedia_elib_abs_uri",
    ]
)
def elib_location(request):
    return request.getfixturevalue(request.param)


def test_read_elib_pandas(elib_location):
    df = read_encyclopedia_elib_pandas(elib_location)

    assert len(df) > 0

    for col in ["PeptideModSeq", "PrecursorCharge", "MassArray"]:
        assert col in df.columns


def test_read_elib_spark(spark_session, elib_location):
    ds = read_encyclopedia_elib(elib_location, spark=spark_session)

    assert ds.data.count() > 0

    for col in ["PeptideModSeq", "PrecursorCharge", "MassArray"]:
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
