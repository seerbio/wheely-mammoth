"""
Test EncyclopeDIA ELIB/spectrum parsing
"""

import os

import pytest

from wheely.mammoth.parsers import read_encyclopedia_features
from wheely.mammoth.spectra.parsers.encyclopedia import *


@pytest.fixture
def first_psmid(real_encyclopedia_features, spark_session):
    """
    Not very useful, as the string might change and we won't know the answers!
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


@pytest.mark.parametrize(
    "elib_location_fixture",
    [
        "real_encyclopedia_elib",  # relative
        "real_encyclopedia_elib_abs",
        "real_encyclopedia_elib_uri",  # relative
        "real_encyclopedia_elib_abs_uri",
    ]
)
def test_read_elib_pandas(request, elib_location_fixture):
    """
    This test reveals some very frustrating behaviors opening URIs with SQLite.

    On MacOS, using `sqlite3` (v3.42.0) at command line:
    ```
     prefix ->  file:   file:/  file://   file:///
              +-------+-------+---------+---------+
     relative |   ok  |  BAD  |   BAD   |   BAD   |
     absolute |   ok  |  BAD  |   BAD   |   ok    |
              +-------+-------+---------+---------+
    ```

    On MacOS / Python 3.11 (sqlite3 version unknown), running this test:
    ```
     prefix ->  file:   file:/  file://   file:///
              +-------+-------+---------+---------+
     relative |  BAD  |  BAD  |   BAD   |   BAD   |
     absolute |  BAD  |  BAD  |   BAD   |   BAD   |
              +-------+-------+---------+---------+
    ```

    On MacOS / Python 3.10 (sqlite3 version unknown), running this test:
    ```
     prefix ->  file:   file:/  file://   file:///
              +-------+-------+---------+---------+
     relative |  ???  |  ???  |   ???   |   ???   |
     absolute |  ???  |  ???  |   ???   |   ???   |
              +-------+-------+---------+---------+
    ```
    """
    elib_loc = request.getfixturevalue(elib_location_fixture)

    df = read_encyclopedia_elib_pandas(elib_loc)

    assert len(df) > 0

    for col in ["PeptideModSeq", "PrecursorCharge", "MassArray"]:
        assert col in df.columns
