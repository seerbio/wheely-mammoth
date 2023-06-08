"""
Test EncyclopeDIA ELIB/spectrum parsing
"""

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
