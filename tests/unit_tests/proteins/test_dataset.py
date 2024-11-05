"""
These are unit tests for the Protein Dataset Class:
"""

import pandas as pd
import pyspark.sql.functions
import pytest

from wheely.mammoth import IntensityDataset
from wheely.mammoth.proteins import *


@pytest.fixture(
    params=[
        ProteinDataset,
        # Test that "quoted" column names work
        lambda *args, **kwargs: ProteinDataset(
            *args,
            **{
                k: (
                    [f"`{c}`" for c in v]
                    if "columns" in k
                    else f"`{v}`" if "column" in k else v
                )
                for k, v in kwargs.items()
            },
        ),
        lambda *args, **kwargs: ProteinConfidenceDataset(
            *args,
            **kwargs,
            qvalue_column="q-value",  # good enough for this test
        ),
        lambda *args, **kwargs: ProteinConfidenceDataset(
            *args,
            **kwargs,
            qvalue_column="q-value",  # good enough for this test
            pi0=0.95,
        ),
        lambda *args, **kwargs: ProteinConfidenceDataset(
            *args,
            **kwargs,
            qvalue_column="q-value",  # good enough for this test
            errprob_column="errprob",  # good enough for this test
            pi0=0.95,
        ),
        lambda *args, **kwargs: ProteinIntensityDataset(
            *args,
            **kwargs,
            sample_column="filename",  # good enough for this test
            intensity_column="intensity",  # good enough for this test
        ),
        lambda *args, **kwargs: ProteinIntensityConfidenceDataset(
            *args,
            **kwargs,
            sample_column="filename",  # good enough for this test
            intensity_column="intensity",  # good enough for this test
            qvalue_column="q-value",  # good enough for this test
            errprob_column="errprob",  # good enough for this test
            pi0=0.95,
        ),
    ]
)
def dataset_type(request):
    return request.param


@pytest.fixture
def basic_protein_df(basic_crux_spark_df):
    return basic_crux_spark_df


def test_properties(basic_protein_df, dataset_type):
    """Check the public properties of the ProteinDataset object."""
    dset = dataset_type(
        data=basic_protein_df,
        protein_column="protein id",
        target_column="target",
        score_columns=["combined p-value", "x"],
        protein_delim=",",
    )

    assert dset.protein_delim == ","

    pd.testing.assert_frame_equal(
        dset.scores.toPandas(),
        basic_protein_df.toPandas().loc[:, ["combined p-value", "x"]],
    )
    pd.testing.assert_frame_equal(
        dset.data.select(dset.targets).toPandas(),
        basic_protein_df.toPandas().loc[:, ["target"]],
    )

    assert all(c is not None for c in dset.columns)
    assert set(dset.columns) == {
        dset.protein_column,
        dset.target_column,
        *dset.score_columns,
        *(
            c
            for a in ["qvalue_column", "errprob_column"]
            if (c := getattr(dset, a, None)) is not None
        ),
        *(
            getattr(dset, a)
            for a in ["sample_column", "intensity_column"]
            if isinstance(dset, IntensityDataset)
        ),
    }


def test_mutate(basic_protein_df, dataset_type):
    """Check mutating a ProteinDataset object."""
    dset = dataset_type(
        data=basic_protein_df,
        protein_column="protein id",
        target_column="target",
        score_columns=["combined p-value", "x"],
        protein_delim=",",
    )

    n_rows = 5
    n_targets = 4

    mut = dset.with_data(
        dset.data.limit(n_rows).withColumn(
            "isDecoy", pyspark.sql.functions.col("target").astype("int") == 0
        ),
        target_column="isDecoy",
    )

    assert isinstance(mut, type(dset))

    assert mut.data.count() == n_rows
    assert (
        mut.data.select(
            pyspark.sql.functions.sum(mut.targets.astype("int"))
        ).collect()[0][0]
        == n_rows - n_targets
    )

    assert mut.protein_delim == ","

    if isinstance(dset, ProteinConfidenceDataset):
        assert mut.qvalue_column == dset.qvalue_column
        assert mut.errprob_column == dset.errprob_column
        assert mut.pi0 == dset.pi0

    if isinstance(dset, IntensityDataset):
        assert mut.sample_column == dset.sample_column
        assert mut.intensity_column == dset.intensity_column
