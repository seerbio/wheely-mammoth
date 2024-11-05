"""
These are unit tests for the PSM Dataset Class:
"""

import pandas as pd
import pyspark.sql.functions
import pytest

import wheely.mammoth.dataset
from wheely.mammoth import *


@pytest.fixture(
    params=[
        PsmDataset,
        lambda *args, **kwargs: ConfidenceDataset(
            *args,
            **kwargs,
            qvalue_column="q-value",  # good enough for this test
        ),
        lambda *args, **kwargs: ConfidenceDataset(
            *args,
            **kwargs,
            qvalue_column="q-value",  # good enough for this test
            pi0=0.95,
        ),
        lambda *args, **kwargs: ConfidenceDataset(
            *args,
            **kwargs,
            qvalue_column="q-value",  # good enough for this test
            errprob_column="errprob",  # good enough for this test
            pi0=0.95,
        ),
        # Test that "quoted" column names work
        lambda *args, **kwargs: PsmDataset(
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
        lambda *args, **kwargs: PsmIntensityDataset(
            *args,
            **kwargs,
            sample_column="filename",  # good enough for this test
            intensity_column="intensity",  # good enough for this test
        ),
        lambda *args, **kwargs: PsmIntensityConfidenceDataset(
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


def test_properties(basic_crux_spark_df, dataset_type):
    """Check the public properties of the PsmDataset object."""
    psms = dataset_type(
        psms=basic_crux_spark_df,
        target_column="target",
        spectrum_columns=["file", "scan"],
        score_columns=["combined p-value", "x"],
        peptide_column="sequence",
        protein_column="protein id",
        protein_delim=",",
        # **kws,
    )

    # assert list(psms.spectrum_columns) == ["file", "scan"]
    # assert list(psms.spectra.columns) == ["file", "scan"]
    # assert list(psms.score_columns) == ["combined p-value", "x"]
    # assert list(psms.scores.columns) == ["combined p-value", "x"]
    # assert psms.peptide_column == "sequence"
    # assert psms.protein_column == "protein id"
    assert psms.protein_delim == ","

    # assert list(psms.spectra.columns) == list(psms.spectrum_columns)
    # assert list(psms.scores.columns) == list(psms.score_columns)

    pd.testing.assert_frame_equal(
        psms.scores.toPandas(),
        basic_crux_spark_df.toPandas().loc[:, ["combined p-value", "x"]],
    )
    pd.testing.assert_frame_equal(
        psms.data.select(psms.targets).toPandas(),
        basic_crux_spark_df.toPandas().loc[:, ["target"]],
    )

    # Check that optional columns' properties are correctly handled
    for ca, a in {
        "charge_column": "charges",
        "qvalue_column": "qvalues",
        "errprob_column": "errprobs",
        "intensity_column": "intensities",
    }.items():
        if not hasattr(psms, ca):
            continue

        try:
            assert hasattr(psms, a), f"Had {ca} but no {a}!"

            assert (getattr(psms, ca) is None) == (getattr(psms, a) is None)
        except Exception as e:
            raise AssertionError(f"Error testing {ca}/{a}") from e

    assert all(c is not None for c in psms.columns)
    assert set(psms.columns) == {
        psms.target_column,
        *psms.spectrum_columns,
        *psms.score_columns,
        psms.peptide_column,
        *[c for c in [psms.charge_column] if c],
        psms.protein_column,
        *[
            c
            for a in ["qvalue_column", "errprob_column"]
            if isinstance(psms, ConfidenceDataset)
            and (c := getattr(psms, a)) is not None
        ],
        *[
            getattr(psms, a)
            for a in ["sample_column", "intensity_column"]
            if isinstance(psms, IntensityDataset)
        ],
    }


def test_optional_cols(basic_crux_spark_df, dataset_type):
    psms = dataset_type(
        psms=basic_crux_spark_df,
        target_column="target",
        spectrum_columns=["file", "scan"],
        score_columns=["combined p-value", "x"],
        peptide_column="sequence",
        # Note: NOT specifying protein columns!
    )

    assert set(psms.columns) == {
        psms.target_column,
        *psms.spectrum_columns,
        *psms.score_columns,
        psms.peptide_column,
        *[c for c in [psms.charge_column] if c],
        *[
            c
            for a in ["qvalue_column", "errprob_column"]
            if isinstance(psms, ConfidenceDataset)
            and (c := getattr(psms, a)) is not None
        ],
        *[
            getattr(psms, a)
            for a in ["sample_column", "intensity_column"]
            if isinstance(psms, IntensityDataset)
        ],
    }
    assert all(c is not None for c in psms.columns)


def test_mutate(basic_crux_spark_df, dataset_type):
    """Check mutating a PsmDataset object."""
    psms = dataset_type(
        psms=basic_crux_spark_df,
        target_column="target",
        spectrum_columns=["file", "scan"],
        score_columns=["combined p-value", "x"],
        peptide_column="sequence",
        protein_column="protein id",
        protein_delim=",",
    )

    n_rows = 5
    n_targets = 4

    mut = psms.with_data(
        psms.data.limit(n_rows).withColumn(
            "isDecoy", pyspark.sql.functions.col("target").astype("int") == 0
        ),
        target_column="isDecoy",
    )

    assert isinstance(mut, type(psms))

    assert mut.data.count() == n_rows
    # assert mut.target_column == "isDecoy"
    assert (
        mut.data.select(
            pyspark.sql.functions.sum(mut.targets.astype("int"))
        ).collect()[0][0]
        == n_rows - n_targets
    )

    # assert list(mut.spectra.columns) == ["file", "scan"]
    # assert list(mut.scores.columns) == ["combined p-value", "x"]
    # assert mut.peptide_column == "sequence"
    # assert mut.protein_column == "protein id"
    assert mut.protein_delim == ","

    if isinstance(psms, ConfidenceDataset):
        assert mut.qvalue_column == psms.qvalue_column
        assert mut.errprob_column == psms.errprob_column
        assert mut.pi0 == psms.pi0

    if isinstance(psms, IntensityDataset):
        assert mut.sample_column == psms.sample_column
        assert mut.intensity_column == psms.intensity_column
