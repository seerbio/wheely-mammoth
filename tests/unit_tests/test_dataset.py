"""
These are unit tests for the PSM Dataset Class:
"""
import pandas as pd
import pyspark.sql.functions
import pytest

from wheely.mammoth import PsmDataset, ConfidenceDataset


@pytest.fixture(
    params=[
        PsmDataset,
        lambda *args, **kwargs: ConfidenceDataset(
            *args,
            **kwargs,
            qvalue_column="combined p-value",  # good enough for this test
        ),
        lambda *args, **kwargs: ConfidenceDataset(
            *args,
            **kwargs,
            qvalue_column="combined p-value",  # good enough for this test
            pi0=0.95,
        ),
        # Test that "quoted" column names work
        lambda *args, **kwargs: ConfidenceDataset(
            *args,
            **{
                k: [f"`{c}`" for c in v]
                if "columns" in k
                else f"`{v}`"
                if "column" in k
                else v
                for k, v in kwargs.items()
            },
            qvalue_column="combined p-value",  # good enough for this test
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
    pd.testing.assert_frame_equal(
        psms.scores.toPandas(),
        basic_crux_spark_df.toPandas().loc[:, ["combined p-value", "x"]],
    )
    pd.testing.assert_frame_equal(
        psms.data.select(psms.targets).toPandas(),
        basic_crux_spark_df.toPandas().loc[:, ["target"]],
    )

    assert set(psms.columns) == {
        psms.target_column,
        *psms.spectrum_columns,
        *psms.score_columns,
        psms.peptide_column,
        psms.protein_column,
    }
    assert all(c is not None for c in psms.columns)


def test_optional_cols(basic_crux_spark_df, dataset_type):
    typ, kws = dataset_type

    psms = typ(
        psms=basic_crux_spark_df,
        target_column="target",
        spectrum_columns=["file", "scan"],
        score_columns=["combined p-value", "x"],
        peptide_column="sequence",
        # Note: NOT specifying protein columns!
        **kws,
    )

    assert set(psms.columns) == {
        psms.target_column,
        *psms.spectrum_columns,
        *psms.score_columns,
        psms.peptide_column,
    }
    assert all(c is not None for c in psms.columns)


def test_mutate(basic_crux_spark_df, dataset_type):
    """Check mutating a PsmDataset object."""
    psms = dataset_type[0](
        psms=basic_crux_spark_df,
        target_column="target",
        spectrum_columns=["file", "scan"],
        score_columns=["combined p-value", "x"],
        peptide_column="sequence",
        protein_column="protein id",
        protein_delim=",",
        **dataset_type[1],
    )

    n_rows = 5
    n_targets = 4

    mut = psms.with_data(
        psms.data.limit(n_rows).withColumn(
            "isDecoy", pyspark.sql.functions.col("target").astype("int") == 0
        ),
        target_column="isDecoy",
    )

    assert isinstance(mut, dataset_type[0])

    assert mut.data.count() == n_rows
    assert mut.target_column == "isDecoy"
    assert (
        mut.data.select(
            pyspark.sql.functions.sum(mut.targets.astype("int"))
        ).collect()[0][0]
        == n_rows - n_targets
    )

    assert list(mut.spectra.columns) == ["file", "scan"]
    assert list(mut.scores.columns) == ["combined p-value", "x"]
    assert mut.peptide_column == "sequence"
    assert mut.protein_column == "protein id"
    assert mut.protein_delim == ","

    if isinstance(mut, ConfidenceDataset):
        assert mut.qvalue_column == psms.qvalue_column
        assert mut.pi0 == psms.pi0
