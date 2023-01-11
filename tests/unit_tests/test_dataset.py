"""
These are unit tests for the PSM Dataset Class:
"""
import pandas as pd

from wheely.mammoth import PsmDataset


def test_create_object(basic_crux_spark_df):
    """Ensures that a PsmDataset object can be initialized properly."""
    psms = PsmDataset(
        psms=basic_crux_spark_df,
        target_column="target",
        spectrum_columns=["file", "scan"],
        score_columns=["combined p-value", "x"],
        peptide_column="sequence",
        protein_column="protein id",
        protein_delim=",",
    )
    assert isinstance(psms, PsmDataset)


def test_properties(basic_crux_spark_df):
    """Check the public properties of the PsmDataset object."""
    psms = PsmDataset(
        psms=basic_crux_spark_df,
        target_column="target",
        spectrum_columns=["file", "scan"],
        score_columns=["combined p-value", "x"],
        peptide_column="sequence",
        protein_column="protein id",
        protein_delim=",",
    )

    assert list(psms.spectra.columns) == ["file", "scan"]
    assert psms.score_columns == ["combined p-value", "x"]
    assert psms.peptide_column == "sequence"
    assert psms.protein_column == "protein id"
    assert psms.protein_delim == ","
    pd.testing.assert_frame_equal(
        psms.scores.toPandas(),
        basic_crux_spark_df.toPandas().loc[:, ["combined p-value", "x"]],
    )
    pd.testing.assert_frame_equal(
        psms.data.select(psms.targets).toPandas(),
        basic_crux_spark_df.toPandas().loc[:, ["target"]],
    )
