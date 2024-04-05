"""Tests for parsing implementations"""

import numpy as np
import pyspark.sql

from wheely.mammoth.parsers import read_encyclopedia_features


def test_read_encyclopedia_features(spark_session, real_encyclopedia_features):
    """Test that we parse EncyclopeDIA files correctly"""
    psms = read_encyclopedia_features(
        real_encyclopedia_features, spark_session
    )

    assert all(
        c in psms.data.columns for c in psms.columns
    ), f"Missing columns in DataFrame!: {[c for c in psms.columns if c not in psms.data.columns]}"

    assert isinstance(psms.data, pyspark.sql.DataFrame)
    assert (
        psms.data.count() >= 1200
    )  # something generic that we should hit in all versions
    assert list(psms.spectrum_columns) == ["id"]
    assert all(col in psms.spectra.columns for col in psms.spectrum_columns)

    assert hasattr(psms, "charges")
    assert psms.charges is not None
    assert hasattr(psms, "charge_column")
    assert psms.charge_column is not None

    assert all(c is not None for c in psms.columns)
    assert set(psms.columns) == {
        psms.target_column,
        *psms.spectrum_columns,
        *psms.score_columns,
        psms.peptide_column,
        *[c for c in [psms.charge_column] if c],
        psms.protein_column,
    }

    # Scores we expect to be present in _all_ flavors we encounter
    # Commented-out scores have been removed in some newer flavors.
    scores = {
        "primary",
        "xCorrLib",
        "xCorrModel",
        # "LogDotProduct",
        "RTinMin",
        # "logWeightedDotProduct",
        "sumOfSquaredErrors",
        # "weightedSumOfSquaredErrors",
        "numberOfMatchingPeaks",
        "numberOfMatchingPeaksAboveThreshold",
        # "averageAbsFragmentDeltaMass",
        "averageFragmentDeltaMasses",
        "isotopeDotProduct",
        # "averageAbsParentDeltaMass",
        "averageParentDeltaMass",
        "charge1",
        "charge2",
        "charge3",
        "charge4",
        # "eValue",
        "deltaRT",
        "numMissedCleavage",
        "pepLength",
        "precursorMass",
        "precursorMz",
    }

    missing_scores = [s for s in scores if s not in psms.score_columns]

    assert (
        len(missing_scores) == 0
    ), f"Failed to find expected columns! {missing_scores}"

    # assert set(psms.score_columns) == scores

    assert psms.scores.toPandas().shape[1] == len(psms.score_columns)

    target_df = psms.data.select(psms.targets).toPandas()

    assert target_df.shape[1] == 1
    assert target_df[target_df.columns[0]].sum() >= 600  # some generic floor
    assert (
        ~target_df[target_df.columns[0]]
    ).sum() >= 600  # some generic floor

    chg_df = psms.data.select(
        psms.charges, *[f"charge{z}" for z in range(1, 5)]
    ).toPandas()
    for z in range(1, 5):
        np.testing.assert_array_equal(
            chg_df["charge"] == z,
            chg_df[f"charge{z}"].astype(bool),
            f"Mismatch z={z}",
        )
