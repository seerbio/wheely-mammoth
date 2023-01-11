"""Tests for parsing implementations"""
import pyspark.sql

from wheely.mammoth.parsers import read_encyclopedia_features


def test_read_encyclopedia_features(spark_session, real_encyclopedia_features):
    """Test that we parse crux files correctly"""
    psms = read_encyclopedia_features(real_encyclopedia_features, spark_session)
    assert isinstance(psms.data, pyspark.sql.DataFrame)
    assert psms.data.count() == 1770
    assert list(psms.spectrum_columns) == ["id"]
    assert all(col in psms.spectra.columns for col in psms.spectrum_columns)

    scores = {
        "primary",
        "xCorrLib",
        "xCorrModel",
        "LogDotProduct",
        "logWeightedDotProduct",
        "sumOfSquaredErrors",
        "weightedSumOfSquaredErrors",
        "numberOfMatchingPeaks",
        "numberOfMatchingPeaksAboveThreshold",
        "averageAbsFragmentDeltaMass",
        "averageFragmentDeltaMasses",
        "isotopeDotProduct",
        "averageAbsParentDeltaMass",
        "averageParentDeltaMass",
        "eValue",
        "deltaRT",
        "numMissedCleavage",
        "pepLength",
    }
    assert set(psms.score_columns) == scores

    assert psms.scores.toPandas().shape == (1770, len(scores))

    target_df = psms.data.select(psms.targets).toPandas()

    assert target_df.shape == (1770, 1)
    assert target_df[target_df.columns[0]].sum() == 901
    assert (~target_df[target_df.columns[0]]).sum() == 1770-901
