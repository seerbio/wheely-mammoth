"""
These are unit tests for precursor/spectrum dataset classes.
"""

import pandas as pd
import pyspark.sql.functions as fns
import pytest

from wheely.mammoth.spectra import *
from wheely.mammoth.semantics import CHARGE, BasicSemantic


@pytest.fixture(
    params=[
        PrecursorDatasetBase,
        # Test that "quoted" column names work
        lambda *args, **kwargs: PrecursorDatasetBase(
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
        # Semantics variant
        lambda *args, **kwargs: PrecursorDatasetBase(
            *args,
            **kwargs,
            semantics={"test_col": BasicSemantic("Test semantic")},
        ),
    ]
)
def prec_dataset_type(request):
    return request.param


@pytest.fixture(
    params=[
        SpectraDatasetBase,
        lambda *args, **kwargs: SpectraDatasetBase(
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
        # Semantics variant
        lambda *args, **kwargs: SpectraDatasetBase(
            *args,
            **kwargs,
            semantics={"test_col": BasicSemantic("Test semantic")},
        ),
    ]
)
def spec_dataset_type(request):
    return request.param


def test_prec_dataset_attrs(basic_crux_spark_df, prec_dataset_type):
    """Check the public properties of the dataset object."""
    psms = prec_dataset_type(
        psms=basic_crux_spark_df.withColumns(
            {
                "charge": fns.lit(2).astype("integer"),
            }
        ),
        spectrum_columns=["file", "scan"],
        charge_column="charge",
        mz_column="spectrum precursor m/z",
        rt_column="scan",
    )

    assert isinstance(psms, PrecursorDataset)

    assert len(psms.spectra.columns) == len(psms.spectrum_columns)
    assert len(psms.data.select(psms.charges).columns) == 1
    assert len(psms.data.select(psms.mzs).columns) == 1
    assert len(psms.data.select(psms.rts).columns) == 1

    # Check that semantics are properly initialized
    assert hasattr(
        psms, "semantics"
    ), "Dataset should have 'semantics' attribute"
    assert isinstance(psms.semantics, dict), "semantics should be a dict"

    # For datasets with custom semantics in fixture:
    if "test_col" in psms.semantics:
        assert psms.semantics["test_col"] is not None
        # Test get_semantics method
        assert psms.get_semantics("test_col") == psms.semantics["test_col"]

    # For PrecursorDatasetBase: verify forced charge semantics
    if psms.charge_column is not None:
        assert psms.charge_column in psms.semantics
        assert psms.semantics[psms.charge_column] == CHARGE
        # Test get_by_semantics method
        assert psms.get_by_semantics(CHARGE) == psms.charge_column

    assert all(c is not None for c in psms.columns)
    assert set(psms.columns) == {
        *psms.spectrum_columns,
        psms.charge_column,
        psms.mz_column,
        psms.rt_column,
    }


def test_spec_dataset_attrs(basic_crux_spark_df, spec_dataset_type):
    """Check the public properties of the dataset object."""
    psms = spec_dataset_type(
        psms=basic_crux_spark_df.withColumns(
            {
                "charge": fns.lit(2).astype("integer"),
                "peaklist": fns.array().astype(PeaklistType),
            }
        ),
        spectrum_columns=["file", "scan"],
        charge_column="charge",
        mz_column="spectrum precursor m/z",
        rt_column="scan",
        peaklist_column="peaklist",
    )

    assert isinstance(psms, PrecursorDataset)
    assert isinstance(psms, SpectraDataset)

    assert len(psms.spectra.columns) == len(psms.spectrum_columns)
    assert len(psms.data.select(psms.charges).columns) == 1
    assert len(psms.data.select(psms.mzs).columns) == 1
    assert len(psms.data.select(psms.rts).columns) == 1
    assert len(psms.data.select(psms.peaklists).columns) == 1

    # Check that semantics are properly initialized
    assert hasattr(
        psms, "semantics"
    ), "Dataset should have 'semantics' attribute"
    assert isinstance(psms.semantics, dict), "semantics should be a dict"

    # For datasets with custom semantics in fixture:
    if "test_col" in psms.semantics:
        assert psms.semantics["test_col"] is not None
        # Test get_semantics method
        assert psms.get_semantics("test_col") == psms.semantics["test_col"]

    # For SpectraDatasetBase: verify forced charge semantics
    if psms.charge_column is not None:
        assert psms.charge_column in psms.semantics
        assert psms.semantics[psms.charge_column] == CHARGE
        assert psms.get_by_semantics(CHARGE) == psms.charge_column

    assert all(c is not None for c in psms.columns)
    assert set(psms.columns) == {
        *psms.spectrum_columns,
        psms.charge_column,
        psms.mz_column,
        psms.rt_column,
        psms.peaklist_column,
    }


@pytest.fixture(
    params=[
        IonMobilityDatasetBase,
        lambda *args, **kwargs: IonMobilityDatasetBase(
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
        # Semantics variant
        lambda *args, **kwargs: IonMobilityDatasetBase(
            *args,
            **kwargs,
            semantics={"test_col": BasicSemantic("Test semantic")},
        ),
    ]
)
def ion_mobility_dataset_type(request):
    return request.param


@pytest.fixture(
    params=[
        IonMobilitySpectraDatasetBase,
        lambda *args, **kwargs: IonMobilitySpectraDatasetBase(
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
        # Semantics variant
        lambda *args, **kwargs: IonMobilitySpectraDatasetBase(
            *args,
            **kwargs,
            semantics={"test_col": BasicSemantic("Test semantic")},
        ),
    ]
)
def ion_mobility_spec_dataset_type(request):
    return request.param


def test_ion_mobility_dataset_attrs(
    basic_crux_spark_df, ion_mobility_dataset_type
):
    """Check the public properties of the ion mobility dataset object."""
    psms = ion_mobility_dataset_type(
        psms=basic_crux_spark_df.withColumns(
            {
                "charge": fns.lit(2).astype("integer"),
                "ion mobility": fns.lit(1.0).astype("double"),
            }
        ),
        spectrum_columns=["file", "scan"],
        charge_column="charge",
        mz_column="spectrum precursor m/z",
        rt_column="scan",
        ion_mobility_column="ion mobility",
    )

    assert isinstance(psms, PrecursorDataset)
    assert isinstance(psms, IonMobilityDataset)

    assert len(psms.data.select(psms.ion_mobilities).columns) == 1
    assert psms.ion_mobility_column in psms.columns

    assert all(c is not None for c in psms.columns)
    assert set(psms.columns) == {
        *psms.spectrum_columns,
        psms.charge_column,
        psms.mz_column,
        psms.rt_column,
        psms.ion_mobility_column,
    }

    mut = psms.with_data(psms.data)
    assert isinstance(mut, type(psms))
    assert mut.ion_mobility_column == psms.ion_mobility_column


def test_ion_mobility_spec_dataset_attrs(
    basic_crux_spark_df, ion_mobility_spec_dataset_type
):
    """Check the public properties of the ion mobility spectra dataset."""
    psms = ion_mobility_spec_dataset_type(
        psms=basic_crux_spark_df.withColumns(
            {
                "charge": fns.lit(2).astype("integer"),
                "peaklist": fns.array().astype(PeaklistType),
                "ion mobility": fns.lit(1.0).astype("double"),
            }
        ),
        spectrum_columns=["file", "scan"],
        charge_column="charge",
        mz_column="spectrum precursor m/z",
        rt_column="scan",
        peaklist_column="peaklist",
        ion_mobility_column="ion mobility",
    )

    assert isinstance(psms, PrecursorDataset)
    assert isinstance(psms, SpectraDataset)
    assert isinstance(psms, IonMobilityDataset)
    assert isinstance(psms, IonMobilitySpectraDataset)

    assert len(psms.data.select(psms.ion_mobilities).columns) == 1
    assert psms.ion_mobility_column in psms.columns

    assert all(c is not None for c in psms.columns)
    assert set(psms.columns) == {
        *psms.spectrum_columns,
        psms.charge_column,
        psms.mz_column,
        psms.rt_column,
        psms.peaklist_column,
        psms.ion_mobility_column,
    }

    mut = psms.with_data(psms.data)
    assert isinstance(mut, type(psms))
    assert mut.ion_mobility_column == psms.ion_mobility_column
