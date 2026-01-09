"""
Objects that define column semantics.
"""

from .semanticinfo import (
    SemanticInfo,
    BasicSemantic,
    CVSemantic,
    CVUnit,
    UnknownSemantic,
)


##### PRECURSOR PROPERTIES #####

CHARGE: SemanticInfo = CVSemantic(
    name="charge state",
    accession="MS:1000041",
)

THEORETICAL_MONO_MASS: SemanticInfo = CVSemantic(
    name="theoretical neutral monoisotopic mass",
    accession="MS:1003637",
    unit=CVUnit(name="Dalton", accession="UO:0000221"),
)

# TODO: add CV term if/when available
THEORETICAL_PRECURSOR_MZ: SemanticInfo = BasicSemantic(
    "theoretical precursor m/z"
)


##### RETENTION TIME #####

RT_IN_SECONDS: SemanticInfo = CVSemantic(
    name="scan start time",
    accession="MS:1000016",
    unit=CVUnit(name="second", accession="UO:0000010"),
)
NORMALIZED_RT_IN_SECONDS: SemanticInfo = CVSemantic(
    name="normalized retention time",
    accession="MS:1000896",
    unit=CVUnit(name="second", accession="UO:0000010"),
)

# TODO: add CV terms if/when available
RT_START_IN_SECONDS: SemanticInfo = BasicSemantic(
    "retention time window start"
)
RT_STOP_IN_SECONDS: SemanticInfo = BasicSemantic("retention time window stop")


##### SCAN NUMBER #####

SCAN_NUMBER: SemanticInfo = CVSemantic(
    name="scan number",
    accession="MS:1003057",
)


##### Q VALUES #####

PSM_QVALUE: SemanticInfo = CVSemantic(
    name="PSM-level q-value",
    accession="MS:1002354",
)

# TODO: add CV term when available
PRECURSOR_QVALUE: SemanticInfo = BasicSemantic("Precursor-level q-value")

# TODO: add CV term when available
PEPTIDE_QVALUE: SemanticInfo = BasicSemantic("Peptide sequence-level q-value")

PROTEIN_GROUP_QVALUE: SemanticInfo = CVSemantic(
    name="protein group-level q-value",  # Note: MS CV term name uses lowercase 'p'
    accession="MS:1002373",
)

# TODO: add CV terms if/when available
PSM_PRECURSOR_QVALUE: SemanticInfo = BasicSemantic(
    "Combined PSM- and precursor-level q-value"
)
PSM_PEPTIDE_QVALUE: SemanticInfo = BasicSemantic(
    "Combined PSM- and peptide sequence-level q-value"
)


##### POSTERIOR ERROR PROBABILITIES #####

# TODO: add CV terms if/when available
PSM_ERRPROB: SemanticInfo = BasicSemantic(
    "PSM-level posterior error probability"
)
PRECURSOR_ERRPROB: SemanticInfo = BasicSemantic(
    "Precursor-level posterior error probability"
)
PEPTIDE_ERRPROB: SemanticInfo = BasicSemantic(
    "Peptide sequence-level posterior error probability"
)
PROTEIN_GROUP_ERRPROB: SemanticInfo = BasicSemantic(
    "Protein group-level posterior error probability"
)


##### UNKNOWN #####

UNKNOWN: SemanticInfo = UnknownSemantic()
