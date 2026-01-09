from typing import (
    Optional as _Optional,
    Protocol as _Protocol,
)


class SemanticInfo(_Protocol):
    """
    Protocol for information about the semantics of a data column.
    """

    name: str

    def __eq__(self, other) -> bool:
        """Compare two semantic info objects."""
        ...

    def __repr__(self) -> str:
        """Return string representation of semantic info."""
        ...


class BasicSemantic(SemanticInfo):
    """
    Basic semantic information with just a name.

    Values of this type are considered equal to `CVSemantic` values with the same name.
    """

    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other):
        if not isinstance(other, (BasicSemantic, CVSemantic)):
            return NotImplemented
        return self.name == other.name

    def __repr__(self):
        return self.name


class CVUnit:
    """
    Controlled vocabulary unit information with name and accession.
    """

    def __init__(self, name: str, accession: str):
        self.name = name
        self.accession = accession

    def __eq__(self, other):
        if not isinstance(other, CVUnit):
            return NotImplemented
        return self.name == other.name and self.accession == other.accession

    def __repr__(self):
        return f"{self.name} ({self.accession})"


class CVSemantic(SemanticInfo):
    """
    Controlled vocabulary semantic information with name, accession, and optional unit.

    Values of this type are equal only if all three match.
    When compared to `BasicSemantic` values, only the name is considered.
    """

    def __init__(
        self, name: str, accession: str, unit: _Optional[CVUnit] = None
    ):
        self.name = name
        self.accession = accession
        self.unit = unit

    def __eq__(self, other):
        if isinstance(other, BasicSemantic):
            return self.name == other.name
        if not isinstance(other, CVSemantic):
            return NotImplemented
        return (
            self.name == other.name
            and self.accession == other.accession
            and self.unit == other.unit
        )

    def __repr__(self):
        if self.unit:
            return f"{self.name} ({self.accession}) [{self.unit}]"
        return f"{self.name} ({self.accession})"


class UnknownSemantic(SemanticInfo):
    """
    Semantic information for unknown semantics.
    Not equal to any other semantic info, including other UnknownSemantic instances.
    """

    name = "<unknown>"

    def __eq__(self, other):
        return False

    def __repr__(self):
        return self.name
