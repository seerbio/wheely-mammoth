import logging as _logging
from typing import (
    Mapping as _Mapping,
)

from .semantics import SemanticInfo

_logger = _logging.getLogger(__name__)


class SemanticDataset:
    def __init__(self, semantics: _Mapping[str, SemanticInfo]):
        self.semantics = semantics

    def get_semantics(self, key: str) -> SemanticInfo:
        return self.semantics[key]

    def get_by_semantics(
        self, semantic: SemanticInfo, require_unique=False
    ) -> str:
        """
        Get the column key for a given semantic.

        Parameters
        ----------
        semantic : SemanticInfo
            The semantic to search for.
        require_unique : bool, optional
            If ``True``, raises an error if multiple columns match the semantic.

        Returns
        -------
        out : str
            The name of the first matching column.
        """
        results = [(k, s) for k, s in self.semantics.items() if s == semantic]

        if len(results) > 1:
            if require_unique:
                raise ValueError(
                    f"Multiple columns found for semantic {semantic}: {[k for k, _ in results]}"
                )
            else:
                _logger.warning(
                    "Multiple columns found for semantic %s: %s",
                    semantic,
                    [k for k, _ in results],
                )

        if not results:
            raise ValueError(f"No columns found for semantic {semantic}")

        return next(iter(k for k, _ in results))
