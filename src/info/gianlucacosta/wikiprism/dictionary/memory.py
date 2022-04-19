from typing import TypeVar

from . import Dictionary, DictionaryViewResult

TTerm = TypeVar("TTerm")


class InMemoryDictionary(Dictionary[TTerm]):
    """
    In-memory dictionary actually adding its terms to a Python set.

    The terms are available via the "terms" property.

    This dictionary does not support commands (and raises a NotImplementedError), whereas
    the other methods are just empty.
    """

    def __init__(self) -> None:
        self._terms: set[TTerm] = set()

    @property
    def terms(self) -> set[TTerm]:
        """
        The terms added to the dictionary up to now.
        """
        return self._terms

    def add_term(self, term: TTerm) -> None:
        self._terms.add(term)

    def execute_command(self, _: str) -> DictionaryViewResult:
        raise NotImplementedError()

    def optimize(self) -> None:
        pass

    def close(self) -> None:
        pass
