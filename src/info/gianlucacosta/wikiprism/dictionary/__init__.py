from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generic, Iterable, Optional, TypeVar

from info.gianlucacosta.eos.core.functional import Result

TTerm = TypeVar("TTerm")

DictionaryViewRow = Iterable[Any]


@dataclass(frozen=True)
class DictionaryView:
    """
    Dataset returned by a dictionary after a client issues a successful command.

    Dictionaries can support any underlying data storage, but views should be presented as tables.
    """

    headers: Optional[Iterable[str]]
    rows: Optional[Iterable[DictionaryViewRow]]


DictionaryViewResult = Result[DictionaryView]


class Dictionary(Generic[TTerm], ABC):
    """
    Generic repository of language-related terms.

    Linguistically speaking, "term" was chosen as a shorter, simpler and more general synonym
    for "lemma".

    The core methods are:

    * add_term() - to insert a single term

    * execute_command() - to execute a command and return either data or an exception

    Dictionaries have a close() method, that gets automatically called at the end of
    a "with" block.
    """

    TSelf = TypeVar("TSelf")

    def __enter__(self: TSelf) -> TSelf:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    @abstractmethod
    def add_term(self, term: TTerm) -> None:
        """
        Adds a term to the dictionary.

        How duplicated entries are handled depends on the specific dictionary.
        """

    @abstractmethod
    def execute_command(self, command: str) -> DictionaryViewResult:
        """
        Executes the given command - whose format is arbitrary and defined by the dictionary.

        This should be a "safe" method, returning either a DictionaryView or an Exception -
        without raising the latter.
        """

    @abstractmethod
    def optimize(self) -> None:
        """
        Optimizes the underlying storage.
        """

    @abstractmethod
    def close(self) -> None:
        """
        Closes the underlying storage.
        """
