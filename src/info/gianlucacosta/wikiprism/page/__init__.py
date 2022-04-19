from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Page:
    """
    Page extracted from a wiki.

    It can be used as the starting point of a more fine-grained extraction process.
    """

    title: str
    text: str
