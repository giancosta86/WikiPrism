from typing import Optional, TextIO, TypeVar, Union

from info.gianlucacosta.eos.core.functional import Consumer, Mapper, Producer

from ..dictionary import Dictionary
from ..page import Page

TTerm = TypeVar("TTerm")

WikiFile = Union[str, TextIO]

PipelineMessageListener = Consumer[str]
PipelineEndedListener = Consumer[Optional[Exception]]

DictionaryFactory = Producer[Dictionary[TTerm]]

TermExtractor = Mapper[Page, list[TTerm]]


class PipelineCanceledException(Exception):
    """
    Exception describing an extraction pipeline canceled because of a request issued
    by the client.

    It is actually never raised by the pipeline; however:

    * it must be raised by the methods of PipelineStrategy to notify that the pipeline
      must be canceled

    * it is passed to the on_ended() method of PipelineStrategy as soon as the extraction
      is actually canceled.
    """
