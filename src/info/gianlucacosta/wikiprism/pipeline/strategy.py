from abc import ABC, abstractmethod
from logging import getLogger
from typing import Generic, Optional, TypeVar

from info.gianlucacosta.eos.core.multiprocessing.pool import AnyProcessPool

from ..dictionary import Dictionary
from .protocol import TermExtractor, WikiFile

TTerm = TypeVar("TTerm")


class PipelineStrategy(Generic[TTerm], ABC):
    """
    Strategy for a term-extraction pipeline.

    In practice, you'll need to extend this class and implement its abstract methods - then,
    you can pass an instance of the subclass to run_extraction_pipeline().

    It is important to note how this class interacts with the pipeline:

    * the strategy methods are called by the pipeline to perform actions or to retrieve
      information

    * strategy method can check the _never_canceled field, which is initially True and
      is set to False by the pipeline as soon as a client performs a cancelation request.

    The strategy can respond to cancelation in different ways:

    * simply ignore it, leaving the burden to the pipeline

    * check the _never_canceled field, and:

      * just return, in methods whose return-type is None, or raise a PipelineCanceledException() -
        whichever you prefer

      * raise a PipelineCanceledException() in methods expected to return a value.

    More generally, PipelineCanceledException() can be raised even when _never_canceled
    is still True - but the strategy noticed a cancelation request in a way unknown
    to the pipeline itself.

    Last but not least, the strategy already provides a _logger field.
    """

    def __init__(self) -> None:
        self._never_canceled = True
        self._logger = getLogger(type(self).__name__)

    def request_cancel(self) -> None:
        self._never_canceled = False

    @abstractmethod
    def create_pool(self) -> AnyProcessPool:
        """
        Creates a process pool for extracting terms. It should return either:

        * a Pool object, for real parallelism

        * an InThreadPool instance, from Eos, especially for debugging and testing
        """

    @abstractmethod
    def initialize_pipeline(self) -> None:
        """
        Custom operations to be run at the very beginning of the pipeline.
        """

    @abstractmethod
    def create_dictionary(self) -> Dictionary[TTerm]:
        """
        Creates a dictionary to store the extracted terms.

        In some contexts, such as testing, it might also return an existing instance.
        """

    @abstractmethod
    def get_wiki_file(self) -> WikiFile:
        """
        Returns the wiki text file - or its path - to be parsed via SAX.
        """

    @abstractmethod
    def get_term_extractor(self) -> TermExtractor[TTerm]:
        """
        Returns a term extractor - a function which, given a Page instance, returns a list
        of the terms within the page.

        The term extractor is sent to a process pool, so it should be a serializable function -
        even better, some module-level function.
        """

    @abstractmethod
    def perform_last_successful_steps(self) -> None:
        """
        Performs the very last steps of a successful pipeline.

        Of course, it can still raise PipelineCanceledException(), thus canceling the pipeline.
        """

    @abstractmethod
    def on_message(self, message: str) -> None:
        """
        Called whenever the pipeline wants to send a message to the client.
        """

    @abstractmethod
    def on_ended(self, exception: Optional[Exception]) -> None:
        """
        Called at the end of the pipeline - be it successful or not.

        In particular, "exception" is a PipelineCanceledException when the client explicitly
        requested to cancel it; otherwise, it can be of any other exception type.
        """
