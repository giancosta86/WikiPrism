from queue import Queue
from typing import Generic, TypeVar

from info.gianlucacosta.eos.core.functional import ContinuationProvider
from info.gianlucacosta.eos.core.logic.ranges import InclusiveRange
from info.gianlucacosta.eos.core.threading.queues.adaptive import create_adaptive_queue_reader
from info.gianlucacosta.eos.core.threading.safe import SafeThread

from .protocol import DictionaryFactory

TTerm = TypeVar("TTerm")


class DictionaryOutputThread(Generic[TTerm], SafeThread):
    def __init__(
        self,
        term_queue: Queue[TTerm],
        dictionary_factory: DictionaryFactory[TTerm],
        continuation_provider: ContinuationProvider,
    ):
        super().__init__()

        self._term_queue = term_queue
        self._dictionary_factory = dictionary_factory
        self._continuation_provider = continuation_provider

    def _safe_run(self) -> None:
        with self._dictionary_factory() as dictionary:

            def process_queue_term(term: TTerm) -> None:
                try:
                    dictionary.add_term(term)
                except Exception as ex:
                    self._logger.warning("Error while writing to the dictionary! %r", ex)

            read_terms_from_queue = create_adaptive_queue_reader(
                item_consumer=process_queue_term,
                timeout_seconds_range=InclusiveRange(lower=0.002, upper=0.025),
                timeout_factor=1.2,
            )

            read_terms_from_queue(
                self._term_queue,
                self._continuation_provider,
            )
