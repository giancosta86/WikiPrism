from logging import getLogger
from queue import Queue
from typing import TypeVar

from info.gianlucacosta.eos.core.functional import ContinuationProvider
from info.gianlucacosta.eos.core.logic.ranges import InclusiveRange
from info.gianlucacosta.eos.core.multiprocessing.pool import ProcessPoolFactory
from info.gianlucacosta.eos.core.multiprocessing.pool.facade import ProcessPoolFacade
from info.gianlucacosta.eos.core.threading.queues import QueueWriter
from info.gianlucacosta.eos.core.threading.queues.adaptive import create_adaptive_queue_writer

from ..page import Page
from .protocol import PipelineMessageListener, TermExtractor

TTerm = TypeVar("TTerm")


worker_logger = getLogger(f"{__name__}.worker")


def worker_function(
    page: Page,
    term_extractor: TermExtractor[TTerm],
) -> list[TTerm]:
    try:
        return term_extractor(page)
    except Exception as ex:
        worker_logger.warning(
            "Error while extracting terms from page '%s' within a process: %r",
            page.title,
            ex,
        )
        return []


class TermExtractionPool(ProcessPoolFacade[list[TTerm]]):
    _PROCESSED_PAGE_BATCH_SIZE = 507

    def __init__(
        self,
        pool_factory: ProcessPoolFactory,
        term_extractor: TermExtractor[TTerm],
        term_queue: Queue[TTerm],
        continuation_provider: ContinuationProvider,
        on_message: PipelineMessageListener,
    ):
        super().__init__(pool_factory=pool_factory, worker_function=worker_function)

        self._term_extractor = term_extractor

        self._processed_page_count = 0

        adaptive_writer: QueueWriter[TTerm] = create_adaptive_queue_writer(
            timeout_seconds_range=InclusiveRange(lower=0.002, upper=0.02),
            timeout_factor=1.2,
        )

        def enqueue_terms(extracted_terms: list[TTerm]) -> None:
            adaptive_writer(term_queue, continuation_provider, extracted_terms)

            self._processed_page_count += 1
            if self._processed_page_count % self._PROCESSED_PAGE_BATCH_SIZE == 0:
                on_message(f"Processed pages: {format(self._processed_page_count, ',')}")

        self._enqueue_terms = enqueue_terms

    @property
    def processed_page_count(self) -> int:
        return self._processed_page_count

    def extract_terms_from_page(self, page: Page) -> None:
        self._send_to_worker(page, self._term_extractor)

    def _on_worker_result(self, worker_result: list[TTerm]) -> None:
        self._enqueue_terms(worker_result)
