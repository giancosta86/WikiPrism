from datetime import datetime
from logging import getLogger
from queue import Queue
from typing import Generic, TypeVar

from info.gianlucacosta.eos.core.threading.cancelable import CancelableThread

from .dictionary import DictionaryOutputThread
from .pool import TermExtractionPool
from .protocol import PipelineCanceledException
from .sax import WikiPageExtractionThread
from .strategy import PipelineStrategy

TTerm = TypeVar("TTerm")


class SupervisorThread(Generic[TTerm], CancelableThread):
    _TERM_QUEUE_SIZE = 1024

    def __init__(
        self,
        strategy: PipelineStrategy[TTerm],
    ) -> None:
        super().__init__()
        self._strategy = strategy
        self._logger = getLogger(type(self).__name__)

    def request_cancel(self) -> None:
        try:
            self._strategy.request_cancel()
        finally:
            super().request_cancel()

    def run(self) -> None:
        start_time = datetime.now()

        try:
            self._logger.info("Pipeline started at %s", start_time)

            self._strategy.initialize_pipeline()

            self._logger.info("Pipeline initialized!")

            if not self._never_canceled:
                raise PipelineCanceledException()

            self._run_term_extraction()

            if not self._never_canceled:
                raise PipelineCanceledException()

            self._logger.info("Now performing optimizations...")
            self._optimize_dictionary()
            self._logger.info("Optimizations performed! ^__^")

            if not self._never_canceled:
                raise PipelineCanceledException()

            self._logger.info("Performing the last steps...")
            self._strategy.perform_last_successful_steps()
            self._logger.info("Last steps performed!")

            self._logger.info("Pipeline successful! ^__^!")
        except Exception as ex:
            self._strategy.on_ended(ex)
        else:
            self._strategy.on_ended(None if self._never_canceled else PipelineCanceledException())
        finally:
            end_time = datetime.now()
            pipeline_duration = end_time - start_time
            self._logger.info("Total time: %s", pipeline_duration)

    def _run_term_extraction(self) -> None:
        wiki_file = self._strategy.get_wiki_file()
        self._logger.info("Wiki file is: %s", wiki_file)

        term_parsing_active = True

        try:
            term_queue = Queue[TTerm](maxsize=self._TERM_QUEUE_SIZE)

            dictionary_thread = DictionaryOutputThread(
                term_queue=term_queue,
                dictionary_factory=self._strategy.create_dictionary,
                continuation_provider=lambda: self._never_canceled and term_parsing_active,
            )

            with TermExtractionPool(
                pool_factory=self._strategy.create_pool,
                term_extractor=self._strategy.get_term_extractor(),
                term_queue=term_queue,
                continuation_provider=lambda: self._never_canceled and term_parsing_active,
                on_message=self._strategy.on_message,
            ) as term_extraction_pool:
                wiki_thread = WikiPageExtractionThread(
                    wiki_file=wiki_file,
                    on_page_extracted=term_extraction_pool.extract_terms_from_page,
                    continuation_provider=lambda: self._never_canceled and term_parsing_active,
                )

                self._logger.info("Now starting the wiki SAX thread!")
                wiki_thread.start()
                self._logger.info("Wiki SAX thread started!")

                self._logger.info("Starting the dictionary thread!")
                dictionary_thread.start()
                self._logger.info("Dictionary thread was started!")

                self._logger.info("Now waiting for the wiki SAX thread to end...")
                wiki_thread.join()
                self._logger.info("The wiki SAX thread has ended!")

                self._logger.info(
                    "Are there exceptions in the wiki SAX thread? %r",
                    wiki_thread.exception,
                )
                if wiki_thread.exception:
                    self._logger.warning(
                        "Errors found in the wiki SAX thread - exiting the pipeline"
                    )
                    raise wiki_thread.exception
        finally:
            term_parsing_active = False

        self._logger.info("Waiting for the dictionary thread to end...")
        dictionary_thread.join()
        self._logger.info("Dictionary thread has ended!")

        self._logger.info(
            "Are there exceptions in the dictionary thread? %r",
            dictionary_thread.exception,
        )
        if dictionary_thread.exception:
            self._logger.warning("Errors found in the dictionary thread - exiting the pipeline")
            raise dictionary_thread.exception

        self._logger.info(
            "Total processed pages: %s",
            term_extraction_pool.processed_page_count,
        )

    def _optimize_dictionary(self) -> None:
        with self._strategy.create_dictionary() as dictionary:
            self._strategy.on_message("Optimizing the dictionary...")

            dictionary.optimize()

            self._strategy.on_message("Dictionary optimized!")
