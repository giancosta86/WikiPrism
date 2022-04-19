from xml.sax import parse

from info.gianlucacosta.eos.core.functional import Consumer, ContinuationProvider
from info.gianlucacosta.eos.core.threading.safe import SafeThread

from ..page import Page
from ..page.sax import WikiContentHandler, WikiErrorHandler, WikiSaxCanceledException
from .protocol import WikiFile


class WikiPageExtractionThread(SafeThread):
    def __init__(
        self,
        wiki_file: WikiFile,
        on_page_extracted: Consumer[Page],
        continuation_provider: ContinuationProvider,
    ):
        super().__init__()
        self._wiki_file = wiki_file
        self._on_page_extracted = on_page_extracted
        self._continuation_provider = continuation_provider

    def _safe_run(self) -> None:
        content_handler = WikiContentHandler(
            on_page_extracted=self._on_page_extracted,
            continuation_provider=self._continuation_provider,
        )

        error_handler = WikiErrorHandler()

        try:
            self._logger.info("Starting the wiki parsing via SAX...")
            parse(self._wiki_file, content_handler, error_handler)
            self._logger.info("Wiki SAX parsing complete!")
        except WikiSaxCanceledException:
            self._logger.info("The wiki SAX parsing was canceled!")
