from io import StringIO
from logging import getLogger
from typing import Any, Optional
from xml.sax.handler import ContentHandler, ErrorHandler

from info.gianlucacosta.eos.core.functional import Consumer, ContinuationProvider

from . import Page

logger = getLogger(__name__)


class WikiSaxCanceledException(Exception):
    """
    Exception raised when the SAX parser - extracting pages from the wiki - stops because of
    a cancelation request.
    """


class WikiContentHandler(ContentHandler):
    """
    ContentHandler to be used as the "handler" argument in Python's xml.sax functions.

    It can be interrupted; more precisely, when reading a page, it calls its ContinuationProvider
    (a () -> bool function) and:

    * if the function returns True, the process goes on

    * if the function returns False, a WikiSaxCanceledException is raised,
      as the parsing ends

    You don't need to use properties or methods of this object - it just interacts
    with the functions passed to its constructor.

    Please, note: not all the pages parsed via SAX actually trigger the
    page-notification function; the parser automatically drops the following:

    * pages with no <title> tag
    * pages with no <text> tag
    """

    def __init__(
        self,
        on_page_extracted: Consumer[Page],
        continuation_provider: ContinuationProvider,
    ) -> None:
        """
        Creates the content handler for extracting wiki pages.

        * on_page_extracted is a (Page) -> None function, called as soon as a page is extracted

        * continuation_provider is a () -> bool function, called at every page and
          stopping the process - by raising a WikiSaxCanceledException - when returning False
        """
        super().__init__()
        self._on_page_extracted = on_page_extracted
        self._continuation_provider = continuation_provider
        self._current_title: Optional[str] = None
        self._current_text: Optional[str] = None
        self._buffer = StringIO()
        self._parse_chars = False

    def startElement(self, name: str, _: Any) -> None:
        match name:
            case "page":
                if not self._continuation_provider():
                    raise WikiSaxCanceledException()

                self._current_title = None
                self._current_text = None
            case "title" | "text":
                self._parse_chars = True

    def characters(self, content: str) -> None:
        if self._parse_chars:
            self._buffer.write(content)

    def endElement(self, name: str) -> None:
        match name:
            case "title":
                self._parse_chars = False
                self._current_title = self._buffer.getvalue()
                self._buffer = StringIO()

            case "text":
                self._parse_chars = False
                self._current_text = self._buffer.getvalue()
                self._buffer = StringIO()

            case "page":
                if self._current_title is None:
                    if __debug__:
                        logger.info("Skipping page with no <title> tag")
                    return

                if self._current_text is None:
                    if __debug__:
                        logger.info(
                            "Skipping page '%s', which has no <text> tag", self._current_title
                        )
                    return

                page = Page(title=self._current_title, text=self._current_text)
                self._on_page_extracted(page)


class WikiErrorHandler(ErrorHandler):
    """
    ErrorHandler to be used as the "error_handler" argument in Python's xml.sax functions.

    You just need to instantiate it and pass it to the function.
    """

    def warning(self, exception: Any) -> None:
        logger.warning(
            "Warning while parsing at [line %s, col %s]: %r",
            exception.getLineNumber(),
            exception.getColumnNumber(),
            exception,
        )

    def error(self, exception: Any) -> None:
        logger.error(
            "Error while parsing at [line %s, col %s]: %r",
            exception.getLineNumber(),
            exception.getColumnNumber(),
            exception,
        )

    def fatalError(self, exception: Any) -> None:
        logger.error(
            "Fatal error while parsing at [line %s, col %s]: %r",
            exception.getLineNumber(),
            exception.getColumnNumber(),
            exception,
        )
