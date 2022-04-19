from contextlib import closing
from sqlite3 import Connection, connect
from typing import Iterable, Optional

from info.gianlucacosta.eos.core.functional import Mapper
from info.gianlucacosta.eos.core.io.files.temporary import Uuid4TemporaryPath
from info.gianlucacosta.eos.core.multiprocessing.pool import AnyProcessPool, InThreadPool

from info.gianlucacosta.wikiprism.dictionary.sqlite import SqliteDictionary
from info.gianlucacosta.wikiprism.pipeline import run_extraction_pipeline
from info.gianlucacosta.wikiprism.pipeline.protocol import TermExtractor, WikiFile
from info.gianlucacosta.wikiprism.pipeline.sqlite import SqlitePipelineStrategy

from ..shared import MyTestSqliteDictionary, MyTestTerm, create_wiki_stream


class BasicTestSqlitePipelineStrategy(SqlitePipelineStrategy[MyTestTerm]):
    def __init__(
        self,
        target_db_path: str,
        sqlite_dictionary_factory: Mapper[Connection, SqliteDictionary[MyTestTerm]],
        extractor: Optional[TermExtractor[MyTestTerm]] = None,
        add_wiki_error: bool = False,
    ) -> None:
        super().__init__(target_db_path=target_db_path)
        self._sqlite_dictionary_factory = sqlite_dictionary_factory
        self._extractor = extractor if extractor else lambda page: [MyTestTerm(page.text)]
        self._add_wiki_error = add_wiki_error
        self.exception: Optional[Exception] = None

    def create_pool(self) -> AnyProcessPool:
        return InThreadPool()

    def get_wiki_file(self) -> WikiFile:
        return create_wiki_stream(add_error=self._add_wiki_error)

    def get_term_extractor(self) -> TermExtractor[MyTestTerm]:
        return self._extractor

    def on_message(self, message: str) -> None:
        pass

    def on_ended(self, exception: Optional[Exception]) -> None:
        self.exception = exception
        super().on_ended(exception)

    def create_dictionary_from_connection(
        self, connection: Connection
    ) -> SqliteDictionary[MyTestTerm]:
        return self._sqlite_dictionary_factory(connection)


class TestRunExtractionPipelineToSqlite:
    def _expect_entries_from_sqlite_pipeline(
        self,
        dictionary_factory: Mapper[Connection, SqliteDictionary[MyTestTerm]],
        expected_entries: Iterable[str],
    ):
        with Uuid4TemporaryPath(extension_including_dot=".db") as test_db_path:
            sqlite_pipeline_strategy = BasicTestSqlitePipelineStrategy(
                target_db_path=test_db_path,
                sqlite_dictionary_factory=dictionary_factory,
            )
            pipeline_handle = run_extraction_pipeline(sqlite_pipeline_strategy)
            pipeline_handle.join()

            assert sqlite_pipeline_strategy.exception is None

            with connect(test_db_path) as checking_connection:
                with closing(checking_connection.cursor()) as cursor:
                    cursor.execute(
                        """
                    SELECT entry
                    FROM my_table
                    ORDER BY entry
                    """
                    )

                    assert cursor.fetchall() == [(entry,) for entry in expected_entries]

    def test_merry_path(self):
        self._expect_entries_from_sqlite_pipeline(
            MyTestSqliteDictionary,
            ["A1", "B2", "C3", "D4", "E5", "Z6"],
        )

    def test_dictionary_errors(self):
        class DictionaryWithCustomErrors(MyTestSqliteDictionary):
            def add_term(self, term: MyTestTerm) -> None:
                if term.entry in {"C3", "E5"}:
                    raise Exception("Custom test exception!")

                return super().add_term(term)

        self._expect_entries_from_sqlite_pipeline(
            DictionaryWithCustomErrors,
            ["A1", "B2", "D4", "Z6"],
        )

    def test_preprocessing_failure(self):
        class FailingSchemaDictionary(MyTestSqliteDictionary):
            def create_schema(self) -> None:
                raise ArithmeticError("This is a test exception!")

        with Uuid4TemporaryPath(extension_including_dot=".db") as test_db_path:
            sqlite_pipeline_strategy = BasicTestSqlitePipelineStrategy(
                target_db_path=test_db_path,
                sqlite_dictionary_factory=FailingSchemaDictionary,
            )

            pipeline_handle = run_extraction_pipeline(sqlite_pipeline_strategy)
            pipeline_handle.join()

            assert isinstance(sqlite_pipeline_strategy.exception, ArithmeticError)

    def test_postprocessing_failure(self):
        sqlite_pipeline_strategy = BasicTestSqlitePipelineStrategy(
            target_db_path="\0",
            sqlite_dictionary_factory=MyTestSqliteDictionary,
        )

        pipeline_handle = run_extraction_pipeline(sqlite_pipeline_strategy)
        pipeline_handle.join()

        assert isinstance(sqlite_pipeline_strategy.exception, OSError)
