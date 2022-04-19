from abc import abstractmethod
from os import makedirs
from os.path import dirname
from shutil import move
from sqlite3 import Connection, connect
from typing import Optional, TypeVar

from info.gianlucacosta.eos.core.functional import Mapper
from info.gianlucacosta.eos.core.functional.retries import call_with_retries
from info.gianlucacosta.eos.core.io.files.temporary import Uuid4TemporaryPath

from ..dictionary import Dictionary
from ..dictionary.sqlite import SqliteDictionary
from .protocol import PipelineCanceledException
from .strategy import PipelineStrategy

TTerm = TypeVar("TTerm")

SqliteDictionaryFactory = Mapper[Connection, SqliteDictionary[TTerm]]


class SqlitePipelineStrategy(PipelineStrategy[TTerm]):
    """
    Pipeline strategy dedicated to storing terms to a SQLite db.

    It implements several of the abstract PipelineStrategy methods - which you can still override,
    better if with a super() call - while introducing other methods that need to be implemented.
    """

    def __init__(self, target_db_path: str) -> None:
        """
        Creates the SQLite-oriented pipeline strategy.

        * target_db_path: the path where the db must reside; it should not exist, because it will
          be overwritten at the very end of the pipeline.
          Furthermore, its directory is not required to exist: the whole path will be created.
        """
        super().__init__()
        self._target_db_path = target_db_path
        self._temp_db_path = Uuid4TemporaryPath(extension_including_dot=".db")

    @abstractmethod
    def create_dictionary_from_connection(self, connection: Connection) -> SqliteDictionary[TTerm]:
        """
        Given a SQLite connection, returns a SqliteDictionary based on it.
        """

    def _create_dictionary_on_temp_db(self) -> SqliteDictionary[TTerm]:
        return self.create_dictionary_from_connection(connect(self._temp_db_path))

    def initialize_pipeline(self) -> None:
        self._logger.info("The temporary db file is: %s", self._temp_db_path)

        with self._create_dictionary_on_temp_db() as schema_dictionary:
            self._logger.info("Creating the db schema...")
            schema_dictionary.create_schema()
            self._logger.info("Schema created!")

    def create_dictionary(self) -> Dictionary[TTerm]:
        return self._create_dictionary_on_temp_db()

    def perform_last_successful_steps(self) -> None:
        self._logger.info(
            "Now moving the temporary db file to '%s'...",
            self._target_db_path,
        )

        target_db_directory = dirname(self._target_db_path)
        makedirs(target_db_directory, exist_ok=True)

        def try_to_move_db_file() -> None:
            if not self._never_canceled:
                raise PipelineCanceledException()

            move(self._temp_db_path, self._target_db_path)

        call_with_retries(try_to_move_db_file, 3, 1.5)

        self._logger.info("Database ready!")

    def on_ended(self, exception: Optional[Exception]) -> None:
        self._logger.info("Trying to delete the temporary db...")
        self._temp_db_path.try_to_remove()
