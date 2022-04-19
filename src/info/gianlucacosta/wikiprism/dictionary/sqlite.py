from abc import abstractmethod
from contextlib import closing
from sqlite3 import Connection
from typing import TypeVar

from info.gianlucacosta.eos.core.db.sqlite import ConnectionLender
from info.gianlucacosta.eos.core.db.sqlite.serializer import BufferedDbSerializer
from info.gianlucacosta.eos.core.functional import Mapper

from . import Dictionary, DictionaryView, DictionaryViewResult

TTerm = TypeVar("TTerm")


class SqliteDictionary(Dictionary[TTerm]):
    """
    Dictionary backed by a SQLite db connection, with delayed insertions.

    More precisely, it internally uses a BufferedDbSerializer (provided by Eos) to write to db,
    within a single transaction, only batches of terms - to vastly improve performance.

    Supports the methods and properties provided by Dictionary, plus:

    * connection - referencing the owned connection

    * flush() - to flush the internal buffered serializer to db

    It is important to note that flush() is also called right before closing the dictionary -
    for example, via a "with" block.
    """

    def __init__(
        self,
        connection: Connection,
        serializer_factory: Mapper[ConnectionLender, BufferedDbSerializer],
    ) -> None:
        """
        Creates the dictionary, taking ownership of the connection - which will be closed with
        the dictionary itself.

        The serializer_factory is usually a function that internally builds a
        BufferedDbSerializer (whose constructor, in turn, gets the ConnectionLender)
        and that must register the serializers for the expected term types.
        """

        super().__init__()
        self._connection = connection
        self._serializer = serializer_factory(lambda: connection)

    @property
    def connection(self) -> Connection:
        return self._connection

    @abstractmethod
    def create_schema(self) -> None:
        """
        Runs the DDL commands to create the schema - tables, indexes, and more - for the
        dictionary.
        """

    def add_term(self, term: TTerm) -> None:
        """
        Adds a term to the dictionary.

        The terms might not be immediately available on db: to ensure that, you'll need to call
        the flush() method.
        """
        self._serializer.request_serialize(term)

    def execute_command(self, command: str) -> DictionaryViewResult:
        """
        Applies a SQL command to the db.

        The command should be a valid SQL statement supported by SQLite, but neither validation
        nor security checks are performed by this method.
        """
        try:
            with closing(self._connection.execute(command)) as cursor:
                return DictionaryView(
                    headers=[description[0] for description in cursor.description],
                    rows=cursor.fetchall(),
                )
        except Exception as ex:
            return ex

    def optimize(self) -> None:
        """
        The optimization process runs a SQLite VACUUM command.

        Consequently, it can save quite a few MBs in terms of disk space.
        """
        with closing(self._connection.cursor()) as cursor:
            cursor.execute("VACUUM")

    def flush(self) -> None:
        """
        Flushes the internal database serializer to db.

        This ensures that subsequent queries return updated results.
        """
        self._serializer.flush()

    def close(self) -> None:
        """
        Flushes the internal buffered serializer to db and then closes the connection.
        """
        self.flush()

        self._connection.close()
