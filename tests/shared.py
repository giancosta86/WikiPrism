from dataclasses import dataclass
from io import StringIO
from sqlite3 import Connection

from info.gianlucacosta.eos.core.db.sqlite import ConnectionLender
from info.gianlucacosta.eos.core.db.sqlite.serializer import BufferedDbSerializer

from info.gianlucacosta.wikiprism.dictionary.sqlite import SqliteDictionary


def create_wiki_stream(add_error: bool = False) -> StringIO:
    result = StringIO(
        """
    <mediawiki>
        <page>
            <title>Alpha</title>
            <text>A1</text>
        </page>

        <page>
            <title>Beta</title>
            <text>B2</text>
        </page>

        <page>
            <text>Untitled page</text>
        </page>

        <page>
            <title>Page without text</title>
        </page>

        <page>
            <title>Gamma</title>
            <text>C3</text>
        </page>

        <page>
            <title>Delta</title>
            <text>D4</text>
        </page>"""
        + ("__ERROR__" if add_error else "")
        + """<page>
            <title>Epsilon</title>
            <text>E5</text>
        </page>

        <page>
            <title>Zeta</title>
            <text>Z6</text>
        </page>
    </mediawiki>
    """
    )

    return result


@dataclass(frozen=True)
class MyTestTerm:
    entry: str


def create_db_serializer(connection_lender: ConnectionLender):
    serializer = BufferedDbSerializer(connection_lender)

    @serializer.register(
        """
        INSERT INTO my_table
        (entry)
        VALUES
        (?)
        """
    )
    def register_test_term(term: MyTestTerm):
        return [term.entry]

    return serializer


class MyTestSqliteDictionary(SqliteDictionary[MyTestTerm]):
    def __init__(self, connection: Connection) -> None:
        super().__init__(connection, create_db_serializer)

    def create_schema(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE my_table (
                entry TEXT PRIMARY KEY
            )
            """
        )
