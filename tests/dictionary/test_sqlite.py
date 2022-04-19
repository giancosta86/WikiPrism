from contextlib import closing
from sqlite3 import OperationalError, connect

from info.gianlucacosta.eos.core.io.files.temporary import Uuid4TemporaryPath

from ..shared import MyTestSqliteDictionary, MyTestTerm


def test_insertion():
    my_term = MyTestTerm("Dodo")

    with Uuid4TemporaryPath(extension_including_dot=".db") as db_path:
        with MyTestSqliteDictionary(connect(db_path)) as dictionary:
            dictionary.create_schema()
            dictionary.add_term(my_term)

        with connect(db_path) as checking_connection:
            with closing(checking_connection.cursor()) as cursor:
                cursor.execute("SELECT entry FROM my_table")

                assert cursor.fetchall() == [("Dodo",)]


def test_successful_command():
    my_term = MyTestTerm("Dodo")

    with Uuid4TemporaryPath(extension_including_dot=".db") as db_path:
        with connect(db_path) as inserting_connection:
            inserting_connection.execute(
                """
            CREATE TABLE my_table (
                entry TEXT PRIMARY KEY
            )
            """
            )

            inserting_connection.execute(
                """
            INSERT INTO my_table
            (entry)
            VALUES
            (?)
            """,
                [my_term.entry],
            )

        with MyTestSqliteDictionary(connect(db_path)) as dictionary:
            result = dictionary.execute_command(
                """
                SELECT entry AS ciop
                FROM my_table
                """
            )

            assert result.headers == ["ciop"]
            assert result.rows == [("Dodo",)]


def test_failing_command():
    with Uuid4TemporaryPath(extension_including_dot=".db") as db_path:
        with MyTestSqliteDictionary(connect(db_path)) as dictionary:
            result = dictionary.execute_command(
                """
                SELECT inexisting_field AS ciop
                FROM my_table
                """
            )

            assert isinstance(result, OperationalError)
