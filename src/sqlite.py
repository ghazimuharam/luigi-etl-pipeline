import sqlite3


class SqliteConnection():
    """SqliteConnection
    This Class initiate SQlite connection

    Attributes
    ----------
    db_name : str
        SQlite Output DB name
    """

    def __init__(self) -> None:
        self.conn = sqlite3.connect("data_warehouse.db")
        self.c = self.conn.cursor()

    def connection(self):
        """
        Return SqliteConnection Object

        Returns
        ----------
        conn : SqliteConnection
            SqliteConnection Object
        """

        return self.conn

    def execute(self, query) -> None:
        """
        Execute given query

        Parameters
        ----------
        query : str
            Query to be execute
        """

        self.c.execute(query)

    def close(self) -> None:
        """
        Close SqliteConnection
        """

        self.conn.close()
