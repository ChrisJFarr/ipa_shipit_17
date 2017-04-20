from pyodbc import connect
from pandas import read_sql


class IPDW_PROD02:
    def __init__(self):
        self.conn = None

    def dataframe(self, sql, params=None):

        self.__openconn()

        if params is None:
            df = read_sql(sql, self.conn)
        else:
            df = read_sql(sql, self.conn, params=params)

        self.__closeconn()

        return df

    def __openconn(self):
        # Establish connection
        self.conn = connect(
            r'DRIVER={SQL Server};'
            r'SERVER=GRDDWMRTWHQIPDW;'
            r'DATABASE=IPDW_Prod02;'
            r'Trusted_connection=True'
            )

    def __closeconn(self):
        # Close the connection
        self.conn.close()
        self.conn = None
        
        print("Connection has been closed.")


class IPDW_TEST02:
    def __init__(self):
        self.conn = None

    def dataframe(self, sql, params=None):

        self.__openconn()

        if params is None:
            df = read_sql(sql, self.conn)
        else:
            df = read_sql(sql, self.conn, params=params)

        self.__closeconn()

        return df

    def execute(self, sql):

        self.__openconn()

        self.conn.execute(sql)

        self.__closeconn()

        response = "Statement has been executed"

        return response


    def __openconn(self):
        # Establish connection
        self.conn = connect(
            r'DRIVER={SQL Server};'
            r'SERVER=GRDDWMRTWHQIPDW;'
            r'DATABASE=IPDW_Test02;'
            r'Trusted_connection=True'
            r'autocommit=True'
        )

    def __closeconn(self):
        # Close the connection
        self.conn.close()
        self.conn = None

        print("Connection has been closed.")
