from config import Config
import pymysql
import oracledb
import os
import pandas as pd
 
class mysqlDB:
    def __init__(self, dbname):
        if dbname.lower() == 'ts':
            self._conn=pymysql.Connect(
                user=Config.DATABASE_CONFIG_TS['user'],
                password=Config.DATABASE_CONFIG_TS['password'],
                host=Config.DATABASE_CONFIG_TS['server'],
                port=3306,
                database=Config.DATABASE_CONFIG_TS['dbname']
            )
        self._cursor = self._conn.cursor(pymysql.cursors.DictCursor)
 
    def __enter__(self):
        return self
 
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
 
    @property
    def connection(self):
        return self._conn
 
    @property
    def cursor(self):
        return self._cursor
 
    def commit(self):
        self.connection.commit()
 
    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()
 
    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())
 
    def fetchall(self):
        return self.cursor.fetchall()
 
    def fetchone(self):
        return self.cursor.fetchone()
 
    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()
 
    def rows(self):
        return self.cursor.rowcount


class oracleDB:
    def __init__(self, dbname):
        if dbname.lower() == 'oradb2':
            oracledb.init_oracle_client()
            db_path = os.path.dirname(os.path.abspath(__file__))
            self._conn=oracledb.connect(os.path.join(db_path, 'config.yml'))
        self._cursor = self._conn.cursor()
 
    def __enter__(self):
        return oracledb
 
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self._conn
 
    @property
    def cursor(self):
        return self._cursor

    def makeDictFactory(self):
        columnNames = [d[0] for d in self.cursor.description]
        def createRow(*args):
            return dict(zip(columnNames, args))
        return createRow

    def commit(self):
        return self.connection.commit()

    def execute(self, sql):
        self.cursor.execute(sql)
        return self.commit()

    def executemany(self, sql, params):
        self.cursor.executemany(sql, params)
        return self.commit()

    def query_to_df(self, sql):
        df = pd.read_sql(sql, self.connection)
        return df

    def close(self):
        self.cursor.close()
        return self.connection.close()