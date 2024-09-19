import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from clickhouse_sqlalchemy import make_session
from sqlalchemy import MetaData, Table


class BaseDatabaseHandler:
    def __init__(self, **kwargs):
        self.engine = None
        self.Session = None
        self.setup(**kwargs)

    def build_url(self, **kwargs):
        raise NotImplementedError("Subclasses must implement this method")

    def setup(self, **kwargs):
        raise NotImplementedError("Subclasses must implement this method")

    def execute_query(self, query: str) -> (object, object):
        session = self.Session()
        try:
            result = session.execute(text(query))
            session.commit()
            if result.returns_rows:
                return result.fetchall(), result.keys()
            else:
                return None, None
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    def close_connection(self):
        self.engine.dispose()


class SQLDatabaseHandler(BaseDatabaseHandler):
    def build_url(self, **kwargs):
        user = kwargs.get('user')
        password = kwargs.get('password')
        host = kwargs.get('host')
        port = kwargs.get('port', '3306')
        database = kwargs.get('database', '')
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}" if database else f"mysql+pymysql://{user}:{password}@{host}:{port}"

    def setup(self, **kwargs):
        url = self.build_url(**kwargs)
        self.engine = create_engine(url)
        self.Session = sessionmaker(bind=self.engine)

    def export_df(self, df: pd.DataFrame, table_name: str):
        if not self.engine:
            raise ValueError("Database engine not initialized")

        # Inserting DataFrame into the database
        session = self.Session()
        try:
            df.to_sql(table_name, con=self.engine, if_exists='append', index=False, chunksize=10000, method="multi")
            session.commit()
        except Exception as _:
            session.rollback()
            raise
        finally:
            session.close()






class PostgreSQLDatabaseHandler(BaseDatabaseHandler):
    def build_url(self, **kwargs):
        user = kwargs.get('user')
        password = kwargs.get('password')
        host = kwargs.get('host')
        port = kwargs.get('port', '5432')
        database = kwargs.get('database', '')
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}" if database else f"postgresql+psycopg2://{user}:{password}@{host}:{port}"

    def setup(self, **kwargs):
        url = self.build_url(**kwargs)
        self.engine = create_engine(url)
        self.Session = sessionmaker(bind=self.engine)


class ClickhouseDatabaseHandler(BaseDatabaseHandler):
    def build_url(self, **kwargs):
        user = kwargs.get('user')
        password = kwargs.get('password')
        host = kwargs.get('host')
        database = kwargs.get('database', '')
        return f"clickhouse://{user}:{password}@{host}/{database}" if database else f"clickhouse://{user}:{password}@{host}"

    def setup(self, **kwargs):
        url = self.build_url(**kwargs)
        print(url)
        self.engine = create_engine(url)

    def execute_query(self, query):
        session = make_session(self.engine)
        try:
            result = session.execute(text(query))
            session.commit()
            if result.returns_rows:
                return result.fetchall(), result.keys()
            else:
                return None, None
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
