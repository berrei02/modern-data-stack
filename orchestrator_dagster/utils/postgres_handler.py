from sqlalchemy import create_engine
import pandas as pd


class PostgresConnector:
    """
    Can be overwritten later with Dagster IO Manager
    in future.
    """
    def _create_engine(self):
        return create_engine("postgresql://postgres:postgres@localhost:5499/postgres")

    def handle_output(self, df: pd.DataFrame, table: str, schema: str | None = None):
        with self._create_engine().connect() as conn:
            df = df.to_sql(con=conn, name=table, schema=schema)

    def load_input(self, table: str, schema: str | None = None):
        with self._create_engine().connect() as conn:
            df = pd.read_sql(sql=f"select * from {table}", con=conn, schema=schema)
        return df
