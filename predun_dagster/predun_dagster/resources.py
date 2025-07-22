import os
import sqlalchemy as sa
import mlflow
from dagster import ConfigurableResource
from typing import Optional

class PostgresResource(ConfigurableResource):
    """SQLAlchemy"""
    conn_uri: Optional[str] = None

    def setup(self, _):
        if not self.conn_uri:
            self.conn_uri = (
                f"postgresql://{os.getenv('DB_USER','siu')}:"
                f"{os.getenv('DB_PASSWORD','siu')}@"
                f"{os.getenv('DB_HOST','localhost')}:5432/"
                f"{os.getenv('DB_NAME','siu')}"
            )
        self.engine = sa.create_engine(self.conn_uri, pool_pre_ping=True)

    def get_conn(self):
        return self.engine.connect()
    def teardown(self, _):
        self.engine.dispose()

class MLflowResource(ConfigurableResource):
    tracking_uri: str = "file:./mlruns"

    def setup(self, _):
        mlflow.set_tracking_uri(self.tracking_uri)