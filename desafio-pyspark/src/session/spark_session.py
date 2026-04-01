import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class SparkManager:
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark = None

    def get_session(self) -> SparkSession:
        if not self.spark:
            logger.info(f"Iniciando SparkSession: {self.app_name}")
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master("local[*]") \
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                .getOrCreate()
        return self.spark