import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, 
                              LongType, FloatType, BooleanType, TimestampType)

logger = logging.getLogger(__name__)

class DataHandler:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pedidos(self) -> StructType:
        """Schema explícito para os arquivos CSV de pedidos."""
        return StructType([
            StructField("id_pedido", StringType(), True),
            StructField("produto", StringType(), True),
            StructField("valor_unitario", FloatType(), True),
            StructField("quantidade", LongType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("uf", StringType(), True),
            StructField("id_cliente", LongType(), True)
        ])

    def _get_schema_pagamentos(self) -> StructType:
        """Schema explícito para os arquivos JSON de pagamentos."""
        return StructType([
            StructField("id_pagamento", StringType(), True),
            StructField("id_pedido", StringType(), True),
            StructField("status", BooleanType(), True),
            StructField("fraude", BooleanType(), True),
            StructField("metodo_pagamento", StringType(), True),
            StructField("data_processamento", TimestampType(), True)
        ])

    def load_pedidos(self, path: str) -> DataFrame:
        logger.info(f"Carregando pedidos de: {path}")
        return self.spark.read \
            .option("header", "true") \
            .option("sep", ";") \
            .option("mode", "FAILFAST") \
            .schema(self._get_schema_pedidos()) \
            .csv(path)

    def load_pagamentos(self, path: str) -> DataFrame:
        logger.info(f"Carregando pagamentos de: {path}")
        return self.spark.read \
            .option("mode", "FAILFAST") \
            .schema(self._get_schema_pagamentos()) \
            .json(path)

    def write_output(self, df: DataFrame, path: str):
        logger.info(f"Escrevendo resultado em Parquet: {path}")
        df.write.mode("overwrite").parquet(path)