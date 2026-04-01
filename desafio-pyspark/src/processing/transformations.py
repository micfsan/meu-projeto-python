import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

class BusinessLogic:
    def filter_legit_refused_2025(self, df_pedidos: DataFrame, df_pagamentos: DataFrame) -> DataFrame:
        try:
            logger.info("Iniciando transformações de negócio...")
            
            # 1. Cruzar Pedidos com Pagamentos
            joined_df = df_pedidos.join(df_pagamentos, "id_pedido", "inner")

            # 2. Aplicar Filtros: 
            # status=false (pagamento recusado), fraude=false (legítimo), ano=2025
            relatorio = joined_df.filter(
                (F.col("status") == False) & 
                (F.col("fraude") == False) & 
                (F.year(F.col("data_criacao")) == 2025)
            )

            # 3. Selecionar atributos solicitados: UF, Forma Pagamento, Valor Total, Data
            
            final_df = relatorio.select(
                F.col("uf").alias("Estado"),
                F.col("metodo_pagamento").alias("Forma_Pagamento"),
                (F.col("valor_unitario") * F.col("quantidade")).alias("Valor_Total"),
                F.col("data_criacao").alias("Data_Pedido")
            )

            return final_df

        except Exception as e:
            logger.error(f"Erro ao processar lógica de negócio: {str(e)}")
            raise e