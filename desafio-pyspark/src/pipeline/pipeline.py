import logging
from src.io_utils.data_handler import DataHandler
from src.processing.transformations import BusinessLogic

logger = logging.getLogger(__name__)

class FraudPipeline:
    def __init__(self, spark_session, data_handler: DataHandler, logic: BusinessLogic):
        self.spark = spark_session
        self.dh = data_handler
        self.logic = logic

    def run(self, config):
        logger.info("Executando Pipeline de Fraude...")
        
        # Leitura com Schemas Explícitos via DataHandler
        df_pedidos = self.dh.load_pedidos(config['paths']['pedidos'])
        df_pagamentos = self.dh.load_pagamentos(config['paths']['pagamentos'])

        # Processamento
        result_df = self.logic.filter_legit_refused_2025(df_pedidos, df_pagamentos)
        
        # Escrita
        self.dh.write_output(result_df, config['paths']['output'])
        logger.info("Pipeline finalizado com sucesso.")