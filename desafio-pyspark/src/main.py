import logging
from src.config.settings import load_config
from src.session.spark_session import SparkManager
from src.io_utils.data_handler import DataHandler
from src.processing.transformations import BusinessLogic
from src.pipeline.pipeline import FraudPipeline

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    setup_logging()
    config = load_config()
    
    # Instanciando dependências
    spark_man = SparkManager(config['spark']['app_name'])
    spark = spark_man.get_session()
    
    dh = DataHandler(spark)
    logic = BusinessLogic()
    
    # Injetando dependências no Pipeline
    pipeline = FraudPipeline(spark, dh, logic)
    
    try:
        pipeline.run(config)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()