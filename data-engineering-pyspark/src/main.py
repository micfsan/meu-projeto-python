# src/main.py
import logging
from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from pipeline.pipeline import Pipeline

logger = logging.getLogger(__name__)


# Crie a configuração do logging
def configurar_logging():
    """Configura o logging para todo o projeto."""
    logging.basicConfig(
        level=logging.INFO,
        # Formato da mensagem de log.
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        # Lista de handlers. Aqui, estamos logando para um arquivo e para o console.
        handlers=[
            logging.FileHandler("dataeng-pyspark-poo.log"),  # Log para arquivo
            logging.StreamHandler(),  # Log para o console (terminal)
        ],
    )
    logging.info("Logging configurado.")


def main():
    # ... carregamento de config ...
    config = carregar_config()
    app_name = config["spark"]["app_name"]

    spark = None  # Inicializa como None para segurança no finally
    try:
        spark = SparkSessionManager.get_spark_session(app_name=app_name)
        pipeline = Pipeline(spark)
        pipeline.run(config=config)

    except Exception as e:
        logging.error(f"FALHA CRÍTICA NO PIPELINE: {e}")
        # Aqui poderíamos adicionar envio de notificação (Slack, Email, PagerDuty)

    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    configurar_logging()
    main()
