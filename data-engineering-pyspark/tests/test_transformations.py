# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from src.processing.transformations import Transformation

@pytest.fixture(scope="session")
def spark_session():
    """
    Cria uma SparkSession para ser usada em todos os testes.
    A sessão é finalizada automaticamente ao final da execução dos testes.
    """
    spark = SparkSession.builder \
        .appName("PySpark Unit Tests") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_add_valor_total_pedidos(spark_session):
    """
    Testa a função add_valor_total_pedidos para garantir que a coluna 'valor_total'
    é calculada corretamente.
    """
    # 1. Arrange (Preparar os dados de entrada e o resultado esperado)
    transformer = Transformation()

    schema_entrada = StructType([
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
    ])
    dados_entrada = [
        ("Produto A", 10.0, 2),
        ("Produto B", 5.5, 3),
        ("Produto C", 100.0, 1)
    ]
    df_entrada = spark_session.createDataFrame(dados_entrada, schema_entrada)

    schema_esperado = StructType([
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
        StructField("valor_total", FloatType(), True)
    ])
    dados_esperados = [
        ("Produto A", 10.0, 2, 20.0),
        ("Produto B", 5.5, 3, 16.5),
        ("Produto C", 100.0, 1, 100.0)
    ]
    df_esperado = spark_session.createDataFrame(dados_esperados, schema_esperado)

    # 2. Act (Executar a função a ser testada)
    df_resultado = transformer.add_valor_total_pedidos(df_entrada)

    # 3. Assert (Verificar se o resultado é o esperado)
    # Coletamos os dados dos DataFrames para comparar como listas de dicionários
    resultado_coletado = sorted([row.asDict() for row in df_resultado.collect()], key=lambda x: x['produto'])
    esperado_coletado = sorted([row.asDict() for row in df_esperado.collect()], key=lambda x: x['produto'])

    assert df_resultado.count() == df_esperado.count(), "O número de linhas não corresponde ao esperado."
    assert df_resultado.columns == df_esperado.columns, "As colunas não correspondem ao esperado."
    assert resultado_coletado == esperado_coletado, "O conteúdo dos DataFrames não é igual."
def test_get_top_10_clientes(spark_session):
    """
    Testa a função get_top_10_clientes para garantir que ela agrupa,
    soma os valores totais por cliente e retorna apenas os 10 maiores,
    ordenados corretamente.
    """
    # 1. Arrange
    transformer = Transformation()

    schema_entrada = StructType([
        StructField("id_cliente", LongType(), True),
        StructField("valor_total", FloatType(), True),
    ])
    # Criando 12 clientes para garantir que o limit(10) funcione
    dados_entrada = [
        (1, 100.0), (2, 200.0), (1, 50.0),   # Cliente 1: total 150.0
        (3, 300.0), (4, 400.0), (5, 500.0),
        (6, 600.0), (7, 700.0), (8, 800.0),
        (9, 900.0), (10, 1000.0), (11, 1100.0),
        (12, 50.0)
    ]
    df_entrada = spark_session.createDataFrame(dados_entrada, schema_entrada)

    # O resultado esperado deve conter os 10 clientes com maiores valores,
    # ordenados de forma decrescente.
    schema_esperado = StructType([
        StructField("id_cliente", LongType(), True),
        StructField("valor_total", FloatType(), True)
    ])
    dados_esperados = [
        (11, 1100.0),
        (10, 1000.0),
        (9, 900.0),
        (8, 800.0),
        (7, 700.0),
        (6, 600.0),
        (5, 500.0),
        (4, 400.0),
        (3, 300.0),
        (2, 200.0) # Cliente 1 (total 150.0) e 12 (total 50.0) devem ficar de fora
    ]
    df_esperado = spark_session.createDataFrame(dados_esperados, schema_esperado)

    # 2. Act
    df_resultado = transformer.get_top_10_clientes(df_entrada)

    # 3. Assert
    # A ordem é importante neste teste, então coletamos os dados como estão
    resultado_coletado = [row.asDict() for row in df_resultado.collect()]
    esperado_coletado = [row.asDict() for row in df_esperado.collect()]

    assert df_resultado.count() == 10, "O DataFrame resultante deve ter exatamente 10 linhas."
    assert df_resultado.columns == df_esperado.columns, "As colunas não correspondem ao esperado."
    assert resultado_coletado == esperado_coletado, "Os dados dos 10 maiores clientes não correspondem ao esperado."
