import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, BooleanType, TimestampType
from src.processing.transformations import BusinessLogic

# --- FIXTURE DO SPARK (Integrada no arquivo) ---
@pytest.fixture(scope="session")
def spark_session():
    """Cria uma SparkSession única para os testes."""
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Desafio-Unit-Tests") \
        .getOrCreate()
    yield spark
    spark.stop()

# --- TESTES DA LÓGICA DE NEGÓCIO ---

def test_filter_legit_refused_2025(spark_session):
    """
    Testa se o filtro de pedidos recusados (status=False), 
    sem fraude (fraude=False) e do ano de 2025 está correto.
    """
    logic = BusinessLogic()

    # 1. Preparar dados de Pedidos (CSV)
    pedidos_data = [
        # Caso 1: Válido (2025, SP)
        Row(id_pedido="101", uf="SP", valor_unitario=100.0, quantidade=2, data_criacao="2025-05-20 10:00:00"),
        # Caso 2: Ano Errado (2024)
        Row(id_pedido="102", uf="RJ", valor_unitario=50.0, quantidade=1, data_criacao="2024-12-31 23:59:59"),
        # Caso 3: Válido (2025, MG)
        Row(id_pedido="103", uf="MG", valor_unitario=10.0, quantidade=10, data_criacao="2025-01-01 00:01:00")
    ]
    
    # 2. Preparar dados de Pagamentos (JSON)
    pagamentos_data = [
        # Link com 101: Recusado e Legítimo (Deve passar)
        Row(id_pedido="101", status=False, fraude=False, metodo_pagamento="cartao"),
        # Link com 102: Recusado e Legítimo, mas ano errado (Deve cair)
        Row(id_pedido="102", status=False, fraude=False, metodo_pagamento="pix"),
        # Link com 103: Com fraude (Deve cair)
        Row(id_pedido="103", status=False, fraude=True, metodo_pagamento="boleto")
    ]

    df_ped = spark_session.createDataFrame(pedidos_data)
    df_pag = spark_session.createDataFrame(pagamentos_data)

    # 3. Executar Lógica
    df_resultado = logic.filter_legit_refused_2025(df_ped, df_pag)

    # 4. Verificações (Asserts)
    resultados = df_resultado.collect()
    
    # Deve sobrar apenas 1 registro (o id_pedido 101)
    assert df_resultado.count() == 1
    assert resultados[0]["Estado"] == "SP"
    assert resultados[0]["Valor_Total"] == 200.0 # 100 * 2
    assert "Forma_Pagamento" in df_resultado.columns

def test_schema_columns(spark_session):
    """Verifica se as colunas finais renomeadas estão presentes."""
    logic = BusinessLogic()
    
    df_ped = spark_session.createDataFrame([Row(id_pedido="1", uf="SC", valor_unitario=10.0, quantidade=1, data_criacao="2025-01-01")])
    df_pag = spark_session.createDataFrame([Row(id_pedido="1", status=False, fraude=False, metodo_pagamento="debito")])
    
    df_res = logic.filter_legit_refused_2025(df_ped, df_pag)
    
    colunas_esperadas = ["Estado", "Forma_Pagamento", "Valor_Total", "Data_Pedido"]
    for col in colunas_esperadas:
        assert col in df_res.columns