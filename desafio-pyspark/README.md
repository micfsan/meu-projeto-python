Markdown
# 🚀 Data Engineering PySpark - Desafio de Vendas 2025

Este projeto é um pipeline de dados robusto desenvolvido em **PySpark**, utilizando as melhores práticas de **Engenharia de Software**, como Programação Orientada a Objetos (POO), Injeção de Dependências, Logging profissional e Testes Unitários.

O objetivo principal é gerar um relatório de pedidos de venda de **2025** cujos pagamentos foram **recusados**, mas que a avaliação de fraude classificou como **legítimos**.

---

## 🛠️ Estrutura do Projeto

```text
.
├── config/
│   └── settings.yaml          # Configurações de caminhos e Spark
├── data/
│   └── input/                 # Datasets de Pedidos e Pagamentos (clonados)
├── dist/                      # Pacote distribuível (.whl)
├── src/
│   ├── config/                # Carregamento de configurações
│   ├── io_utils/              # Leitura (CSV/JSON) e Escrita (Parquet)
│   ├── pipeline/              # Orquestrador (Fluxo de execução)
│   ├── processing/            # Regras de Negócio e Transformações
│   ├── session/               # Gerenciamento da SparkSession
│   └── main.py                # Ponto de entrada (Aggregation Root)
├── tests/                     # Testes automatizados com Pytest
├── pyproject.toml             # Metadados de empacotamento
└── requirements.txt           # Dependências do projeto
⚙️ Pré-requisitos
Python 3.8+

Java 8 ou 11 (necessário para o PySpark)

Datasets clonados na pasta data/input/

🚀 Como Executar
1. Instalar Dependências

Bash
pip install -r requirements.txt
2. Configurar o Ambiente

Certifique-se de que os datasets estão nos caminhos definidos em config/settings.yaml. Se necessário, ajuste os paths no arquivo YAML.

3. Rodar o Pipeline

Para executar o processamento e gerar o relatório final:

Bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
python3 src/main.py
🧪 Testes Automatizados
Para garantir que a lógica de negócio está correta (filtros de data, status e fraude), execute os testes unitários:

Bash
export PYTHONPATH=$PYTHONPATH:$(pwd)
pytest tests/test_transformations.py
📦 Empacotamento e Distribuição
Para gerar o arquivo distribuível .whl com a versão correta (0.1.0):

Limpar builds antigos:

Bash
rm -rf dist/ build/ *.egg-info
Gerar o pacote:

Bash
python3 -m build
Instalar o pacote gerado:

Bash
pip install dist/dataeng_pyspark_data_pipeline-0.1.0-py3-none-any.whl