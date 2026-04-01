spark:
  app_name: "Relatorio_Fraude_Legitima_2025"
paths:
  pedidos: "./data/input/pedidos/data/pedidos/"
  pagamentos: "./data/input/pagamentos/data/pagamentos/"
  output: "./data/output/relatorio_final"
  
import yaml
import os

def load_config(config_path: str = "config/settings.yaml") -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Arquivo de configuração não encontrado em: {config_path}")
        
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)