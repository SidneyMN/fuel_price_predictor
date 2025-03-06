import os
import sys
import pandas as pd
from datetime import datetime

# Adicionar o diretório 'src' ao caminho do sistema
sys.path.append(os.path.abspath("../src"))

# Importar as funções do script fetch_macroeconomic_data.py
from fetch_macroeconomic_data import (
    fetch_exchange_rate_historical,
    fetch_inflation_data,
    fetch_oil_prices,
    fetch_selic_data,
    integrate_macroeconomic_data,
)

# Definir intervalo de datas
start_date = "2018-01-01"
end_date = "2025-01-01"

# Criar pasta de saída caso não exista
output_dir = "data/processed/"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "macroeconomic_data.csv")


def run_macroeconomic_pipeline():
    """
    Executa a coleta e integração dos dados macroeconômicos e salva em um arquivo CSV.
    """

    print("🔄 Buscando dados de câmbio (USD)...")
    exchange_rates = fetch_exchange_rate_historical("USD", 2019, 2025)

    print("🔄 Buscando dados de inflação (IPCA)...")
    inflation_data_df = fetch_inflation_data(start_date, end_date)

    print("🔄 Buscando preços do petróleo (Brent e WTI)...")
    oil_prices = fetch_oil_prices(start_date, end_date)

    print("🔄 Buscando taxa Selic...")
    selic_data = fetch_selic_data(start_date, end_date)

    print("🔄 Integrando dados macroeconômicos...")
    consolidated_data = integrate_macroeconomic_data(
        exchange_rates, inflation_data_df, oil_prices, selic_data
    )

    print(consolidated_data.head())
    # Filtrar para garantir que os dados começam em 2019
    consolidated_data = consolidated_data[
        consolidated_data["Date"] >= "2019-01-01"
    ].copy()

    print(f"💾 Salvando os dados macroeconômicos em '{output_file}'...")
    consolidated_data.to_csv(output_file, index=False, encoding="utf-8")

    print("✅ Processo concluído com sucesso!")


if __name__ == "__main__":
    run_macroeconomic_pipeline()
