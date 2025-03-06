import requests
import yfinance as yf
import pandas as pd
import os
from datetime import datetime
import time

# Diretório para armazenar os arquivos brutos
RAW_DATA_DIR = "data/raw/"
os.makedirs(RAW_DATA_DIR, exist_ok=True)


# --------------------------------------------
# Funções auxiliares
# --------------------------------------------
def fetch_api_data(url, max_retries=5, delay=10):
    """Faz uma requisição GET a uma URL com re-tentativas e retorna o JSON da resposta."""
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(
                    f"⚠️ Erro {response.status_code} ao acessar {url}. Tentando novamente em {delay} segundos."
                )
                time.sleep(delay)
        except Exception as e:
            print(f"⚠️ Erro na requisição: {e}. Tentando novamente em {delay} segundos.")
            time.sleep(delay)
        retries += 1

    print(f"❌ Falha ao obter dados da API após {max_retries} tentativas.")
    return None


def save_to_csv(df, filename):
    """Salva um DataFrame como CSV na pasta raw."""
    filepath = os.path.join(RAW_DATA_DIR, filename)
    df.to_csv(filepath, index=False, encoding="utf-8")
    print(f"✅ Dados salvos em {filepath}")


def load_existing_csv(filename):
    """Carrega um CSV existente se disponível."""
    filepath = os.path.join(RAW_DATA_DIR, filename)
    if os.path.exists(filepath):
        print(f"📂 Carregando dados de {filepath}")
        return pd.read_csv(filepath, parse_dates=["Date"])
    return None


# --------------------------------------------
# Funções principais de busca
# --------------------------------------------
def fetch_exchange_rate_historical(currency, start_year, end_year):
    """Busca dados históricos da taxa de câmbio para um intervalo de anos."""
    filename = f"exchange_rate_{currency}_{start_year}_{end_year}.csv"
    existing_data = load_existing_csv(filename)
    if existing_data is not None:
        return existing_data

    all_data = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        start_date = f"{year}0101"
        end_date = f"{year}1231"
        url = f"https://economia.awesomeapi.com.br/json/daily/{currency}-BRL/365?start_date={start_date}&end_date={end_date}"
        data = fetch_api_data(url)

        if data:
            df = pd.DataFrame(
                [
                    {
                        "Date": datetime.fromtimestamp(int(item["timestamp"])).strftime(
                            "%Y-%m-%d"
                        ),
                        f"taxa_{currency.lower()}": float(item["bid"]),
                    }
                    for item in data
                ]
            )
            all_data = pd.concat([all_data, df], ignore_index=True)

    save_to_csv(all_data, filename)
    return all_data


def fetch_inflation_data(start_date="2019-01-01", end_date="2024-12-31"):
    """Busca dados históricos do IPCA (inflação acumulada mensal)."""
    filename = "inflation_data.csv"
    existing_data = load_existing_csv(filename)
    if existing_data is not None:
        return existing_data

    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json"
    data = fetch_api_data(url)

    if data:
        df = pd.DataFrame(data)
        df["Date"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
        df["ipca"] = df["valor"].astype(float)
        df = df[["Date", "ipca"]].query(f"'{start_date}' <= Date <= '{end_date}'")
        save_to_csv(df, filename)
        return df

    return pd.DataFrame(columns=["Date", "ipca"])


import os
import time
import pandas as pd
import yfinance as yf


def fetch_oil_prices(start_date, end_date):
    """
    Busca os preços históricos do petróleo (Brent e WTI) utilizando yfinance,
    garantindo que cada série de dados seja obtida separadamente e mesclada posteriormente.
    """

    def get_price_data(ticker, price_name):
        """
        Baixa os preços do petróleo para um ticker específico e retorna um DataFrame formatado.
        """
        for attempt in range(5):  # Máximo de 5 tentativas em caso de erro
            try:
                df = yf.download(
                    ticker, start=start_date, end=end_date, auto_adjust=False
                )

                # Verifica se há colunas de preço e renomeia corretamente
                for col in ["Adj Close", "Close"]:
                    if col in df.columns:
                        df = df[[col]].rename(columns={col: price_name})
                        break
                else:
                    raise KeyError(f"Colunas esperadas não encontradas para {ticker}")

                df.reset_index(
                    inplace=True
                )  # Garante que a "Date" seja uma coluna normal

                # 🔄 Removendo MultiIndex se existir
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                print(f"✅ {ticker} baixado com sucesso.")
                return df

            except Exception as e:
                print(
                    f"⚠️ Erro ao buscar preços de {ticker} (tentativa {attempt+1}/5): {e}"
                )
                if attempt < 4:
                    print("Tentando novamente em 60 segundos...")
                    time.sleep(60)
                else:
                    print(
                        f"❌ Falha ao obter dados de {ticker} após múltiplas tentativas."
                    )
                    return (
                        pd.DataFrame()
                    )  # Retorna um DataFrame vazio se falhar todas as tentativas

    # 🔄 Buscar os preços separadamente
    brent = get_price_data("BZ=F", "brent_price")
    wti = get_price_data("CL=F", "wti_price")

    # 🚨 Corrigindo o problema do MultiIndex (Ticker sendo interpretado como nível)
    if "Ticker" in brent.columns:
        brent.drop(columns=["Ticker"], inplace=True)
    if "Ticker" in wti.columns:
        wti.drop(columns=["Ticker"], inplace=True)

    # 🏗️ Mesclar os preços do Brent e WTI pela coluna "Date"
    oil_prices = pd.merge(brent, wti, on="Date", how="outer")

    # 🚨 Certificar-se de que não há MultiIndex
    if isinstance(oil_prices.index, pd.MultiIndex):
        oil_prices = oil_prices.reset_index(drop=True)

    # 📂 Salvar os dados brutos na pasta "data/raw/"
    raw_data_dir = "data/raw"
    os.makedirs(raw_data_dir, exist_ok=True)  # Garante que o diretório existe
    file_path = os.path.join(raw_data_dir, "oil_prices.csv")
    oil_prices.to_csv(file_path, index=False, encoding="utf-8")

    print(f"✅ Dados do petróleo salvos em '{file_path}'.")

    return oil_prices


def fetch_selic_data(start_date, end_date):
    """Busca os dados históricos da Taxa Selic entre duas datas especificadas."""
    filename = "selic_data.csv"
    existing_data = load_existing_csv(filename)
    if existing_data is not None:
        return existing_data

    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json"
    data = fetch_api_data(url)

    if data:
        df = pd.DataFrame(data)
        df["Date"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
        df["selic"] = df["valor"].astype(float)
        df = df[["Date", "selic"]].query(f"'{start_date}' <= Date <= '{end_date}'")
        save_to_csv(df, filename)
        return df

    return pd.DataFrame(columns=["Date", "selic"])


# --------------------------------------------
# Integração dos dados macroeconômicos
# --------------------------------------------
def integrate_macroeconomic_data(
    exchange_rates, inflation_data, oil_prices, selic_data
):
    """Mescla todos os dados macroeconômicos em um único DataFrame."""

    dfs = {
        "exchange_rates": exchange_rates,
        "inflation_data": inflation_data,
        "oil_prices": oil_prices,
        "selic_data": selic_data,
    }

    # 🚨 Debug: Verifica se os DataFrames estão vazios antes do merge
    if (
        exchange_rates.empty
        or inflation_data.empty
        or oil_prices.empty
        or selic_data.empty
    ):
        print("⚠️ Atenção: Um ou mais DataFrames estão vazios antes da integração!")
        print(f"exchange_rates: {exchange_rates.shape}")
        print(f"inflation_data: {inflation_data.shape}")
        print(f"oil_prices: {oil_prices.shape}")
        print(f"selic_data: {selic_data.shape}")

    # Verificando os índices antes do merge
    for name, df in dfs.items():
        print(f"\n📊 Analisando {name}...")
        print(f"Índices: {df.index.names}")
        print(df.head())

        # Resetar índice caso seja MultiIndex
        if isinstance(df.index, pd.MultiIndex):
            print(f"⚠️ {name} tem MultiIndex! Resetando índice...")
            df.reset_index(inplace=True)

        # Garantir que a coluna "Date" é datetime
        df["Date"] = pd.to_datetime(df["Date"])
        df.reset_index(drop=True, inplace=True)

    # Mesclar DataFrames
    merged_data = pd.merge(exchange_rates, inflation_data, on="Date", how="outer")
    merged_data = pd.merge(merged_data, oil_prices, on="Date", how="outer")
    merged_data = pd.merge(merged_data, selic_data, on="Date", how="outer")

    # Organizar os dados por data
    merged_data = merged_data.sort_values(by="Date").reset_index(drop=True)
    merged_data.ffill(inplace=True)

    return merged_data
