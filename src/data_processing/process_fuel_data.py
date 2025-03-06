import pandas as pd
import os

# Diretórios de entrada e saída
data_dir = "data/raw/combustiveis/"
processed_dir = "data/processed/"
log_dir = "data/logs/"
processed_file = os.path.join(processed_dir, "precos_combustiveis_unificado.csv")


# Função para criar diretórios dinamicamente
def create_directories():
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)


# Função para registrar logs
def write_log(message):
    log_file = os.path.join(log_dir, "unification_log.txt")
    with open(log_file, "a") as log:
        log.write(message + "\n")


# Função para ler arquivos CSV com fallback de encoding
def read_csv_with_fallback(file_path):
    try:
        return pd.read_csv(file_path, delimiter=";", encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(
            file_path, delimiter=";", encoding="cp1252", low_memory=False
        )


# Função para validar tipos de dados
def validate_data_types(df):
    if not pd.api.types.is_datetime64_any_dtype(df["Data da Coleta"]):
        write_log("Coluna 'Data da Coleta' não está no formato datetime.")
    if not pd.api.types.is_numeric_dtype(df["Valor de Venda"]):
        write_log("Coluna 'Valor de Venda' não está no formato numérico.")


# Função para remover outliers
def remove_outliers(df, column, lower_limit, upper_limit):
    initial_count = len(df)
    df = df[(df[column] >= lower_limit) & (df[column] <= upper_limit)]
    removed_count = initial_count - len(df)
    write_log(f"{removed_count} registros removidos como outliers na coluna {column}.")
    return df


# Função para processar cada arquivo
def process_file(file_name):
    file_path = os.path.join(data_dir, file_name)
    print(f"Lendo arquivo: {file_name}")
    try:
        df = read_csv_with_fallback(file_path)

        # Verificar se contém as colunas esperadas
        expected_columns = {
            "Regiao - Sigla",
            "Estado - Sigla",
            "Municipio",
            "Produto",
            "Valor de Venda",
            "Data da Coleta",
        }
        if not expected_columns.issubset(df.columns):
            write_log(
                f"Aviso: O arquivo {file_name} não contém todas as colunas esperadas."
            )
            return None

        # Tratar colunas específicas
        df["Data da Coleta"] = pd.to_datetime(
            df["Data da Coleta"], format="%d/%m/%Y", errors="coerce"
        )
        df["Valor de Venda"] = df["Valor de Venda"].str.replace(",", ".").astype(float)

        # Remover colunas irrelevantes
        cols_to_drop = ["Complemento", "Valor de Compra"]
        df.drop(
            columns=[col for col in cols_to_drop if col in df.columns], inplace=True
        )

        # Tratar valores nulos
        df["Numero Rua"] = df["Numero Rua"].fillna("Desconhecido")
        df["Bairro"] = df["Bairro"].fillna("Bairro Não Informado")

        # Validar tipos de dados
        validate_data_types(df)

        # Remover outliers em 'Valor de Venda'
        df = remove_outliers(df, "Valor de Venda", lower_limit=1, upper_limit=50)

        return df
    except Exception as e:
        write_log(f"Erro ao processar {file_name}: {str(e)}")
        return None


# Função principal para unificar os arquivos
def unify_files():
    data_frames = []

    for file_name in os.listdir(data_dir):
        if file_name.endswith(".csv"):
            df = process_file(file_name)
            if df is not None:
                data_frames.append(df)

    if not data_frames:
        print("Nenhum arquivo válido encontrado para unificação.")
        return

    # Concatenar DataFrames
    combined_df = pd.concat(data_frames, ignore_index=True)

    # Remover colunas duplicadas
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]

    # Renomear colunas problemáticas
    if "﻿Regiao - Sigla" in combined_df.columns:
        combined_df.rename(columns={"﻿Regiao - Sigla": "Regiao - Sigla"}, inplace=True)

    # Filtrar apenas os dados do estado do Mato Grosso do Sul (MS)
    # combined_df = combined_df[combined_df["Estado - Sigla"] == "MS"]

    # Filtrar apenas os combustíveis relevantes (Gasolina, Diesel e Etanol)
    combustiveis_filtrados = ["GASOLINA", "DIESEL", "ETANOL"]
    combined_df = combined_df[combined_df["Produto"].isin(combustiveis_filtrados)]

    # Validar consistência e salvar o arquivo final
    print("Resumo de valores nulos:")
    print(combined_df.isnull().sum())

    # Estatísticas básicas
    print("Estatísticas descritivas de 'Valor de Venda':")
    print(combined_df["Valor de Venda"].describe())

    combined_df.to_csv(processed_file, index=False, encoding="utf-8")
    print(f"Arquivo unificado salvo em: {processed_file}")
    write_log("Processo de unificação concluído com sucesso.")


if __name__ == "__main__":
    create_directories()
    unify_files()
