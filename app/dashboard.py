import streamlit as st
import pandas as pd
import joblib
import numpy as np
import datetime
import plotly.express as px

# 📌 1️⃣ Configuração Inicial
st.set_page_config(page_title="Previsão de Preços de Combustíveis", layout="wide")

# 📌 2️⃣ Carregar modelo treinado com Label Encoding
modelo_path = "./data/models/random_forest_model_label.pkl"
modelo = joblib.load(modelo_path)

# 📌 3️⃣ Carregar LabelEncoders usados no treinamento
encoders_path = "./data/models/label_encoders.pkl"
label_encoders = joblib.load(encoders_path)

# 📌 4️⃣ Carregar as colunas que o modelo espera
colunas_modelo_path = "./data/models/colunas_modelo_label.pkl"
colunas_modelo = joblib.load(colunas_modelo_path)

# 📌 5️⃣ Carregar dados históricos
dados_path = "./data/processed/historico_precos.csv"
df_historico = pd.read_csv(dados_path)

# 📌 6️⃣ Interface do Usuário (Sidebar)
st.sidebar.header("Filtros de Pesquisa")

# Escolha do combustível
combustivel = st.sidebar.selectbox(
    "Selecione o Combustível", df_historico["produto"].unique()
)

# Filtrar estados com base no combustível selecionado
estados_disponiveis = df_historico[df_historico["produto"] == combustivel][
    "estado_sigla"
].unique()
estado = st.sidebar.selectbox(
    "Selecione o Estado", ["Todos"] + list(estados_disponiveis)
)

# Filtrar municípios com base no estado selecionado
municipios_disponiveis = df_historico[
    (df_historico["produto"] == combustivel) & (df_historico["estado_sigla"] == estado)
]["municipio"].unique()

municipio = (
    st.sidebar.selectbox(
        "Selecione o Município", ["Todos"] + list(municipios_disponiveis)
    )
    if estado != "Todos"
    else "Todos"
)

# Escolha da bandeira do posto
bandeiras_disponiveis = df_historico["bandeira"].unique()
bandeira = st.sidebar.selectbox(
    "Selecione a Bandeira", ["Todas"] + list(bandeiras_disponiveis)
)

# Escolher uma data futura para prever preços
data_futura = st.sidebar.date_input(
    "Selecione a Data para Previsão", datetime.date.today()
)


# 📌 7️⃣ Criar função para preparar a entrada da previsão
def preparar_entrada_para_previsao(
    df_historico, combustivel, estado, municipio, bandeira, data_futura
):
    """
    Prepara um DataFrame de entrada com todas as features necessárias para a previsão,
    garantindo que os valores categóricos sejam convertidos corretamente com Label Encoding.
    """

    # Criar um dicionário com valores médios para variáveis numéricas
    entrada = {
        "taxa_usd": df_historico["taxa_usd"].mean(),
        "ipca": df_historico["ipca"].mean(),
        "brent_price": df_historico["brent_price"].mean(),
        "wti_price": df_historico["wti_price"].mean(),
        "selic": df_historico["selic"].mean(),
        "ano": data_futura.year,
        "mes": data_futura.month,
    }

    # Aplicar Label Encoding nas variáveis categóricas
    for coluna, encoder in label_encoders.items():
        if coluna == "produto":
            entrada[coluna] = encoder.transform([combustivel])[0]
        elif coluna == "estado_sigla" and estado != "Todos":
            entrada[coluna] = encoder.transform([estado])[0]
        elif coluna == "municipio" and municipio != "Todos":
            entrada[coluna] = encoder.transform([municipio])[0]
        elif coluna == "bandeira" and bandeira != "Todas":
            entrada[coluna] = encoder.transform([bandeira])[0]
        else:
            entrada[coluna] = -1  # Valor padrão para categorias não selecionadas

    # Converter para DataFrame
    df_entrada = pd.DataFrame([entrada])

    # 📌 Ajustar para garantir que as colunas da entrada sejam iguais às do modelo
    for col in colunas_modelo:
        if col not in df_entrada.columns:
            df_entrada[col] = 0  # Adiciona colunas faltantes com valor 0

    # Remover colunas que não fazem parte do modelo
    df_entrada = df_entrada[colunas_modelo]

    return df_entrada


# Criar o DataFrame de entrada para previsão
entrada_futura = preparar_entrada_para_previsao(
    df_historico, combustivel, estado, municipio, bandeira, data_futura
)

# Fazer previsão usando o modelo treinado com Label Encoding
previsao_futura = modelo.predict(entrada_futura)[0]

st.markdown(
    f"### 📅 Previsão para **{data_futura.strftime('%d/%m/%Y')}**: **R$ {previsao_futura:.2f}**"
)


# 📌 8️⃣ Filtragem de Dados Históricos para Exibição no Streamlit
df_filtrado = df_historico[df_historico["produto"] == combustivel]

if estado != "Todos":
    df_filtrado = df_filtrado[df_filtrado["estado_sigla"] == estado]

if municipio != "Todos":
    df_filtrado = df_filtrado[df_filtrado["municipio"] == municipio]

if bandeira != "Todas":
    df_filtrado = df_filtrado[df_filtrado["bandeira"] == bandeira]

# 📌 Verificar se há dados após a filtragem
if df_filtrado.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
else:
    # 📌 Criar o CSV e botão de download (corrigindo erro de duplicação)
    csv = df_filtrado.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Baixar Dados Filtrados",
        data=csv,
        file_name="previsoes_combustiveis.csv",
        mime="text/csv",
        key="download_filtrado",
    )

# 📌 9️⃣ Gráfico Interativo de Evolução dos Preços + Previsão
df_grafico = df_filtrado[["data_da_coleta", "valor_de_venda"]].copy()
df_grafico["data_da_coleta"] = pd.to_datetime(df_grafico["data_da_coleta"])
df_grafico = df_grafico.groupby("data_da_coleta").mean().reset_index()

# Criar um DataFrame para prever preços futuros desde o início do ano
data_inicio = pd.to_datetime("2025-01-01")
data_futura_lista = pd.date_range(start=data_inicio, end=data_futura, freq="D")

previsoes_futuras = []
for data in data_futura_lista:
    entrada_futura = preparar_entrada_para_previsao(
        df_historico, combustivel, estado, municipio, bandeira, data
    )
    previsao = modelo.predict(entrada_futura)[0]
    previsoes_futuras.append([data, previsao])

df_previsao = pd.DataFrame(
    previsoes_futuras, columns=["data_da_coleta", "valor_de_venda"]
)

# Criar gráfico interativo com Plotly
fig = px.line(
    df_previsao,
    x="data_da_coleta",
    y="valor_de_venda",
    title=f"Evolução e Previsão do Preço do {combustivel}",
    labels={"data_da_coleta": "Data", "valor_de_venda": "Preço Médio (R$)"},
    template="plotly_white",
)

st.plotly_chart(fig, use_container_width=True)

st.success("Aplicação carregada com sucesso! 🚀")
