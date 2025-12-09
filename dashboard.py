import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from datetime import datetime


DB_USER = "postgres"
DB_PASSWORD = "2fast2YOU"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "esus_srag_db"
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# configs iniciais do streamlit
st.set_page_config(layout="wide", page_title="Dashboard SRAG - e-SUS Notifica")


# pega o cache do streamlit e consulta nas iterações do bd
@st.cache_data(ttl=600) # Cache por 600 segundos (10 minutos)
def get_data_from_db(query):
    """Cria a conexão e executa uma query SQL."""
    try:
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ou executar query no banco de dados: {e}")
        return pd.DataFrame()


# View 1: Casos por Município (para série temporal e mapa)
df_casos_municipio = get_data_from_db("SELECT * FROM vw_casos_por_municipio")

# View 2: Vacinação por Resultado
df_vacinacao = get_data_from_db("SELECT * FROM vw_vacinacao_por_resultado")

# View 3: Sintomas Frequentes
df_sintomas = get_data_from_db("SELECT * FROM vw_sintomas_frequentes")

# Indicadores (Tabela atualizada pela Stored Function)
df_indicadores = get_data_from_db("SELECT * FROM indicadores_municipais")


st.title("🦠 Dashboard Analítico: Notificações de Síndrome Gripal (SRAG)")
st.caption(f"Dados atualizados em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

# Criando filtro lateral p/ estado
if not df_casos_municipio.empty:
    estados_unicos = df_casos_municipio['estado_uf'].unique()
    estado_selecionado = st.sidebar.selectbox(
        "Selecione o Estado:",
        options=['Todos'] + list(estados_unicos)
    )
    
    if estado_selecionado != 'Todos':
        df_casos_filtrado = df_casos_municipio[df_casos_municipio['estado_uf'] == estado_selecionado]
    else:
        df_casos_filtrado = df_casos_municipio
else:
    st.warning("Não foi possível carregar os dados de casos por município.")
    df_casos_filtrado = pd.DataFrame()

#Indicadores principais
if not df_casos_filtrado.empty:
    
    
    total_notificacoes = df_casos_filtrado['total_notificacoes'].sum()
    total_confirmados = df_casos_filtrado['casos_confirmados'].sum()
    total_obitos = df_casos_filtrado['obitos'].sum()
    
    # Cálculo da Taxa de Positividade (Baseado apenas em casos com resultado final)
    taxa_positividade = (total_confirmados / (df_casos_filtrado['casos_descartados'].sum() + total_confirmados)) * 100
    
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("Total de Notificações", f"{total_notificacoes:,}".replace(",", "."))
    col2.metric("Casos Confirmados", f"{total_confirmados:,}".replace(",", "."), f"{taxa_positividade:.2f}% Positividade")
    col3.metric("Óbitos Registrados", f"{total_obitos:,}".replace(",", "."))
    col4.metric("Taxa de Letalidade", f"{ (total_obitos / total_notificacoes) * 100:.2f}%")

st.markdown("---")

# sessão dos gráficos

# temporalidade dos casos 
st.header("Evolução Diária de Casos Confirmados")

# Agrega por data para a série temporal
df_serie = df_casos_filtrado.groupby('data_notificacao')['casos_confirmados'].sum().reset_index()
fig_serie = px.line(
    df_serie, 
    x='data_notificacao', 
    y='casos_confirmados', 
    title='Casos Confirmados ao Longo do Tempo',
    labels={'data_notificacao': 'Data da Notificação', 'casos_confirmados': 'Total de Casos Confirmados'}
)
st.plotly_chart(fig_serie, use_container_width=True)


# distribuicao de vacinação x classificação
st.header("Relação Vacinação vs. Classificação Final")

if not df_vacinacao.empty:
    fig_vacinacao = px.bar(
        df_vacinacao,
        x='status_vacinal',
        y='total_casos',
        color='classificacao_final',
        barmode='group',
        title='Contagem de Casos por Status Vacinal e Classificação',
        labels={'status_vacinal': 'Status Vacinal', 'total_casos': 'Total de Casos', 'classificacao_final': 'Classificação Final'},
        height=500
    )
    st.plotly_chart(fig_vacinacao, use_container_width=True)


# Sintomas frequentes
st.header("Top 10 Sintomas Mais Frequentes (Casos Confirmados)")

if not df_sintomas.empty:
    df_top_sintomas = df_sintomas.sort_values(by='total_ocorrencias', ascending=False).head(10)
    fig_sintomas = px.bar(
        df_top_sintomas,
        x='total_ocorrencias',
        y='nome_sintoma',
        orientation='h',
        title='Sintomas Mais Comuns em Casos Confirmados',
        labels={'total_ocorrencias': 'Total de Ocorrências', 'nome_sintoma': 'Sintoma'},
        color='percentual_casos_confirmados'
    )
    st.plotly_chart(fig_sintomas, use_container_width=True)

st.markdown("---")

# Indicadores regionais (por município)
st.header("Taxa de Positividade por Município (tabela gerada pela Stored Function)")

if not df_indicadores.empty:
    # Por simplicidade, exibiremos o DF de indicadores se a tabela não estiver vazia
    st.dataframe(df_indicadores, use_container_width=True)
else:
    st.info("A tabela de indicadores está vazia. Rode a função fx_calcular_taxa_positividade() no seu PGAdmin para popular a tabela.")