import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from datetime import datetime
import json
from urllib.request import urlopen


st.set_page_config(layout="wide", page_title="Dashboard SRAG - Brasil", page_icon="🇧🇷")


st.markdown("""
<style>
    [data-testid="stMetricValue"] { font-size: 1.8rem; }
    div.stPlotlyChart { border: 1px solid #e6e6e6; border-radius: 5px; padding: 10px; box-shadow: 2px 2px 5px rgba(0,0,0,0.05);}
</style>
""", unsafe_allow_html=True)

DB_USER = "postgres"
DB_PASSWORD = "admin" 
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "esus_srag_db"
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@st.cache_data(ttl=600)
def get_data(query):
    try:
        engine = create_engine(DATABASE_URL, connect_args={'client_encoding': 'utf8'})
        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as e:
        st.error(f"Erro ao conectar no banco: {e}")
        return pd.DataFrame()

@st.cache_data
def get_geojson_brasil():
    url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
    try:
        with urlopen(url) as r: return json.load(r)
    except: return None

with st.spinner('Processando dados do Data Warehouse...'):
    df_perfil = get_data("SELECT * FROM vw_perfil_epidemiologico")
    df_temporal = get_data("SELECT * FROM vw_casos_por_municipio")
    df_vacinacao = get_data("SELECT * FROM vw_vacinacao_por_resultado")
    df_sintomas = get_data("SELECT * FROM vw_sintomas_frequentes")
    df_laboratorio = get_data("SELECT * FROM vw_analise_laboratorial")

    coluna_id_lab = 'source_id' # Nome da coluna que traz o ID

    if not df_laboratorio.empty and coluna_id_lab not in df_laboratorio.columns:
        df_laboratorio[coluna_id_lab] = 1 

    mapa_labs = {
        1: "Laboratório Central de Saúde Pública (LACEN)",
        2: "Instituto Adolfo Lutz",
        3: "Fiocruz",
    }

    if not df_laboratorio.empty:
        df_laboratorio['nome_laboratorio'] = df_laboratorio[coluna_id_lab].map(mapa_labs).fillna("Laboratório Externo/Outro")
    else:
        df_laboratorio['nome_laboratorio'] = []

# --- FILTROS ---
st.sidebar.header("Filtros")

if not df_perfil.empty and 'estado_uf' in df_perfil.columns:
    lista_estados = ['Todos'] + sorted(df_perfil['estado_uf'].dropna().unique().tolist())
else:
    lista_estados = ['Todos']
estado_sel = st.sidebar.selectbox("Estado (UF):", lista_estados)

lista_municipios = ['Todos']
if not df_perfil.empty and 'municipio_nome' in df_perfil.columns:
    if estado_sel != 'Todos':
        cidades = df_perfil[df_perfil['estado_uf'] == estado_sel]['municipio_nome'].unique()
        lista_municipios = ['Todos'] + sorted(cidades.tolist())
    else:
        lista_municipios = ['Todos'] + sorted(df_perfil['municipio_nome'].dropna().unique().tolist())

municipio_sel = st.sidebar.selectbox("Município:", lista_municipios)

# Função de filtro universal
def filtrar(df, estado, municipio):
    if df.empty: return df
    out = df.copy()
    if estado != 'Todos' and 'estado_uf' in out.columns:
        out = out[out['estado_uf'] == estado]
    if municipio != 'Todos' and 'municipio_nome' in out.columns:
        out = out[out['municipio_nome'] == municipio]
    return out

# Aplicando filtros
df_perfil_f = filtrar(df_perfil, estado_sel, municipio_sel)
df_temporal_f = filtrar(df_temporal, estado_sel, municipio_sel)
df_vacina_f = filtrar(df_vacinacao, estado_sel, municipio_sel)
df_sintomas_f = filtrar(df_sintomas, estado_sel, municipio_sel)
df_laboratorio_f = filtrar(df_laboratorio, estado_sel, municipio_sel)

# --- KPIs ---
st.title("🇧🇷 Monitoramento SRAG - Visão Nacional")
st.markdown(f"**Atualizado em:** {datetime.now().strftime('%d/%m/%Y %H:%M')}")
st.markdown("---")

if not df_perfil_f.empty:
    total_conf = df_perfil_f['casos_confirmados'].sum()
    total_ob = df_perfil_f['obitos'].sum()
    total_geral = df_perfil_f['total_casos'].sum()
    letalidade = (total_ob / total_conf * 100) if total_conf > 0 else 0
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Notificações", f"{total_geral:,.0f}".replace(",", "."))
    c2.metric("Confirmados", f"{total_conf:,.0f}".replace(",", "."))
    c3.metric("Óbitos", f"{total_ob:,.0f}".replace(",", "."))
    c4.metric("Letalidade", f"{letalidade:.2f}%")
else:
    st.warning("Sem dados para exibir.")

# abas e graficos
tab1, tab2, tab3, tab4, tab5 = st.tabs(["🗺️ Mapa", "📈 Evolução", "👥 Perfil", "💉 Vacina & Sintomas", "🧪 Laboratório"])

# Chave dinâmica
key_suffix = f"{estado_sel}_{municipio_sel}"

with tab1:
    st.subheader("Distribuição Geográfica")
    if not df_perfil_f.empty:
        df_mapa = df_perfil_f.groupby(['estado_uf'])['casos_confirmados'].sum().reset_index()
        geo = get_geojson_brasil()
        if geo:
            fig_mapa = px.choropleth_mapbox(
                df_mapa, geojson=geo, locations='estado_uf', featureidkey='properties.sigla',
                color='casos_confirmados', color_continuous_scale="Reds",
                mapbox_style="carto-positron", zoom=3, center={"lat": -14.2, "lon": -51.9},
                opacity=0.7, hover_name='estado_uf'
            )
            fig_mapa.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500)
            st.plotly_chart(fig_mapa, use_container_width=True, key=f"mapa_{key_suffix}")

with tab2:
    st.subheader("Curva Epidêmica")
    if not df_temporal_f.empty:
        df_temporal_f['data_notificacao'] = pd.to_datetime(df_temporal_f['data_notificacao'])
        df_line = df_temporal_f.groupby('data_notificacao')[['casos_confirmados', 'obitos']].sum().reset_index()
        
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### 🦠 Casos Confirmados")
            if len(df_line) <= 1 or df_line['casos_confirmados'].sum() == 0:
                 fig_c = px.bar(df_line, x='data_notificacao', y='casos_confirmados')
            else:
                 fig_c = px.area(df_line, x='data_notificacao', y='casos_confirmados', color_discrete_sequence=['#3366CC'])
            fig_c.update_layout(height=400)
            st.plotly_chart(fig_c, use_container_width=True, key=f"chart_casos_{key_suffix}")

        with c2:
            st.markdown("### 💀 Óbitos")
            if len(df_line) <= 1 or df_line['obitos'].sum() == 0:
                fig_d = px.bar(df_line, x='data_notificacao', y='obitos', color_discrete_sequence=['#DC3912'])
            else:
                fig_d = px.area(df_line, x='data_notificacao', y='obitos', color_discrete_sequence=['#DC3912'])
            fig_d.update_layout(height=400)
            st.plotly_chart(fig_d, use_container_width=True, key=f"chart_obitos_{key_suffix}")

with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Sexo")
        if 'sexo' in df_perfil_f.columns and not df_perfil_f.empty:
            df_sexo = df_perfil_f.groupby('sexo')['total_casos'].sum().reset_index()
            fig_sexo = px.pie(df_sexo, values='total_casos', names='sexo', hole=0.5)
            fig_sexo.update_layout(height=400)
            st.plotly_chart(fig_sexo, use_container_width=True, key=f"chart_sexo_{key_suffix}")
            
    with c2:
        st.subheader("Faixa Etária (Confirmados)")
        if 'faixa_etaria' in df_perfil_f.columns and not df_perfil_f.empty:
            df_idade = df_perfil_f.groupby('faixa_etaria')['casos_confirmados'].sum().reset_index()
            fig_idade = px.bar(df_idade, x='faixa_etaria', y='casos_confirmados', color_discrete_sequence=['#FF4B4B'])
            fig_idade.update_layout(height=400)
            st.plotly_chart(fig_idade, use_container_width=True, key=f"chart_idade_{key_suffix}")

with tab4:
    c1, c2 = st.columns([2,1])
    with c1:
        st.subheader("Vacinação vs Desfecho")
        if not df_vacina_f.empty:
         
            cols_group = [c for c in ['status_vacinal', 'classificacao_final'] if c in df_vacina_f.columns]
            if len(cols_group) == 2:
                df_v_agrupado = df_vacina_f.groupby(cols_group)['total_casos'].sum().reset_index()
                fig_vac = px.bar(
                    df_v_agrupado, x='status_vacinal', y='total_casos', 
                    color='classificacao_final', barmode='group',
                    color_discrete_map={'Confirmado Laboratorial': '#FF9900', 'Descartado': '#109618'}
                )
                fig_vac.update_layout(height=400)
                st.plotly_chart(fig_vac, use_container_width=True, key=f"chart_vacina_{key_suffix}")
            else:
                st.warning("Dados de vacinação insuficientes para gerar o gráfico.")
            
    with c2:
        st.subheader("Top Sintomas")
        if not df_sintomas_f.empty:
            df_s_agrupado = df_sintomas_f.groupby('nome_sintoma')['total_ocorrencias'].sum().reset_index()
            df_top = df_s_agrupado.sort_values('total_ocorrencias').tail(10)
            fig_sint = px.bar(df_top, x='total_ocorrencias', y='nome_sintoma', orientation='h')
            fig_sint.update_layout(height=400)
            st.plotly_chart(fig_sint, use_container_width=True, key=f"chart_sintomas_{key_suffix}")

with tab5:
    st.subheader("Análise Laboratorial")
    
    if not df_laboratorio_f.empty:
        # Identifica o Laboratório Principal
        top_lab = df_laboratorio_f.groupby('nome_laboratorio')['total_testes'].sum().reset_index().sort_values('total_testes', ascending=False)
        
        if not top_lab.empty:
            nome_principal = top_lab.iloc[0]['nome_laboratorio']
            total_exams = top_lab.iloc[0]['total_testes']
        else:
            nome_principal = "Dados Indisponíveis"
            total_exams = 0

        st.metric(label="Principal Laboratório Processador", value=nome_principal, delta=f"{total_exams:,.0f} exames processados".replace(",", "."))
        st.markdown("---")

        c1, c2 = st.columns(2)
        
        # GRÁFICO 1: Top Municípios Solicitantes
        with c1:
            st.markdown("#### 📍 Top Municípios Solicitantes")
            df_city = df_laboratorio_f.groupby('municipio_nome')['total_testes'].sum().reset_index()
            df_city_top = df_city.sort_values('total_testes', ascending=True).tail(10)
            
            if not df_city_top.empty:
                fig_city = px.bar(
                    df_city_top, x='total_testes', y='municipio_nome', orientation='h', text_auto='.2s',
                    color_discrete_sequence=['#0083B8']
                )
                fig_city.update_layout(height=500, xaxis_title="Volume de Exames", yaxis_title=None)
                st.plotly_chart(fig_city, use_container_width=True, key=f"chart_muni_{key_suffix}")
            else:
                st.info("Dados de município não disponíveis.")

        # GRÁFICO 2: Volume por Laboratório
        with c2:
            st.markdown("#### 🏥 Volume por Laboratório Executante")
            df_quem_fez = df_laboratorio_f.groupby('nome_laboratorio')['total_testes'].sum().reset_index()
            df_quem_fez_top = df_quem_fez.sort_values('total_testes', ascending=True).tail(15)
            
            if not df_quem_fez_top.empty:
                fig_quem = px.bar(
                    df_quem_fez_top, x='total_testes', y='nome_laboratorio', orientation='h', text_auto='.2s',
                    color_discrete_sequence=['#005F87']
                )
                fig_quem.update_layout(height=500, xaxis_title="Total de Testes", yaxis_title=None)
                st.plotly_chart(fig_quem, use_container_width=True, key=f"chart_lab_exec_{key_suffix}")
            else:
                st.info("Dados de laboratório não disponíveis.")
            
    else:
        st.info("Nenhum dado laboratorial encontrado para os filtros selecionados.")