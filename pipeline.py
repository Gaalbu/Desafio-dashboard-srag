import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import psycopg2 

DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "localhost" 
DB_PORT = "5432"
DB_NAME = "esus_srag_db"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

"""
    Normaliza as 4 colunas de testes.
    ALTERAÇÃO: Agora buscamos o TEXTO do resultado (resultadoTeste) e não mais o código.
"""
def process_testes_realizados(df):
    print("Processando Fato Testes Realizados (Unpivot)...")

    test_metrics = [
        'codigoEstadoTeste', 
        'codigoTipoTeste', 
        'codigoFabricanteTeste', 
        'dataColetaTeste'
    ]
    
    id_vars = ['id_notificacao']
    
    value_vars = [f'{metric}{i}' for i in range(1, 5) for metric in test_metrics]
    
    cols_present = [c for c in value_vars if c in df.columns]
    
    df_testes = df[id_vars + cols_present].copy()

    df_long = pd.melt(
        df_testes,
        id_vars=['id_notificacao'],
        value_vars=cols_present,
        var_name='test_variable',
        value_name='test_value'
    ).dropna(subset=['test_value']) 

    df_long['test_number'] = df_long['test_variable'].str[-1].astype(int)
    df_long['metric_name'] = df_long['test_variable'].str[:-1]

    df_final = df_long.pivot_table(
        index=['id_notificacao', 'test_number'],
        columns='metric_name',
        values='test_value',
        aggfunc='first'
    ).reset_index()

    df_final.columns.name = None
    
    df_final = df_final.rename(columns={
        'codigoEstadoTeste': 'codigo_estado_teste',
        'codigoTipoTeste': 'codigo_tipo_teste',
        'codigoFabricanteTeste': 'codigo_fabricante_teste',
        'dataColetaTeste': 'data_coleta'
    })
    
    df_final['id_registro'] = df_final.index + 1
    
    print(f"Fato_Testes_Realizados criada com {len(df_final)} testes individuais.")
    return df_final


"""
    Cria a Dim_Localidades (Mantido igual)
"""
def process_localidades(df):
    print("Processando Dimensão Localidades...")
    
    df_residencia = df[['estado', 'estadoIBGE', 'municipio', 'municipioIBGE']].copy()
    df_residencia.columns = ['estado_nome', 'estado_uf', 'municipio_nome', 'codigo_ibge_municipio']
    
    df_notificacao = df[['estadoNotificacao', 'estadoNotificacaoIBGE', 'municipioNotificacao', 'municipioNotificacaoIBGE']].copy()
    df_notificacao.columns = ['estado_nome', 'estado_uf', 'municipio_nome', 'codigo_ibge_municipio']
    
    dim_localidades = pd.concat([df_residencia, df_notificacao])
    dim_localidades = dim_localidades.dropna(subset=['codigo_ibge_municipio'])
    dim_localidades['codigo_ibge_municipio'] = dim_localidades['codigo_ibge_municipio'].astype(int)
    dim_localidades = dim_localidades.drop_duplicates(subset=['codigo_ibge_municipio']).reset_index(drop=True)
    dim_localidades['codigo_ibge_estado'] = dim_localidades['codigo_ibge_municipio'].astype(str).str[:2].astype(int)
    dim_localidades['id_localidade'] = dim_localidades.index + 1
    
    dim_localidades = dim_localidades[[
        'id_localidade', 'estado_uf', 'estado_nome', 'codigo_ibge_estado', 
        'municipio_nome', 'codigo_ibge_municipio'
    ]]
    
    print(f"Dim_Localidades criada com {len(dim_localidades)} registros.")
    return dim_localidades

"""
    Tratamento de Nulos (Mantido igual)
"""
def intelligent_null_imputation(df):
    print("Iniciando tratamento de nulos...")
    
    if 'codigoResultadoTeste1' in df.columns:
        df['classificacaoFinal'] = np.where(
            (df['classificacaoFinal'].isnull()) & (df['codigoResultadoTeste1'] == 1),
            'Confirmado Laboratorial',
            df['classificacaoFinal']
        )
        df['classificacaoFinal'] = np.where(
            (df['classificacaoFinal'].isnull()) & (df['codigoResultadoTeste1'] == 2),
            'Descartado',
            df['classificacaoFinal']
        )
    
    df['classificacaoFinal'] = df['classificacaoFinal'].fillna('Suspeito')

    df['dataInicioSintomas'] = np.where(
        df['dataInicioSintomas'].isnull(),
        df['dataNotificacao'] - pd.Timedelta(days=1),
        df['dataInicioSintomas']
    )
    
    mask_erro_data = df['dataInicioSintomas'] > df['dataNotificacao']
    df.loc[mask_erro_data, 'dataInicioSintomas'] = df.loc[mask_erro_data, 'dataNotificacao']
    
    mask_erro_fim = (df['dataEncerramento'] < df['dataNotificacao']) & (df['dataEncerramento'].notnull())
    df.loc[mask_erro_fim, 'dataEncerramento'] = pd.NaT 

    median_age = df['idade'].median()
    df['idade'] = df['idade'].fillna(median_age).astype(int)
    df['sexo'] = df['sexo'].fillna('IGNORADO')
    df['racaCor'] = df['racaCor'].fillna('NAO INFORMADO')
    df['evolucaoCaso'] = df['evolucaoCaso'].fillna('EM ABERTO')
    
    print("Tratamento concluído.")
    return df

"""
    Leitura CSV (Mantido igual)
"""
def extract_and_initial_transform(file_path):
    print(f"Lendo arquivo: {file_path}")
    COLUNAS_DESCARTADAS = [
        'source_id', 'excluido', 'validado',
        'outroBuscaAtivaAssintomatico', 'outroTriagemPopulacaoEspecifica', 'outroLocalRealizacaoTestagem'
    ]
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em {file_path}")
        return None

    df = df.drop(columns=COLUNAS_DESCARTADAS, errors='ignore')
    
    date_cols = [col for col in df.columns if 'data' in col.lower()]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
        
    print(f"Registros lidos: {len(df)}")
    return df

"""
    Normalização multivalorada (Mantido igual)
"""
def normalize_multivalued_data(df, column_name, dim_name):
    df_temp = df[['id_notificacao', column_name]].copy()
    df_temp = df_temp.rename(columns={column_name: dim_name})
    df_temp = df_temp.dropna(subset=[dim_name])
    
    df_exploded = (df_temp[dim_name]
                    .str.split(', ')
                    .explode()
                    .to_frame())
    
    df_exploded = df_exploded.merge(df_temp[['id_notificacao']], left_index=True, right_index=True)
    df_exploded[dim_name] = df_exploded[dim_name].str.strip()
    df_exploded = df_exploded.drop_duplicates(subset=['id_notificacao', dim_name]).reset_index(drop=True)
    
    return df_exploded


"""
    PIPELINE PRINCIPAL (Com ajustes de mapeamento)
"""
def run_etl_pipeline(file_path):
    
    df_raw = extract_and_initial_transform(file_path)
    if df_raw is None: return

    df_raw['id_notificacao'] = df_raw.index + 1
    
    df_clean = intelligent_null_imputation(df_raw.copy())
    df_clean = df_clean.dropna(subset=['dataNotificacao'])

    df_sintomas_exploded = normalize_multivalued_data(df_clean, 'sintomas', 'nome_sintoma')
    dim_sintomas = df_sintomas_exploded[['nome_sintoma']].drop_duplicates().reset_index(drop=True)
    dim_sintomas['id_sintoma'] = dim_sintomas.index + 1
    
    df_condicoes_exploded = normalize_multivalued_data(df_clean, 'condicoes', 'nome_condicao')
    dim_condicoes = df_condicoes_exploded[['nome_condicao']].drop_duplicates().reset_index(drop=True)
    dim_condicoes['id_condicao'] = dim_condicoes.index + 1
    
    dim_raca_cor = df_clean[['racaCor']].drop_duplicates().dropna().reset_index(drop=True)
    dim_raca_cor['id_raca_cor'] = dim_raca_cor.index + 1
    dim_raca_cor = dim_raca_cor.rename(columns={'racaCor': 'descricao_raca_cor'})
    
    dim_localidades = process_localidades(df_clean)
    
    dim_evolucao = df_clean[['evolucaoCaso']].drop_duplicates().dropna().reset_index(drop=True)
    dim_evolucao['id_evolucao'] = dim_evolucao.index + 1
    dim_evolucao = dim_evolucao.rename(columns={'evolucaoCaso': 'descricao_evolucao'})

    df_fato_testes_realizados = process_testes_realizados(df_clean)

    df_fato_testes_realizados = df_fato_testes_realizados.rename(columns={
        'id_notificacao': 'fk_notificacao',
        'codigo_tipo_teste': 'fk_tipo_teste',
        'codigo_fabricante_teste': 'fk_fabricante',
    })
    
    colunas_banco_testes = [
        'fk_notificacao', 
        'data_coleta', 
        'codigo_estado_teste', 
        'fk_tipo_teste', 
        'fk_fabricante', 
    ]

    cols_existentes = [c for c in colunas_banco_testes if c in df_fato_testes_realizados.columns]
    df_fato_testes_realizados = df_fato_testes_realizados[cols_existentes]

    df_fato_sintoma = df_sintomas_exploded.merge(dim_sintomas, on='nome_sintoma', how='left')
    df_fato_sintoma = df_fato_sintoma.rename(columns={'id_notificacao': 'fk_notificacao', 'id_sintoma': 'fk_sintoma'})
    df_fato_sintoma = df_fato_sintoma[['fk_notificacao', 'fk_sintoma']] 

    df_fato_condicao = df_condicoes_exploded.merge(dim_condicoes, on='nome_condicao', how='left')
    df_fato_condicao = df_fato_condicao.rename(columns={'id_notificacao': 'fk_notificacao', 'id_condicao': 'fk_condicao'})
    df_fato_condicao = df_fato_condicao[['fk_notificacao', 'fk_condicao']]  
    
    df_fato_notificacoes = df_clean.copy()
    
    df_fato_notificacoes = df_fato_notificacoes.merge(
        dim_localidades[['codigo_ibge_municipio', 'id_localidade']],
        left_on='municipioIBGE', right_on='codigo_ibge_municipio', how='left'
    ).rename(columns={'id_localidade': 'fk_localidade_residencia'})
    
    df_fato_notificacoes = df_fato_notificacoes.merge(
        dim_raca_cor, left_on='racaCor', right_on='descricao_raca_cor', how='left'
    ).rename(columns={'id_raca_cor': 'fk_raca_cor'})

    df_fato_notificacoes = df_fato_notificacoes.merge(
        dim_evolucao, left_on='evolucaoCaso', right_on='descricao_evolucao', how='left'
    ).rename(columns={'id_evolucao': 'fk_evolucao_caso'})
    
    bool_map = {'Sim': True, 'Não': False}
    df_fato_notificacoes['profissionalSaude'] = df_fato_notificacoes['profissionalSaude'].map(bool_map).fillna(False)
    df_fato_notificacoes['profissionalSeguranca'] = df_fato_notificacoes['profissionalSeguranca'].map(bool_map).fillna(False)

    cols_map = {
        'dataNotificacao': 'data_notificacao',
        'codigoLaboratorioPrimeiraDose': 'nome_fabricante_vacina',
        'dataInicioSintomas': 'data_inicio_sintomas',
        'dataEncerramento': 'data_encerramento',
        'classificacaoFinal': 'classificacao_final',
        'codigoRecebeuVacina': 'codigo_recebeu_vacina',
        'codigoDosesVacina': 'codigo_doses_vacina',
        'dataPrimeiraDose': 'data_primeira_dose',
        'dataSegundaDose': 'data_segunda_dose',
        'profissionalSaude': 'profissional_saude',
        'profissionalSeguranca': 'profissional_seguranca',
        'cbo': 'codigo_cbo',
        'codigoEstrategiaCovid': 'codigo_estrategia_covid'
    }
    df_fato_notificacoes = df_fato_notificacoes.rename(columns=cols_map)

    final_columns = [
        'nome_fabricante_vacina', 'id_notificacao', 'sexo', 'idade', 'profissional_saude', 'profissional_seguranca', 
        'codigo_cbo', 'fk_raca_cor', 'fk_localidade_residencia', 
        'fk_localidade_notificacao', 'fk_evolucao_caso', 'data_notificacao', 
        'data_inicio_sintomas', 'data_encerramento', 'classificacao_final', 
        'codigo_recebeu_vacina', 'codigo_doses_vacina', 'data_primeira_dose', 
        'data_segunda_dose', 'codigo_estrategia_covid'
    ]
    cols_to_load = [c for c in final_columns if c in df_fato_notificacoes.columns]
    df_fato_notificacoes = df_fato_notificacoes[cols_to_load]

    try:
        engine = create_engine(DATABASE_URL)
        print("\nConexão com o banco de dados estabelecida.")
        print("Iniciando Carga...")
        
        dim_localidades.to_sql('dim_localidades', engine, if_exists='append', index=False)
        dim_sintomas.to_sql('dim_sintomas', engine, if_exists='append', index=False)
        dim_condicoes.to_sql('dim_condicoes', engine, if_exists='append', index=False)
        dim_raca_cor.to_sql('dim_raca_cor', engine, if_exists='append', index=False)
        dim_evolucao.to_sql('dim_evolucao_caso', engine, if_exists='append', index=False)
        
        df_fato_notificacoes.to_sql('fato_notificacoes', engine, if_exists='append', index=False)
        df_fato_sintoma.to_sql('fato_notificacao_sintoma', engine, if_exists='append', index=False)
        df_fato_condicao.to_sql('fato_notificacao_condicao', engine, if_exists='append', index=False)
        
        df_fato_testes_realizados.to_sql('fato_testes_realizados', engine, if_exists='append', index=False)
        
        engine.dispose()
        print("\n Pipeline ETL concluído com sucesso!")

    except Exception as e:
        print(f"\n ERRO na Carga de Dados (LOAD): {e}")


if __name__ == "__main__":
    CSV_FILE_PATH = "dataset_notif_sus.csv"
    run_etl_pipeline(CSV_FILE_PATH)