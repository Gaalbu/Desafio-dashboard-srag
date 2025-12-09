import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import psycopg2 
# psycogp2 é o driver que o SQLAlchemy usa para o Postgres


# Configurações de conexão (Substitua pelos seus dados)
DB_USER = "postgres"
DB_PASSWORD = "2fast2YOU"
DB_HOST = "localhost" # Ou endereço do seu servidor
DB_PORT = "5432"
DB_NAME = "esus_srag_db"

# URL de conexão com o PostgreSQL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

"""
    Carrega o CSV e aplica as transformações iniciais (descarte de colunas).
"""
def extract_and_initial_transform(file_path):
    
    print(f"Lendo arquivo: {file_path}")
    
    # Lista das colunas a serem descartadas (conforme o Audit Log)
    COLUNAS_DESCARTADAS = [
        'source_id',
        'excluido',
        'validado',
        'outroBuscaAtivaAssintomatico',
        'outroTriagemPopulacaoEspecifica',
        'outroLocalRealizacaoTestagem'
    ]
    
    # Carrega o CSV, tratando a coluna 'idade' como string para evitar problemas de tipo
    # Se o arquivo for muito grande, use o chunksize= para carregar em partes
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado em {file_path}")
        return None

    #dropando colunas inúteis
    df = df.drop(columns=COLUNAS_DESCARTADAS, errors='ignore')

    
    date_cols = [col for col in df.columns if 'data' in col.lower()]
    for col in date_cols:
        # Tenta converter para data, forçando inválidos (como 'None') para NaT (Not a Time)
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
        
    print(f"Colunas descartadas: {len(COLUNAS_DESCARTADAS)}. Registros lidos: {len(df)}")
    return df


"""
    Transforma uma coluna multivalorada (string separada por vírgula)
    em um DataFrame limpo para criar a Tabela de Junção.
    Ex: 'sintomas' -> Fato_Notificacao_Sintoma
"""
def normalize_multivalued_data(df, column_name, dim_name):

    df_temp = df[['id_notificacao', column_name]].copy()
    
    #Remove nulos
    df_temp = df_temp.rename(columns={column_name: dim_name})
    df_temp = df_temp.dropna(subset=[dim_name])
    
    #'Exploded' serve para separar linhas multivaloradas
    df_exploded = (df_temp[dim_name]
                   .str.split(', ')
                   .explode()
                   .to_frame()
                   )
    
    # Dá uma limpeza nas entradas e junta por id
    df_exploded = df_exploded.merge(df_temp[['id_notificacao']], left_index=True, right_index=True)
    df_exploded[dim_name] = df_exploded[dim_name].str.strip()
    
    #trata entradas duplicadas
    df_exploded = df_exploded.drop_duplicates(subset=['id_notificacao', dim_name]).reset_index(drop=True)
    
    print(f"DataFrame {dim_name} gerado com {len(df_exploded)} registros de junção.")
    return df_exploded



"""
    Função principal que executa o pipeline completo.
"""
def run_etl_pipeline(file_path):
    
    # pegando e arrumando os dados
    df_raw = extract_and_initial_transform(file_path)
    if df_raw is None:
        return

    # Adiciona a Chave Primária Artificial (BIGSERIAL)
    df_raw['id_notificacao'] = df_raw.index + 1
    
    # create tables e mapeamento interno
    # Ex: Mapeia todos os valores únicos de racaCor para criar Dim_Raca_Cor
    
    # exemplo: tabela Dim_Sintomas
    df_sintomas_exploded = normalize_multivalued_data(df_raw, 'sintomas', 'nome_sintoma')
    
    # tabela de domínio (valores únicos para inserir em Dim_Sintomas)
    dim_sintomas = df_sintomas_exploded[['nome_sintoma']].drop_duplicates().reset_index(drop=True)
    dim_sintomas['id_sintoma'] = dim_sintomas.index + 1
    
    # criacao de dataframes para realocar os dados
    
    # Fato_Notificacao_Sintoma: junta o DF Explodido com a Tabela de Domínio para obter o FK
    df_fato_sintoma = df_sintomas_exploded.merge(dim_sintomas, on='nome_sintoma', how='left')
    df_fato_sintoma = df_fato_sintoma[['id_notificacao', 'id_sintoma']] 
    
    # Relatório de integridade
    
    print("\n--- Relatório Estatístico de Integridade Inicial ---")
    
    # Porcentagem de Dados Faltantes em colunas-chave
    cols_check = ['dataNotificacao', 'dataInicioSintomas', 'sexo', 'idade', 'classificacaoFinal']
    missing_data = df_raw[cols_check].isnull().sum() / len(df_raw) * 100
    print("\nPercentual de Dados Faltantes:")
    print(missing_data.to_string(float_format="%.2f%%"))

    # Outliers (Exemplo: Idade)
    q1 = df_raw['idade'].quantile(0.25)
    q3 = df_raw['idade'].quantile(0.75)
    iqr = q3 - q1
    outlier_count = df_raw[(df_raw['idade'] < q1 - 1.5 * iqr) | (df_raw['idade'] > q3 + 1.5 * iqr)].shape[0]
    print(f"\nNúmero de Outliers (Idade): {outlier_count}")
    
    # alocando os dados no banco de dados
    
    try:
        engine = create_engine(DATABASE_URL)
        print("\nConexão com o banco de dados estabelecida.")
        
        #teste
        dim_sintomas.to_sql('dim_sintomas', engine, if_exists='append', index=False)
        print("Tabela dim_sintomas carregada.")
        
        
        # Libera a conexão
        engine.dispose()
        print("Pipeline ETL concluído com sucesso.")

    except Exception as e:
        print(f"\nERRO na Carga de Dados (LOAD): {e}")


if __name__ == "__main__":
    CSV_FILE_PATH = "dataset_notif_sus.csv"
    
    # Nota: Lembre-se de colocar o arquivo CSV no mesmo diretório do script, ou alterar o caminho.
    run_etl_pipeline(CSV_FILE_PATH)