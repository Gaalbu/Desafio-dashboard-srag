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



# Na próxima sessão, há todas os método principais que serão usados na pipeline.
# Antes de cada um, há uma descrição comentada através de aspas duplas repetidas do que cada um faz
# Se em um método necessitou de uma escolha, a mesma foi relatada no arquivo de auditoria.


"""
    Normaliza as 4 colunas de testes (Teste1 a Teste4) em uma única tabela Fato_Testes_Realizados.
"""
def process_testes_realizados(df):
    print("Processando Fato Testes Realizados (Unpivot)...")

    # lista de colunas a serem unpivotadas, agrupadas por métrica (Estado, Tipo, Fabricante, Resultado, Data Coleta)
    test_metrics = ['codigoEstadoTeste', 'codigoTipoTeste', 'codigoFabricanteTeste', 'codigoResultadoTeste', 'dataColetaTeste']
    
    # Cria uma lista de colunas "id_" para manter na linha (id_notificacao)
    id_vars = ['id_notificacao']
    
    # Lista de colunas específicas de teste
    value_vars = [f'{metric}{i}' for i in range(1, 5) for metric in test_metrics]
    
    # Cria o DataFrame base com id e colunas
    df_testes = df[id_vars + value_vars].copy()

    # destrói as colunas de teste, separando
    df_long = pd.melt(
        df_testes,
        id_vars=['id_notificacao'],
        value_vars=value_vars,
        var_name='test_variable',
        value_name='test_value'
    ).dropna(subset=['test_value']) # Remove todas as linhas onde o valor da métrica é nulo

    # extrai o número e o nome
    df_long['test_number'] = df_long['test_variable'].str[-1].astype(int)
    df_long['metric_name'] = df_long['test_variable'].str[:-1]

    # volta para a organização de uma métrica por teste
    df_final = df_long.pivot_table(
        index=['id_notificacao', 'test_number'],
        columns='metric_name',
        values='test_value',
        aggfunc='first' # Usa o primeiro valor (deve ser único após o melt)
    ).reset_index()

    # renomeação das colunas que sofreram o melt
    df_final.columns.name = None
    df_final = df_final.rename(columns={
        'codigoEstadoTeste': 'codigo_estado_teste',
        'codigoTipoTeste': 'codigo_tipo_teste',
        'codigoFabricanteTeste': 'codigo_fabricante_teste',
        'codigoResultadoTeste': 'codigo_resultado_teste',
        'dataColetaTeste': 'data_coleta'
    })
    
    # add o id para registro
    df_final['id_registro'] = df_final.index + 1
    
    print(f"Fato_Testes_Realizados criada com {len(df_final)} testes individuais.")
    return df_final


"""
    Cria a Dim_Localidades consolidando os dados de município/estado de
    residência e notificação.
"""
def process_localidades(df):
    
    print("Processando Dimensão Localidades...")
    
    # seleciona renomenando as colunas de residência
    df_residencia = df[['estado', 'estadoIBGE', 'municipio', 'municipioIBGE']].copy()
    df_residencia.columns = ['estado_uf', 'codigo_ibge_estado', 'municipio_nome', 'codigo_ibge_municipio']
    
    # seleciona renomenando as colunas de Notificação
    df_notificacao = df[['estadoNotificacao', 'estadoNotificacaoIBGE', 'municipioNotificacao', 'municipioNotificacaoIBGE']].copy()
    df_notificacao.columns = ['estado_uf', 'codigo_ibge_estado', 'municipio_nome', 'codigo_ibge_municipio']
    
    # da uma concatenacao e remove as duplicatas.
    dim_localidades = pd.concat([df_residencia, df_notificacao]).drop_duplicates(
        subset=['codigo_ibge_municipio', 'municipio_nome']
    ).dropna(subset=['codigo_ibge_municipio']).reset_index(drop=True)
    
    # criando ID
    dim_localidades['id_localidade'] = dim_localidades.index + 1
    
    # select final
    dim_localidades = dim_localidades[['id_localidade', 'estado_uf', 'codigo_ibge_estado', 'municipio_nome', 'codigo_ibge_municipio']]
    
    print(f"Dim_Localidades criada com {len(dim_localidades)} registros.")
    return dim_localidades


"""
    Aplica regras de negócio para preencher Nulos em colunas-chave.
"""
def intelligent_null_imputation(df):
    
    print("Iniciando preenchimento inteligente de nulos...")
    # Coluna usada para teste: codigoResultadoTeste1. (1=Positivo, 2=Negativo)
    
    # se nulo E resultado teste 1 = Positivo (código 1), classificar como 'Confirmado Laboratorial'
    df['classificacaoFinal'] = np.where(
        (df['classificacaoFinal'].isnull()) & (df['codigoResultadoTeste1'] == 1),
        'Confirmado Laboratorial',
        df['classificacaoFinal']
    )
    
    # se nulo E resultado teste 1 = Negativo (código 2), classificar como 'Descartado'
    df['classificacaoFinal'] = np.where(
        (df['classificacaoFinal'].isnull()) & (df['codigoResultadoTeste1'] == 2),
        'Descartado',
        df['classificacaoFinal']
    )
    
    # caso ainda seja nulo, preencher como "Suspeito" para casos ativos.
    df['classificacaoFinal'] = df['classificacaoFinal'].fillna('Suspeito')
    
    # Preenchendo datas chave.
    # Se nula, estimar usando dataNotificacao - 1 dia (para análise de tempo de latência)
    df['dataInicioSintomas'] = np.where(
        df['dataInicioSintomas'].isnull(),
        df['dataNotificacao'] - pd.Timedelta(days=1),
        df['dataInicioSintomas']
    )
    
    # idade: Preencher com a mediana e garantir que seja INT
    median_age = df['idade'].median()
    df['idade'] = df['idade'].fillna(median_age).astype(int)
    
    # sexo, raça, evolução: Preencher com "IGNORADO" ou "NAO INFORMADO"
    df['sexo'] = df['sexo'].fillna('IGNORADO')
    df['racaCor'] = df['racaCor'].fillna('NAO INFORMADO')
    df['evolucaoCaso'] = df['evolucaoCaso'].fillna('EM ABERTO')
    
    print("Preenchimento inteligente de nulos concluído.")
    return df


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
    
    # 1. EXTRAÇÃO E TRANSFORMAÇÃO INICIAL
    df_raw = extract_and_initial_transform(file_path)
    if df_raw is None:
        return

    # Adiciona a Chave Primária Artificial (BIGSERIAL)
    df_raw['id_notificacao'] = df_raw.index + 1
    
    # 2. TRATAMENTO DE NULOS E VALORES INCONSISTENTES
    df_clean = intelligent_null_imputation(df_raw.copy())

    # 3. CRIAÇÃO E PROCESSAMENTO DAS DIMENSÕES (MAPEAMENTO)
    
    # Dimensões Simples (Valores Únicos)
    # Sintomas
    df_sintomas_exploded = normalize_multivalued_data(df_clean, 'sintomas', 'nome_sintoma')
    dim_sintomas = df_sintomas_exploded[['nome_sintoma']].drop_duplicates().reset_index(drop=True)
    dim_sintomas['id_sintoma'] = dim_sintomas.index + 1
    
    # Condições Pré-existentes (Exemplo similar aos Sintomas)
    df_condicoes_exploded = normalize_multivalued_data(df_clean, 'condicoes', 'nome_condicao')
    dim_condicoes = df_condicoes_exploded[['nome_condicao']].drop_duplicates().reset_index(drop=True)
    dim_condicoes['id_condicao'] = dim_condicoes.index + 1
    
    # Dimensões de Categoria (RacaCor)
    dim_raca_cor = df_clean[['racaCor']].drop_duplicates().dropna().reset_index(drop=True)
    dim_raca_cor['id_raca_cor'] = dim_raca_cor.index + 1
    dim_raca_cor = dim_raca_cor.rename(columns={'racaCor': 'descricao_raca_cor'})
    
    # Dimensões Complexas
    dim_localidades = process_localidades(df_clean)
    
    # 4. CRIAÇÃO DAS TABELAS FATO E MAPEAMENTO DE CHAVES ESTRANGEIRAS
    
    # Tabela Fato Testes Realizados
    df_fato_testes_realizados = process_testes_realizados(df_clean)
    
    # Tabela Fato Notificacao Sintoma (Join)
    df_fato_sintoma = df_sintomas_exploded.merge(dim_sintomas, on='nome_sintoma', how='left')
    df_fato_sintoma = df_fato_sintoma[['id_notificacao', 'id_sintoma']] 

    # Tabela Fato Notificacao Condicao (Join)
    df_fato_condicao = df_condicoes_exploded.merge(dim_condicoes, on='nome_condicao', how='left')
    df_fato_condicao = df_fato_condicao[['id_notificacao', 'id_condicao']]

    # Tabela FATO Principal (Mapeamento de FKs)
    df_fato_notificacoes = df_clean.copy()
    
    # Mapeamento para FK_Localidade_Residencia
    df_fato_notificacoes = df_fato_notificacoes.merge(
        dim_localidades[['codigo_ibge_municipio', 'id_localidade']],
        left_on='municipioIBGE',
        right_on='codigo_ibge_municipio',
        how='left'
    ).rename(columns={'id_localidade': 'fk_localidade_residencia'})
    
    # Mapeamento para FK_RacaCor
    df_fato_notificacoes = df_fato_notificacoes.merge(
        dim_raca_cor,
        left_on='racaCor',
        right_on='descricao_raca_cor',
        how='left'
    ).rename(columns={'id_raca_cor': 'fk_raca_cor'})
    
    # Selecionar as colunas finais para o FATO principal (incluir FKs e remover colunas brutas)
    COLS_FATO_FINAL = [
        'id_notificacao', 'dataNotificacao', 'dataInicioSintomas', 'dataEncerramento',
        'sexo', 'idade', 'profissionalSaude', 'profissionalSeguranca',
        'cbo', 'evolucaoCaso', 'classificacaoFinal', 'codigoEstrategiaCovid',
        'codigoRecebeuVacina', 'codigoDosesVacina', 'dataPrimeiraDose', 'dataSegundaDose',
        'fk_raca_cor', 'fk_localidade_residencia'
        # ... adicionar outras colunas simples e FKs faltantes ...
    ]
    df_fato_notificacoes = df_fato_notificacoes[COLS_FATO_FINAL]

    # 5. RELATÓRIO ESTATÍSTICO DE INTEGRIDADE FINAL (Requisito da Etapa 2)
    print("\n--- Relatório Estatístico de Integridade Final ---")
    
    # Exemplo de Registros Limpos
    print(f"Total de Registros Limpos (Notificações): {len(df_fato_notificacoes)}")
    # Outros relatórios de integridade (já presentes)
    
    # 6. CARGA NO BANCO DE DADOS (LOAD SEQUENCIAL)
    
    try:
        engine = create_engine(DATABASE_URL)
        print("\nConexão com o banco de dados estabelecida.")
        
        # Sequência de Carga: DIMENSÕES PRIMEIRO
        print("Iniciando Carga das Dimensões...")
        
        dim_localidades.to_sql('dim_localidades', engine, if_exists='append', index=False)
        dim_sintomas.to_sql('dim_sintomas', engine, if_exists='append', index=False)
        dim_condicoes.to_sql('dim_condicoes', engine, if_exists='append', index=False)
        dim_raca_cor.to_sql('dim_raca_cor', engine, if_exists='append', index=False)
        # ... (Outras dimensões)
        
        # Sequência de Carga: FATOS DEPOIS (dependem das FKs)
        print("Iniciando Carga das Tabelas FATO...")
        
        df_fato_notificacoes.to_sql('fato_notificacoes', engine, if_exists='append', index=False)
        df_fato_sintoma.to_sql('fato_notificacao_sintoma', engine, if_exists='append', index=False)
        df_fato_condicao.to_sql('fato_notificacao_condicao', engine, if_exists='append', index=False)
        df_fato_testes_realizados.to_sql('fato_testes_realizados', engine, if_exists='append', index=False)
        
        # Libera a conexão
        engine.dispose()
        print("\n✅ Pipeline ETL e Carga no Banco de Dados concluídos com sucesso!")

    except Exception as e:
        print(f"\n❌ ERRO na Carga de Dados (LOAD): {e}")

if __name__ == "__main__":
    CSV_FILE_PATH = "dataset_notif_sus.csv"
    
    # Nota: Lembre-se de colocar o arquivo CSV no mesmo diretório do script, ou alterar o caminho.
    run_etl_pipeline(CSV_FILE_PATH)