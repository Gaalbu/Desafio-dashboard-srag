import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import psycopg2 
# psycogp2 é o driver que o SQLAlchemy usa para o Postgres


# Configurações de conexão (Substitua pelos seus dados)
DB_USER = "postgres"
DB_PASSWORD = "admin"
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
    CORREÇÃO: Mapeia corretamente Sigla vs Nome e gera o código IBGE do Estado.
"""
def process_localidades(df):
    
    print("Processando Dimensão Localidades...")
    
    # CSV 'estado' = Nome (Pará) -> Vai para 'estado_nome'
    # CSV 'estadoIBGE' = Sigla (PA) -> Vai para 'estado_uf'
    
    df_residencia = df[['estado', 'estadoIBGE', 'municipio', 'municipioIBGE']].copy()
    df_residencia.columns = ['estado_nome', 'estado_uf', 'municipio_nome', 'codigo_ibge_municipio']
    
    df_notificacao = df[['estadoNotificacao', 'estadoNotificacaoIBGE', 'municipioNotificacao', 'municipioNotificacaoIBGE']].copy()
    df_notificacao.columns = ['estado_nome', 'estado_uf', 'municipio_nome', 'codigo_ibge_municipio']
    
    dim_localidades = pd.concat([df_residencia, df_notificacao])
    
    # Remove linhas onde o código do município é nulo
    dim_localidades = dim_localidades.dropna(subset=['codigo_ibge_municipio'])
    
    # Garante que o código do município seja Inteiro
    dim_localidades['codigo_ibge_municipio'] = dim_localidades['codigo_ibge_municipio'].astype(int)
    
    # Remove duplicatas
    dim_localidades = dim_localidades.drop_duplicates(subset=['codigo_ibge_municipio']).reset_index(drop=True)
    
    dim_localidades['codigo_ibge_estado'] = dim_localidades['codigo_ibge_municipio'].astype(str).str[:2].astype(int)
    
    # Adiciona ID Sequencial
    dim_localidades['id_localidade'] = dim_localidades.index + 1
    
    # Seleciona e Ordena as colunas EXATAMENTE como no Banco de Dados
    dim_localidades = dim_localidades[[
        'id_localidade', 
        'estado_uf',          # Agora recebe 'PA' (2 chars)
        'estado_nome',        # Agora recebe 'Pará'
        'codigo_ibge_estado', # Agora recebe 15 (Int)
        'municipio_nome', 
        'codigo_ibge_municipio'
    ]]
    
    print(f"Dim_Localidades criada com {len(dim_localidades)} registros.")
    return dim_localidades

"""
    Aplica regras de negócio para preencher Nulos em colunas-chave.
"""
def intelligent_null_imputation(df):
    
    print("Iniciando tratamento de nulos e correção cronológica...")
    
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

    # Regra: dataInicioSintomas NÃO pode ser maior que dataNotificacao.
    # Se for, assumimos que o início foi no mesmo dia da notificação.
    
    df['dataInicioSintomas'] = np.where(
        df['dataInicioSintomas'].isnull(),
        df['dataNotificacao'] - pd.Timedelta(days=1),
        df['dataInicioSintomas']
    )
    
    mask_erro_data = df['dataInicioSintomas'] > df['dataNotificacao']
    
    # Se DATA sintomas > DATA notificação, define DATA sintomas = DATA notificação
    df.loc[mask_erro_data, 'dataInicioSintomas'] = df.loc[mask_erro_data, 'dataNotificacao']
    
    # Mesma correção para dataEncerramento (não pode ser antes da notificação)
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

    print(f"Registros antes da limpeza de data: {len(df_clean)}")
    df_clean = df_clean.dropna(subset=['dataNotificacao'])
    print(f"Registros após remover datas nulas: {len(df_clean)}")

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
    
    print("Processando Dimensões Faltantes e Mapeamentos...")

    # Cria a dimensão a partir dos dados
    dim_evolucao = df_clean[['evolucaoCaso']].drop_duplicates().dropna().reset_index(drop=True)
    dim_evolucao['id_evolucao'] = dim_evolucao.index + 1
    dim_evolucao = dim_evolucao.rename(columns={'evolucaoCaso': 'descricao_evolucao'})

    df_fato_testes_realizados = process_testes_realizados(df_clean)

    #Renomear para bater com o Banco de Dados
    df_fato_testes_realizados = df_fato_testes_realizados.rename(columns={
        'id_notificacao': 'fk_notificacao',
        'codigo_tipo_teste': 'fk_tipo_teste',
        'codigo_fabricante_teste': 'fk_fabricante',
        'codigo_resultado_teste': 'fk_resultado_teste'
        # codigo_estado_teste e data_coleta já estão certos
    })
    
    #Selecionar APENAS as colunas que existem no banco
    colunas_banco_testes = [
        'fk_notificacao', 
        'data_coleta', 
        'codigo_estado_teste', 
        'fk_tipo_teste', 
        'fk_fabricante', 
        'fk_resultado_teste'
    ]

    # Filtra o DataFrame mantendo apenas essas colunas (se existirem)
    cols_existentes = [c for c in colunas_banco_testes if c in df_fato_testes_realizados.columns]
    df_fato_testes_realizados = df_fato_testes_realizados[cols_existentes]

    df_fato_sintoma = df_sintomas_exploded.merge(dim_sintomas, on='nome_sintoma', how='left')
    # Renomeia para bater com o banco (fk_notificacao, fk_sintoma)
    df_fato_sintoma = df_fato_sintoma.rename(columns={
        'id_notificacao': 'fk_notificacao',
        'id_sintoma': 'fk_sintoma'
    })
    # Seleciona as colunas com os nomes certos
    df_fato_sintoma = df_fato_sintoma[['fk_notificacao', 'fk_sintoma']] 

    # Condições
    df_fato_condicao = df_condicoes_exploded.merge(dim_condicoes, on='nome_condicao', how='left')
    # Renomeia para bater com o banco (fk_notificacao, fk_condicao)
    df_fato_condicao = df_fato_condicao.rename(columns={
        'id_notificacao': 'fk_notificacao',
        'id_condicao': 'fk_condicao'
    })
    # Seleciona as colunas com os nomes certos
    df_fato_condicao = df_fato_condicao[['fk_notificacao', 'fk_condicao']]  
    
    df_fato_notificacoes = df_clean.copy()
    
    # Mapeamento FK Localidade
    df_fato_notificacoes = df_fato_notificacoes.merge(
        dim_localidades[['codigo_ibge_municipio', 'id_localidade']],
        left_on='municipioIBGE',
        right_on='codigo_ibge_municipio',
        how='left'
    ).rename(columns={'id_localidade': 'fk_localidade_residencia'})
    
    # Mapeamento FK RacaCor
    df_fato_notificacoes = df_fato_notificacoes.merge(
        dim_raca_cor,
        left_on='racaCor',
        right_on='descricao_raca_cor',
        how='left'
    ).rename(columns={'id_raca_cor': 'fk_raca_cor'})

    # Mapeamento FK Evolucao
    df_fato_notificacoes = df_fato_notificacoes.merge(
        dim_evolucao,
        left_on='evolucaoCaso',
        right_on='descricao_evolucao',
        how='left'
    ).rename(columns={'id_evolucao': 'fk_evolucao_caso'})
    
    # Tratamento de Booleanos (Postgres não aceita 'Sim'/'Não' automaticamente)
    bool_map = {'Sim': True, 'Não': False}
    df_fato_notificacoes['profissionalSaude'] = df_fato_notificacoes['profissionalSaude'].map(bool_map).fillna(False)
    df_fato_notificacoes['profissionalSeguranca'] = df_fato_notificacoes['profissionalSeguranca'].map(bool_map).fillna(False)

    # De: Nome no CSV/Pandas -> Para: Nome no PostgreSQL
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
        # id_notificacao, sexo, idade já estão iguais
        # fks já foram renomeadas acima
    }
    df_fato_notificacoes = df_fato_notificacoes.rename(columns=cols_map)

    # Seleção final de colunas para garantir que não vá lixo
    final_columns = [
        'nome_fabricante_vacina', 'id_notificacao', 'sexo', 'idade', 'profissional_saude', 'profissional_seguranca', 
        'codigo_cbo', 'fk_raca_cor', 'fk_localidade_residencia', 
        'fk_localidade_notificacao', 'fk_evolucao_caso', 'data_notificacao', 
        'data_inicio_sintomas', 'data_encerramento', 'classificacao_final', 
        'codigo_recebeu_vacina', 'codigo_doses_vacina', 'data_primeira_dose', 
        'data_segunda_dose', 'codigo_estrategia_covid'
    ]
    # Filtra apenas colunas que existem no DF (algumas podem ser nulas)
    cols_to_load = [c for c in final_columns if c in df_fato_notificacoes.columns]
    df_fato_notificacoes = df_fato_notificacoes[cols_to_load]

    # ... (Relatório de Integridade) ...

    # 6. CARGA NO BANCO DE DADOS
    try:
        engine = create_engine(DATABASE_URL)
        print("\nConexão com o banco de dados estabelecida.")
        
        print("Iniciando Carga das Dimensões...")
        # Idealmente truncar antes no PgAdmin ou usar if_exists='replace' (mas replace mata as chaves primárias)
        
        dim_localidades.to_sql('dim_localidades', engine, if_exists='append', index=False)
        dim_sintomas.to_sql('dim_sintomas', engine, if_exists='append', index=False)
        dim_condicoes.to_sql('dim_condicoes', engine, if_exists='append', index=False)
        dim_raca_cor.to_sql('dim_raca_cor', engine, if_exists='append', index=False)
        
        dim_evolucao.to_sql('dim_evolucao_caso', engine, if_exists='append', index=False)
        
        # Por simplicidade, assumimos que fato_testes vai carregar usando IDs se mapeados
        
        print("Iniciando Carga das Tabelas FATO...")
        
        df_fato_notificacoes.to_sql('fato_notificacoes', engine, if_exists='append', index=False)
        print("Fato Notificações carregada!")
        
        df_fato_sintoma.to_sql('fato_notificacao_sintoma', engine, if_exists='append', index=False)
        df_fato_condicao.to_sql('fato_notificacao_condicao', engine, if_exists='append', index=False)
        
        # Fato Testes - Precisamos garantir que as FKs (Tipo, Fabricante) existam. 
        # Se der erro aqui, comente temporariamente ou crie as dimensões de teste.
        df_fato_testes_realizados.to_sql('fato_testes_realizados', engine, if_exists='append', index=False)
        
        engine.dispose()
        print("\n✅ Pipeline ETL concluído com sucesso!")

    except Exception as e:
        print(f"\n❌ ERRO na Carga de Dados (LOAD): {e}")


if __name__ == "__main__":
    CSV_FILE_PATH = "dataset_notif_sus.csv"
    
    # Nota: Lembre-se de colocar o arquivo CSV no mesmo diretório do script, ou alterar o caminho.
    run_etl_pipeline(CSV_FILE_PATH)