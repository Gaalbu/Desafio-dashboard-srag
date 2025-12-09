-- Arrumando tabelas de informações que se repetem.
CREATE TABLE Dim_Localidades (
    id_localidade SERIAL PRIMARY KEY,
    estado_uf VARCHAR(2), -- Ex: PA
    estado_nome VARCHAR(50), -- Ex: Pará
    codigo_ibge_estado INT,
    municipio_nome VARCHAR(100),
    codigo_ibge_municipio INT UNIQUE NOT NULL
);

CREATE TABLE Dim_Condicoes (
    id_condicao SMALLINT PRIMARY KEY,
    nome_condicao VARCHAR(100) UNIQUE NOT NULL
);

COMMENT ON TABLE Dim_Condicoes IS 'Tabela de dimensão contendo a lista única de condições pré-existentes dos pacientes.';

CREATE TABLE Dim_Evolucao_Caso (
    id_evolucao SMALLINT PRIMARY KEY, -- Ex: 1  , 2, 3
    descricao_evolucao VARCHAR(50) UNIQUE NOT NULL -- Ex: Em tratamento domiciliar, Óbito
);

CREATE TABLE Dim_Raca_Cor (
    id_raca_cor SMALLINT PRIMARY KEY, -- Mapear os códigos do dataset
    descricao_raca_cor VARCHAR(50) UNIQUE NOT NULL -- Ex: Parda, Branca, Amarela
);

CREATE TABLE Dim_Sintomas (
    id_sintoma SMALLINT PRIMARY KEY,
    nome_sintoma VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE Fato_Notificacoes (
    id_notificacao BIGSERIAL PRIMARY KEY,
    -- Dados do Paciente (Para 3FN, Paciente deveria ser uma dimensão separada)
    sexo VARCHAR(10) CHECK (sexo IN ('Feminino', 'Masculino', 'IGNORADO', 'INDEFINIDO')),
    idade INT CHECK (idade >= 0),
    profissional_saude BOOLEAN NOT NULL,
    profissional_seguranca BOOLEAN NOT NULL,
    codigo_cbo VARCHAR(10),
    
    fk_raca_cor SMALLINT REFERENCES Dim_Raca_Cor(id_raca_cor),
    fk_localidade_residencia INT REFERENCES Dim_Localidades(id_localidade),
    fk_localidade_notificacao INT REFERENCES Dim_Localidades(id_localidade),
    fk_evolucao_caso SMALLINT REFERENCES Dim_Evolucao_Caso(id_evolucao),
    
    -- Dados de Tempo e classificação
    data_notificacao DATE NOT NULL,
    data_inicio_sintomas DATE NOT NULL,
    data_encerramento DATE,
    classificacao_final VARCHAR(50) NOT NULL, -- Ex: Confirmado Laboratorial, Descartado
    
    codigo_recebeu_vacina SMALLINT,
    codigo_doses_vacina SMALLINT,
    data_primeira_dose DATE,
    data_segunda_dose DATE,
    
    -- Restrições de Domínio
    CHECK (data_inicio_sintomas <= data_notificacao),
    CHECK (data_encerramento >= data_notificacao OR data_encerramento IS NULL)
);
CREATE TABLE Fato_Notificacao_Sintoma (
    fk_notificacao BIGINT REFERENCES Fato_Notificacoes(id_notificacao) ON DELETE CASCADE,
    fk_sintoma SMALLINT REFERENCES Dim_Sintomas(id_sintoma),
    PRIMARY KEY (fk_notificacao, fk_sintoma)
);

CREATE TABLE Fato_Notificacao_Condicao (
    fk_notificacao BIGINT REFERENCES Fato_Notificacoes(id_notificacao) ON DELETE CASCADE,
    fk_condicao SMALLINT REFERENCES Dim_Condicoes(id_condicao),
    PRIMARY KEY (fk_notificacao, fk_condicao)
);

-- Tabela para o tipo de teste (Ex: RT-PCR, Teste Rápido Antígeno, Sorologia)
CREATE TABLE Dim_Tipos_Testes (
    id_tipo_teste SMALLINT PRIMARY KEY, 
    descricao_tipo_teste VARCHAR(100) UNIQUE NOT NULL
);

-- Tabela para os fabricantes dos kits.......... isso é mto repetitivo meu deus
CREATE TABLE Dim_Fabricantes (
    id_fabricante SMALLINT PRIMARY KEY,
    nome_fabricante VARCHAR(100) UNIQUE NOT NULL
);

-- Tabela para os resultados possíveis (Ex: Positivo, Negativo, Inconclusivo)
CREATE TABLE Dim_Resultados_Teste (
    id_resultado SMALLINT PRIMARY KEY,
    descricao_resultado VARCHAR(50) UNIQUE NOT NULL
);

--substitui a ocorrência dos vários testes
CREATE TABLE Fato_Testes_Realizados (
    id_registro BIGSERIAL PRIMARY KEY,
    fk_notificacao BIGINT REFERENCES Fato_Notificacoes(id_notificacao) ON DELETE CASCADE,
    
    -- Dados de Tempo e Metadados
    data_coleta DATE,
    data_resultado DATE, -- Campo adicional, útil p/ análise
    codigo_estado_teste SMALLINT, -- Indica se o teste foi realizado no estado, fora ou ignorado
    fk_tipo_teste SMALLINT REFERENCES Dim_Tipos_Testes(id_tipo_teste),
    fk_fabricante SMALLINT REFERENCES Dim_Fabricantes(id_fabricante),
    fk_resultado_teste SMALLINT REFERENCES Dim_Resultados_Teste(id_resultado)

);

CREATE TABLE indicadores_municipais (
    id_indicador SERIAL PRIMARY KEY,
    fk_localidade INT REFERENCES dim_localidades(id_localidade), -- Chave estrangeira para a localidade
    data_referencia DATE NOT NULL, 
    taxa_positividade NUMERIC(5, 2) NOT NULL, -- Percentual 
    total_testes INT NOT NULL,
    total_positivos INT NOT NULL,
    
    -- Restrição de unicidade: garante apenas um indicador por município por dia
    UNIQUE (fk_localidade, data_referencia)
);
