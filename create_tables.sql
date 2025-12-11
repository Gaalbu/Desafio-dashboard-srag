
CREATE TABLE Dim_Localidades (
    id_localidade SERIAL PRIMARY KEY,
    estado_uf VARCHAR(2), 
    estado_nome VARCHAR(50), 
    codigo_ibge_estado INT,
    municipio_nome VARCHAR(100),
    codigo_ibge_municipio INT UNIQUE NOT NULL
);

CREATE TABLE Dim_Condicoes (
    id_condicao SMALLINT PRIMARY KEY,
    nome_condicao VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE Dim_Evolucao_Caso (
    id_evolucao SMALLINT PRIMARY KEY,
    descricao_evolucao VARCHAR(50) UNIQUE NOT NULL 
);

CREATE TABLE Dim_Raca_Cor (
    id_raca_cor SMALLINT PRIMARY KEY,
    descricao_raca_cor VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE Dim_Sintomas (
    id_sintoma SMALLINT PRIMARY KEY,
    nome_sintoma VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE Dim_Tipos_Testes (
    id_tipo_teste SMALLINT PRIMARY KEY, 
    descricao_tipo_teste VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE Dim_Fabricantes (
    id_fabricante SMALLINT PRIMARY KEY,
    nome_fabricante VARCHAR(100) UNIQUE NOT NULL
);


CREATE TABLE Fato_Notificacoes (
    id_notificacao BIGSERIAL PRIMARY KEY,
    sexo VARCHAR(20),
    idade INT CHECK (idade >= 0),
    profissional_saude BOOLEAN NOT NULL,
    profissional_seguranca BOOLEAN NOT NULL,
    codigo_cbo VARCHAR(255),
    
    fk_raca_cor SMALLINT REFERENCES Dim_Raca_Cor(id_raca_cor),
    fk_localidade_residencia INT REFERENCES Dim_Localidades(id_localidade),
    fk_localidade_notificacao INT REFERENCES Dim_Localidades(id_localidade),
    fk_evolucao_caso SMALLINT REFERENCES Dim_Evolucao_Caso(id_evolucao),
    
    data_notificacao DATE NOT NULL,
    data_inicio_sintomas DATE NOT NULL,
    data_encerramento DATE,
    classificacao_final VARCHAR(50) NOT NULL,
    
    codigo_recebeu_vacina SMALLINT,
    codigo_doses_vacina VARCHAR(100),
    data_primeira_dose DATE,
    data_segunda_dose DATE,
    nome_fabricante_vacina VARCHAR(255),
    codigo_estrategia_covid INT,
    
    CHECK (data_inicio_sintomas <= data_notificacao)
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

CREATE TABLE Fato_Testes_Realizados (
    id_registro BIGSERIAL PRIMARY KEY,
    fk_notificacao BIGINT REFERENCES Fato_Notificacoes(id_notificacao) ON DELETE CASCADE,
    data_coleta DATE,
    data_resultado DATE,
    codigo_estado_teste SMALLINT,
    fk_fabricante SMALLINT,       
);

CREATE TABLE indicadores_municipais (
    id_indicador SERIAL PRIMARY KEY,
    fk_localidade INT REFERENCES dim_localidades(id_localidade),
    data_referencia DATE NOT NULL, 
    taxa_positividade NUMERIC(5, 2) NOT NULL,
    total_testes INT NOT NULL,
    total_positivos INT NOT NULL,
    UNIQUE (fk_localidade, data_referencia)
);

CREATE TABLE log_alteracoes (
    id_log BIGSERIAL PRIMARY KEY,
    tabela_afetada VARCHAR(50) NOT NULL,
    operacao VARCHAR(10) NOT NULL,
    registro_id BIGINT,
    data_alteracao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    usuario_bd VARCHAR(100) DEFAULT CURRENT_USER,
    dados_antigos JSONB,
    dados_novos JSONB
);


CREATE OR REPLACE VIEW vw_casos_por_municipio AS
SELECT
    dl.municipio_nome,
    dl.estado_uf,
    fn.data_notificacao,
    COUNT(fn.id_notificacao) AS total_notificacoes,
    SUM(CASE WHEN fn.classificacao_final = 'Confirmado Laboratorial' THEN 1 ELSE 0 END) AS casos_confirmados,
    SUM(CASE WHEN fn.classificacao_final = 'Descartado' THEN 1 ELSE 0 END) AS casos_descartados,
    SUM(CASE WHEN de.descricao_evolucao = 'Óbito' THEN 1 ELSE 0 END) AS obitos
FROM fato_notificacoes fn
JOIN dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
LEFT JOIN dim_evolucao_caso de ON fn.fk_evolucao_caso = de.id_evolucao
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW vw_vacinacao_por_resultado AS
SELECT
    fn.classificacao_final,
    CASE
        WHEN fn.codigo_recebeu_vacina = 2 THEN 'Não Vacinado'
        WHEN fn.codigo_doses_vacina IS NULL OR fn.codigo_doses_vacina = '' THEN 'Não Informado/Sem Doses'
        WHEN fn.codigo_doses_vacina LIKE '%,%' THEN '2 ou Mais Doses'
        ELSE '1 Dose / Outros'
    END AS status_vacinal,
    dl.estado_uf,
    dl.municipio_nome,
    COUNT(fn.id_notificacao) AS total_casos
FROM fato_notificacoes fn
JOIN dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
WHERE fn.classificacao_final IN ('Confirmado Laboratorial', 'Descartado')
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW vw_sintomas_frequentes AS
SELECT
    dl.estado_uf,
    dl.municipio_nome,
    ds.nome_sintoma,
    COUNT(fns.fk_notificacao) AS total_ocorrencias
FROM fato_notificacoes fn
JOIN dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
JOIN fato_notificacao_sintoma fns ON fn.id_notificacao = fns.fk_notificacao
JOIN dim_sintomas ds ON fns.fk_sintoma = ds.id_sintoma
WHERE fn.classificacao_final = 'Confirmado Laboratorial'
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW vw_perfil_epidemiologico AS
SELECT
    dl.municipio_nome,
    dl.estado_uf,
    dl.codigo_ibge_municipio, 
    fn.sexo,
    CASE 
        WHEN fn.idade < 10 THEN '0-9 anos'
        WHEN fn.idade BETWEEN 10 AND 19 THEN '10-19 anos'
        WHEN fn.idade BETWEEN 20 AND 39 THEN '20-39 anos'
        WHEN fn.idade BETWEEN 40 AND 59 THEN '40-59 anos'
        WHEN fn.idade >= 60 THEN '60+ anos'
        ELSE 'Não Informado'
    END AS faixa_etaria,
    drc.descricao_raca_cor,
    fn.classificacao_final,
    COUNT(fn.id_notificacao) AS total_casos,
    SUM(CASE WHEN fn.classificacao_final = 'Confirmado Laboratorial' THEN 1 ELSE 0 END) AS casos_confirmados,
    SUM(CASE WHEN de.descricao_evolucao = 'Óbito' THEN 1 ELSE 0 END) AS obitos
FROM fato_notificacoes fn
JOIN dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
LEFT JOIN dim_raca_cor drc ON fn.fk_raca_cor = drc.id_raca_cor
LEFT JOIN dim_evolucao_caso de ON fn.fk_evolucao_caso = de.id_evolucao
GROUP BY 1, 2, 3, 4, 5, 6, 7;

CREATE OR REPLACE VIEW vw_analise_laboratorial AS
SELECT
    dl.estado_uf,
    dl.municipio_nome,
    COALESCE(dt.descricao_tipo_teste, 'Tipo Não Informado') as tipo_teste,
    COALESCE(df.nome_fabricante, 'Fabricante Não Informado') as fabricante,
    ftr.fk_notificacao as source_id,
    COUNT(*) as total_testes
FROM fato_testes_realizados ftr
JOIN fato_notificacoes fn ON ftr.fk_notificacao = fn.id_notificacao
JOIN dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
LEFT JOIN dim_tipos_testes dt ON ftr.fk_tipo_teste = dt.id_tipo_teste
LEFT JOIN dim_fabricantes df ON ftr.fk_fabricante = df.id_fabricante
GROUP BY 1, 2, 3, 4, 5, 6;


CREATE OR REPLACE FUNCTION ft_auditoria_notificacoes()
RETURNS TRIGGER AS $$
DECLARE
    v_old_data JSONB;
    v_new_data JSONB;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        v_new_data := to_jsonb(NEW);
        INSERT INTO log_alteracoes (tabela_afetada, operacao, registro_id, dados_novos)
        VALUES (TG_TABLE_NAME, TG_OP, NEW.id_notificacao, v_new_data);
        RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        v_old_data := to_jsonb(OLD);
        v_new_data := to_jsonb(NEW);
        IF v_old_data IS DISTINCT FROM v_new_data THEN
            INSERT INTO log_alteracoes (tabela_afetada, operacao, registro_id, dados_antigos, dados_novos)
            VALUES (TG_TABLE_NAME, TG_OP, NEW.id_notificacao, v_old_data, v_new_data);
        END IF;
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        v_old_data := to_jsonb(OLD);
        INSERT INTO log_alteracoes (tabela_afetada, operacao, registro_id, dados_antigos)
        VALUES (TG_TABLE_NAME, TG_OP, OLD.id_notificacao, v_old_data);
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_auditoria_notificacoes
AFTER INSERT OR UPDATE OR DELETE ON fato_notificacoes
FOR EACH ROW
EXECUTE FUNCTION ft_auditoria_notificacoes();