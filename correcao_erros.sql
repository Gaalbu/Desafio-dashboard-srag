ALTER TABLE fato_notificacoes 
ALTER COLUMN codigo_cbo TYPE VARCHAR(255);

DROP VIEW IF EXISTS vw_vacinacao_por_resultado;

ALTER TABLE fato_notificacoes 
ADD COLUMN IF NOT EXISTS codigo_estrategia_covid INT;

ALTER TABLE fato_notificacoes 
ALTER COLUMN codigo_doses_vacina TYPE VARCHAR(100);

CREATE OR REPLACE VIEW vw_vacinacao_por_resultado AS
SELECT
    fn.classificacao_final,
    CASE
        -- Como agora é texto, comparamos com strings ou verificamos nulos
        WHEN fn.codigo_recebeu_vacina = 2 THEN 'Não Vacinado'
        WHEN fn.codigo_doses_vacina IS NULL OR fn.codigo_doses_vacina = '' THEN 'Não Informado/Sem Doses'
        -- Tenta categorizar baseado no texto (simplificado)
        WHEN fn.codigo_doses_vacina LIKE '%,%' THEN '2 ou Mais Doses' -- Se tem vírgula, tem mais de uma
        ELSE '1 Dose / Outros'
    END AS status_vacinal,
    COUNT(fn.id_notificacao) AS total_casos
FROM
    fato_notificacoes fn
WHERE
    fn.classificacao_final IN ('Confirmado Laboratorial', 'Descartado')
GROUP BY
    1, 2
ORDER BY
    fn.classificacao_final, total_casos DESC;

COMMENT ON VIEW vw_vacinacao_por_resultado IS 'Cruza o status vacinal (agora em texto) com a classificação final.';

ALTER TABLE fato_notificacoes 
ADD COLUMN codigo_estrategia_covid INT;

ALTER TABLE fato_notificacoes 
ALTER COLUMN codigo_doses_vacina TYPE VARCHAR(100);

CREATE OR REPLACE FUNCTION fx_calcular_taxa_positividade(
    data_inicio DATE,
    data_fim DATE
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO indicadores_municipais (
        fk_localidade, 
        data_referencia, 
        total_testes, 
        total_positivos, 
        taxa_positividade
    )
    SELECT
        n.fk_localidade_residencia AS fk_localidade,
        data_fim AS data_referencia,
        COUNT(t.id_registro) AS total_testes,
        -- CORREÇÃO: Usando 'fk_resultado_teste' em vez de 'codigo_resultado_teste'
        COUNT(CASE WHEN t.fk_resultado_teste = 1 THEN 1 END) AS total_positivos, 
        
        -- Calcula a taxa de positividade (Positivos / Total de Testes) * 100
        COALESCE(
            (COUNT(CASE WHEN t.fk_resultado_teste = 1 THEN 1 END)::NUMERIC / NULLIF(COUNT(t.id_registro), 0)) * 100, 
            0
        ) AS taxa_positividade
    FROM 
        fato_notificacoes n
    JOIN 
        fato_testes_realizados t ON n.id_notificacao = t.fk_notificacao
    -- Filtra as datas de coleta
    WHERE 
        t.data_coleta BETWEEN data_inicio AND data_fim
    -- Agrupa por Localidade de Residência (município)
    GROUP BY 
        n.fk_localidade_residencia
        
    -- Estratégia de UPSERT (INSERT ou UPDATE)
    ON CONFLICT (fk_localidade, data_referencia) DO UPDATE
    SET 
        total_testes = EXCLUDED.total_testes,
        total_positivos = EXCLUDED.total_positivos,
        taxa_positividade = EXCLUDED.taxa_positividade;

    RAISE NOTICE 'Taxas de positividade calculadas e atualizadas para o período de % a %.', data_inicio, data_fim;
END;
$$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS fato_testes_realizados CASCADE;

CREATE TABLE fato_testes_realizados (
    id_registro BIGSERIAL PRIMARY KEY,
    fk_notificacao BIGINT REFERENCES fato_notificacoes(id_notificacao) ON DELETE CASCADE,
    data_coleta DATE,
    data_resultado DATE,
    codigo_estado_teste SMALLINT,
    fk_tipo_teste SMALLINT,       -- Sem REFERENCES por enquanto
    fk_fabricante SMALLINT,       -- Sem REFERENCES por enquanto
    fk_resultado_teste SMALLINT   -- Sem REFERENCES por enquanto
);


TRUNCATE TABLE 
    fato_notificacoes, 
    fato_testes_realizados, 
    fato_notificacao_sintoma, 
    fato_notificacao_condicao,
    dim_localidades, 
    dim_sintomas, 
    dim_condicoes, 
    dim_raca_cor, 
    dim_evolucao_caso
CASCADE;
