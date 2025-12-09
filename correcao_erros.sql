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