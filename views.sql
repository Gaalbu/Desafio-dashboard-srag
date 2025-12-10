-- =========================================================
-- SCRIPT DE VIEWS CORRIGIDO (COM DROP PARA EVITAR ERROS)
-- =========================================================

-- 1. Casos por Município
DROP VIEW IF EXISTS vw_casos_por_municipio CASCADE;
CREATE OR REPLACE VIEW vw_casos_por_municipio AS
SELECT
    dl.municipio_nome,
    dl.estado_uf,
    fn.data_notificacao,
    COUNT(fn.id_notificacao) AS total_notificacoes,
    SUM(CASE WHEN fn.classificacao_final = 'Confirmado Laboratorial' THEN 1 ELSE 0 END) AS casos_confirmados,
    SUM(CASE WHEN fn.classificacao_final = 'Descartado' THEN 1 ELSE 0 END) AS casos_descartados,
    SUM(CASE WHEN de.descricao_evolucao = 'Óbito' THEN 1 ELSE 0 END) AS obitos
FROM
    fato_notificacoes fn
JOIN
    dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
JOIN
    dim_evolucao_caso de ON fn.fk_evolucao_caso = de.id_evolucao
GROUP BY
    1, 2, 3
ORDER BY
    fn.data_notificacao, dl.municipio_nome;


-- 2. Vacinação por Resultado
DROP VIEW IF EXISTS vw_vacinacao_por_resultado CASCADE;
CREATE OR REPLACE VIEW vw_vacinacao_por_resultado AS
SELECT
    dl.estado_uf,
    dl.municipio_nome,
    fn.classificacao_final,
    CASE
        WHEN fn.codigo_recebeu_vacina = 2 THEN 'Não Vacinado' -- Assumindo que no banco está como número (SMALLINT)
        WHEN fn.codigo_doses_vacina = '0' THEN '0 Doses'
        WHEN fn.codigo_doses_vacina = '1' THEN '1 Dose'
        WHEN fn.codigo_doses_vacina = '2' THEN '2 Doses'
        WHEN fn.codigo_doses_vacina = '3' OR fn.codigo_doses_vacina LIKE '%,%' THEN '3 ou Mais Doses'
        ELSE 'Não Informado'
    END AS status_vacinal,
    COUNT(fn.id_notificacao) AS total_casos
FROM
    fato_notificacoes fn
JOIN
    dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
WHERE
    fn.classificacao_final IN ('Confirmado Laboratorial', 'Descartado')
GROUP BY
    1, 2, 3, 4;


-- 3. Sintomas Frequentes
DROP VIEW IF EXISTS vw_sintomas_frequentes CASCADE;
CREATE OR REPLACE VIEW vw_sintomas_frequentes AS
SELECT
    dl.estado_uf,
    dl.municipio_nome,
    ds.nome_sintoma,
    COUNT(fns.fk_notificacao) AS total_ocorrencias
FROM
    fato_notificacoes fn
JOIN
    dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
JOIN
    fato_notificacao_sintoma fns ON fn.id_notificacao = fns.fk_notificacao
JOIN
    dim_sintomas ds ON fns.fk_sintoma = ds.id_sintoma
WHERE
    fn.classificacao_final = 'Confirmado Laboratorial'
GROUP BY
    1, 2, 3;


-- 4. Perfil Epidemiológico
DROP VIEW IF EXISTS vw_perfil_epidemiologico CASCADE;
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
FROM
    fato_notificacoes fn
JOIN
    dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
LEFT JOIN
    dim_raca_cor drc ON fn.fk_raca_cor = drc.id_raca_cor
JOIN
    dim_evolucao_caso de ON fn.fk_evolucao_caso = de.id_evolucao
GROUP BY
    1, 2, 3, 4, 5, 6, 7;


-- 5. Análise Laboratorial
-- NOTA: Mantive o "1 as source_id" para o Dashboard identificar como LACEN
DROP VIEW IF EXISTS vw_analise_laboratorial CASCADE;
CREATE OR REPLACE VIEW vw_analise_laboratorial AS
SELECT
    dl.estado_uf,
    dl.municipio_nome,
    COALESCE(dt.descricao_tipo_teste, 'Tipo Não Informado') as tipo_teste,
    COALESCE(df.nome_fabricante, 'Fabricante Não Informado') as fabricante,
    fr.descricao_resultado as resultado,
    1 AS source_id, -- Força o ID 1 para o Python reconhecer como LACEN
    COUNT(*) as total_testes
FROM
    fato_testes_realizados ftr
JOIN fato_notificacoes fn ON ftr.fk_notificacao = fn.id_notificacao
JOIN dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
LEFT JOIN dim_tipos_testes dt ON ftr.fk_tipo_teste = dt.id_tipo_teste
LEFT JOIN dim_fabricantes df ON ftr.fk_fabricante = df.id_fabricante
LEFT JOIN dim_resultados_teste fr ON ftr.fk_resultado_teste = fr.id_resultado
GROUP BY 
    1, 2, 3, 4, 5;