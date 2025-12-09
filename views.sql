CREATE OR REPLACE VIEW vw_casos_por_municipio AS
SELECT
    dl.municipio_nome,
    dl.estado_uf,
    fn.data_notificacao,
    COUNT(fn.id_notificacao) AS total_notificacoes,
    SUM(CASE WHEN fn.classificacao_final = 'Confirmado Laboratorial' THEN 1 ELSE 0 END) AS casos_confirmados,
    SUM(CASE WHEN fn.classificacao_final = 'Descartado' THEN 1 ELSE 0 END) AS casos_descartados,
    SUM(CASE WHEN fn.evolucao_caso = 'Óbito por SRAG' THEN 1 ELSE 0 END) AS obitos
FROM
    fato_notificacoes fn
JOIN
    dim_localidades dl ON fn.fk_localidade_residencia = dl.id_localidade
GROUP BY
    1, 2, 3
ORDER BY
    fn.data_notificacao, dl.municipio_nome;

COMMENT ON VIEW vw_casos_por_municipio IS 'Consolida casos confirmados, descartados e óbitos por município e data de notificação.';

CREATE OR REPLACE VIEW vw_vacinacao_por_resultado AS
SELECT
    fn.classificacao_final,
    -- Converte o código de doses em uma descrição mais legível
    CASE
        WHEN fn.codigo_recebeu_vacina = 2 THEN 'Não Vacinado'
        WHEN fn.codigo_doses_vacina = 0 THEN '0 Doses'
        WHEN fn.codigo_doses_vacina = 1 THEN '1 Dose'
        WHEN fn.codigo_doses_vacina = 2 THEN '2 Doses'
        WHEN fn.codigo_doses_vacina = 3 THEN '3 ou Mais Doses'
        ELSE 'Não Informado'
    END AS status_vacinal,
    COUNT(fn.id_notificacao) AS total_casos
FROM
    fato_notificacoes fn
WHERE
    fn.classificacao_final IN ('Confirmado Laboratorial', 'Descartado') -- Foca apenas em casos com classificação final
GROUP BY
    1, 2
ORDER BY
    fn.classificacao_final, total_casos DESC;

COMMENT ON VIEW vw_vacinacao_por_resultado IS 'Cruza o status vacinal do paciente com a classificação final do caso (confirmado/descartado).';

CREATE OR REPLACE VIEW vw_sintomas_frequentes AS
SELECT
    ds.nome_sintoma,
    COUNT(fns.fk_notificacao) AS total_ocorrencias,
    -- Percentual sobre o total de casos CONFIRMADOS
    (COUNT(fns.fk_notificacao) * 100.0 / 
     (SELECT COUNT(id_notificacao) FROM fato_notificacoes WHERE classificacao_final = 'Confirmado Laboratorial')
    ) AS percentual_casos_confirmados
FROM
    fato_notificacoes fn
JOIN
    fato_notificacao_sintoma fns ON fn.id_notificacao = fns.fk_notificacao
JOIN
    dim_sintomas ds ON fns.fk_sintoma = ds.id_sintoma
WHERE
    fn.classificacao_final = 'Confirmado Laboratorial' -- Filtra apenas casos confirmados
GROUP BY
    ds.nome_sintoma
ORDER BY
    total_ocorrencias DESC;

COMMENT ON VIEW vw_sintomas_frequentes IS 'Lista a frequência dos sintomas relatados em casos com classificação final "Confirmado Laboratorial".';
