-- SCRIPT DDL (Data Definition Language)
CREATE TABLE log_alteracoes (
    id_log BIGSERIAL PRIMARY KEY,
    tabela_afetada VARCHAR(50) NOT NULL,
    operacao VARCHAR(10) NOT NULL, -- Ex: 'INSERT', 'UPDATE', 'DELETE'
    registro_id BIGINT, -- A chave primária (id_notificacao, id_teste, etc.) do registro afetado
    data_alteracao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    usuario_bd VARCHAR(100) DEFAULT CURRENT_USER,
    dados_antigos JSONB, -- Armazena os dados da linha ANTES da alteração (para UPDATE e DELETE)
    dados_novos JSONB -- Armazena os dados da linha DEPOIS da alteração (para INSERT e UPDATE)
);



CREATE OR REPLACE FUNCTION ft_auditoria_notificacoes()
RETURNS TRIGGER AS $$
DECLARE
    v_old_data JSONB;
    v_new_data JSONB;
BEGIN
    -- Determina o tipo de operação e registra os dados
    IF (TG_OP = 'INSERT') THEN
        v_new_data := to_jsonb(NEW);
        
        INSERT INTO log_alteracoes (tabela_afetada, operacao, registro_id, dados_novos)
        VALUES (TG_TABLE_NAME, TG_OP, NEW.id_notificacao, v_new_data);
        
        RETURN NEW;
        
    ELSIF (TG_OP = 'UPDATE') THEN
        v_old_data := to_jsonb(OLD);
        v_new_data := to_jsonb(NEW);
        
        -- Garante que só registre se houver mudança real no dado (opcional, mas bom para performance)
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

CREATE OR REPLACE FUNCTION fx_calcular_taxa_positividade(
    data_inicio DATE,
    data_fim DATE
)
RETURNS VOID AS $$
BEGIN
    -- Insere ou atualiza a tabela indicadores_municipais
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
        COUNT(CASE WHEN t.codigo_resultado_teste = 1 THEN 1 END) AS total_positivos, -- Código 1 = Positivo
        -- Calcula a taxa de positividade (Positivos / Total de Testes) * 100
        COALESCE(
            (COUNT(CASE WHEN t.codigo_resultado_teste = 1 THEN 1 END)::NUMERIC / NULLIF(COUNT(t.id_registro), 0)) * 100, 
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

    -- Log de sucesso (opcional)
    RAISE NOTICE 'Taxas de positividade calculadas e atualizadas para o período de % a %.', data_inicio, data_fim;
END;
$$ LANGUAGE plpgsql;