-- ================================================================
--  FASE 3 – CONSULTAS ANALÍTICAS
--  Data Warehouse de Transações de Cartão de Crédito
--  Banco: PostgreSQL
-- ================================================================

-- ────────────────────────────────────────────────────────────────
-- 1. KPIs GERAIS — visão consolidada do período
-- ────────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                    AS total_transacoes,
    ROUND(SUM(valor_brl)::numeric, 2)           AS gasto_total_brl,
    ROUND(AVG(valor_brl)::numeric, 2)           AS ticket_medio,
    ROUND(MAX(valor_brl)::numeric, 2)           AS maior_compra,
    ROUND(MIN(valor_brl)::numeric, 2)           AS menor_compra,
    COUNT(DISTINCT id_titular)                  AS qtd_titulares,
    COUNT(DISTINCT id_categoria)                AS qtd_categorias,
    COUNT(*) FILTER (WHERE valor_brl < 0)       AS qtd_estornos,
    ROUND(SUM(valor_brl) FILTER (WHERE valor_brl < 0)::numeric, 2) AS total_estornado
FROM fato_transacao;

-- ────────────────────────────────────────────────────────────────
-- 2. GASTO TOTAL POR TITULAR no período
-- ────────────────────────────────────────────────────────────────
SELECT
    t.nome_titular,
    t.final_cartao,
    COUNT(*)                                    AS qtd_compras,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_gasto,
    ROUND(AVG(f.valor_brl)::numeric, 2)         AS ticket_medio
FROM fato_transacao f
JOIN dim_titular t ON f.id_titular = t.id_titular
WHERE f.valor_brl > 0
GROUP BY t.nome_titular, t.final_cartao
ORDER BY total_gasto DESC;

-- ────────────────────────────────────────────────────────────────
-- 3. GASTO POR TITULAR E MÊS
-- ────────────────────────────────────────────────────────────────
SELECT
    t.nome_titular,
    d.ano,
    d.mes,
    COUNT(*)                                    AS qtd_compras,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_mes
FROM fato_transacao f
JOIN dim_titular t ON f.id_titular = t.id_titular
JOIN dim_data    d ON f.id_data    = d.id_data
WHERE f.valor_brl > 0
GROUP BY t.nome_titular, d.ano, d.mes
ORDER BY t.nome_titular, d.ano, d.mes;

-- ────────────────────────────────────────────────────────────────
-- 4. TOP 10 CATEGORIAS por valor gasto
-- ────────────────────────────────────────────────────────────────
SELECT
    c.nome_categoria,
    COUNT(*)                                    AS qtd_compras,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_gasto,
    ROUND(AVG(f.valor_brl)::numeric, 2)         AS ticket_medio,
    ROUND(
        SUM(f.valor_brl) * 100.0 /
        (SELECT SUM(valor_brl) FROM fato_transacao WHERE valor_brl > 0)
    , 2)                                        AS pct_total
FROM fato_transacao f
JOIN dim_categoria c ON f.id_categoria = c.id_categoria
WHERE f.valor_brl > 0
GROUP BY c.nome_categoria
ORDER BY total_gasto DESC
LIMIT 10;

-- ────────────────────────────────────────────────────────────────
-- 5. EVOLUÇÃO MENSAL DO GASTO TOTAL (série temporal)
-- ────────────────────────────────────────────────────────────────
SELECT
    d.ano,
    d.mes,
    COUNT(*)                                    AS qtd_compras,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_mes,
    ROUND(AVG(f.valor_brl)::numeric, 2)         AS ticket_medio_mes
FROM fato_transacao f
JOIN dim_data d ON f.id_data = d.id_data
WHERE f.valor_brl > 0
GROUP BY d.ano, d.mes
ORDER BY d.ano, d.mes;

-- ────────────────────────────────────────────────────────────────
-- 6. COMPARATIVO ENTRE TITULARES
--    (ticket médio, quantidade, total)
-- ────────────────────────────────────────────────────────────────
SELECT
    t.nome_titular,
    COUNT(*)                                    AS qtd_transacoes,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_gasto,
    ROUND(AVG(f.valor_brl)::numeric, 2)         AS valor_medio_transacao,
    ROUND(MAX(f.valor_brl)::numeric, 2)         AS maior_compra,
    ROUND(MIN(f.valor_brl)::numeric, 2)         AS menor_compra
FROM fato_transacao f
JOIN dim_titular t ON f.id_titular = t.id_titular
WHERE f.valor_brl > 0
GROUP BY t.nome_titular
ORDER BY total_gasto DESC;

-- ────────────────────────────────────────────────────────────────
-- 7. TOP 10 ESTABELECIMENTOS por valor
-- ────────────────────────────────────────────────────────────────
SELECT
    e.nome_estabelecimento,
    COUNT(*)                                    AS qtd_visitas,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_gasto,
    ROUND(AVG(f.valor_brl)::numeric, 2)         AS ticket_medio
FROM fato_transacao f
JOIN dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
WHERE f.valor_brl > 0
GROUP BY e.nome_estabelecimento
ORDER BY total_gasto DESC
LIMIT 10;

-- ────────────────────────────────────────────────────────────────
-- 8. PARCELAMENTO — à vista vs parcelado
-- ────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN total_parcelas = 1             THEN 'À Vista'
        WHEN total_parcelas BETWEEN 2 AND 6 THEN 'Parcelado (2–6x)'
        ELSE                                     'Parcelado (7x+)'
    END                                         AS tipo_pagamento,
    COUNT(*)                                    AS qtd_compras,
    ROUND(SUM(valor_brl)::numeric, 2)           AS total_gasto,
    ROUND(AVG(valor_brl)::numeric, 2)           AS valor_medio
FROM fato_transacao
WHERE valor_brl > 0
GROUP BY tipo_pagamento
ORDER BY total_gasto DESC;

-- ────────────────────────────────────────────────────────────────
-- 9. DIA DA SEMANA — volume e valor
-- ────────────────────────────────────────────────────────────────
SELECT
    d.dia_semana,
    COUNT(*)                                    AS qtd_transacoes,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_gasto,
    ROUND(AVG(f.valor_brl)::numeric, 2)         AS ticket_medio
FROM fato_transacao f
JOIN dim_data d ON f.id_data = d.id_data
WHERE f.valor_brl > 0
GROUP BY d.dia_semana
ORDER BY total_gasto DESC;

-- ────────────────────────────────────────────────────────────────
-- 10. ESTORNOS E CRÉDITOS por titular e categoria
-- ────────────────────────────────────────────────────────────────
SELECT
    t.nome_titular,
    c.nome_categoria,
    COUNT(*)                                    AS qtd_estornos,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_estornado
FROM fato_transacao f
JOIN dim_titular   t ON f.id_titular   = t.id_titular
JOIN dim_categoria c ON f.id_categoria = c.id_categoria
WHERE f.valor_brl < 0
GROUP BY t.nome_titular, c.nome_categoria
ORDER BY total_estornado ASC;

-- ────────────────────────────────────────────────────────────────
-- 11. TRANSAÇÕES INTERNACIONAIS (valor_usd > 0)
-- ────────────────────────────────────────────────────────────────
SELECT
    t.nome_titular,
    e.nome_estabelecimento,
    d.data_completa,
    ROUND(f.valor_usd::numeric, 2)              AS valor_usd,
    ROUND(f.cotacao::numeric, 4)                AS cotacao,
    ROUND(f.valor_brl::numeric, 2)              AS valor_brl
FROM fato_transacao f
JOIN dim_titular         t ON f.id_titular         = t.id_titular
JOIN dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
JOIN dim_data            d ON f.id_data            = d.id_data
WHERE f.valor_usd > 0
ORDER BY f.valor_brl DESC;

-- ────────────────────────────────────────────────────────────────
-- 12. GASTO POR TRIMESTRE
-- ────────────────────────────────────────────────────────────────
SELECT
    d.ano,
    d.trimestre,
    COUNT(*)                                    AS qtd_compras,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_trimestre
FROM fato_transacao f
JOIN dim_data d ON f.id_data = d.id_data
WHERE f.valor_brl > 0
GROUP BY d.ano, d.trimestre
ORDER BY d.ano, d.trimestre;

-- ────────────────────────────────────────────────────────────────
-- 13. CATEGORIA × MÊS — para heat map no Power BI
-- ────────────────────────────────────────────────────────────────
SELECT
    d.ano,
    d.mes,
    c.nome_categoria,
    ROUND(SUM(f.valor_brl)::numeric, 2)         AS total_gasto
FROM fato_transacao f
JOIN dim_data      d ON f.id_data      = d.id_data
JOIN dim_categoria c ON f.id_categoria = c.id_categoria
WHERE f.valor_brl > 0
GROUP BY d.ano, d.mes, c.nome_categoria
ORDER BY d.ano, d.mes, total_gasto DESC;