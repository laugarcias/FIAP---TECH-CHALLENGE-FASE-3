
-- TECH CHALLENGE - FASE 3 | VALIDAÇÃO DAS CAMADAS NO ATHENA
-- Database: db_state_of_data
-- Os valores "esperados" abaixo vieram do teste local dos jobs


-- 1. BRONZE: linhas por edição (esperado: 5293 | 5217 | 3495)

SELECT '2023-2024' AS edicao, COUNT(*) AS linhas FROM db_state_of_data.bronze_state_of_data_2023_2024
UNION ALL
SELECT '2024-2025', COUNT(*) FROM db_state_of_data.bronze_state_of_data_2024_2025
UNION ALL
SELECT '2025-2026', COUNT(*) FROM db_state_of_data.bronze_state_of_data_2025_2026;

-- Dicionário de colunas (esperado: 399 | 403 | 388 colunas)

SELECT ano_coleta, COUNT(*) AS qtd_colunas
FROM db_state_of_data.bronze_dicionario_colunas
GROUP BY ano_coleta
ORDER BY ano_coleta;

-- 2. SILVER: respondentes por edição após deduplicação
--    (esperado: 2023 = 5293 | 2024 = 5215 | 2025 = 3494)
--    2024 perde 2 linhas e 2025 perde 1 (tokens repetidos no arquivo).

SELECT ano_coleta, edicao, COUNT(*) AS respondentes,
       COUNT(DISTINCT id_respondente) AS ids_distintos
FROM db_state_of_data.silver_respondentes
GROUP BY ano_coleta, edicao
ORDER BY ano_coleta;

-- Completude das principais colunas (% de nulos)

SELECT ano_coleta,
       ROUND(100.0 * SUM(CASE WHEN genero IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1)            AS pct_nulo_genero,
       ROUND(100.0 * SUM(CASE WHEN regiao IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1)            AS pct_nulo_regiao,
       ROUND(100.0 * SUM(CASE WHEN cargo_padronizado IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_nulo_cargo,
       ROUND(100.0 * SUM(CASE WHEN salario_estimado IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1)  AS pct_nulo_salario
FROM db_state_of_data.silver_respondentes
GROUP BY ano_coleta
ORDER BY ano_coleta;

-- Faixas salariais fora do padrão (esperado: 1 em 2023 e 1 em 2025)
SELECT ano_coleta, faixa_salarial, COUNT(*) AS qtd
FROM db_state_of_data.silver_respondentes
WHERE faixa_salarial_inconsistente
GROUP BY ano_coleta, faixa_salarial;

-- Correção da cloud 2023 (esperado em 2023: AWS 1529 | Azure 1141 | GCP 1106)
SELECT ano_coleta, item, SUM(marcado) AS qtd
FROM db_state_of_data.silver_respostas_multiplas
WHERE categoria = 'cloud'
GROUP BY ano_coleta, item
ORDER BY ano_coleta, qtd DESC;

-- 3. GOLD: conferência cruzada com a Silver

-- 3.1 A soma do fato deve bater com a Silver
SELECT f.ano_coleta, f.total_fato, s.total_silver
FROM (SELECT ano_coleta, SUM(qtd_respondentes) AS total_fato
      FROM db_state_of_data.gold_fato_mercado GROUP BY ano_coleta) f
JOIN (SELECT ano_coleta, COUNT(*) AS total_silver
      FROM db_state_of_data.silver_respondentes GROUP BY ano_coleta) s
  ON f.ano_coleta = s.ano_coleta
ORDER BY f.ano_coleta;

-- 3.2 Salário médio geral (esperado: 10438.23 | 11928.13 | 13059.29)
SELECT ano_coleta, qtd_respondentes, salario_medio_estimado, salario_mediana,
       var_pct_salario_vs_ano_anterior
FROM db_state_of_data.gold_remuneracao
WHERE corte = 'geral'
ORDER BY ano_coleta;

-- 3.3 Recalcular o mesmo número direto da Silver (tem que dar igual ao 3.2)
SELECT ano_coleta, ROUND(AVG(salario_estimado), 2) AS salario_medio
FROM db_state_of_data.silver_respondentes
WHERE ocupado AND possui_salario
GROUP BY ano_coleta
ORDER BY ano_coleta;

-- 3.4 Tabelas Gold geradas e quantidade de linhas
SELECT 'gold_fato_mercado' AS tabela, COUNT(*) AS linhas FROM db_state_of_data.gold_fato_mercado
UNION ALL SELECT 'gold_remuneracao', COUNT(*) FROM db_state_of_data.gold_remuneracao
UNION ALL SELECT 'gold_diversidade_genero', COUNT(*) FROM db_state_of_data.gold_diversidade_genero
UNION ALL SELECT 'gold_adocao_tecnologias', COUNT(*) FROM db_state_of_data.gold_adocao_tecnologias
UNION ALL SELECT 'gold_adocao_ia', COUNT(*) FROM db_state_of_data.gold_adocao_ia
UNION ALL SELECT 'gold_trabalho_regiao', COUNT(*) FROM db_state_of_data.gold_trabalho_regiao;
-- esperado: 13437 | 214 | 349 | 8252 | 202 | 848

-- 4. CONSULTAS DE EXEMPLO PARA O TIME DE ANÁLISE

-- Top 5 linguagens por edição
SELECT ano_coleta, item, pct_adocao, ranking
FROM db_state_of_data.gold_adocao_tecnologias
WHERE corte = 'geral' AND categoria = 'linguagem' AND ranking <= 5
ORDER BY ano_coleta, ranking;

-- Participação feminina por nível
SELECT ano_coleta, categoria AS nivel, pct_na_categoria AS pct_mulheres, qtd_respondentes
FROM db_state_of_data.gold_diversidade_genero
WHERE corte = 'nivel' AND genero_padronizado = 'Feminino'
ORDER BY nivel, ano_coleta;

-- Salário médio por região e modelo de trabalho (média ponderada correta)
SELECT ano_coleta, regiao, modelo_trabalho_atual_pad,
       SUM(qtd_com_salario) AS qtd,
       ROUND(SUM(soma_salario_estimado) / NULLIF(SUM(qtd_com_salario), 0), 2) AS salario_medio
FROM db_state_of_data.gold_trabalho_regiao
GROUP BY ano_coleta, regiao, modelo_trabalho_atual_pad
HAVING SUM(qtd_com_salario) >= 30
ORDER BY ano_coleta, regiao, salario_medio DESC;
