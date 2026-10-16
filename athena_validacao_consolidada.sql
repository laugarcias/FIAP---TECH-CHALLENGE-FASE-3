-- =====================================================================
-- TECH CHALLENGE - FASE 3 | VALIDAÇÃO CONSOLIDADA (1 consulta, 24 checagens)
-- Executada no Athena em 14/10/2026: 24 de 24 checagens com status OK.
-- Rode no editor do Athena com o database db_state_of_data.
-- =====================================================================
WITH c AS (
SELECT 01 AS n, 'Bronze 2023-2024: linhas' AS verificacao, '5293' AS esperado, CAST(COUNT(*) AS varchar) AS obtido FROM db_state_of_data.bronze_state_of_data_2023_2024
UNION ALL SELECT 02, 'Bronze 2024-2025: linhas', '5217', CAST(COUNT(*) AS varchar) FROM db_state_of_data.bronze_state_of_data_2024_2025
UNION ALL SELECT 03, 'Bronze 2025-2026: linhas', '3495', CAST(COUNT(*) AS varchar) FROM db_state_of_data.bronze_state_of_data_2025_2026
UNION ALL SELECT 04, 'Dicionario 2023: colunas', '399', CAST(COUNT(*) AS varchar) FROM db_state_of_data.bronze_dicionario_colunas WHERE ano_coleta = 2023
UNION ALL SELECT 05, 'Dicionario 2024: colunas', '403', CAST(COUNT(*) AS varchar) FROM db_state_of_data.bronze_dicionario_colunas WHERE ano_coleta = 2024
UNION ALL SELECT 06, 'Dicionario 2025: colunas', '388', CAST(COUNT(*) AS varchar) FROM db_state_of_data.bronze_dicionario_colunas WHERE ano_coleta = 2025
UNION ALL SELECT 07, 'Silver 2023: respondentes', '5293', CAST(COUNT(*) AS varchar) FROM db_state_of_data.silver_respondentes WHERE ano_coleta = 2023
UNION ALL SELECT 08, 'Silver 2024: respondentes', '5215', CAST(COUNT(*) AS varchar) FROM db_state_of_data.silver_respondentes WHERE ano_coleta = 2024
UNION ALL SELECT 09, 'Silver 2025: respondentes', '3494', CAST(COUNT(*) AS varchar) FROM db_state_of_data.silver_respondentes WHERE ano_coleta = 2025
UNION ALL SELECT 10, 'Silver: ids duplicados', '0', CAST(COUNT(*) - COUNT(DISTINCT CAST(ano_coleta AS varchar) || id_respondente) AS varchar) FROM db_state_of_data.silver_respondentes
UNION ALL SELECT 11, 'Silver: faixas salariais fora do padrao', '2', CAST(COUNT(*) AS varchar) FROM db_state_of_data.silver_respondentes WHERE faixa_salarial_inconsistente
UNION ALL SELECT 12, 'Cloud 2023: AWS', '1529', CAST(SUM(marcado) AS varchar) FROM db_state_of_data.silver_respostas_multiplas WHERE ano_coleta = 2023 AND categoria = 'cloud' AND item = 'Amazon Web Services (AWS)'
UNION ALL SELECT 13, 'Cloud 2023: Azure', '1141', CAST(SUM(marcado) AS varchar) FROM db_state_of_data.silver_respostas_multiplas WHERE ano_coleta = 2023 AND categoria = 'cloud' AND item = 'Azure (Microsoft)'
UNION ALL SELECT 14, 'Cloud 2023: GCP', '1106', CAST(SUM(marcado) AS varchar) FROM db_state_of_data.silver_respostas_multiplas WHERE ano_coleta = 2023 AND categoria = 'cloud' AND item = 'Google Cloud (GCP)'
UNION ALL SELECT 15, 'Gold: salario medio 2023', '10438.23', CAST(MAX(salario_medio_estimado) AS varchar) FROM db_state_of_data.gold_remuneracao WHERE corte = 'geral' AND ano_coleta = 2023
UNION ALL SELECT 16, 'Gold: salario medio 2024', '11928.13', CAST(MAX(salario_medio_estimado) AS varchar) FROM db_state_of_data.gold_remuneracao WHERE corte = 'geral' AND ano_coleta = 2024
UNION ALL SELECT 17, 'Gold: salario medio 2025', '13059.29', CAST(MAX(salario_medio_estimado) AS varchar) FROM db_state_of_data.gold_remuneracao WHERE corte = 'geral' AND ano_coleta = 2025
UNION ALL SELECT 18, 'Gold fato: soma respondentes (3 anos)', '14002', CAST(SUM(qtd_respondentes) AS varchar) FROM db_state_of_data.gold_fato_mercado
UNION ALL SELECT 19, 'gold_fato_mercado: linhas', '13437', CAST(COUNT(*) AS varchar) FROM db_state_of_data.gold_fato_mercado
UNION ALL SELECT 20, 'gold_remuneracao: linhas', '214', CAST(COUNT(*) AS varchar) FROM db_state_of_data.gold_remuneracao
UNION ALL SELECT 21, 'gold_diversidade_genero: linhas', '349', CAST(COUNT(*) AS varchar) FROM db_state_of_data.gold_diversidade_genero
UNION ALL SELECT 22, 'gold_adocao_tecnologias: linhas', '8252', CAST(COUNT(*) AS varchar) FROM db_state_of_data.gold_adocao_tecnologias
UNION ALL SELECT 23, 'gold_adocao_ia: linhas', '202', CAST(COUNT(*) AS varchar) FROM db_state_of_data.gold_adocao_ia
UNION ALL SELECT 24, 'gold_trabalho_regiao: linhas', '848', CAST(COUNT(*) AS varchar) FROM db_state_of_data.gold_trabalho_regiao
)
SELECT n, verificacao, esperado, obtido,
       CASE WHEN esperado = obtido THEN 'OK' ELSE 'DIFERENTE' END AS status
FROM c
ORDER BY n;
