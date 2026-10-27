# 📊 Mercado de Dados no Brasil

## Análise do Perfil dos Profissionais de Dados com Big Data na AWS

Projeto desenvolvido para o **Tech Challenge — Fase 3 da Pós-Graduação em Data Analytics da FIAP**, com o objetivo de construir um pipeline de dados na AWS e analisar a pesquisa **State of Data Brasil** para apoiar uma instituição financeira em suas decisões sobre Dados, Analytics e Inteligência Artificial.

---

## 👩‍💻 Integrantes

| Nome | RM |
|------|----|
| Elaine Hernandes | RM375521 |
| Isabela Bersan Mariano Pádua | RM374811 |
| Laura Garcias Barreto Soares dos Santos | RM373850 |
| Lidiany Christiany da Silva | RM374699 |

**Turma:** 14DTAT

---

## 📊 Sobre o Projeto

Uma instituição financeira quer expandir sua área de Dados, Analytics e IA e, antes disso, precisa entender o mercado brasileiro de profissionais de dados.

O mercado paga mais a cada edição da pesquisa, está mais sênior e já usa IA de forma quase universal. O que ainda não amadureceu é a entrega de IA em escala nas empresas, a equidade de gênero e a retenção de talentos. Para uma instituição financeira, esses três pontos definem onde investir.

---

## 🎯 Objetivos

### Objetivo geral

Construir um pipeline de dados em camadas na AWS e usar a pesquisa State of Data Brasil para orientar decisões de contratação, capacitação e investimento em Dados e IA.

### Objetivos específicos

- Organizar os dados das três edições em camadas Bronze, Silver e Gold no Amazon S3;
- Padronizar as três edições em um único formato;
- Catalogar as tabelas no Glue Data Catalog e consultá-las com o Amazon Athena;
- Responder às sete perguntas de negócio do desafio;
- Gerar gráficos e um material executivo com os principais achados.

---

## 🗂️ Base de Dados

A análise usa as três últimas edições da pesquisa **State of Data Brasil**, da Data Hackers em parceria com a Bain & Company.

| Edição | Ano de coleta | Respondentes |
|---|---|---:|
| 2023-2024 | 2023 | 5.293 |
| 2024-2025 | 2024 | 5.215 |
| 2025-2026 | 2025 | 3.494 |
| **Total** | | **14.002** |

### Como ler os números

- O salário é estimado pelo ponto médio da faixa salarial informada, em reais por mês, sem correção pela inflação;
- Os percentuais usam como base quem respondeu à pergunta. Perguntas de múltipla escolha não somam 100%;
- As comparações entre grupos usam só grupos com 30 ou mais pessoas.

### Fonte

State of Data Brasil — Data Hackers (Kaggle):

https://www.kaggle.com/datasets/datahackers/state-of-data-brazil-20242025/data

---

## ☁️ Arquitetura da Solução

Os CSVs das três edições foram enviados ao **Amazon S3** e processados por três Jobs do **AWS Glue** em PySpark. As tabelas foram registradas no **Glue Data Catalog** e consultadas com o **Amazon Athena**. O ambiente utilizado foi o AWS Academy Learner Lab (us-east-1).

### Ingestão no S3

O bucket do projeto está organizado nas pastas `landing/`, `bronze/`, `silver/`, `gold/`, `athena-results/`, `scripts/`, `notebooks/` e `graficos/`.

### Camada Bronze

Conversão dos CSVs para Parquet, com os nomes das colunas padronizados. Os valores ficam iguais aos originais.

### Camada Silver

Harmonização das três edições, ajuste de tipos, remoção de duplicidades e correções.

### Camada Gold

Tabelas agregadas por pergunta de negócio: `gold_fato_mercado`, `gold_remuneracao`, `gold_diversidade_genero`, `gold_adocao_tecnologias`, `gold_adocao_ia` e `gold_trabalho_regiao`.

### Catalogação e consultas

O Glue Data Catalog registra **12 tabelas** no banco `db_state_of_data` (4 Bronze, 2 Silver e 6 Gold). No Athena foram feitas **24 checagens de validação** das camadas, todas iguais ao esperado, e **32 consultas SQL** para responder às perguntas de negócio.

---

## 🔎 Geração dos Gráficos

Os gráficos foram gerados no **AWS Glue Notebook**, com o kernel Glue PySpark:

1. Cada gráfico executa uma consulta com `spark.sql` sobre as tabelas Gold;
2. O resultado é convertido para pandas e desenhado com matplotlib;
3. As imagens são salvas em PNG na pasta `graficos/` do bucket.

Antes do notebook, os números foram conferidos no Athena.

---

## 📈 Resultados

### Estrutura do mercado

O mercado se concentra em analistas e gestores, em grandes empresas, no setor financeiro e no Sudeste. A participação de júniores caiu de **22,0%** para **16,1%** entre 2023 e 2025, enquanto a de gestores subiu de **16,9%** para **20,8%**. Finanças ou bancos é o setor com mais respondentes em 2025 (**18,5%**).

### Perfis mais valorizados

O salário médio estimado subiu de **R$ 10,4 mil** (2023) para **R$ 13,1 mil** (2025), cerca de 25% em dois anos. Gestão tem média de **R$ 19,8 mil**, e Engenharia de ML/IA de **R$ 18,6 mil**, com alta de 19% em um ano. O salário quase dobra a cada degrau de senioridade.

### Diversidade de gênero

As mulheres são **22,0%** da amostra em 2025, contra 24,4% em 2023, e ganham cerca de **18% menos** que os homens. No nível Sênior, a diferença passou de −10,9% para **−18,6%**. A participação feminina cai ao longo da carreira: 28,2% entre júniores e 18,4% entre gestores.

### Tecnologias mais adotadas

A stack converge para **AWS (48,3%)**, **Python**, **SQL**, **Power BI (59,0%)**, **Databricks** e **PostgreSQL**. Databricks foi a tecnologia que mais cresceu entre 2023 e 2025 (**+11,5 pp**), e Tableau a que mais perdeu espaço (**−5,3 pp**).

### Adoção de Inteligência Artificial

O uso de IA no trabalho chegou a **98,2%** em 2025, e **42,3%** já têm a ferramenta paga pela empresa (eram 6,4% em 2023). Mas só **26,5%** dos gestores têm projetos de IA generativa em produção com resultado. As principais barreiras são falta de expertise (38,8%) e dados não prontos para IA (36,8%).

### Região, senioridade e modelo de trabalho

O modelo de trabalho pesa mais que a região. A satisfação é de **74,6%** no remoto e **54,3%** no presencial, e **92,9%** dos remotos procurariam outro emprego se a presença integral fosse exigida. No mesmo nível de senioridade, os salários entre regiões ficam próximos.

### Oportunidades e desafios

Em 2025, o setor financeiro paga **R$ 14.445**, contra R$ 12.743 dos demais setores (**+13,4%**), e tem satisfação cerca de 8 pontos maior. A principal oportunidade é levar a IA do piloto à produção. O principal desafio é reter talentos: **75,2%** dos profissionais ocupados buscam emprego ou estão abertos a propostas.

### ⚠️ Interpretação dos resultados

- A amostra é voluntária e não representa estatisticamente todo o mercado. A edição de 2025 é menor (3.494 respondentes);
- O salário é estimado pelo ponto médio da faixa, e a faixa "acima de R$ 40.001" não tem teto, o que subestima as médias do topo;
- Os valores são nominais, sem correção pela inflação;
- O nível Especialista/Staff+ só existe a partir de 2025, o que afeta a comparação do nível Sênior entre os anos;
- Alguns grupos têm amostra pequena, como Engenharia de ML/IA (106 pessoas) e Product Manager (33);
- A relação entre uso de IA e salário é uma correlação, e não uma relação de causa.

---

## 💡 Recomendações Estratégicas

### Contratação

_a preencher_

### Capacitação

_a preencher_

### Investimento em tecnologia

_a preencher_

---

## 🛠️ Tecnologias Utilizadas

- Amazon S3
- AWS Glue (Jobs em PySpark e Glue Notebook)
- AWS Glue Data Catalog
- Amazon Athena
- Amazon CloudWatch
- Python
- Pandas
- Matplotlib
- SQL

---

## 📁 Estrutura do Repositório

```text
FIAP---TECH-CHALLENGE-FASE-3/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── scripts/
│   └── glue_jobs/
│
├── sql/
├── diagrama/
├── graficos/
├── apresentacao/
├── docs/
│
└── README.md
```

---

## 📚 Fontes

- State of Data Brazil 2024/2025, Data Hackers (Kaggle): https://www.kaggle.com/datasets/datahackers/state-of-data-brazil-20242025/data
- State of Data 2025-2026, Bain & Company e Data Hackers: https://www.bain.com/link/9b0a3b17f0514d3d94866e11b65e9563.aspx
- Relatório State of Data Brazil 2025/2026, Data Hackers: https://www.datahackers.news/p/download-do-relatorio-state-of-data-brazil-2025-2026
