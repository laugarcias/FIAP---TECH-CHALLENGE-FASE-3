"""
=====================================================================
TECH CHALLENGE - FASE 3 | JOB 03 - CAMADA GOLD
=====================================================================
Objetivo
    Gerar tabelas agregadas e prontas para consumo (Athena, gráficos,
    BI), cada uma ligada a uma das perguntas de negócio do desafio:

    gold_fato_mercado            -> base agregada multidimensional
                                    (estrutura do mercado, cortes livres)
    gold_remuneracao             -> salário estimado por cargo x nível
    gold_diversidade_genero      -> participação e salário por gênero
    gold_adocao_tecnologias      -> % de adoção de cada tecnologia
    gold_adocao_ia               -> indicadores de uso de IA generativa
    gold_trabalho_regiao         -> modelo de trabalho x região x nível

Entrada  : s3://<BUCKET>/silver/...
Saída    : s3://<BUCKET>/gold/<tabela>/ano_coleta=<ano>/
Parâmetros do Job: --BUCKET  --DATABASE

Convenções
    - qtd_*   : contagem de respondentes
    - pct_*   : percentual entre 0 e 100, com 2 casas
    - salario : valor ESTIMADO pelo ponto médio da faixa salarial
                (a pesquisa só coleta faixas, não valores exatos)
    - Indicadores de remuneração usam apenas respondentes ocupados e
      com faixa salarial válida.
=====================================================================
"""
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "DATABASE"])
BUCKET = args["BUCKET"]
DATABASE = args["DATABASE"]
RAIZ = f"s3://{BUCKET}"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Grupos com poucos respondentes geram médias instáveis. As tabelas
# trazem a contagem (qtd_*) e a coluna amostra_suficiente para filtrar.
MIN_AMOSTRA = 30


# ------------------------------------------------------------------
# Funções auxiliares
# ------------------------------------------------------------------
def pct(numerador, denominador):
    return F.round(F.col(numerador) * 100.0 / F.col(denominador), 2)


def metricas_salario():
    """Agregações padrão de remuneração (usar após groupBy)."""
    return [
        F.count("*").alias("qtd_respondentes"),
        F.round(F.avg("salario_estimado"), 2).alias("salario_medio_estimado"),
        F.expr("percentile_approx(salario_estimado, 0.25)").alias("salario_p25"),
        F.expr("percentile_approx(salario_estimado, 0.5)").alias("salario_mediana"),
        F.expr("percentile_approx(salario_estimado, 0.75)").alias("salario_p75"),
        F.round(F.avg(F.when(F.col("salario_min") > 20000, 1).otherwise(0)) * 100, 2).alias("pct_acima_20k"),
    ]


def gravar_tabela(df_saida, tabela):
    caminho = f"{RAIZ}/gold/{tabela.replace('gold_', '')}/"
    colunas = [c for c in df_saida.columns if c != "ano_coleta"] + ["ano_coleta"]
    df_saida = df_saida.select(*colunas)
    glueContext.purge_s3_path(caminho, {"retentionPeriod": 0})
    dyf = DynamicFrame.fromDF(df_saida.coalesce(1), glueContext, tabela)
    sink = glueContext.getSink(
        connection_type="s3",
        path=caminho,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["ano_coleta"],
        compression="snappy",
    )
    sink.setFormat("parquet", useGlueParquetWriter=True)
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=tabela)
    sink.writeFrame(dyf)
    print(f"[OK] {DATABASE}.{tabela}: {df_saida.count()} linhas")


# ------------------------------------------------------------------
# Leitura da Silver
# ------------------------------------------------------------------
resp = spark.read.parquet(f"{RAIZ}/silver/respondentes/").cache()
multi = spark.read.parquet(f"{RAIZ}/silver/respostas_multiplas/")

# Base de remuneração: ocupados com faixa salarial válida
base_sal = resp.where(F.col("ocupado") & F.col("possui_salario"))

NAO_INFORMADO = "Não informado"
def preencher(df, colunas):
    return df.fillna(NAO_INFORMADO, subset=colunas)


# ------------------------------------------------------------------
# 1. gold_fato_mercado
#    Uma linha por combinação de dimensões. Permite responder
#    "como está estruturado o mercado" com qualquer corte.
#    Use SUM(qtd_respondentes) e, para salário,
#    SUM(soma_salario_estimado) / SUM(qtd_com_salario).
# ------------------------------------------------------------------
dims_fato = [
    "ano_coleta", "edicao", "cargo_padronizado", "nivel_padronizado", "atuacao",
    "genero_padronizado", "faixa_idade", "regiao", "uf", "setor", "porte_empresa",
    "nivel_ensino", "tempo_exp_dados_grupo", "modelo_trabalho_atual_pad", "situacao_trabalho",
]
fato = (
    preencher(resp, [d for d in dims_fato if d not in ("ano_coleta", "edicao")])
    .groupBy(*dims_fato)
    .agg(
        F.count("*").alias("qtd_respondentes"),
        F.sum(F.when(F.col("ocupado"), 1).otherwise(0)).alias("qtd_ocupados"),
        F.sum(F.when(F.col("gestor"), 1).otherwise(0)).alias("qtd_gestores"),
        F.sum(F.when(F.col("ocupado") & F.col("possui_salario"), 1).otherwise(0)).alias("qtd_com_salario"),
        F.sum(F.when(F.col("ocupado") & F.col("possui_salario"), F.col("salario_estimado"))).alias("soma_salario_estimado"),
        F.sum(F.when(F.col("satisfeito"), 1).otherwise(0)).alias("qtd_satisfeitos"),
        F.sum(F.when(F.col("usa_ia_produtividade"), 1).otherwise(0)).alias("qtd_usa_ia"),
        F.sum(F.when(F.col("usa_ia_produtividade").isNotNull(), 1).otherwise(0)).alias("qtd_respondeu_ia"),
    )
)
gravar_tabela(fato, "gold_fato_mercado")

# ------------------------------------------------------------------
# 2. gold_remuneracao  ("Quais perfis são mais valorizados?")
#    Cortes: geral, cargo, nível e cargo x nível
# ------------------------------------------------------------------
def remuneracao_por(dimensoes, rotulo):
    df = preencher(base_sal, dimensoes).groupBy("ano_coleta", *dimensoes).agg(*metricas_salario())
    for d in ["cargo_padronizado", "nivel_padronizado"]:
        if d not in dimensoes:
            df = df.withColumn(d, F.lit("Todos"))
    return df.withColumn("corte", F.lit(rotulo))


remuneracao = (
    remuneracao_por([], "geral")
    .unionByName(remuneracao_por(["cargo_padronizado"], "cargo"))
    .unionByName(remuneracao_por(["nivel_padronizado"], "nivel"))
    .unionByName(remuneracao_por(["cargo_padronizado", "nivel_padronizado"], "cargo_nivel"))
    .withColumn("amostra_suficiente", F.col("qtd_respondentes") >= MIN_AMOSTRA)
)
# variação do salário médio em relação à edição anterior (mesmo corte)
janela_ano = Window.partitionBy("corte", "cargo_padronizado", "nivel_padronizado").orderBy("ano_coleta")
remuneracao = remuneracao.withColumn(
    "var_pct_salario_vs_ano_anterior",
    F.round((F.col("salario_medio_estimado") / F.lag("salario_medio_estimado").over(janela_ano) - 1) * 100, 2),
)
gravar_tabela(remuneracao, "gold_remuneracao")

# ------------------------------------------------------------------
# 3. gold_diversidade_genero  ("Cenário de diversidade de gênero")
#    Para cada corte (geral/cargo/nível/região): participação de cada
#    gênero e salário médio estimado (ocupados com salário).
# ------------------------------------------------------------------
partes = []
for corte in ["geral", "cargo_padronizado", "nivel_padronizado", "regiao", "faixa_idade"]:
    base = resp if corte == "geral" else preencher(resp, [corte])
    categoria = F.lit("Todos") if corte == "geral" else F.col(corte)
    df = (
        base.withColumn("categoria", categoria)
        .groupBy("ano_coleta", "categoria", "genero_padronizado")
        .agg(
            F.count("*").alias("qtd_respondentes"),
            F.round(F.avg(F.when(F.col("ocupado") & F.col("possui_salario"), F.col("salario_estimado"))), 2)
             .alias("salario_medio_estimado"),
            F.sum(F.when(F.col("ocupado") & F.col("possui_salario"), 1).otherwise(0)).alias("qtd_com_salario"),
        )
        .withColumn("corte", F.lit(corte.replace("_padronizado", "")))
    )
    partes.append(df)

diversidade = partes[0]
for p in partes[1:]:
    diversidade = diversidade.unionByName(p)
janela_cat = Window.partitionBy("ano_coleta", "corte", "categoria")
diversidade = (
    diversidade
    .withColumn("qtd_total_categoria", F.sum("qtd_respondentes").over(janela_cat))
    .withColumn("pct_na_categoria", pct("qtd_respondentes", "qtd_total_categoria"))
    .withColumn("amostra_suficiente", F.col("qtd_com_salario") >= MIN_AMOSTRA)
)
gravar_tabela(diversidade, "gold_diversidade_genero")

# ------------------------------------------------------------------
# 4. gold_adocao_tecnologias  ("Quais tecnologias têm maior adoção?")
#    Denominador = quem respondeu aquela pergunta na edição.
#    Também traz o corte por cargo para ver tecnologias por perfil.
# ------------------------------------------------------------------
multi_perfil = multi.join(
    resp.select("ano_coleta", "id_respondente", "cargo_padronizado", "nivel_padronizado"),
    ["ano_coleta", "id_respondente"], "left",
)
multi_perfil = preencher(multi_perfil, ["cargo_padronizado", "nivel_padronizado"])


def adocao_por(dimensao):
    grupo = ["ano_coleta", "categoria"] + ([dimensao] if dimensao else [])
    base = (
        multi_perfil.groupBy(*grupo)
        .agg(F.countDistinct("id_respondente").alias("qtd_respondentes_pergunta"))
    )
    marcados = (
        multi_perfil.groupBy(*(grupo + ["item"]))
        .agg(F.sum("marcado").alias("qtd_marcou"))
    )
    df = marcados.join(base, grupo, "inner")
    if dimensao is None:
        df = df.withColumn("cargo_padronizado", F.lit("Todos"))
    return df.withColumn("corte", F.lit("cargo" if dimensao else "geral"))


adocao = adocao_por(None).unionByName(adocao_por("cargo_padronizado"))
adocao = (
    adocao.withColumn("pct_adocao", pct("qtd_marcou", "qtd_respondentes_pergunta"))
    .withColumn(
        "ranking",
        F.dense_rank().over(
            Window.partitionBy("ano_coleta", "corte", "cargo_padronizado", "categoria")
            .orderBy(F.col("qtd_marcou").desc())
        ),
    )
    .withColumn("amostra_suficiente", F.col("qtd_respondentes_pergunta") >= MIN_AMOSTRA)
)
gravar_tabela(adocao, "gold_adocao_tecnologias")

# ------------------------------------------------------------------
# 5. gold_adocao_ia  ("Índice de adoção de IA e seu impacto")
#    Formato longo: indicador / resposta / qtd / base / pct,
#    com cortes geral, nível e cargo.
# ------------------------------------------------------------------
def distribuicao(coluna, indicador, dimensao=None):
    base = resp.where(F.col(coluna).isNotNull())
    grupo = ["ano_coleta"] + ([dimensao] if dimensao else [])
    if dimensao:
        base = preencher(base, [dimensao])
    df = (
        base.groupBy(*(grupo + [coluna]))
        .agg(F.count("*").alias("qtd"))
        .withColumn("base", F.sum("qtd").over(Window.partitionBy(*grupo)))
        .withColumn("pct", pct("qtd", "base"))
        .withColumn("resposta", F.col(coluna).cast("string"))
        .withColumn("indicador", F.lit(indicador))
        .withColumn("corte", F.lit(dimensao.replace("_padronizado", "") if dimensao else "geral"))
        .withColumn("categoria", F.col(dimensao) if dimensao else F.lit("Todos"))
        .select("ano_coleta", "indicador", "corte", "categoria", "resposta", "qtd", "base", "pct")
    )
    return df


ia_partes = [
    distribuicao("usa_ia_produtividade", "usa_ia_para_produtividade"),
    distribuicao("usa_ia_produtividade", "usa_ia_para_produtividade", "nivel_padronizado"),
    distribuicao("usa_ia_produtividade", "usa_ia_para_produtividade", "cargo_padronizado"),
    distribuicao("ia_prioridade_empresa", "ia_prioridade_na_empresa_visao_gestor"),
    distribuicao("ia_resultados_llm", "resultados_com_llm_visao_gestor"),
]
# alternativas de múltipla escolha ligadas a IA
for categoria, indicador in [
    ("ia_produtividade", "tipo_de_uso_ia_produtividade"),
    ("ia_uso_empresa", "uso_ia_na_empresa"),
    ("ia_barreiras", "barreiras_para_ia_visao_gestor"),
]:
    m = multi.where(F.col("categoria") == categoria)
    ia_partes.append(
        m.groupBy("ano_coleta", "item")
        .agg(F.sum("marcado").alias("qtd"))
        .join(m.groupBy("ano_coleta").agg(F.countDistinct("id_respondente").alias("base")), "ano_coleta")
        .withColumn("pct", pct("qtd", "base"))
        .withColumn("indicador", F.lit(indicador))
        .withColumn("corte", F.lit("geral"))
        .withColumn("categoria", F.lit("Todos"))
        .withColumnRenamed("item", "resposta")
        .select("ano_coleta", "indicador", "corte", "categoria", "resposta", "qtd", "base", "pct")
    )

# impacto: salário médio de quem usa vs. não usa IA
ia_salario = (
    base_sal.where(F.col("usa_ia_produtividade").isNotNull())
    .groupBy("ano_coleta", "usa_ia_produtividade")
    .agg(F.count("*").alias("qtd"), F.round(F.avg("salario_estimado"), 2).alias("pct"))
    .withColumn("base", F.col("qtd"))
    .withColumn("indicador", F.lit("salario_medio_por_uso_ia"))
    .withColumn("corte", F.lit("geral"))
    .withColumn("categoria", F.lit("Todos"))
    .withColumn("resposta", F.col("usa_ia_produtividade").cast("string"))
    .select("ano_coleta", "indicador", "corte", "categoria", "resposta", "qtd", "base", "pct")
)
ia_partes.append(ia_salario)

adocao_ia = ia_partes[0]
for p in ia_partes[1:]:
    adocao_ia = adocao_ia.unionByName(p)
adocao_ia = (
    adocao_ia
    .withColumnRenamed("pct", "valor")  # % para todos os indicadores, exceto salario_medio_por_uso_ia (R$)
    .withColumn("unidade", F.when(F.col("indicador") == "salario_medio_por_uso_ia", F.lit("R$")).otherwise(F.lit("%")))
)
gravar_tabela(adocao_ia, "gold_adocao_ia")

# ------------------------------------------------------------------
# 6. gold_trabalho_regiao
#    ("Diferenças entre regiões, senioridades e modelos de trabalho")
# ------------------------------------------------------------------
dims_reg = ["regiao", "nivel_padronizado", "modelo_trabalho_atual_pad", "modelo_trabalho_ideal_pad"]
trabalho = (
    preencher(resp.where(F.col("ocupado")), dims_reg)
    .groupBy("ano_coleta", *dims_reg)
    .agg(
        F.count("*").alias("qtd_respondentes"),
        F.sum(F.when(F.col("possui_salario"), 1).otherwise(0)).alias("qtd_com_salario"),
        F.sum(F.when(F.col("possui_salario"), F.col("salario_estimado"))).alias("soma_salario_estimado"),
        F.sum(F.when(F.col("satisfeito"), 1).otherwise(0)).alias("qtd_satisfeitos"),
        F.sum(F.when(F.col("atitude_retorno_presencial").startswith("Vou procurar"), 1).otherwise(0))
         .alias("qtd_sairia_se_presencial"),
    )
    .withColumn("salario_medio_estimado", F.round(F.col("soma_salario_estimado") / F.col("qtd_com_salario"), 2))
)
gravar_tabela(trabalho, "gold_trabalho_regiao")

job.commit()
