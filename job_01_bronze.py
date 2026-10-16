"""

TECH CHALLENGE - FASE 3 | JOB 01 - CAMADA BRONZE
Objetivo
    Ler os 3 CSVs brutos do State of Data Brasil (pasta landing/ do S3),
    padronizar somente  os nomes das colunas (os valores ficam intactos),
    acrescentar metadados de ingestão e gravar em Parquet na camada
    Bronze, criando/atualizando as tabelas no Glue Data Catalog.

Entrada  : s3://<BUCKET>/landing/state_of_data/<arquivo>.csv
Saída    : s3://<BUCKET>/bronze/state_of_data_<edicao>/ano_coleta=<ano>/
Catálogo : <DATABASE>.bronze_state_of_data_<edicao>      (1 por edição)
           <DATABASE>.bronze_dicionario_colunas          (de-para de nomes)

Parâmetros do Job (Job details > Advanced > Job parameters)
    --BUCKET    nome do bucket (sem s3://)
    --DATABASE  nome do database do Glue Catalog (ex.: db_state_of_data)

"""
import re
import sys
import unicodedata
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# 1. Inicialização

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "DATABASE"])
BUCKET = args["BUCKET"]
DATABASE = args["DATABASE"]
RAIZ = f"s3://{BUCKET}"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Edições a ingerir: (arquivo na landing, ano de coleta, sufixo da tabela)
# ano_coleta = ano em que o questionário foi respondido (out-dez).
EDICOES = [
    ("2023-2024.csv", 2023, "2023_2024"),
    ("2024-2025.csv", 2024, "2024_2025"),
    ("2025-2026.csv", 2025, "2025_2026"),
]

DT_INGESTAO = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# 2. Funções auxiliares

def remover_acentos(texto):
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


# Cabeçalho no formato tupla (edição 2023-2024): "('P1_b ', 'Genero')".
# Aceita aspas simples ou duplas e tolera a aspa final ausente, que ocorre
# no arquivo original em "('P6_b_16 ', 'SQL Server Integration Services (SSIS))".
PADRAO_TUPLA = re.compile(r"""^\(\s*['"]\s*(.*?)\s*['"]\s*,\s*['"](.*?)['"]?\s*\)$""", re.DOTALL)


def padronizar_nome(nome_original, max_len=100):
    """Converte o cabeçalho original em snake_case ASCII.

    Exemplos
        "1.b_genero"                 = "p1_b_genero"
        "('P1_b ', 'Genero')"        = "p1_b_genero"
        "4.d.3_Python"               = "p4_d_3_python"
        "('P4_d_3 ', 'Python')"      =  "p4_d_3_python"
    """
    nome = nome_original.strip()
    m = PADRAO_TUPLA.match(nome)
    if m:  # junta código + rótulo da tupla
        nome = f"{m.group(1)}_{m.group(2)}"
    nome = remover_acentos(nome).lower()
    nome = re.sub(r"[^a-z0-9]+", "_", nome).strip("_")
    if not nome:
        nome = "coluna"
    if nome[0].isdigit():  # Athena não gosta de nomes iniciados por número
        nome = "p" + nome
    return nome[:max_len].rstrip("_")


def nomes_unicos(nomes):
    """Garante que não haja colunas repetidas após o truncamento."""
    vistos, saida = {}, []
    for n in nomes:
        if n in vistos:
            vistos[n] += 1
            n = f"{n}_{vistos[n]}"
        else:
            vistos[n] = 0
        saida.append(n)
    return saida


def gravar_tabela(df, caminho, tabela, particoes):
    """Grava em Parquet e cria/atualiza a tabela no Glue Data Catalog."""
    glueContext.purge_s3_path(caminho, {"retentionPeriod": 0})  # reexecução idempotente
    dyf = DynamicFrame.fromDF(df, glueContext, tabela)
    sink = glueContext.getSink(
        connection_type="s3",
        path=caminho,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=particoes,
        compression="snappy",
    )
    sink.setFormat("parquet", useGlueParquetWriter=True)
    sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=tabela)
    sink.writeFrame(dyf)
    print(f"[OK] {DATABASE}.{tabela} -> {caminho}")

# 3. Ingestão de cada edição


linhas_dicionario = []

for arquivo, ano, sufixo in EDICOES:
    origem = f"{RAIZ}/landing/state_of_data/{arquivo}"
    print(f"Lendo {origem}")

    df_raw = (
        spark.read
        .option("header", True)
        .option("multiLine", True)      # há respostas com quebra de linha
        .option("quote", '"')
        .option("escape", '"')          # aspas duplas escapadas no padrão CSV
        .option("encoding", "UTF-8")
        .option("inferSchema", False)   # Bronze = tudo string
        .csv(origem)
    )

    originais = df_raw.columns
    novos = nomes_unicos([padronizar_nome(c) for c in originais])
    df = df_raw.toDF(*novos)

    for pos, (orig, novo) in enumerate(zip(originais, novos), start=1):
        linhas_dicionario.append((ano, sufixo, pos, orig, novo))

    df = (
        df.withColumn("edicao", F.lit(sufixo.replace("_", "-")))
          .withColumn("arquivo_origem", F.lit(arquivo))
          .withColumn("dt_ingestao", F.lit(DT_INGESTAO))
          .withColumn("ano_coleta", F.lit(ano).cast("int"))
    )

    qtd = df.count()
    print(f"{arquivo}: {qtd} linhas, {len(novos)} colunas")

    tabela = f"bronze_state_of_data_{sufixo}"
    gravar_tabela(df, f"{RAIZ}/bronze/state_of_data_{sufixo}/", tabela, ["ano_coleta"])

# 4. Dicionário de colunas (de-para original - padronizado)
#    Usado pelo Job 02 para recuperar os rótulos originais.

df_dic = spark.createDataFrame(
    linhas_dicionario,
    "ano_coleta int, edicao string, posicao int, coluna_original string, coluna_bronze string",
)
gravar_tabela(df_dic, f"{RAIZ}/bronze/dicionario_colunas/", "bronze_dicionario_colunas", ["ano_coleta"])

job.commit()
