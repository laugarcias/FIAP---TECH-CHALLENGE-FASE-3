"""
TECH CHALLENGE - FASE 3 | JOB 02 - CAMADA SILVER
Harmonizar as 3 edições da Bronze em um modelo único e comparável:
1) silver_respondentes = 1 linha por respondente/edição,colunas com o MESMO nome nos 3 anos, tipos corretos e categorias padronizadas.
2) silver_respostas_multiplas = formato longo (respondente x item) das perguntas de múltipla escolha(linguagens, cloud, BI, IA, ETL...).

Entrada  : s3://<BUCKET>/bronze/...
Saída    : s3://<BUCKET>/silver/respondentes/ano_coleta=<ano>/
           s3://<BUCKET>/silver/respostas_multiplas/ano_coleta=<ano>/
Catálogo : <DATABASE>.silver_respondentes
           <DATABASE>.silver_respostas_multiplas

Parâmetros do Job: --BUCKET  --DATABASE

"""
import re
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
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

EDICOES = {2023: "2023_2024", 2024: "2024_2025", 2025: "2025_2026"}

# 2. DE-PARA de colunas: nome Silver = coluna Bronze em cada edição
#    None = a pergunta não existe naquela edição (vira nulo).
#    As perguntas mudaram de código entre os anos (ex.: modelo de
#    trabalho é 2.r em 2024 e 2.q em 2025), por isso o mapeamento manual.

MAPA_COLUNAS = {
    # identificação
    "id_respondente":       {2023: "p0_id", 2024: "p0_a_token", 2025: "p0_a_token"},
    "dt_envio_texto":       {2023: None, 2024: "p0_d_data_hora_envio", 2025: "p0_d_data_hora_envio"},
    # perfil demográfico
    "idade":                {2023: "p1_a_idade", 2024: "p1_a_idade", 2025: "p1_a_idade"},
    "faixa_idade":          {2023: "p1_a_1_faixa_idade", 2024: "p1_a_1_faixa_idade", 2025: "p1_a_1_faixa_idade"},
    "genero":               {2023: "p1_b_genero", 2024: "p1_b_genero", 2025: "p1_b_genero"},
    "cor_raca_etnia":       {2023: "p1_c_cor_raca_etnia", 2024: "p1_c_cor_raca_etnia", 2025: "p1_c_cor_raca_etnia"},
    "pcd":                  {2023: "p1_d_pcd", 2024: "p1_d_pcd", 2025: "p1_d_pcd"},
    "vive_no_brasil_txt":   {2023: "p1_g_vive_no_brasil", 2024: "p1_g_vive_no_brasil", 2025: "p1_g_vive_no_brasil"},
    "uf":                   {2023: "p1_i_1_uf_onde_mora", 2024: "p1_i_1_uf_onde_mora", 2025: "p1_i_1_uf_onde_mora"},
    "regiao":               {2023: "p1_i_2_regiao_onde_mora", 2024: "p1_i_2_regiao_onde_mora", 2025: "p1_i_2_regiao_onde_mora"},
    "nivel_ensino":         {2023: "p1_l_nivel_de_ensino", 2024: "p1_l_nivel_de_ensino", 2025: "p1_l_nivel_de_ensino"},
    "area_formacao":        {2023: "p1_m_area_de_formacao", 2024: "p1_m_area_de_formacao", 2025: "p1_m_area_de_formacao"},
    # carreira
    "situacao_trabalho":    {2023: "p2_a_qual_sua_situacao_atual_de_trabalho", 2024: "p2_a_situacao_de_trabalho", 2025: "p2_a_situacao_de_trabalho"},
    "setor":                {2023: "p2_b_setor", 2024: "p2_b_setor", 2025: "p2_b_setor"},
    "porte_empresa":        {2023: "p2_c_numero_de_funcionarios", 2024: "p2_c_numero_de_funcionarios", 2025: "p2_c_numero_de_funcionarios"},
    "gestor_txt":           {2023: "p2_d_gestor", 2024: "p2_d_atua_como_gestor", 2025: "p2_d_atua_como_gestor"},
    "cargo_gestor":         {2023: "p2_e_cargo_como_gestor", 2024: "p2_e_cargo_como_gestor", 2025: "p2_e_cargo_como_gestor"},
    "cargo_atual":          {2023: "p2_f_cargo_atual", 2024: "p2_f_cargo_atual", 2025: "p2_f_cargo_atual"},
    "nivel":                {2023: "p2_g_nivel", 2024: "p2_g_nivel", 2025: "p2_g_nivel"},
    "faixa_salarial":       {2023: "p2_h_faixa_salarial", 2024: "p2_h_faixa_salarial", 2025: "p2_h_faixa_salarial"},
    "tempo_exp_dados":      {2023: "p2_i_quanto_tempo_de_experiencia_na_area_de_dados_voce_tem", 2024: "p2_i_tempo_de_experiencia_em_dados", 2025: "p2_i_tempo_de_experiencia_em_dados"},
    "tempo_exp_ti":         {2023: "p2_j_quanto_tempo_de_experiencia_na_area_de_ti_engenharia_de_software_voce_teve_antes_de_comecar_a_t", 2024: "p2_j_tempo_de_experiencia_em_ti", 2025: "p2_j_tempo_de_experiencia_em_ti"},
    "satisfeito_txt":       {2023: "p2_k_voce_esta_satisfeito_na_sua_empresa_atual", 2024: "p2_k_satisfeito_atualmente", 2025: "p2_k_satisfeito_atualmente"},
    "pretende_mudar_emprego_6m": {2023: "p2_n_voce_pretende_mudar_de_emprego_nos_proximos_6_meses", 2024: "p2_n_planos_de_mudar_de_emprego_6m", 2025: "p2_n_planos_de_mudar_de_emprego_6m"},
    "layoff_empresa":       {2023: "p2_q_empresa_que_trabaha_passou_por_layoff_em_2023", 2024: "p2_q_empresa_passou_por_layoff_em_2024", 2025: "p2_p_empresa_passou_por_layoff_em_2025"},
    "modelo_trabalho_atual": {2023: "p2_r_atualmente_qual_a_sua_forma_de_trabalho", 2024: "p2_r_modelo_de_trabalho_atual", 2025: "p2_q_modelo_de_trabalho_atual"},
    "modelo_trabalho_ideal": {2023: "p2_s_qual_a_forma_de_trabalho_ideal_para_voce", 2024: "p2_s_modelo_de_trabalho_ideal", 2025: "p2_r_modelo_de_trabalho_ideal"},
    "atitude_retorno_presencial": {2023: "p2_t_caso_sua_empresa_decida_pelo_modelo_100_presencial_qual_sera_sua_atitude", 2024: "p2_t_atitude_em_caso_de_retorno_presencial", 2025: "p2_s_atitude_em_caso_de_retorno_presencial"},
    "atuacao":              {2023: "p4_a_1_atuacao", 2024: "p4_a_1_atuacao_em_dados", 2025: "p4_a_1_atuacao_em_dados"},
    # empresa / dados / IA
    "tamanho_time_dados":   {2023: "p3_a_qual_o_numero_aproximado_de_pessoas_que_atuam_com_dados_na_sua_empresa_hoje", 2024: "p3_a_numero_de_pessoas_em_dados", 2025: "p3_a_numero_de_pessoas_em_dados"},
    "ia_prioridade_empresa": {2023: "p3_e_ai_generativa_e_uma_prioridade_em_sua_empresa", 2024: "p3_e_ai_generativa_e_llm_e_uma_prioridade", 2025: "p3_e_ai_generativa_e_llm_e_uma_prioridade"},
    "ia_resultados_llm":    {2023: None, 2024: None, 2025: "p3_g_empresa_esta_conseguindo_ter_bons_resultados_com_llms"},
    "linguagem_mais_usada": {2023: "p4_e_entre_as_linguagens_listadas_abaixo_qual_e_a_que_voce_mais_utiliza_no_trabalho", 2024: "p4_e_linguagem_mais_usada", 2025: None},
    "cloud_preferida":      {2023: "p4_i_cloud_preferida", 2024: "p4_i_cloud_preferida", 2025: "p4_f_cloud_preferida"},
    "bi_preferida":         {2023: "p4_k_qual_sua_ferramenta_de_bi_preferida", 2024: "p4_k_ferramenta_de_bi_preferida", 2025: "p4_h_ferramenta_de_bi_preferida"},
    "possui_data_lake_txt": {2023: "p6_c_sua_organizacao_possui_um_data_lake", 2024: "p6_c_possui_data_lake", 2025: "p6_c_possui_data_lake"},
    "possui_dw_txt":        {2023: "p6_e_sua_organizacao_possui_um_data_warehouse", 2024: "p6_e_possui_data_warehouse", 2025: "p6_e_possui_data_warehouse"},
}

# Perguntas de múltipla escolha: código da pergunta (já padronizado na
# Bronze) em cada edição. As alternativas são as colunas "<codigo>_<n>_...".
PERGUNTAS_MULTIPLAS = {
    "linguagem":          {2023: "p4_d", 2024: "p4_d", 2025: "p4_c"},
    "banco_dados":        {2023: "p4_g", 2024: "p4_g", 2025: "p4_d"},
    "cloud":              {2023: "p4_h", 2024: "p4_h", 2025: "p4_e"},
    "ferramenta_bi":      {2023: "p4_j", 2024: "p4_j", 2025: "p4_g"},
    "fonte_dados":        {2023: "p4_b", 2024: "p4_b", 2025: "p4_b"},
    "ia_uso_empresa":     {2023: "p4_l", 2024: "p4_l", 2025: "p4_i"},
    "ia_produtividade":   {2023: "p4_m", 2024: "p4_m", 2025: "p4_j"},
    "ia_barreiras":       {2023: "p3_g", 2024: "p3_g", 2025: "p3_h"},
    "cargos_time_dados":  {2023: "p3_b", 2024: "p3_b", 2025: "p3_b"},
    "desafios_gestor":    {2023: "p3_d", 2024: "p3_d", 2025: "p3_d"},
    "criterios_emprego":  {2023: "p2_o", 2024: "p2_o", 2025: "p2_o"},
    "etl_engenharia":     {2023: "p6_b", 2024: "p6_b", 2025: "p6_b"},
    "etl_analise":        {2023: "p7_b", 2024: "p7_b", 2025: "p7_b"},
    "ds_tecnicas":        {2023: "p8_b", 2024: "p8_b", 2025: "p8_b"},
    "ds_tecnologias":     {2023: "p8_c", 2024: "p8_c", 2025: "p8_c"},
}

# Correções de problemas encontrados nos arquivos originais
# (auditoria: colunas 0/1 comparadas com a resposta em texto da mesma
#  pergunta, linha a linha)

# 2023-2024, pergunta P4_h (cloud): os rótulos das 3 primeiras colunas
# estão trocados no arquivo. Conferência com o texto da resposta:
#   P4_h_1 rotulada "Azure"  -> bate 100% com quem marcou AWS
#   P4_h_2 rotulada "AWS"    -> bate 100% com quem marcou GCP
#   P4_h_3 rotulada "GCP"    -> bate 100% com quem marcou Azure
CORRECAO_ROTULOS = {
    (2023, "cloud"): {
        1: "Amazon Web Services (AWS)",
        2: "Google Cloud (GCP)",
        3: "Azure (Microsoft)",
    },
}

# Coluna 0/1 de "Não utilizo nenhuma linguagem" vem zerada em 2024 e
# 2025, mas o texto da resposta tem essa opção. Recalculamos pelo texto.
# chave: (ano, categoria, ordem_item) -> (coluna texto na Bronze, trecho)
CORRECAO_PELO_TEXTO = {
    (2024, "linguagem", 15): ("p4_d_linguagem_de_programacao_dia_a_dia", "Não utilizo"),
    (2025, "linguagem", 10): ("p4_c_linguagem_preferida", "Não utilizo"),
}

# Alternativas de "uso de IA para produtividade" (mesma ordem nas 3 edições)
IA_PRODUTIVIDADE = {
    "ia_nao_usa": 1,
    "ia_gratuita": 2,
    "ia_paga_proprio_bolso": 3,
    "ia_paga_pela_empresa": 4,
    "ia_assistente_codigo": 5,
}

# 3. Tabelas de padronização de valores

# faixa -> (ordem, mínimo, máximo, ponto médio estimado)
# "Acima de R$ 40.001" não tem teto: usamos o piso (40.001) como valor
# conservador e marcamos faixa_aberta = true. Médias ficam subestimadas
# no topo; documente isso na apresentação.
FAIXAS_SALARIAIS = {
    "Menos de R$ 1.000/mês":            (1, 0, 1000, 500),
    "de R$ 1.001/mês a R$ 2.000/mês":   (2, 1001, 2000, 1500),
    "de R$ 2.001/mês a R$ 3.000/mês":   (3, 2001, 3000, 2500),
    "de R$ 3.001/mês a R$ 4.000/mês":   (4, 3001, 4000, 3500),
    "de R$ 4.001/mês a R$ 6.000/mês":   (5, 4001, 6000, 5000),
    "de R$ 6.001/mês a R$ 8.000/mês":   (6, 6001, 8000, 7000),
    "de R$ 8.001/mês a R$ 12.000/mês":  (7, 8001, 12000, 10000),
    "de R$ 12.001/mês a R$ 16.000/mês": (8, 12001, 16000, 14000),
    "de R$ 16.001/mês a R$ 20.000/mês": (9, 16001, 20000, 18000),
    "de R$ 20.001/mês a R$ 25.000/mês": (10, 20001, 25000, 22500),
    "de R$ 25.001/mês a R$ 30.000/mês": (11, 25001, 30000, 27500),
    "de R$ 30.001/mês a R$ 40.000/mês": (12, 30001, 40000, 35000),
    "Acima de R$ 40.001/mês":           (13, 40001, None, 40001),
}

CARGOS = {
    "Analista de Dados/Data Analyst": "Analista de Dados",
    "Cientista de Dados/Data Scientist": "Cientista de Dados",
    # 2023 juntava engenharia e arquitetura em uma única opção;
    # para comparar os 3 anos, agrupamos as duas nos 3 anos.
    "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect": "Engenharia/Arquitetura de Dados",
    "Engenheiro de Dados/Data Engineer/Data Architect": "Engenharia/Arquitetura de Dados",
    "Arquiteto de Dados/Data Architect": "Engenharia/Arquitetura de Dados",
    "Analista de BI/BI Analyst": "Analista de BI",
    "Analytics Engineer": "Analytics Engineer",
    "Analista de Negócios/Business Analyst": "Analista de Negócios",
    "Engenheiro de Machine Learning/ML Engineer/AI Engineer": "Engenharia de ML/IA",
    "Data Product Manager/ Product Manager (PM/APM/DPM/GPM/PO)": "Product Manager",
    "Desenvolvedor/ Engenheiro de Software/ Analista de Sistemas": "Desenvolvimento de Software",
    "Analista de Suporte/Analista Técnico": "Suporte/Técnico",
    "DBA/Administrador de Banco de Dados": "DBA",
    "Analista de Inteligência de Mercado/Market Intelligence": "Inteligência de Mercado",
    "Estatístico": "Estatística",
    "Economista": "Economia",
    "Professor/Pesquisador": "Academia/Pesquisa",
    "Outras Engenharias (não inclui dev)": "Outras Engenharias",
    "Outra Opção": "Outros",
}

MODELOS_TRABALHO = {
    "Modelo 100% remoto": "Remoto",
    "Modelo 100% presencial": "Presencial",
    "Modelo híbrido flexível (o funcionário tem liberdade para escolher quando estar no escritório presencialmente)": "Híbrido flexível",
    "Modelo híbrido com dias fixos de trabalho presencial": "Híbrido com dias fixos",
}

# Experiência em dados: 2023 tem as opções sobrepostas "de 4 a 6 anos" e
# "de 5 a 6 anos". Para comparar os anos, agrupamos em faixas mais largas.
EXPERIENCIA_GRUPO = {
    "Não tenho experiência na área de dados": "0. Sem experiência",
    "Menos de 1 ano": "1. Até 2 anos",
    "de 1 a 2 anos": "1. Até 2 anos",
    "de 3 a 4 anos": "2. De 3 a 6 anos",
    "de 4 a 6 anos": "2. De 3 a 6 anos",
    "de 5 a 6 anos": "2. De 3 a 6 anos",
    "de 7 a 10 anos": "3. De 7 a 10 anos",
    "Mais de 10 anos": "4. Mais de 10 anos",
}

SITUACOES_OCUPADAS = [
    "Empregado (CLT)",
    "Empreendedor ou Empregado (CNPJ)",
    "Estagiário",
    "Servidor Público",
    "Vivo no Brasil e trabalho remoto para empresa de fora do Brasil",
    "Vivo fora do Brasil e trabalho para empresa de fora do Brasil",
    "Trabalho na área Acadêmica/Pesquisador",
    "Freelancer",
]



# 4. Funções auxiliares

def mapear(coluna, dicionario, manter_original=False):
    """Traduz valores usando um dicionário Python (vira CASE WHEN)."""
    expr = None
    for origem, destino in dicionario.items():
        cond = F.col(coluna) == F.lit(origem)
        expr = F.when(cond, F.lit(destino)) if expr is None else expr.when(cond, F.lit(destino))
    return expr.otherwise(F.col(coluna) if manter_original else F.lit(None))


def para_booleano(coluna):
    """'1', '1.0', 'True', 'true' -> true | '0', '0.0', 'False' -> false."""
    v = F.lower(F.trim(F.col(coluna)))
    return (
        F.when(v.isin("1", "1.0", "true", "sim"), F.lit(True))
         .when(v.isin("0", "0.0", "false", "nao", "não"), F.lit(False))
         .otherwise(F.lit(None).cast("boolean"))
    )


def para_inteiro(coluna):
    return F.col(coluna).cast("double").cast("int")


def texto_limpo(coluna):
    """Remove espaços nas pontas e transforma '' em nulo."""
    v = F.trim(F.col(coluna))
    return F.when(v == "", F.lit(None)).otherwise(v)


def rotulo_alternativa(coluna_original):
    """Extrai o texto da alternativa a partir do cabeçalho original.

    "4.d.3_Python"             -> "Python"
    "3.f.1 Colaboradores ..."  -> "Colaboradores ..."
    "('P4_d_3 ', 'Python')"    -> "Python"
    """
    m = re.match(r"""^\(\s*['"](.*?)['"]\s*,\s*['"](.*?)['"]?\s*\)$""", coluna_original.strip(), re.DOTALL)
    if m:
        rotulo = m.group(2)
    else:
        rotulo = re.sub(r"^\d+(\.[a-z])+\.\d+[_ ]", "", coluna_original.strip())
    rotulo = rotulo.replace('""', '"').strip()
    if rotulo.lower().startswith("não utilizo nenhuma"):
        rotulo = "Não utilizo nenhuma das opções"
    return rotulo


# 5. Leitura da Bronze e do dicionário de colunas

dicionario = spark.read.parquet(f"{RAIZ}/bronze/dicionario_colunas/").collect()
original_por_coluna = {(r["ano_coleta"], r["coluna_bronze"]): r["coluna_original"] for r in dicionario}

respondentes_por_ano = []
multiplas_por_ano = []

for ano, sufixo in EDICOES.items():
    df = spark.read.parquet(f"{RAIZ}/bronze/state_of_data_{sufixo}/")
    colunas = set(df.columns)

    # 5.1 Validação do de-para (falha cedo, com mensagem clara)
    faltantes = [v[ano] for v in MAPA_COLUNAS.values() if v[ano] and v[ano] not in colunas]
    if faltantes:
        raise ValueError(f"[{ano}] Colunas do de-para não encontradas na Bronze: {faltantes}")

    # 5.2 Seleção e renomeação para o padrão Silver
    selecao = [F.col("ano_coleta"), F.col("edicao")]
    for destino, origem in MAPA_COLUNAS.items():
        selecao.append(texto_limpo(origem[ano]).alias(destino) if origem[ano] else F.lit(None).cast("string").alias(destino))

    # 5.3 Flags de uso de IA para produtividade (colunas p4_m_<n>_... etc.)
    codigo_ia = PERGUNTAS_MULTIPLAS["ia_produtividade"][ano]
    for nome_flag, indice in IA_PRODUTIVIDADE.items():
        candidatas = [c for c in df.columns if re.match(rf"^{codigo_ia}_{indice}_", c)]
        if len(candidatas) != 1:
            raise ValueError(f"[{ano}] Alternativa {indice} de IA não encontrada de forma única: {candidatas}")
        selecao.append(para_inteiro(candidatas[0]).alias(nome_flag))

    respondentes_por_ano.append(df.select(*selecao))

    # 5.4 Perguntas de múltipla escolha -> formato longo
    for categoria, codigos in PERGUNTAS_MULTIPLAS.items():
        codigo = codigos[ano]
        alternativas = [c for c in df.columns if re.match(rf"^{codigo}_\d+_", c)]
        if not alternativas:
            raise ValueError(f"[{ano}] Nenhuma alternativa encontrada para {categoria} ({codigo})")
        estruturas = []
        for c in alternativas:
            ordem = int(re.match(rf"^{codigo}_(\d+)_", c).group(1))
            rotulo = CORRECAO_ROTULOS.get((ano, categoria), {}).get(
                ordem, rotulo_alternativa(original_por_coluna[(ano, c)])
            )
            marcado = para_inteiro(c)
            if (ano, categoria, ordem) in CORRECAO_PELO_TEXTO:
                col_texto, trecho = CORRECAO_PELO_TEXTO[(ano, categoria, ordem)]
                marcado = (
                    F.when(texto_limpo(col_texto).isNull(), F.lit(None).cast("int"))
                     .when(F.col(col_texto).contains(trecho), F.lit(1))
                     .otherwise(F.lit(0))
                )
            estruturas.append(
                F.struct(
                    F.lit(ordem).alias("ordem_item"),
                    F.lit(rotulo).alias("item"),
                    marcado.alias("marcado"),
                )
            )
        id_col = MAPA_COLUNAS["id_respondente"][ano]
        longo = (
            df.select(
                F.col("ano_coleta"),
                F.col("edicao"),
                F.trim(F.col(id_col)).alias("id_respondente"),
                F.explode(F.array(*estruturas)).alias("r"),
            )
            .select(
                "ano_coleta", "edicao", "id_respondente",
                F.lit(categoria).alias("categoria"),
                F.lit(codigo).alias("codigo_pergunta"),
                "r.ordem_item", "r.item", "r.marcado",
            )
            .where(F.col("marcado").isNotNull())  # nulo = não respondeu a seção
        )
        multiplas_por_ano.append(longo)


def unir(lista):
    base = lista[0]
    for outro in lista[1:]:
        base = base.unionByName(outro)
    return base

# 6. Tratamentos e colunas derivadas (silver_respondentes)

df = unir(respondentes_por_ano)

# 6.1 Remove respostas duplicadas (mesmo token na mesma edição)
janela_dup = Window.partitionBy("ano_coleta", "id_respondente").orderBy(F.col("dt_envio_texto").asc_nulls_last())
df = (
    df.withColumn("_rn", F.row_number().over(janela_dup))
      .where(F.col("_rn") == 1)
      .drop("_rn")
)

# 6.2 Tipos
df = (
    df.withColumn("idade", para_inteiro("idade"))
      .withColumn("dt_envio", F.to_timestamp("dt_envio_texto", "dd/MM/yyyy HH:mm:ss"))
      .withColumn("vive_no_brasil", para_booleano("vive_no_brasil_txt"))
      .withColumn("gestor", para_booleano("gestor_txt"))
      .withColumn("satisfeito", para_booleano("satisfeito_txt"))
      .withColumn("possui_data_lake", para_booleano("possui_data_lake_txt"))
      .withColumn("possui_data_warehouse", para_booleano("possui_dw_txt"))
      .drop("dt_envio_texto", "vive_no_brasil_txt", "gestor_txt", "satisfeito_txt",
            "possui_data_lake_txt", "possui_dw_txt")
)

# 6.3 Remuneração (faixa -> valores numéricos estimados)
faixa = F.col("faixa_salarial")
df = (
    df.withColumn("faixa_salarial_ordem", mapear("faixa_salarial", {k: v[0] for k, v in FAIXAS_SALARIAIS.items()}).cast("int"))
      .withColumn("salario_min", mapear("faixa_salarial", {k: v[1] for k, v in FAIXAS_SALARIAIS.items()}).cast("int"))
      .withColumn("salario_max", mapear("faixa_salarial", {k: v[2] for k, v in FAIXAS_SALARIAIS.items() if v[2] is not None}).cast("int"))
      .withColumn("salario_estimado", mapear("faixa_salarial", {k: v[3] for k, v in FAIXAS_SALARIAIS.items()}).cast("int"))
      .withColumn("faixa_aberta", F.coalesce(faixa == F.lit("Acima de R$ 40.001/mês"), F.lit(False)))
      # faixas digitadas fora do padrão (ex.: "de R$ 101/mês a R$ 2.000/mês")
      .withColumn("faixa_salarial_inconsistente", faixa.isNotNull() & F.col("faixa_salarial_ordem").isNull())
)

# 6.4 Categorias padronizadas
df = (
    df.withColumn("genero_padronizado", F.coalesce(F.col("genero"), F.lit("Não informado")))
      .withColumn("cargo_padronizado",
                  F.when(F.col("cargo_atual").isNotNull(), mapear("cargo_atual", CARGOS, manter_original=True))
                   .when(F.col("gestor"), F.lit("Gestão"))
                   .otherwise(F.lit(None)))
      .withColumn("nivel_padronizado",
                  F.when(F.col("nivel").isNotNull(), F.col("nivel"))
                   .when(F.col("gestor"), F.lit("Gestor"))
                   .otherwise(F.lit(None)))
      .withColumn("modelo_trabalho_atual_pad", mapear("modelo_trabalho_atual", MODELOS_TRABALHO, manter_original=True))
      .withColumn("modelo_trabalho_ideal_pad", mapear("modelo_trabalho_ideal", MODELOS_TRABALHO, manter_original=True))
      .withColumn("tempo_exp_dados_grupo", mapear("tempo_exp_dados", EXPERIENCIA_GRUPO))
      .withColumn("ocupado", F.coalesce(F.col("situacao_trabalho").isin(SITUACOES_OCUPADAS), F.lit(False)))
      .withColumn("possui_salario", F.col("salario_estimado").isNotNull())
)

# 6.5 Indicador consolidado de uso de IA para produtividade
alguma_ia = (
    (F.coalesce(F.col("ia_gratuita"), F.lit(0)) + F.coalesce(F.col("ia_paga_proprio_bolso"), F.lit(0))
     + F.coalesce(F.col("ia_paga_pela_empresa"), F.lit(0)) + F.coalesce(F.col("ia_assistente_codigo"), F.lit(0))) > 0
)
respondeu_ia = (
    F.col("ia_nao_usa").isNotNull() | F.col("ia_gratuita").isNotNull() | F.col("ia_paga_proprio_bolso").isNotNull()
    | F.col("ia_paga_pela_empresa").isNotNull() | F.col("ia_assistente_codigo").isNotNull()
)
df = df.withColumn(
    "usa_ia_produtividade",
    F.when(~respondeu_ia, F.lit(None).cast("boolean")).otherwise(alguma_ia),
)

# 6.6 Ordem final das colunas
COLUNAS_SILVER = [
    "edicao", "id_respondente", "dt_envio",
    "idade", "faixa_idade", "genero", "genero_padronizado", "cor_raca_etnia", "pcd",
    "vive_no_brasil", "uf", "regiao", "nivel_ensino", "area_formacao",
    "situacao_trabalho", "ocupado", "setor", "porte_empresa",
    "gestor", "cargo_gestor", "cargo_atual", "cargo_padronizado", "nivel", "nivel_padronizado", "atuacao",
    "faixa_salarial", "faixa_salarial_ordem", "salario_min", "salario_max", "salario_estimado",
    "faixa_aberta", "faixa_salarial_inconsistente", "possui_salario",
    "tempo_exp_dados", "tempo_exp_dados_grupo", "tempo_exp_ti",
    "satisfeito", "pretende_mudar_emprego_6m", "layoff_empresa",
    "modelo_trabalho_atual", "modelo_trabalho_atual_pad", "modelo_trabalho_ideal", "modelo_trabalho_ideal_pad",
    "atitude_retorno_presencial", "tamanho_time_dados",
    "ia_prioridade_empresa", "ia_resultados_llm", "usa_ia_produtividade",
    "ia_nao_usa", "ia_gratuita", "ia_paga_proprio_bolso", "ia_paga_pela_empresa", "ia_assistente_codigo",
    "linguagem_mais_usada", "cloud_preferida", "bi_preferida",
    "possui_data_lake", "possui_data_warehouse",
    "ano_coleta",  # partição sempre por último
]
df_respondentes = df.select(*COLUNAS_SILVER)

# Respostas múltiplas: mantém só respondentes que sobreviveram à deduplicação
df_multiplas = (
    unir(multiplas_por_ano)
    .dropDuplicates(["ano_coleta", "id_respondente", "categoria", "ordem_item"])
    .join(df_respondentes.select("ano_coleta", "id_respondente"), ["ano_coleta", "id_respondente"], "inner")
    .select("edicao", "id_respondente", "categoria", "codigo_pergunta", "ordem_item", "item", "marcado", "ano_coleta")
)

# 7. Checagens de qualidade (aparecem no log do CloudWatch)

print("== Respondentes por edição ==")
df_respondentes.groupBy("ano_coleta").count().orderBy("ano_coleta").show()
print("== Faixas salariais fora do padrão ==")
df_respondentes.where("faixa_salarial_inconsistente").groupBy("ano_coleta", "faixa_salarial").count().show(truncate=False)
print("== Cargos sem tradução no de-para (devem ser 0 ou poucos) ==")
(df_respondentes.where(F.col("cargo_atual").isNotNull() & ~F.col("cargo_atual").isin(list(CARGOS.keys())))
    .groupBy("cargo_atual").count().show(truncate=False))

# 8. Gravação + catálogo

def gravar_tabela(df_saida, caminho, tabela, particoes):
    glueContext.purge_s3_path(caminho, {"retentionPeriod": 0})
    dyf = DynamicFrame.fromDF(df_saida, glueContext, tabela)
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


gravar_tabela(df_respondentes, f"{RAIZ}/silver/respondentes/", "silver_respondentes", ["ano_coleta"])
gravar_tabela(df_multiplas, f"{RAIZ}/silver/respostas_multiplas/", "silver_respostas_multiplas", ["ano_coleta"])

job.commit()
