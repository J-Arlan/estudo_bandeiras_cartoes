
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ================================
# CONFIGURAÇÕES - CAMPOS QUE PODEM SER ALTERADOS
# ================================

# Número total de transações a gerar (altere para mais dados se necessário)
N = 100000  # Aumentado para cobrir 10 anos de dados

# Bandeiras de cartão a incluir (altere para adicionar/remover bandeiras)
BANDEIRAS = ["Visa", "Mastercard", "Elo"]

# Status possíveis das transações
STATUS = ["Aprovado", "Negado"]

# Probabilidades dos status (88% aprovado, 12% negado - altere se necessário)
PROBABILIDADES_STATUS = [0.88, 0.12]

# Faixa de valores das transações em reais (mínimo e máximo - altere se necessário)
VALOR_MIN = 5000
VALOR_MAX = 50000

# Período de datas: 10 anos (de 2016-01-01 a 2025-12-31 - altere start_date e end_date se necessário)
START_DATE = datetime(2016, 1, 1)
END_DATE = datetime(2025, 12, 31)

# Probabilidade de chargeback para transações aprovadas (5% - altere se necessário)
PROB_CHARGEBACK = 0.05

# Probabilidade de fraude geral (1% - altere se necessário)
PROB_FRAUDE = 0.01

# Faixa de tempo de processamento em segundos (0.5 a 2.5 - altere se necessário)
TEMPO_PROC_MIN = 0.5
TEMPO_PROC_MAX = 2.5

# ================================
# GERAÇÃO DE DADOS FICTÍCIOS
# ================================

# Definir sementes para reprodutibilidade
random.seed(42)
np.random.seed(42)

# Calcular diferença de dias entre as datas
days_diff = (END_DATE - START_DATE).days

# Gerar datas aleatórias dentro do período de 10 anos
datas = [
    START_DATE + timedelta(days=random.randint(0, days_diff))
    for _ in range(N)
]

# Criar DataFrame principal
df = pd.DataFrame({
    "Data": datas,
    "Bandeira": np.random.choice(BANDEIRAS, N),
    "Status": np.random.choice(STATUS, N, p=PROBABILIDADES_STATUS),
    "Valor": np.round(np.random.uniform(VALOR_MIN, VALOR_MAX, N), 2)
})

# ================================
# GERAR CAMPOS DERIVADOS
# ================================

# Normalizar status para criar coluna Aprovado
status_norm = (
    df["Status"]
    .astype(str)
    .str.normalize("NFKD")
    .str.encode("ascii", errors="ignore")
    .str.decode("ascii")
    .str.strip()
    .str.lower()
)

df["Aprovado"] = status_norm.eq("aprovado").astype(int)

# Gerar Chargeback: apenas para transações aprovadas, com probabilidade PROB_CHARGEBACK
df["Chargeback"] = df.apply(
    lambda x: 1 if x["Aprovado"] == 1 and random.random() < PROB_CHARGEBACK else 0,
    axis=1
)

# Gerar Fraude: probabilidade geral PROB_FRAUDE
df["Fraude"] = df.apply(
    lambda x: 1 if random.random() < PROB_FRAUDE else 0,
    axis=1
)

# Tempo de processamento aleatório
df["Tempo_Processamento"] = np.round(np.random.uniform(TEMPO_PROC_MIN, TEMPO_PROC_MAX, N), 2)

# Garantir que a coluna Data seja datetime
df["Data"] = pd.to_datetime(df["Data"])

# Criar coluna AnoMes para agrupamento mensal (formato YYYY-MM)
df["AnoMes"] = df["Data"].dt.year.astype(str) + "-" + df["Data"].dt.month.astype(str).str.zfill(2)

# ================================
# VALIDAÇÕES
# ================================

# Verificar se status são válidos
validos = {"aprovado", "negado"}
assert set(status_norm.unique()).issubset(validos), \
    f"Status inválido encontrado: {set(status_norm.unique()) - validos}"

# Verificar se Aprovado é apenas 0 ou 1
assert df["Aprovado"].isin([0, 1]).all(), "Erro: Aprovado contém valores inválidos."

# Verificar proporção esperada de aprovação (70% a 95%)
aprov_rate = df["Aprovado"].mean()
assert 0.70 <= aprov_rate <= 0.95, f"Taxa de aprovação fora do esperado: {aprov_rate:.2%}"

# ================================
# DIMENSÃO DE BANDEIRAS
# ================================

# Agrupar por Bandeira e calcular métricas
dim = df.groupby("Bandeira").agg(
    TPV=("Valor", "sum"),
    Transacoes=("Valor", "count"),
    Aprovacoes=("Aprovado", "sum"),
    TicketMedio=("Valor", "mean"),
    Chargebacks=("Chargeback", "sum"),
    Fraudes=("Fraude", "sum"),
    TempoMedioProc=("Tempo_Processamento", "mean")
)

# Calcular KPIs
dim["TaxaAprovacao"] = dim["Aprovacoes"] / dim["Transacoes"]
dim["ChargebackRate"] = dim["Chargebacks"] / dim["Transacoes"]
dim["FraudeRate"] = dim["Fraudes"] / dim["Transacoes"]

dim = dim.reset_index()

# ================================
# SALVAR ARQUIVOS
# ================================

# Salvar fato das transações
df.to_excel("fato_transacoes.xlsx", index=False, engine="openpyxl")

# Salvar dimensão das bandeiras
dim.to_excel("dim_bandeiras.xlsx", index=False, engine="openpyxl")

print("Arquivos gerados com sucesso!")
