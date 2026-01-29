
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# ================================
# Configurações (ajuste se necessário)
# ================================

# Nº transações
N = 100000  # Nº transações

# Bandeiras (ex.: Visa, Mastercard, Elo)
BANDEIRAS = ["Visa", "Mastercard", "Elo"]

# Status possíveis
STATUS = ["Aprovado", "Negado"]

# Probabilidades (Aprovado/Negado)
PROBABILIDADES_STATUS = [0.88, 0.12]

# Faixa de valores (min, max)
VALOR_MIN = 5000
VALOR_MAX = 50000

# Período (2016-01-01 a 2025-12-31)
START_DATE = datetime(2016, 1, 1)
END_DATE = datetime(2025, 12, 31)

# Probabilidade de chargeback (p/ aprovadas)
PROB_CHARGEBACK = 0.05

# Probabilidade de fraude
PROB_FRAUDE = 0.01

# Tempo de processamento (s)
TEMPO_PROC_MIN = 0.5
TEMPO_PROC_MAX = 2.5

# ================================
# Geração de dados
# ================================

# Seeds (reprodutibilidade)
random.seed(42)
np.random.seed(42)

# Dias no período
days_diff = (END_DATE - START_DATE).days

# Datas aleatórias
datas = [
    START_DATE + timedelta(days=random.randint(0, days_diff))
    for _ in range(N)
]

# Criar DataFrame
df = pd.DataFrame({
    "Data": datas,
    "Bandeira": np.random.choice(BANDEIRAS, N),
    "Status": np.random.choice(STATUS, N, p=PROBABILIDADES_STATUS),
    "Valor": np.round(np.random.uniform(VALOR_MIN, VALOR_MAX, N), 2)
})

# ================================
# Campos derivados
# ================================

# Normalizar status
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

# Chargeback (só aprovadas)
df["Chargeback"] = df.apply(
    lambda x: 1 if x["Aprovado"] == 1 and random.random() < PROB_CHARGEBACK else 0,
    axis=1
)

# Fraude (prob. geral)
df["Fraude"] = df.apply(
    lambda x: 1 if random.random() < PROB_FRAUDE else 0,
    axis=1
)

# Tempo de processamento
df["Tempo_Processamento"] = np.round(np.random.uniform(TEMPO_PROC_MIN, TEMPO_PROC_MAX, N), 2)

# Converter Data p/ datetime
df["Data"] = pd.to_datetime(df["Data"])

# Criar AnoMes (YYYY-MM)
df["AnoMes"] = df["Data"].dt.year.astype(str) + "-" + df["Data"].dt.month.astype(str).str.zfill(2)

# ================================
# Validações
# ================================

# Validar status
validos = {"aprovado", "negado"}
assert set(status_norm.unique()).issubset(validos), \
    f"Status inválido encontrado: {set(status_norm.unique()) - validos}"

# Validar Aprovado (0/1)
assert df["Aprovado"].isin([0, 1]).all(), "Erro: Aprovado contém valores inválidos."

# Validar taxa de aprovação (70%-95%)
aprov_rate = df["Aprovado"].mean()
assert 0.70 <= aprov_rate <= 0.95, f"Taxa de aprovação fora do esperado: {aprov_rate:.2%}"

# ================================
# Dimensão bandeiras
# ================================

# Agrupar e calcular métricas
dim = df.groupby("Bandeira").agg(
    TPV=("Valor", "sum"),
    Transacoes=("Valor", "count"),
    Aprovacoes=("Aprovado", "sum"),
    TicketMedio=("Valor", "mean"),
    Chargebacks=("Chargeback", "sum"),
    Fraudes=("Fraude", "sum"),
    TempoMedioProc=("Tempo_Processamento", "mean")
)

# KPI: taxas
dim["TaxaAprovacao"] = dim["Aprovacoes"] / dim["Transacoes"]
dim["ChargebackRate"] = dim["Chargebacks"] / dim["Transacoes"]
dim["FraudeRate"] = dim["Fraudes"] / dim["Transacoes"]

dim = dim.reset_index()

# ================================
# Salvar arquivos
# ================================

# Salvar fato
df.to_excel("fato_transacoes.xlsx", index=False, engine="openpyxl")

# Salvar dimensão
dim.to_excel("dim_bandeiras.xlsx", index=False, engine="openpyxl")

print("Arquivos gerados com sucesso!")
