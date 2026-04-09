"""
ETL – Data Warehouse de Transações de Cartão de Crédito
Projeto BI & Data Warehouse | ADS
Fase 2 – Implementação do Pipeline ETL

Star Schema:
    FATO_TRANSACAO
        ├── DIM_DATA
        ├── DIM_TITULAR
        ├── DIM_CATEGORIA
        └── DIM_ESTABELECIMENTO

Como executar:
    python etl.py

Requisitos:
    pip install pandas sqlalchemy psycopg2-binary
"""

import glob
import pandas as pd
from sqlalchemy import create_engine

# ══════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════
PASTA_DADOS = "dados/"   # pasta com os 12 CSVs Fatura_*.csv

# PostgreSQL — ajuste usuário/senha/banco conforme sua instalação
DB_URL = "postgresql://postgres:masterkey@localhost:5432/dw_cartoes"

# Para usar SQLite (sem PostgreSQL), descomente a linha abaixo:
# DB_URL = "sqlite:///dw_cartoes.db"

# ══════════════════════════════════════════════════════════
# FASE E — EXTRACT
# Lê todos os arquivos Fatura_*.csv da pasta dados/
# ══════════════════════════════════════════════════════════
print("=" * 55)
print("[ETL] FASE E – EXTRAÇÃO")
print("=" * 55)

arquivos = glob.glob(PASTA_DADOS + "Fatura_*.csv")

if not arquivos:
    raise FileNotFoundError(
        f"Nenhum arquivo Fatura_*.csv encontrado em '{PASTA_DADOS}'.\n"
        "Certifique-se de que os CSVs estão na pasta dados/."
    )

dfs = []
for arquivo in sorted(arquivos):
    df = pd.read_csv(arquivo, sep=';', encoding='utf-8')
    df['arquivo_origem'] = arquivo
    dfs.append(df)
    print(f"  ✔ {arquivo} → {len(df)} linhas")

dados = pd.concat(dfs, ignore_index=True)
print(f"\n[ETL] Total extraído: {len(dados)} registros de {len(arquivos)} fatura(s).\n")

# ══════════════════════════════════════════════════════════
# FASE T — TRANSFORM
# Limpeza, padronização e derivação de colunas
# ══════════════════════════════════════════════════════════
print("=" * 55)
print("[ETL] FASE T – TRANSFORMAÇÃO")
print("=" * 55)

# ── Datas ──────────────────────────────────────────────
dados['Data de Compra'] = pd.to_datetime(
    dados['Data de Compra'], format='%d/%m/%Y', errors='coerce'
)
dados['ano']        = dados['Data de Compra'].dt.year
dados['mes']        = dados['Data de Compra'].dt.month
dados['dia']        = dados['Data de Compra'].dt.day
dados['trimestre']  = dados['Data de Compra'].dt.quarter
dados['dia_semana'] = dados['Data de Compra'].dt.day_name()

# ── Valores numéricos ──────────────────────────────────
for col in ['Valor (em R$)', 'Valor (em US$)', 'Cotação (em R$)']:
    dados[col] = (
        dados[col]
        .astype(str)
        .str.replace(',', '.', regex=False)
        .pipe(pd.to_numeric, errors='coerce')
        .fillna(0)
    )

# ── Categoria ──────────────────────────────────────────
dados['Categoria'] = (
    dados['Categoria']
    .fillna('Não categorizado')
    .str.strip()
    .replace('-', 'Não categorizado')
    .replace('', 'Não categorizado')
)

# ── Descrição (estabelecimento) ────────────────────────
dados['Descrição'] = (
    dados['Descrição']
    .fillna('Não informado')
    .str.strip()
    .replace('-', 'Não informado')
)

# ── Titular ────────────────────────────────────────────
dados['Nome no Cartão']  = dados['Nome no Cartão'].fillna('Desconhecido').str.strip()
dados['Final do Cartão'] = dados['Final do Cartão'].astype(str).str.strip()

# ── Parcelas: "1/3" → num=1, total=3 ──────────────────
def tratar_parcela(p):
    p = str(p).strip()
    if p.lower() in ['única', 'unica', '']:
        return 1, 1
    elif '/' in p:
        try:
            n, t = p.split('/')
            return int(n), int(t)
        except ValueError:
            return None, None
    return None, None

dados[['num_parcela', 'total_parcelas']] = dados['Parcela'].apply(
    lambda x: pd.Series(tratar_parcela(x))
)

print(f"  ✔ Datas convertidas e colunas derivadas (ano, mes, trimestre, dia_semana)")
print(f"  ✔ Valores numéricos padronizados")
print(f"  ✔ Categorias: {dados['Categoria'].nunique()} únicas")
print(f"  ✔ Estabelecimentos: {dados['Descrição'].nunique()} únicos")
print(f"  ✔ Titulares: {dados['Nome no Cartão'].nunique()} único(s)")
print(f"  ✔ Parcelas tratadas")
print()

# ══════════════════════════════════════════════════════════
# CONSTRUÇÃO DO STAR SCHEMA
# ══════════════════════════════════════════════════════════
print("=" * 55)
print("[ETL] CONSTRUÇÃO DAS DIMENSÕES (Star Schema)")
print("=" * 55)

# DIM_DATA
dim_data = (
    dados[['Data de Compra', 'dia', 'mes', 'trimestre', 'ano', 'dia_semana']]
    .drop_duplicates(subset=['Data de Compra'])
    .sort_values('Data de Compra')
    .reset_index(drop=True)
)
dim_data.insert(0, 'id_data', dim_data.index + 1)
dim_data = dim_data.rename(columns={'Data de Compra': 'data_completa'})

# DIM_TITULAR
dim_titular = (
    dados[['Nome no Cartão', 'Final do Cartão']]
    .drop_duplicates()
    .sort_values('Nome no Cartão')
    .reset_index(drop=True)
)
dim_titular.insert(0, 'id_titular', dim_titular.index + 1)
dim_titular = dim_titular.rename(columns={
    'Nome no Cartão':  'nome_titular',
    'Final do Cartão': 'final_cartao'
})

# DIM_CATEGORIA
dim_categoria = (
    dados[['Categoria']]
    .drop_duplicates()
    .sort_values('Categoria')
    .reset_index(drop=True)
)
dim_categoria.insert(0, 'id_categoria', dim_categoria.index + 1)
dim_categoria = dim_categoria.rename(columns={'Categoria': 'nome_categoria'})

# DIM_ESTABELECIMENTO
dim_estabelecimento = (
    dados[['Descrição']]
    .drop_duplicates()
    .sort_values('Descrição')
    .reset_index(drop=True)
)
dim_estabelecimento.insert(0, 'id_estabelecimento', dim_estabelecimento.index + 1)
dim_estabelecimento = dim_estabelecimento.rename(columns={'Descrição': 'nome_estabelecimento'})

# FATO_TRANSACAO — merge para obter os IDs das dimensões
fato = dados.copy()
fato = fato.merge(dim_data[['id_data', 'data_completa']],
                  left_on='Data de Compra', right_on='data_completa', how='left')
fato = fato.merge(dim_titular[['id_titular', 'nome_titular', 'final_cartao']],
                  left_on=['Nome no Cartão', 'Final do Cartão'],
                  right_on=['nome_titular', 'final_cartao'], how='left')
fato = fato.merge(dim_categoria[['id_categoria', 'nome_categoria']],
                  left_on='Categoria', right_on='nome_categoria', how='left')
fato = fato.merge(dim_estabelecimento[['id_estabelecimento', 'nome_estabelecimento']],
                  left_on='Descrição', right_on='nome_estabelecimento', how='left')

fato_transacao = fato[[
    'id_data', 'id_titular', 'id_categoria', 'id_estabelecimento',
    'Valor (em R$)', 'Valor (em US$)', 'Cotação (em R$)',
    'Parcela', 'num_parcela', 'total_parcelas',
]].rename(columns={
    'Valor (em R$)':   'valor_brl',
    'Valor (em US$)':  'valor_usd',
    'Cotação (em R$)': 'cotacao',
    'Parcela':         'parcela_texto',
}).reset_index(drop=True)
fato_transacao.insert(0, 'id_transacao', fato_transacao.index + 1)

print(f"  ✔ DIM_DATA:              {len(dim_data):>4} registros")
print(f"  ✔ DIM_TITULAR:           {len(dim_titular):>4} registros")
print(f"  ✔ DIM_CATEGORIA:         {len(dim_categoria):>4} registros")
print(f"  ✔ DIM_ESTABELECIMENTO:   {len(dim_estabelecimento):>4} registros")
print(f"  ✔ FATO_TRANSACAO:        {len(fato_transacao):>4} registros")
print()

# ══════════════════════════════════════════════════════════
# FASE L — LOAD
# Carrega no banco de dados (dimensões primeiro, fato depois)
# ══════════════════════════════════════════════════════════
print("=" * 55)
print("[ETL] FASE L – CARGA NO BANCO DE DADOS")
print("=" * 55)

engine = create_engine(DB_URL)

dim_data.to_sql('dim_data',               engine, if_exists='replace', index=False)
print("  ✔ dim_data carregada")
dim_titular.to_sql('dim_titular',         engine, if_exists='replace', index=False)
print("  ✔ dim_titular carregada")
dim_categoria.to_sql('dim_categoria',     engine, if_exists='replace', index=False)
print("  ✔ dim_categoria carregada")
dim_estabelecimento.to_sql('dim_estabelecimento', engine, if_exists='replace', index=False)
print("  ✔ dim_estabelecimento carregada")
fato_transacao.to_sql('fato_transacao',   engine, if_exists='replace', index=False)
print("  ✔ fato_transacao carregada")

print()
print("=" * 55)
print("[ETL] ✅ PROCESSO CONCLUÍDO COM SUCESSO!")
print(f"       {len(fato_transacao)} transações carregadas no DW.")
print("=" * 55)