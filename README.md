# Data Warehouse — Transações de Cartão de Crédito

Projeto de BI & Data Warehouse | Análise e Desenvolvimento de Sistemas

---

## Estrutura do Projeto

```
trabalho-main/
├── dados/                        ← coloque aqui os 12 CSVs Fatura_*.csv
│   ├── Fatura_2025-03-20.csv
│   ├── Fatura_2025-04-20.csv
│   └── ... (12 arquivos no total)
├── etl.py                        ← Fase 2: pipeline ETL
├── consultas.sql                 ← Fase 3: queries analíticas
└── README.md
```

---

## Pré-requisitos

```bash
pip install pandas sqlalchemy psycopg2-binary
```

PostgreSQL instalado e rodando localmente. Crie o banco antes de rodar o ETL:

```sql
CREATE DATABASE dw_cartoes;
```

---

## Como executar o ETL (Fase 2)

1. Coloque todos os 12 CSVs na pasta `dados/`
2. Abra `etl.py` e ajuste a senha do PostgreSQL na linha `DB_URL`
3. Execute:

```bash
python etl.py
```

O script vai:
- Ler todos os `Fatura_*.csv` da pasta `dados/`
- Limpar e transformar os dados
- Criar as 4 dimensões + tabela fato no PostgreSQL

---

## Star Schema (Fase 1)

```
              DIM_TITULAR
              (id_titular, nome_titular, final_cartao)
                    |
DIM_DATA ──── FATO_TRANSACAO ──── DIM_CATEGORIA
              (id_transacao,
               id_data,
               id_titular,
               id_categoria,
               id_estabelecimento,
               valor_brl, valor_usd,
               cotacao, parcela_texto,
               num_parcela, total_parcelas)
                    |
              DIM_ESTABELECIMENTO
```

---

## Consultas SQL (Fase 3)

O arquivo `consultas.sql` contém 13 queries cobrindo todas as perguntas de negócio:

1. KPIs gerais do período
2. Gasto total por titular
3. Gasto por titular e mês
4. Top 10 categorias
5. Evolução mensal (série temporal)
6. Comparativo entre titulares
7. Top 10 estabelecimentos
8. Parcelamento: à vista vs parcelado
9. Dia da semana com mais gastos
10. Estornos e créditos
11. Transações internacionais (USD)
12. Gasto por trimestre
13. Categoria × Mês (heat map)

---

## Power BI — Fase 4 (Dashboards)

### Conectar ao PostgreSQL

1. Abra o Power BI Desktop
2. **Obter dados → Banco de Dados → PostgreSQL**
3. Servidor: `localhost` | Banco: `dw_cartoes`
4. Usuário: `postgres` | Senha: sua senha
5. Selecione as 5 tabelas: `fato_transacao`, `dim_data`, `dim_titular`, `dim_categoria`, `dim_estabelecimento`

### Criar os Relacionamentos (Vista de Modelo)

| De | Para | Cardinalidade |
|---|---|---|
| fato_transacao[id_data] | dim_data[id_data] | N:1 |
| fato_transacao[id_titular] | dim_titular[id_titular] | N:1 |
| fato_transacao[id_categoria] | dim_categoria[id_categoria] | N:1 |
| fato_transacao[id_estabelecimento] | dim_estabelecimento[id_estabelecimento] | N:1 |

### Medidas DAX principais

```dax
Total Gasto = CALCULATE(SUM(fato_transacao[valor_brl]), fato_transacao[valor_brl] > 0)

Ticket Médio = CALCULATE(AVERAGE(fato_transacao[valor_brl]), fato_transacao[valor_brl] > 0)

Qtd Compras = CALCULATE(COUNT(fato_transacao[id_transacao]), fato_transacao[valor_brl] > 0)

Total Estornos = CALCULATE(SUM(fato_transacao[valor_brl]), fato_transacao[valor_brl] < 0)

% por Categoria = DIVIDE([Total Gasto], CALCULATE([Total Gasto], ALL(dim_categoria)))
```

### Dashboards sugeridos

**Dashboard 1 — Visão Geral**
- 3 cartões KPI: Total Gasto, Qtd Compras, Ticket Médio
- Gráfico de barras: Categoria × Total Gasto
- Gráfico de linhas: Mês/Ano × Total Gasto
- Segmentações: Titular, Ano

**Dashboard 2 — Análise por Titular**
- Gráfico de barras agrupadas: Titular × Total por Mês
- Tabela: Titular, Qtd Compras, Total, Ticket Médio
- Gráfico de pizza: % por Titular

**Dashboard 3 — Parcelamento e Comportamento**
- Gráfico de rosca: À Vista vs Parcelado
- Barras: Dia da semana × Volume
- Top 10 Estabelecimentos

### Por que Power BI Desktop e não Web?

A fonte de dados é o PostgreSQL em localhost. O Power BI Web exige que o banco esteja acessível em nuvem ou via On-premises Data Gateway. Como o banco é local, o Power BI Desktop é a escolha correta — critério de seleção baseado na fonte de dados, conforme discutido em aula.
