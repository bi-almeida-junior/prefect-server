# Integração Zapt Tech

Pipeline PostgreSQL → API Zapt Tech com SQL editável.

---

## 🚀 Setup Rápido

### 1. Instalar

```bash
pip install psycopg2-binary==2.9.9 requests
```

### 2. Criar Blocks

Acesse: `http://localhost:4200/blocks` → **"+ Create Block"**

#### **Block 1: PostgreSQL Credentials**

```
Tipo: PostgreSQL Credentials
Nome: zapt-tech-postgres

┌─────────────────────────────┐
│ host: seu_host              │
│ port: 5432                  │
│ database: seu_database      │
│ user: seu_usuario           │
│ password: sua_senha         │
│ schema: public              │
└─────────────────────────────┘
```

#### **Block 2: String (API URL)**

```
Tipo: String
Nome: zapt-tech-api-url

┌──────────────────────────────────────────────────────────────────────────┐
│ value: https://us-central1-zapt-backend.cloudfunctions.net/saveBulkData │
└──────────────────────────────────────────────────────────────────────────┘
```

#### **Block 3: Secret (API Key) - Opcional**

```
Tipo: Secret
Nome: zapt-tech-api-key

┌─────────────────────────────┐
│ value: (sua key aqui)        │
└─────────────────────────────┘
```

### 3. Deploy

```bash
python flows/zapt_tech/zapt_tech_to_api.py
```

---

## 📊 Executar

### Interface

1. Acesse: `http://localhost:4200/deployments`
2. Clique em **zapt-tech-to-api** → **Run** → **Custom**
3. Parâmetros:
   - `sigla_shopping`: **NK** | **BS** | **GS** | **NR** | **CS** | **NS**
   - `sql_query`: (opcional) Query customizada

### CLI

```bash
prefect deployment run zapt-tech-to-api/zapt-tech-to-api --param sigla_shopping="NK"
```

---

## 📝 Query SQL

Use `{sigla_shopping}` como placeholder:

```sql
SELECT
    id_shopping as shopping,
    id_luc as luc,
    qt_metragem as abl,
    ...
FROM tabela
WHERE id_shopping = '{sigla_shopping}'
```

**Campos obrigatórios (24):**
shopping, luc, abl, vitrine, lojista, segmento, atividade, contrato, vencimento, competencia, media_venda, venda_m2, amm, amm_m2, aluguel_variavel, condominio, condominio_m2, fundo_promocao, fundo_promocao_m2, faturamento_total, co_percentual, tipo_contrato, cdu_m2, status

---

## 🌐 API

**Endpoint:** `https://us-central1-zapt-backend.cloudfunctions.net/saveBulkData`

**Formato:**
```json
{
  "organizationId": 6291090961989632,
  "schema": {"name": "Stores_Almeida_Junior", "fields": [...]},
  "items": [{"shopping": "NK", "luc": "L-23", ...}]
}
```

---

## ⚙️ Configuração

### Agendamento

Padrão: Diariamente às 8h (America/Sao_Paulo)

Editar: `zapt_tech_to_api.py` linha ~982

### Siglas Válidas

| Sigla | Shopping |
|-------|----------|
| NK | Neumarkt |
| BS | Balneário |
| GS | Garten |
| NR | Norte |
| CS | Continente |
| NS | Nações |

---

## 🛠️ Troubleshooting

| Erro | Solução |
|------|---------|
| Block not found | Criar 3 blocks: `zapt-tech-postgres`, `zapt-tech-api-url`, `zapt-tech-api-key` |
| Sigla inválida | Usar: NK, BS, GS, NR, CS, NS |
| Connection refused | Verificar credenciais no block `zapt-tech-postgres` |
| API failed | Verificar URL no block `zapt-tech-api-url` |
| Query error | Usar `{sigla_shopping}` e testar query no PostgreSQL |

---

## 📂 Arquivos

```
flows/zapt_tech/
├── zapt_tech_to_api.py    # Flow + Blocks inline
└── README.md              # Este arquivo

shared/connections/
└── postgresql.py          # Helpers PostgreSQL
```
