# Weather API to Snowflake

Flow de integração de dados climáticos da API HGBrasil para o Snowflake.

## 📋 Descrição

Este flow coleta dados climáticos de 5 cidades de Santa Catarina (Blumenau, Balneário Camboriú, Joinville, São José e Criciúma) usando a API HGBrasil e armazena no Snowflake em duas tabelas distintas com estratégias diferentes.

## 🎯 Estratégia de Dados

### Tabela 1: `BRZ_CLIMA_TEMPO` (Histórico - APPEND ONLY)
- **Objetivo**: Registrar condições climáticas REAIS observadas
- **Dados**: Apenas o primeiro registro da resposta API (condições atuais)
- **Frequência**: A cada hora (24 registros/dia/cidade)
- **Estratégia**: INSERT simples (acumula histórico)
- **Uso**: Análise histórica do clima real

### Tabela 2: `BRZ_CLIMA_TEMPO_PREVISAO` (Previsão - FULL REFRESH)
- **Objetivo**: Manter previsão atualizada dos próximos 15 dias
- **Dados**: Todos os 15 dias de previsão
- **Frequência**: A cada hora (sobrescreve dados anteriores)
- **Estratégia**: TRUNCATE + INSERT (sempre com dados mais recentes)
- **Uso**: Consulta de previsão futura

## 🏙️ Cidades Monitoradas

As cidades estão fixadas no código (não consulta Snowflake):

| ID | Cidade | Nome API |
|----|--------|----------|
| 1 | Blumenau | Blumenau,SC |
| 2 | Balneário Camboriú | Balneário Camboriú,SC |
| 3 | Joinville | Joinville,SC |
| 4 | São José | São José,SC |
| 5 | Criciúma | Criciúma,SC |

*IDs correspondem à tabela `DIM_CIDADE` no Snowflake*

## 📊 Estrutura de Dados

### Campos Coletados
- Temperatura atual, máxima e mínima
- Umidade
- Nebulosidade
- Volume de chuva (mm)
- Probabilidade de chuva (%)
- Velocidade do vento
- Horários de nascer e pôr do sol
- Fase da lua
- Descrição e condição do tempo
- Latitude e longitude

## 🔧 Configuração

### Pré-requisitos

1. **API Key HGBrasil**
   ```bash
   # Criar secret no Prefect
   prefect block register -m prefect.blocks.system

   # Adicionar no Prefect UI:
   # Nome: hgbrasil-weather-api-key
   # Valor: sua_api_key_aqui
   ```

2. **Credenciais Snowflake**
   - Configuradas via `.env` ou parâmetros do flow
   - Variáveis necessárias:
     - `SNOWFLAKE_ACCOUNT`
     - `SNOWFLAKE_USER`
     - `SNOWFLAKE_PRIVATE_KEY`
     - `SNOWFLAKE_WAREHOUSE`
     - `SNOWFLAKE_ROLE`

3. **Criar Tabelas no Snowflake**
   ```bash
   # Execute o script DDL fornecido
   sql flows/weather/create_tables.sql
   ```

### Deploy do Flow

```bash
# Navegue até o diretório do projeto
cd C:\Users\jonas.hamerski\PycharmProjects\prefect-server

# Execute o script para fazer deploy
python flows/weather/weather_api_to_snowflake.py
```

## ⏱️ Agendamento

- **Frequência**: A cada hora (cron: `0 * * * *`)
- **Timezone**: America/Sao_Paulo
- **Work Pool**: local-pool

## 📈 Métricas e Monitoramento

O flow gera artefatos Prefect com:
- Número de cidades coletadas
- Registros de clima atual inseridos
- Registros de previsão inseridos
- Duração da execução

## 🔔 Alertas

O flow envia alertas via função `send_flow_success_alert` e `send_flow_error_alert`:
- Sucesso: Resumo com métricas
- Erro: Detalhes da falha e stack trace

## 📝 Exemplo de Uso

### Consultar clima atual das últimas 24 horas
```sql
SELECT *
FROM BRZ_CLIMA_TEMPO
WHERE ID_CIDADE = 1  -- Blumenau
ORDER BY DT_COLETA_API DESC
LIMIT 24;
```

### Consultar previsão dos próximos dias
```sql
SELECT
    ID_CIDADE,
    DT_PREVISAO,
    DS_DIA_SEMANA,
    NR_TEMP_MINIMA,
    NR_TEMP_MAXIMA,
    NR_PROB_CHUVA,
    DS_DESCRICAO_TEMPO
FROM BRZ_CLIMA_TEMPO_PREVISAO
ORDER BY ID_CIDADE, DT_PREVISAO;
```

### Comparar previsão vs realidade
```sql
SELECT
    p.ID_CIDADE,
    p.DT_PREVISAO,
    p.NR_TEMP_MAXIMA AS PREV_MAX,
    r.NR_TEMP_MAXIMA AS REAL_MAX,
    p.NR_PROB_CHUVA AS PREV_CHUVA,
    r.NR_CHUVA_MM AS REAL_CHUVA
FROM BRZ_CLIMA_TEMPO_PREVISAO p
LEFT JOIN BRZ_CLIMA_TEMPO r
    ON p.ID_CIDADE = r.ID_CIDADE
    AND p.DT_PREVISAO = r.DT_PREVISAO
WHERE p.DT_PREVISAO < CURRENT_DATE()
ORDER BY p.ID_CIDADE, p.DT_PREVISAO;
```

## 🔍 Troubleshooting

### Erro: "API Key inválida"
- Verifique se o secret `hgbrasil-weather-api-key` está configurado corretamente no Prefect
- Teste a API Key manualmente: https://api.hgbrasil.com/weather?key=SUA_KEY

### Erro: "Tabela não existe"
- Execute o script `create_tables.sql` no Snowflake
- Verifique se está usando o database e schema corretos

### Erro: "Nenhum dado coletado"
- Verifique conexão com a internet
- Verifique se a API HGBrasil está disponível
- Verifique logs de requisições individuais

## 📦 Dependências

- `requests`: Requisições HTTP para API HGBrasil
- `pandas`: Manipulação de dados
- `prefect`: Orquestração do flow
- `urllib3`: Manipulação de SSL warnings
- `snowflake-connector-python`: Conexão com Snowflake (via shared/connections)

## 🎨 Estrutura de Arquivos

```
flows/weather/
├── weather_api_to_snowflake.py  # Flow principal
├── create_tables.sql             # Script DDL das tabelas
└── README.md                     # Esta documentação
```

## 📄 Licença

Propriedade de Almeida Junior.

## 👥 Contato

Para dúvidas ou suporte, entre em contato com a equipe de Data Engineering.