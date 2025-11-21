import time
from datetime import datetime
from typing import Optional, List, Dict, Any

import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.cache_policies import NONE
from prefect.client.schemas.schedules import CronSchedule

from flows.weather.client import WeatherAPIClient
from flows.weather.schemas import parse_api_response, transform_to_snowflake_row
from shared.connections.snowflake import snowflake_connection
from shared.decorators import flow_alerts
from shared.utils import load_secret

# Carrega variáveis de ambiente
load_dotenv()

# ====== CONFIGURAÇÕES ======

# Snowflake
DATABASE = "AJ_DATALAKEHOUSE_RPA"
SCHEMA = "BRONZE"

# Mapeamento de cidades com IDs da tabela AJ_DATALAKEHOUSE_RPA.SILVER.DIM_CIDADE
CIDADES = [
    {"id": 1, "name": "Blumenau", "api_name": "Blumenau,SC"},
    {"id": 2, "name": "Balneário Camboriú", "api_name": "Balneário Camboriú,SC"},
    {"id": 3, "name": "Joinville", "api_name": "Joinville,SC"},
    {"id": 4, "name": "São José", "api_name": "São José,SC"},
    {"id": 5, "name": "Criciúma", "api_name": "Criciúma,SC"}
]

# Colunas das tabelas de clima
WEATHER_COLUMNS = [
    'ID_CIDADE', 'NR_LATITUDE', 'NR_LONGITUDE', 'NR_TEMPERATURA_ATUAL', 'NR_UMIDADE_ATUAL', 'DT_PREVISAO', 'DS_DATA_FORMATADA', 'DS_DATA_COMPLETA',
    'DS_DIA_SEMANA', 'NR_TEMP_MAXIMA', 'NR_TEMP_MINIMA', 'NR_UMIDADE', 'NR_NEBULOSIDADE', 'NR_CHUVA_MM', 'NR_PROB_CHUVA', 'DS_VENTO_VELOCIDADE',
    'DS_HORARIO_NASCER_SOL', 'DS_HORARIO_POR_SOL', 'DS_FASE_LUA', 'DS_DESCRICAO_TEMPO', 'DS_CONDICAO_TEMPO', 'DT_COLETA_API'
]


# ====== ESTRATÉGIA DE DADOS ======
# TABELA 1: BRZ_CLIMA_TEMPO (Histórico - APPEND ONLY)
#   - Armazena APENAS o primeiro registro (condições atuais no momento da coleta)
#   - Executado a cada hora = 24 registros/dia/cidade
#   - Nunca sobrescreve, sempre INSERT
#   - Dados reais observados
#
# TABELA 2: BRZ_CLIMA_TEMPO_PREVISAO (Previsão - FULL REFRESH)
#   - Armazena os 15 dias de previsão futura
#   - TRUNCATE + INSERT a cada execução
#   - Sempre tem a previsão mais atualizada
#   - Dados previstos pela API


@task(name="load_api_key", log_prints=True, cache_policy=NONE)
def load_api_key() -> Optional[str]:
    """
    Carrega a API Key do HGBrasil do Prefect Blocks.

    Returns:
        String com a API Key ou None se falhar
    """
    return load_secret("hgbrasil-weather-api-key")


@task(name="fetch_weather_data", log_prints=True, cache_policy=NONE)
def fetch_weather_data(client: WeatherAPIClient, cidade: Dict[str, Any]) -> Optional[tuple]:
    """Coleta e valida dados climáticos de uma cidade."""
    logger = get_run_logger()

    try:
        logger.info(f"🌤️  Coletando dados de {cidade['name']}...")

        # Requisição HTTP
        raw_data = client.fetch_weather(cidade["api_name"])

        if not raw_data:
            return None

        # Validação Pydantic
        api_response = parse_api_response(raw_data)
        logger.info(f"✅ {api_response.results.city} - {api_response.results.temp}°C - {len(api_response.results.forecast)} dias")

        return cidade["id"], api_response

    except Exception as e:
        logger.error(f"❌ Erro em {cidade['name']}: {e}")
        return None


@task(name="process_weather_data", log_prints=True, cache_policy=NONE)
def process_weather_data(weather_responses: List[tuple], only_first: bool = False) -> pd.DataFrame:
    """
    Processa dados climáticos para Snowflake.

    Args:
        weather_responses: Lista de tuplas (cidade_id, WeatherAPIResponse)
        only_first: True para clima atual (1º dia), False para todos os dias

    Returns:
        DataFrame pronto para inserção
    """
    logger = get_run_logger()
    records = []

    for cidade_id, api_response in weather_responses:
        # Pega apenas primeiro ou todos os dias de previsão
        forecast_days = api_response.results.forecast[:1] if only_first else api_response.results.forecast

        for forecast_day in forecast_days:
            row = transform_to_snowflake_row(cidade_id, api_response.results, forecast_day)
            records.append(row)

    df = pd.DataFrame(records)
    tipo = "ATUAL" if only_first else "PREVISÃO"
    logger.info(f"✅ Processados {len(df)} registros de {tipo}")

    return df


@task(name="insert_weather_data", log_prints=True, cache_policy=NONE)
def insert_weather_data(conn, table_name: str, df: pd.DataFrame, truncate: bool = False) -> int:
    """
    Insere dados climáticos no Snowflake.

    Args:
        conn: Conexão Snowflake
        table_name: Nome da tabela (BRZ_CLIMA_TEMPO ou BRZ_CLIMA_TEMPO_PREVISAO)
        df: DataFrame com dados a inserir
        truncate: Se True, executa TRUNCATE antes do INSERT

    Returns:
        Número de registros inseridos
    """
    logger = get_run_logger()

    if df.empty:
        logger.info(f"Nenhum dado para inserir em {table_name}")
        return 0

    cursor = conn.cursor()
    full_table = f"{DATABASE}.{SCHEMA}.{table_name}"

    try:
        # TRUNCATE se solicitado
        if truncate:
            logger.info(f"🗑️  Limpando dados anteriores de {full_table}...")
            cursor.execute(f"TRUNCATE TABLE {full_table}")
            conn.commit()
            logger.info("✅ Tabela truncada")

        # INSERT
        strategy = "FULL REFRESH" if truncate else "APPEND"
        logger.info(f"📊 Inserindo {len(df)} registros em {full_table} ({strategy})...")

        columns_list = ", ".join(WEATHER_COLUMNS)
        placeholders = ", ".join(["%s"] * len(WEATHER_COLUMNS))
        insert_sql = f"INSERT INTO {full_table} ({columns_list}) VALUES ({placeholders})"

        records = [tuple(row[col] for col in WEATHER_COLUMNS) for _, row in df.iterrows()]

        cursor.executemany(insert_sql, records)
        rows_inserted = cursor.rowcount
        conn.commit()

        logger.info(f"✅ {rows_inserted} registros inseridos com sucesso")
        return rows_inserted

    except Exception as e:
        logger.error(f"❌ Erro ao inserir dados: {e}")
        raise
    finally:
        cursor.close()


@flow(name="weather_api_to_snowflake", log_prints=True)
@flow_alerts(
    flow_name="Clima HGBrasil",
    source="API HGBrasil Weather",
    destination="Snowflake (BRONZE)",
    extract_summary=lambda result: {
        "cities_processed": result.get("cities_processed", 0),
        "records_loaded": result.get("current_inserted", 0) + result.get("forecast_inserted", 0)
    }
)
def main():
    """
    Flow principal: Coleta dados climáticos da API HGBrasil e insere no Snowflake.

    Executa a cada hora e gera dois tipos de registros:
    1. Clima Atual (BRZ_CLIMA_TEMPO): Condições atuais - APPEND ONLY
    2. Previsão 15 dias (BRZ_CLIMA_TEMPO_PREVISAO): Dados futuros - FULL REFRESH
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("🌤️  CLIMA: API HGBrasil → SNOWFLAKE")
    logger.info("=" * 80)

    # Carrega API Key e cria client
    api_key = load_api_key()
    client = WeatherAPIClient(api_key)

    # Coleta dados de todas as cidades
    logger.info(f"Coletando dados de {len(CIDADES)} cidades...")
    weather_data_list = []

    for i, cidade in enumerate(CIDADES, 1):
        logger.info(f"[{i}/{len(CIDADES)}] Processando {cidade['name']}...")
        weather_data = fetch_weather_data(client, cidade)

        if weather_data:
            weather_data_list.append(weather_data)

        # Pausa de 2 segundos entre requisições (evita sobrecarga)
        if i < len(CIDADES):
            time.sleep(2)

    if not weather_data_list:
        logger.error("❌ Nenhum dado coletado. Encerrando.")
        raise Exception("Falha ao coletar dados climáticos de todas as cidades")

    logger.info(f"✅ Dados coletados de {len(weather_data_list)}/{len(CIDADES)} cidades")

    with snowflake_connection(database=DATABASE, schema=SCHEMA) as conn:
        # Processa e insere clima ATUAL (apenas 1º dia)
        df_current = process_weather_data(weather_data_list, only_first=True)
        current_inserted = insert_weather_data(conn, "BRZ_CLIMA_TEMPO", df_current, truncate=False)

        # Processa e insere PREVISÃO (todos os 15 dias)
        df_forecast = process_weather_data(weather_data_list, only_first=False)
        forecast_inserted = insert_weather_data(conn, "BRZ_CLIMA_TEMPO_PREVISAO", df_forecast, truncate=True)

    # Resumo
    end_time = datetime.now()
    elapsed = end_time - start_time
    m, s = divmod(elapsed.total_seconds(), 60)

    logger.info("=" * 80)
    logger.info("✅ PROCESSO CONCLUÍDO COM SUCESSO")
    logger.info("=" * 80)
    logger.info(f"Início:   {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Fim:      {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Duração:  {int(m)}m {int(s)}s")
    logger.info(f"Cidades coletadas: {len(weather_data_list)}/{len(CIDADES)}")
    logger.info(f"Clima atual inserido: {current_inserted} registros (BRZ_CLIMA_TEMPO)")
    logger.info(f"Previsão inserida: {forecast_inserted} registros (BRZ_CLIMA_TEMPO_PREVISAO)")
    logger.info("=" * 80)

    # Artifact
    try:
        create_table_artifact(
            key="weather-results",
            table=[{
                "Métrica": "Cidades Coletadas",
                "Valor": f"{len(weather_data_list)}/{len(CIDADES)}"
            }, {
                "Métrica": "Clima Atual Inserido",
                "Valor": current_inserted
            }, {
                "Métrica": "Previsão Inserida",
                "Valor": forecast_inserted
            }, {
                "Métrica": "Duração (min)",
                "Valor": f"{int(m)}m {int(s)}s"
            }],
            description=f"✅ {current_inserted} atual + {forecast_inserted} previsão inseridos"
        )
    except Exception as e:
        logger.warning(f"Erro criando artifact: {e}")

    # Retorna resumo para o decorador @flow_alerts
    return {
        "cities_processed": len(weather_data_list),
        "current_inserted": current_inserted,
        "forecast_inserted": forecast_inserted
    }


if __name__ == "__main__":
    # Execução local para teste
    # main()

    # Deployment para execução agendada
    main.from_source(
        source=".",
        entrypoint="flows/weather/main.py:main"
    ).deploy(
        name="weather-api-to-snowflake",
        work_pool_name="local-pool",
        schedules=[
            CronSchedule(cron="0 */4 * * *", timezone="America/Sao_Paulo")
        ],
        tags=["rpa", "api", "snowflake", "bronze"],
        parameters={},
        description="🌤️ Integração API HGBrasil → Snowflake | Coleta dados climáticos de 5 cidades (Blumenau, Balneário Camboriú, Joinville, São José, Criciúma). Executa a cada hora gerando: (1) Clima Atual em BRZ_CLIMA_TEMPO (APPEND) e (2) Previsão 15 dias em BRZ_CLIMA_TEMPO_PREVISAO (FULL REFRESH).",
        version="1.0.0"
    )
