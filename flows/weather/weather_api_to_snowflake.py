import sys
import os
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
import urllib3

import requests
import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.cache_policies import NONE
from prefect.artifacts import create_table_artifact
from prefect.client.schemas.schedules import CronSchedule
from prefect.blocks.system import Secret

# Imports das conexões compartilhadas
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from shared.connections.snowflake import connect_snowflake, close_snowflake_connection
from shared.alerts import send_flow_success_alert, send_flow_error_alert

# Carrega variáveis de ambiente
load_dotenv()

# Desabilita warnings de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ====== CONFIGURAÇÕES ======
BASE_URL = "https://api.hgbrasil.com/weather"

# Mapeamento de cidades com IDs da tabela DIM_CIDADE
# Não consulta Snowflake - valores fixos conforme solicitado
CIDADES = [
    {"id": 1, "nome": "Blumenau", "nome_api": "Blumenau,SC"},
    {"id": 2, "nome": "Balneário Camboriú", "nome_api": "Balneário Camboriú,SC"},
    {"id": 3, "nome": "Joinville", "nome_api": "Joinville,SC"},
    {"id": 4, "nome": "São José", "nome_api": "São José,SC"},
    {"id": 5, "nome": "Criciúma", "nome_api": "Criciúma,SC"}
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
    logger = get_run_logger()
    try:
        logger.info("🔐 Carregando API Key do HGBrasil do Prefect Blocks...")

        api_key_block = Secret.load("hgbrasil-weather-api-key")
        api_key = api_key_block.get()

        logger.info("✓ API Key carregada com sucesso")
        return api_key

    except Exception as e:
        logger.error(f"❌ Erro ao carregar API Key: {e}")
        logger.error("Certifique-se de que o secret 'hgbrasil-weather-api-key' existe no Prefect")
        raise


@task(name="fetch_weather_data", log_prints=True, cache_policy=NONE)
def fetch_weather_data(api_key: str, cidade: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Coleta dados climáticos de uma cidade da API HGBrasil.

    Args:
        api_key: Chave da API HGBrasil
        cidade: Dict com informações da cidade (id, nome, nome_api)

    Returns:
        Dict com dados climáticos ou None se falhar
    """
    logger = get_run_logger()

    try:
        cidade_nome = cidade["nome"]
        cidade_api = cidade["nome_api"]

        logger.info(f"🌤️  Coletando dados de {cidade_nome}...")

        params = {
            'key': api_key,
            'city_name': cidade_api
        }

        response = requests.get(BASE_URL, params=params, verify=True, timeout=10)

        if response.status_code == 200:
            data = response.json()

            if data.get('valid_key'):
                results = data['results']

                logger.info(f"   ✓ Cidade: {results.get('city', 'N/A')}")
                logger.info(f"   ✓ Temp atual: {results.get('temp', 'N/A')}°C")
                logger.info(f"   ✓ Dias previsão: {len(results.get('forecast', []))}")

                return {
                    'id_cidade': cidade['id'],
                    'cidade': results.get('city'),
                    'latitude': results.get('latitude', results.get('lat', None)),
                    'longitude': results.get('longitude', results.get('lon', None)),
                    'temperatura_atual': results.get('temp'),
                    'umidade_atual': results.get('humidity'),
                    'data_coleta': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'previsao_15_dias': results.get('forecast', [])
                }
            else:
                logger.warning(f"⚠️ API Key inválida para {cidade_nome}")
                return None
        else:
            logger.error(f"❌ Erro HTTP {response.status_code} para {cidade_nome}")
            return None

    except Exception as e:
        logger.error(f"❌ Erro ao consultar {cidade.get('nome', 'N/A')}: {e}")
        return None


@task(name="process_current_weather", log_prints=True, cache_policy=NONE)
def process_current_weather(weather_data_list: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Processa dados climáticos ATUAIS (primeiro registro da resposta).

    Este DataFrame será inserido na tabela BRZ_CLIMA_TEMPO (APPEND ONLY).
    Registra as condições climáticas no momento da coleta.

    Args:
        weather_data_list: Lista com dados climáticos coletados

    Returns:
        DataFrame formatado para inserção em BRZ_CLIMA_TEMPO
    """
    logger = get_run_logger()

    current_records = []

    for weather_data in weather_data_list:
        if not weather_data or not weather_data.get('previsao_15_dias'):
            continue

        # IMPORTANTE: Pega APENAS o primeiro registro (hoje - condições atuais)
        first_forecast = weather_data['previsao_15_dias'][0]

        # Converte data da previsão para formato DATE
        data_previsao = datetime.strptime(first_forecast['full_date'], '%d/%m/%Y').date()

        current_records.append({
            'ID_CIDADE': weather_data['id_cidade'],
            'NR_LATITUDE': str(weather_data['latitude']) if weather_data['latitude'] else None,
            'NR_LONGITUDE': str(weather_data['longitude']) if weather_data['longitude'] else None,
            'NR_TEMPERATURA_ATUAL': weather_data['temperatura_atual'],
            'NR_UMIDADE_ATUAL': weather_data['umidade_atual'],
            'DT_PREVISAO': data_previsao,
            'DS_DATA_FORMATADA': first_forecast['date'],
            'DS_DATA_COMPLETA': first_forecast['full_date'],
            'DS_DIA_SEMANA': first_forecast['weekday'],
            'NR_TEMP_MAXIMA': first_forecast['max'],
            'NR_TEMP_MINIMA': first_forecast['min'],
            'NR_UMIDADE': first_forecast['humidity'],
            'NR_NEBULOSIDADE': first_forecast['cloudiness'],
            'NR_CHUVA_MM': first_forecast['rain'],
            'NR_PROB_CHUVA': first_forecast['rain_probability'],
            'DS_VENTO_VELOCIDADE': first_forecast['wind_speedy'],
            'DS_HORARIO_NASCER_SOL': first_forecast['sunrise'],
            'DS_HORARIO_POR_SOL': first_forecast['sunset'],
            'DS_FASE_LUA': first_forecast['moon_phase'],
            'DS_DESCRICAO_TEMPO': first_forecast['description'],
            'DS_CONDICAO_TEMPO': first_forecast['condition'],
            'DT_COLETA_API': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    df = pd.DataFrame(current_records)
    logger.info(f"✓ Processados {len(df)} registros de clima ATUAL")

    return df


@task(name="process_forecast_weather", log_prints=True, cache_policy=NONE)
def process_forecast_weather(weather_data_list: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Processa dados de PREVISÃO (todos os 15 dias).

    Este DataFrame será inserido na tabela BRZ_CLIMA_TEMPO_PREVISAO (FULL REFRESH).
    Contém a previsão dos próximos dias.

    Args:
        weather_data_list: Lista com dados climáticos coletados

    Returns:
        DataFrame formatado para inserção em BRZ_CLIMA_TEMPO_PREVISAO
    """
    logger = get_run_logger()

    forecast_records = []

    for weather_data in weather_data_list:
        if not weather_data or not weather_data.get('previsao_15_dias'):
            continue

        # IMPORTANTE: Processa TODOS os registros de previsão (15 dias)
        for forecast_day in weather_data['previsao_15_dias']:
            # Converte data da previsão para formato DATE
            data_previsao = datetime.strptime(forecast_day['full_date'], '%d/%m/%Y').date()

            forecast_records.append({
                'ID_CIDADE': weather_data['id_cidade'],
                'NR_LATITUDE': str(weather_data['latitude']) if weather_data['latitude'] else None,
                'NR_LONGITUDE': str(weather_data['longitude']) if weather_data['longitude'] else None,
                'NR_TEMPERATURA_ATUAL': weather_data['temperatura_atual'],
                'NR_UMIDADE_ATUAL': weather_data['umidade_atual'],
                'DT_PREVISAO': data_previsao,
                'DS_DATA_FORMATADA': forecast_day['date'],
                'DS_DATA_COMPLETA': forecast_day['full_date'],
                'DS_DIA_SEMANA': forecast_day['weekday'],
                'NR_TEMP_MAXIMA': forecast_day['max'],
                'NR_TEMP_MINIMA': forecast_day['min'],
                'NR_UMIDADE': forecast_day['humidity'],
                'NR_NEBULOSIDADE': forecast_day['cloudiness'],
                'NR_CHUVA_MM': forecast_day['rain'],
                'NR_PROB_CHUVA': forecast_day['rain_probability'],
                'DS_VENTO_VELOCIDADE': forecast_day['wind_speedy'],
                'DS_HORARIO_NASCER_SOL': forecast_day['sunrise'],
                'DS_HORARIO_POR_SOL': forecast_day['sunset'],
                'DS_FASE_LUA': forecast_day['moon_phase'],
                'DS_DESCRICAO_TEMPO': forecast_day['description'],
                'DS_CONDICAO_TEMPO': forecast_day['condition'],
                'DT_COLETA_API': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

    df = pd.DataFrame(forecast_records)
    logger.info(f"✓ Processados {len(df)} registros de PREVISÃO (15 dias x {len(weather_data_list)} cidades)")

    return df


@task(name="insert_current_weather", log_prints=True, cache_policy=NONE)
def insert_current_weather(conn, database: str, schema: str, df: pd.DataFrame) -> int:
    """
    Insere dados de clima ATUAL no Snowflake (APPEND ONLY).

    Tabela: BRZ_CLIMA_TEMPO
    Estratégia: INSERT simples (acumula histórico)

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        df: DataFrame com dados a inserir

    Returns:
        Número de registros inseridos
    """
    logger = get_run_logger()

    if df.empty:
        logger.info("Nenhum dado de clima atual para inserir")
        return 0

    cursor = conn.cursor()

    try:
        logger.info(f"📊 Inserindo {len(df)} registros em {database}.{schema}.BRZ_CLIMA_TEMPO (APPEND)...")

        insert_sql = f"""
        INSERT INTO {database}.{schema}.BRZ_CLIMA_TEMPO
            (ID_CIDADE, NR_LATITUDE, NR_LONGITUDE, NR_TEMPERATURA_ATUAL, NR_UMIDADE_ATUAL,
             DT_PREVISAO, DS_DATA_FORMATADA, DS_DATA_COMPLETA, DS_DIA_SEMANA,
             NR_TEMP_MAXIMA, NR_TEMP_MINIMA, NR_UMIDADE, NR_NEBULOSIDADE,
             NR_CHUVA_MM, NR_PROB_CHUVA, DS_VENTO_VELOCIDADE,
             DS_HORARIO_NASCER_SOL, DS_HORARIO_POR_SOL, DS_FASE_LUA,
             DS_DESCRICAO_TEMPO, DS_CONDICAO_TEMPO, DT_COLETA_API)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        records = [
            (
                row['ID_CIDADE'], row['NR_LATITUDE'], row['NR_LONGITUDE'],
                row['NR_TEMPERATURA_ATUAL'], row['NR_UMIDADE_ATUAL'],
                row['DT_PREVISAO'], row['DS_DATA_FORMATADA'], row['DS_DATA_COMPLETA'],
                row['DS_DIA_SEMANA'], row['NR_TEMP_MAXIMA'], row['NR_TEMP_MINIMA'],
                row['NR_UMIDADE'], row['NR_NEBULOSIDADE'], row['NR_CHUVA_MM'],
                row['NR_PROB_CHUVA'], row['DS_VENTO_VELOCIDADE'],
                row['DS_HORARIO_NASCER_SOL'], row['DS_HORARIO_POR_SOL'],
                row['DS_FASE_LUA'], row['DS_DESCRICAO_TEMPO'],
                row['DS_CONDICAO_TEMPO'], row['DT_COLETA_API']
            )
            for _, row in df.iterrows()
        ]

        cursor.executemany(insert_sql, records)
        rows_inserted = cursor.rowcount
        conn.commit()

        logger.info(f"✓ {rows_inserted} registros de clima atual inseridos com sucesso")
        return rows_inserted

    except Exception as e:
        logger.error(f"❌ Erro ao inserir dados de clima atual: {e}")
        raise
    finally:
        cursor.close()


@task(name="insert_forecast_weather", log_prints=True, cache_policy=NONE)
def insert_forecast_weather(conn, database: str, schema: str, df: pd.DataFrame) -> int:
    """
    Insere dados de PREVISÃO no Snowflake (FULL REFRESH).

    Tabela: BRZ_CLIMA_TEMPO_PREVISAO
    Estratégia: TRUNCATE + INSERT (sempre sobrescreve com dados mais recentes)

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        df: DataFrame com dados a inserir

    Returns:
        Número de registros inseridos
    """
    logger = get_run_logger()

    if df.empty:
        logger.info("Nenhum dado de previsão para inserir")
        return 0

    cursor = conn.cursor()

    try:
        # TRUNCATE - Remove todos os dados anteriores
        logger.info(f"🗑️  Limpando dados anteriores de {database}.{schema}.BRZ_CLIMA_TEMPO_PREVISAO...")
        truncate_sql = f"TRUNCATE TABLE {database}.{schema}.BRZ_CLIMA_TEMPO_PREVISAO"
        cursor.execute(truncate_sql)
        conn.commit()
        logger.info("✓ Tabela truncada com sucesso")

        # INSERT - Insere novos dados
        logger.info(f"📊 Inserindo {len(df)} registros em {database}.{schema}.BRZ_CLIMA_TEMPO_PREVISAO (FULL REFRESH)...")

        insert_sql = f"""
        INSERT INTO {database}.{schema}.BRZ_CLIMA_TEMPO_PREVISAO
            (ID_CIDADE, NR_LATITUDE, NR_LONGITUDE, NR_TEMPERATURA_ATUAL, NR_UMIDADE_ATUAL,
             DT_PREVISAO, DS_DATA_FORMATADA, DS_DATA_COMPLETA, DS_DIA_SEMANA,
             NR_TEMP_MAXIMA, NR_TEMP_MINIMA, NR_UMIDADE, NR_NEBULOSIDADE,
             NR_CHUVA_MM, NR_PROB_CHUVA, DS_VENTO_VELOCIDADE,
             DS_HORARIO_NASCER_SOL, DS_HORARIO_POR_SOL, DS_FASE_LUA,
             DS_DESCRICAO_TEMPO, DS_CONDICAO_TEMPO, DT_COLETA_API)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        records = [
            (
                row['ID_CIDADE'], row['NR_LATITUDE'], row['NR_LONGITUDE'],
                row['NR_TEMPERATURA_ATUAL'], row['NR_UMIDADE_ATUAL'],
                row['DT_PREVISAO'], row['DS_DATA_FORMATADA'], row['DS_DATA_COMPLETA'],
                row['DS_DIA_SEMANA'], row['NR_TEMP_MAXIMA'], row['NR_TEMP_MINIMA'],
                row['NR_UMIDADE'], row['NR_NEBULOSIDADE'], row['NR_CHUVA_MM'],
                row['NR_PROB_CHUVA'], row['DS_VENTO_VELOCIDADE'],
                row['DS_HORARIO_NASCER_SOL'], row['DS_HORARIO_POR_SOL'],
                row['DS_FASE_LUA'], row['DS_DESCRICAO_TEMPO'],
                row['DS_CONDICAO_TEMPO'], row['DT_COLETA_API']
            )
            for _, row in df.iterrows()
        ]

        cursor.executemany(insert_sql, records)
        rows_inserted = cursor.rowcount
        conn.commit()

        logger.info(f"✓ {rows_inserted} registros de previsão inseridos com sucesso")
        return rows_inserted

    except Exception as e:
        logger.error(f"❌ Erro ao inserir dados de previsão: {e}")
        raise
    finally:
        cursor.close()


@flow(name="weather_api_to_snowflake", log_prints=True)
def weather_api_to_snowflake(
        snowflake_account: Optional[str] = None,
        snowflake_user: Optional[str] = None,
        snowflake_private_key: Optional[str] = None,
        snowflake_warehouse: Optional[str] = None,
        snowflake_role: Optional[str] = None
):
    """
    Flow principal: Coleta dados climáticos da API HGBrasil e insere no Snowflake.

    Executa a cada hora e gera dois tipos de registros:
    1. Clima Atual (BRZ_CLIMA_TEMPO): Condições atuais - APPEND ONLY
    2. Previsão 15 dias (BRZ_CLIMA_TEMPO_PREVISAO): Dados futuros - FULL REFRESH

    Args:
        snowflake_account: Conta Snowflake (padrão: .env)
        snowflake_user: Usuário Snowflake (padrão: .env)
        snowflake_private_key: Chave privada Snowflake (padrão: .env)
        snowflake_warehouse: Warehouse Snowflake (padrão: .env)
        snowflake_role: Role Snowflake (padrão: .env)
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("🌤️  CLIMA: API HGBrasil → SNOWFLAKE")
    logger.info("=" * 80)

    # Carrega configurações do ambiente
    snowflake_account = snowflake_account or os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user = snowflake_user or os.getenv("SNOWFLAKE_USER")
    snowflake_private_key = snowflake_private_key or os.getenv("SNOWFLAKE_PRIVATE_KEY")
    snowflake_warehouse = snowflake_warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_role = snowflake_role or os.getenv("SNOWFLAKE_ROLE")

    # Databases e schemas
    dest_database = "AJ_DATALAKEHOUSE_RPA"
    dest_schema = "BRONZE"

    conn = None  # Inicializa conexão como None
    try:
        # Conexão Snowflake
        conn = connect_snowflake(
            account=snowflake_account,
            user=snowflake_user,
            private_key=snowflake_private_key,
            warehouse=snowflake_warehouse,
            database=dest_database,
            schema=dest_schema,
            role=snowflake_role
        )

        # Carrega API Key
        api_key = load_api_key()

        # Coleta dados de todas as cidades
        logger.info(f"Coletando dados de {len(CIDADES)} cidades...")
        weather_data_list = []

        for i, cidade in enumerate(CIDADES, 1):
            logger.info(f"[{i}/{len(CIDADES)}] Processando {cidade['nome']}...")
            weather_data = fetch_weather_data(api_key, cidade)

            if weather_data:
                weather_data_list.append(weather_data)

            # Pausa de 2 segundos entre requisições (evita sobrecarga)
            if i < len(CIDADES):
                time.sleep(2)

        if not weather_data_list:
            logger.error("❌ Nenhum dado coletado. Encerrando.")
            raise Exception("Falha ao coletar dados climáticos de todas as cidades")

        logger.info(f"✓ Dados coletados de {len(weather_data_list)}/{len(CIDADES)} cidades")

        # Processa dados de clima ATUAL (primeiro registro)
        df_current = process_current_weather(weather_data_list)

        # Processa dados de PREVISÃO (todos os 15 dias)
        df_forecast = process_forecast_weather(weather_data_list)

        # Insere clima ATUAL (APPEND)
        current_inserted = insert_current_weather(conn, dest_database, dest_schema, df_current)

        # Insere PREVISÃO (FULL REFRESH)
        forecast_inserted = insert_forecast_weather(conn, dest_database, dest_schema, df_forecast)

        # Resumo
        end_time = datetime.now()
        elapsed = end_time - start_time
        m, s = divmod(elapsed.total_seconds(), 60)

        logger.info("=" * 80)
        logger.info("✓ PROCESSO CONCLUÍDO COM SUCESSO")
        logger.info("=" * 80)
        logger.info(f"Database: {dest_database}")
        logger.info(f"Schema:   {dest_schema}")
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

        # # Alerta de sucesso
        # try:
        #     send_flow_success_alert(
        #         flow_name="Clima HGBrasil",
        #         source="API HGBrasil Weather",
        #         destination="Snowflake",
        #         summary={
        #             "cities_processed": len(weather_data_list),
        #             "current_weather_records": current_inserted,
        #             "forecast_records": forecast_inserted
        #         },
        #         duration_seconds=elapsed.total_seconds()
        #     )
        # except Exception as e:
        #     logger.warning(f"Falha ao enviar alerta de sucesso: {e}")

    except Exception as e:
        logger.error(f"❌ Erro no flow: {e}")
        import traceback
        traceback.print_exc()

        # Alerta de erro
        try:
            elapsed_error = (datetime.now() - start_time).total_seconds()
            send_flow_error_alert(
                flow_name="Clima HGBrasil",
                source="API HGBrasil Weather",
                destination="Snowflake",
                error_message=str(e),
                duration_seconds=elapsed_error
            )
        except Exception as alert_error:
            logger.warning(f"Falha ao enviar alerta de erro: {alert_error}")

        raise

    finally:
        # Garante que a conexão seja fechada mesmo em caso de erro
        if conn is not None:
            try:
                close_snowflake_connection(conn)
                logger.info("✓ Conexão Snowflake fechada com sucesso")
            except Exception as close_error:
                logger.warning(f"Erro ao fechar conexão Snowflake: {close_error}")


if __name__ == "__main__":
    # Execução local para teste
    # weather_api_to_snowflake()

    # Deployment para execução agendada
    weather_api_to_snowflake.from_source(
        source=".",
        entrypoint="flows/weather/weather_api_to_snowflake.py:weather_api_to_snowflake"
    ).deploy(
        name="weather-api-to-snowflake",
        work_pool_name="local-pool",
        schedules=[
            CronSchedule(cron="0 * * * *", timezone="America/Sao_Paulo")
        ],
        tags=["rpa", "api", "snowflake", "bronze"],
        parameters={},
        description="🌤️ Integração API HGBrasil → Snowflake | Coleta dados climáticos de 5 cidades (Blumenau, Balneário Camboriú, Joinville, São José, Criciúma). Executa a cada hora gerando: (1) Clima Atual em BRZ_CLIMA_TEMPO (APPEND) e (2) Previsão 15 dias em BRZ_CLIMA_TEMPO_PREVISAO (FULL REFRESH).",
        version="1.0.0"
    )
