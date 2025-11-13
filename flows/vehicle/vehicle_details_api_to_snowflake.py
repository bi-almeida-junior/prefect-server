import sys
import os
import time
from datetime import datetime
from typing import Optional, List, Dict, Any

import cloudscraper
import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.cache_policies import NONE
from prefect.artifacts import create_table_artifact
from prefect.client.schemas.schedules import CronSchedule

# Imports das conexões compartilhadas
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from shared.connections.snowflake import connect_snowflake, close_snowflake_connection
from shared.alerts import send_flow_success_alert, send_flow_error_alert

# Carrega variáveis de ambiente
load_dotenv()

# ====== CONFIGURAÇÕES ======
BATCH_SIZE = 5  # Quantidade de placas por lote (ajustar conforme necessidade)
RATE_LIMIT = 5  # Requisições por minuto (limite da API)
API_URL = "https://placamaster.com/api/consulta-gratuita"

# ESTRATÉGIA:
# - Busca 250 placas por execução
# - Rate limit interno aguarda automaticamente (5 req/min)
# - 250 placas = ~50 minutos de processamento
# - Executar a cada 1 hora = eficiente e seguro
# - Uma única conexão Snowflake + INSERT direto = otimizado


@task(name="get_pending_plates", log_prints=True, cache_policy=NONE)
def get_pending_plates(conn, database, schema, batch_size: int = 250) -> List[Dict[str, Any]]:
    """
    Busca registros com DS_STATUS = 'N' da tabela DIM_PLACA.

    Lógica:
    1. Busca até BATCH_SIZE registros com DS_STATUS = 'N'
    2. Ordena por DT_INSERCAO DESC (mais novos primeiro)
    3. Retorna lista com placas e seus IDs
    """
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        logger.info(f"Buscando até {batch_size} placas pendentes (DS_STATUS = 'N') de DIM_PLACA...")
        cur.execute(f"""
            SELECT DS_PLACA, DT_INSERCAO
            FROM {database}.{schema}.DIM_PLACA
            WHERE DS_STATUS = 'N'
            ORDER BY DT_INSERCAO DESC
            LIMIT {batch_size}
        """)
        results = cur.fetchall()

        plates = [{"plate": row[0], "date": row[1]} for row in results]
        logger.info(f"Encontradas {len(plates)} placas pendentes para processar")

        return plates

    except Exception as e:
        logger.warning(f"Erro ao buscar placas pendentes: {e}")
        logger.info("Tabela DIM_PLACA pode não existir ainda. Retornando lista vazia.")
        return []
    finally:
        cur.close()


@task(name="update_status_processing", log_prints=True, cache_policy=NONE)
def update_status_processing(conn, database, schema, plates: List[str]) -> int:
    """
    Atualiza DS_STATUS de 'N' para 'P' nas placas selecionadas.

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        plates: Lista de placas a atualizar

    Returns:
        Número de registros atualizados
    """
    logger = get_run_logger()

    if not plates:
        logger.info("Nenhuma placa para atualizar status")
        return 0

    cur = conn.cursor()

    try:
        plates_str = "', '".join(plates)
        logger.info(f"Atualizando status 'N' → 'P' para {len(plates)} placas...")

        update_sql = f"""
            UPDATE {database}.{schema}.DIM_PLACA
            SET DS_STATUS = 'P'
            WHERE DS_PLACA IN ('{plates_str}')
              AND DS_STATUS = 'N'
        """

        cur.execute(update_sql)
        rows_updated = cur.rowcount

        logger.info(f"✓ {rows_updated} registros atualizados para status 'P'")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status para 'P': {e}")
        raise
    finally:
        cur.close()


@task(name="query_plate_api", retries=0, log_prints=True, cache_policy=NONE)
def query_plate_api(plate: str) -> Optional[Dict[str, Any]]:
    """Consulta dados de uma placa na API usando cloudscraper."""
    logger = get_run_logger()
    try:
        plate_no_dash = plate.replace("-", "")
        scraper = cloudscraper.create_scraper()
        json_data = {"placa": plate_no_dash}
        response = scraper.post(API_URL, json=json_data, timeout=30)

        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                return data.get("data")
            else:
                return None
        elif response.status_code == 429:
            return {"status": 429}
        else:
            return None
    except Exception as e:
        logger.error(f"Erro ao consultar {plate}: {e}")
        return None


@task(name="process_plate_batch", log_prints=True, cache_policy=NONE)
def process_plate_batch(plates_info: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Processa um lote de placas respeitando rate limit de 5 req/min."""
    logger = get_run_logger()

    # ✅ CONFIGURAÇÃO
    REQUESTS_PER_WINDOW = 5
    WINDOW_DURATION = 60
    SAFETY_MARGIN = 3
    MIN_INTERVAL = 10
    MAX_RETRIES = 5
    RETRY_BACKOFF = 15

    results = []
    request_timestamps = []
    last_request_time = None
    total_429 = 0
    total_retries = 0
    failed_plates = []

    logger.info(f"Processando {len(plates_info)} placas (5 req/min, max {MAX_RETRIES} retries)")

    for i, plate_info in enumerate(plates_info, 1):
        plate = plate_info["plate"]
        plate_date = plate_info["date"]
        current_time = time.time()

        # REGRA 1: Intervalo mínimo
        if last_request_time is not None:
            time_since_last = current_time - last_request_time
            if time_since_last < MIN_INTERVAL:
                time.sleep(MIN_INTERVAL - time_since_last)
                current_time = time.time()

        # REGRA 2: Limpa janela
        request_timestamps = [t for t in request_timestamps if current_time - t < WINDOW_DURATION]

        # REGRA 3: Controla janela
        if len(request_timestamps) >= REQUESTS_PER_WINDOW:
            first_request_time = request_timestamps[0]
            wait_time = WINDOW_DURATION + SAFETY_MARGIN - (current_time - first_request_time)

            if wait_time > 0:
                time.sleep(wait_time)
                current_time = time.time()

            request_timestamps.pop(0)

        # ✅ LOG COMPACTO - A cada 10 placas
        if i % 10 == 0 or i == 1 or i == len(plates_info):
            logger.info(f"[{i}/{len(plates_info)}] Processando: {plate}")

        # Requisição com retry
        attempts = 0
        vehicle_data = None

        while attempts < MAX_RETRIES:
            request_time = time.time()
            vehicle_data = query_plate_api(plate)
            request_timestamps.append(request_time)
            last_request_time = request_time

            if vehicle_data and vehicle_data.get("status") == 429:
                attempts += 1
                total_429 += 1
                total_retries += 1

                if attempts < MAX_RETRIES:
                    backoff_time = RETRY_BACKOFF + (attempts * 5)
                    logger.warning(f"[{plate}] 429 - Retry {attempts}/{MAX_RETRIES} em {backoff_time}s")
                    time.sleep(backoff_time)
                    current_time = time.time()
                    request_timestamps = [t for t in request_timestamps if current_time - t < WINDOW_DURATION]
                else:
                    logger.error(f"[{plate}] FALHOU após {MAX_RETRIES} tentativas")
                    failed_plates.append(plate)
                    vehicle_data = None
                    break
            else:
                if attempts > 0:
                    logger.info(f"[{plate}] ✓ Sucesso após {attempts} retry(s)")
                break

        if vehicle_data and vehicle_data.get("status") != 429:
            color = vehicle_data.get("cor")
            results.append({
                "DS_PLACA": plate,
                "DS_MARCA": vehicle_data.get("marca"),
                "DS_MODELO": vehicle_data.get("modelo"),
                "NR_ANO_FABRICACAO": vehicle_data.get("ano_fabricacao"),
                "NR_ANO_MODELO": vehicle_data.get("ano_modelo"),
                "DS_COR": color.upper() if color else None,
                "DT_COLETA_API": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            })
        elif not vehicle_data or vehicle_data.get("status") != 429:
            # Se não retornou dados (None ou inválido), marca como falha
            if plate not in failed_plates:
                failed_plates.append(plate)
                logger.warning(f"[{plate}] API não retornou dados válidos")

    # ✅ RESUMO FINAL OBJETIVO
    logger.info("=" * 60)
    logger.info(f"RESULTADO: {len(results)}/{len(plates_info)} placas processadas")
    if total_429 > 0:
        logger.warning(f"Total 429 recebidos: {total_429} | Retries executados: {total_retries}")
    if failed_plates:
        logger.error(f"Placas não processadas ({len(failed_plates)}): {', '.join(failed_plates)}")
    logger.info("=" * 60)

    return {
        "df": pd.DataFrame(results) if results else pd.DataFrame(),
        "failed_plates": failed_plates
    }


@task(name="update_status_error", log_prints=True, cache_policy=NONE)
def update_status_error(conn, database, schema, plates: List[str]) -> int:
    """
    Atualiza DS_STATUS de 'P' para 'E' nas placas que falharam.

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        plates: Lista de placas que falharam

    Returns:
        Número de registros atualizados
    """
    logger = get_run_logger()

    if not plates:
        return 0

    cur = conn.cursor()

    try:
        plates_str = "', '".join(plates)
        logger.info(f"Atualizando status 'P' → 'E' para {len(plates)} placas com erro...")

        update_sql = f"""
            UPDATE {database}.{schema}.DIM_PLACA
            SET DS_STATUS = 'E'
            WHERE DS_PLACA IN ('{plates_str}')
              AND DS_STATUS = 'P'
        """

        cur.execute(update_sql)
        rows_updated = cur.rowcount

        logger.info(f"✓ {rows_updated} registros atualizados para status 'E' (erro)")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status para 'E': {e}")
        raise
    finally:
        cur.close()


@task(name="insert_data_snowflake", log_prints=True, cache_policy=NONE)
def insert_data_snowflake(conn, database, schema, df: pd.DataFrame) -> Dict[str, Any]:
    """
    Insere dados no Snowflake e atualiza status na DIM_PLACA.

    Returns:
        Dict com contadores de sucesso e erro
    """
    logger = get_run_logger()

    if df.empty:
        logger.info("Nenhum dado para inserir")
        return {"success": 0, "error": 0}

    cursor = conn.cursor()

    try:
        logger.info(f"Inserindo {len(df)} registros em {database}.{schema}.VEICULO_DETALHE...")

        # INSERT na tabela VEICULO_DETALHE
        insert_sql = f"""
        INSERT INTO {database}.{schema}.VEICULO_DETALHE
            (DS_PLACA, DS_MARCA, DS_MODELO, NR_ANO_FABRICACAO, NR_ANO_MODELO, DS_COR, DT_COLETA_API)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        """

        rows_inserted = 0
        success_plates = []

        for _, row in df.iterrows():
            try:
                record = (
                    row['DS_PLACA'],
                    row['DS_MARCA'],
                    row['DS_MODELO'],
                    row['NR_ANO_FABRICACAO'],
                    row['NR_ANO_MODELO'],
                    row['DS_COR'],
                    row['DT_COLETA_API']
                )

                cursor.execute(insert_sql, record)
                rows_inserted += 1
                success_plates.append(row['DS_PLACA'])

            except Exception as insert_error:
                logger.error(f"Erro ao inserir placa {row['DS_PLACA']}: {insert_error}")
                continue

        logger.info(f"✓ INSERT concluído: {rows_inserted} registros inseridos")

        # Atualiza status 'P' → 'S' para placas inseridas com sucesso
        if success_plates:
            plates_str = "', '".join(success_plates)
            update_success_sql = f"""
                UPDATE {database}.{schema}.DIM_PLACA
                SET DS_STATUS = 'S'
                WHERE DS_PLACA IN ('{plates_str}')
                  AND DS_STATUS = 'P'
            """
            cursor.execute(update_success_sql)
            logger.info(f"✓ {len(success_plates)} placas atualizadas para status 'S' (sucesso)")

        return {"success": rows_inserted, "error": len(df) - rows_inserted}

    except Exception as e:
        logger.error(f"Erro ao inserir dados: {e}")
        raise
    finally:
        cursor.close()


@flow(name="vehicle_details_api_to_snowflake", log_prints=True)
def vehicle_details_api_to_snowflake(
        snowflake_account: Optional[str] = None,
        snowflake_user: Optional[str] = None,
        snowflake_private_key: Optional[str] = None,
        snowflake_warehouse: Optional[str] = None,
        snowflake_role: Optional[str] = None,
        batch_size: int = BATCH_SIZE
):
    """
    Flow principal: Consulta API de placas e insere no Snowflake.

    Args:
        snowflake_account: Conta Snowflake (padrão: .env)
        snowflake_user: Usuário Snowflake (padrão: .env)
        snowflake_private_key: Chave privada Snowflake (padrão: .env)
        snowflake_warehouse: Warehouse Snowflake (padrão: .env)
        snowflake_role: Role Snowflake (padrão: .env)
        batch_size: Tamanho do lote de processamento
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("PLACA CONSULTA: API → SNOWFLAKE")
    logger.info("=" * 80)

    # Carrega configurações do ambiente
    snowflake_account = snowflake_account or os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user = snowflake_user or os.getenv("SNOWFLAKE_USER")
    snowflake_private_key = snowflake_private_key or os.getenv("SNOWFLAKE_PRIVATE_KEY")
    snowflake_warehouse = snowflake_warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_role = snowflake_role or os.getenv("SNOWFLAKE_ROLE")

    # Databases e schemas
    source_database = "AJ_DATALAKEHOUSE_WPS"
    source_schema = "SILVER"
    dest_database = "AJ_DATALAKEHOUSE_RPA"
    dest_schema = "BRONZE"

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

        # Busca placas pendentes (DS_STATUS = 'N')
        plates_info = get_pending_plates(conn, dest_database, dest_schema, batch_size)

        if not plates_info:
            logger.info("Nenhuma placa pendente para processar. Encerrando.")
            close_snowflake_connection(conn)
            return

        # Atualiza status para 'P' (processando)
        plates_list = [p["plate"] for p in plates_info]
        update_status_processing(conn, dest_database, dest_schema, plates_list)

        # Processa em lotes
        total_processed = 0
        total_inserted = 0
        total_errors = 0

        for batch_start in range(0, len(plates_info), batch_size):
            batch_end = min(batch_start + batch_size, len(plates_info))
            batch = plates_info[batch_start:batch_end]

            logger.info(f"Lote {batch_start // batch_size + 1}: Processando placas {batch_start + 1}-{batch_end}/{len(plates_info)}")

            # Processa lote
            result = process_plate_batch(batch)
            df = result["df"]
            failed_plates = result["failed_plates"]

            # Insere no Snowflake e atualiza status 'S' para sucesso
            if not df.empty:
                insert_result = insert_data_snowflake(conn, dest_database, dest_schema, df)
                total_inserted += insert_result["success"]
                total_errors += insert_result["error"]

            # Atualiza status 'E' para placas que falharam na API
            if failed_plates:
                update_status_error(conn, dest_database, dest_schema, failed_plates)
                total_errors += len(failed_plates)

            total_processed += len(batch)

        close_snowflake_connection(conn)

        # Resumo
        end_time = datetime.now()
        elapsed = end_time - start_time
        m, s = divmod(elapsed.total_seconds(), 70)

        logger.info("=" * 80)
        logger.info("PROCESSO CONCLUÍDO")
        logger.info("=" * 80)
        logger.info(f"Database: {dest_database}")
        logger.info(f"Schema:   {dest_schema}")
        logger.info(f"Tabela:   VEICULO_DETALHE")
        logger.info(f"Início:   {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Fim:      {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duração:  {int(m)}m {int(s)}s")
        logger.info(f"Placas processadas: {total_processed}")
        logger.info(f"Registros inseridos: {total_inserted}")
        logger.info(f"Erros encontrados: {total_errors}")

        # Artifact
        try:
            create_table_artifact(
                key="placa-consulta-results",
                table=[{
                    "Métrica": "Placas Processadas",
                    "Valor": total_processed
                }, {
                    "Métrica": "Registros Inseridos",
                    "Valor": total_inserted
                }, {
                    "Métrica": "Erros",
                    "Valor": total_errors
                }, {
                    "Métrica": "Duração (min)",
                    "Valor": f"{int(m)}m {int(s)}s"
                }],
                description=f"✅ {total_inserted} registros inseridos | {total_errors} erros"
            )
        except Exception as e:
            logger.warning(f"Erro criando artifact: {e}")

        # Alerta de sucesso
        try:
            send_flow_success_alert(
                flow_name="Placa Consulta",
                source="API Placamaster",
                destination="Snowflake",
                summary={
                    "records_loaded": total_inserted,
                    "plates_processed": total_processed,
                    "errors": total_errors
                },
                duration_seconds=elapsed.total_seconds()
            )
        except Exception as e:
            logger.warning(f"Falha ao enviar alerta de sucesso: {e}")

    except Exception as e:
        logger.error(f"Erro no flow: {e}")
        import traceback
        traceback.print_exc()

        # Alerta de erro
        try:
            elapsed_error = (datetime.now() - start_time).total_seconds()
            send_flow_error_alert(
                flow_name="Placa Consulta",
                source="API Placamaster",
                destination="Snowflake",
                error_message=str(e),
                duration_seconds=elapsed_error
            )
        except Exception as alert_error:
            logger.warning(f"Falha ao enviar alerta de erro: {alert_error}")

        raise


if __name__ == "__main__":
    # # Execução local para teste
    # vehicle_details_api_to_snowflake()

    # Deployment para execução agendada
    vehicle_details_api_to_snowflake.from_source(
        source=".",
        entrypoint="flows/vehicle/vehicle_details_api_to_snowflake.py:vehicle_details_api_to_snowflake"
    ).deploy(
        name="vehicle-details-api-to-snowflake",
        work_pool_name="local-pool",
        schedules=[
            CronSchedule(cron="0 * * * *", timezone="America/Sao_Paulo")
        ],
        tags=["rpa", "api", "snowflake", "bronze", "dimension"],
        parameters={},
        description="Pipeline: API Placamaster → Snowflake VEICULO_DETALHE (Bronze)",
        version="1.0.0"
    )
