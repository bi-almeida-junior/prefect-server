import time
from datetime import datetime
from typing import Optional, List, Dict

import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
from prefect.cache_policies import NONE
from prefect.client.schemas.schedules import CronSchedule

from flows.vehicle.client import PlacaAPIClient
from flows.vehicle.schemas import PlateRecord, transform_plate_to_snowflake_row
from shared.connections.snowflake import snowflake_connection
from shared.decorators import flow_alerts
from shared.utils import get_datetime_brasilia

load_dotenv()

# Constantes
BATCH_SIZE = 50
RATE_LIMIT_PER_MIN = 5
DATABASE = "AJ_DATALAKEHOUSE_RPA"
SCHEMA = "BRONZE"

# Tables
TABLE_PLATE_RAW = "BRZ_01_PLACA_RAW"
TABLE_VEHICLE_DETAILS = "BRZ_02_VEICULO_DETALHE"

# Colunas
PLATE_COLUMNS = [
    'DS_PLACA', 'DS_MARCA', 'DS_MODELO', 'NR_ANO_FABRICACAO',
    'NR_ANO_MODELO', 'DS_COR', 'DT_COLETA_API'
]


@task(name="load_proxy", log_prints=True, cache_policy=NONE)
def load_proxy() -> Optional[Dict[str, str]]:
    """Carrega configurações de proxy."""
    logger = get_run_logger()
    try:
        proxy_http = Secret.load("dataimpulse-proxy-http").get()
        proxy_https = Secret.load("dataimpulse-proxy-https").get()
        logger.info("✓ Proxy carregado")
        return {'http': proxy_http, 'https': proxy_https}
    except Exception as e:
        logger.warning(f"⚠️ Proxy não disponível: {e}")
        return None


@task(name="get_pending_plates", log_prints=True, cache_policy=NONE)
def get_pending_plates(conn, batch_size: int) -> List[PlateRecord]:
    """Busca placas pendentes (status P, E, N)."""
    logger = get_run_logger()
    cur = conn.cursor()
    full_table = f"{DATABASE}.{SCHEMA}.{TABLE_PLATE_RAW}"

    try:
        cur.execute(f"""
            SELECT DS_PLACA, DT_INSERCAO
            FROM {full_table}
            WHERE DS_STATUS IN ('P', 'E', 'N')
            ORDER BY
                CASE DS_STATUS WHEN 'P' THEN 1 WHEN 'E' THEN 2 WHEN 'N' THEN 3 END,
                DT_INSERCAO DESC
            LIMIT {batch_size}
        """)
        results = cur.fetchall()

        plates = [PlateRecord(plate=row[0], date=row[1]) for row in results]
        logger.info(f"Encontradas {len(plates)} placas pendentes")
        return plates

    except Exception as e:
        logger.warning(f"Erro ao buscar placas: {e}")
        return []
    finally:
        cur.close()


@task(name="update_status", log_prints=True, cache_policy=NONE)
def update_status(conn, plates: List[str], status: str, reasons: Optional[Dict[str, str]] = None, update_collection_date: bool = False) -> int:
    """Atualiza status de placas."""
    logger = get_run_logger()

    if not plates:
        return 0

    cur = conn.cursor()
    full_table = f"{DATABASE}.{SCHEMA}.{TABLE_PLATE_RAW}"

    try:
        rows_updated = 0

        if reasons:
            # Atualiza com motivo (para status 'I')
            for plate in plates:
                if update_collection_date:
                    cur.execute(f"""
                        UPDATE {full_table}
                        SET DS_STATUS = %s, DS_MOTIVO_ERRO = %s,
                            NR_TENTATIVAS = NR_TENTATIVAS + 1,
                            DT_COLETA = %s
                        WHERE DS_PLACA = %s
                    """, (status, reasons.get(plate, 'UNKNOWN'), get_datetime_brasilia(), plate))
                else:
                    cur.execute(f"""
                        UPDATE {full_table}
                        SET DS_STATUS = %s, DS_MOTIVO_ERRO = %s
                        WHERE DS_PLACA = %s
                    """, (status, reasons.get(plate, 'UNKNOWN'), plate))
                rows_updated += cur.rowcount
        else:
            # Atualiza sem motivo
            placeholders = ','.join(['%s'] * len(plates))
            if update_collection_date:
                cur.execute(f"""
                    UPDATE {full_table}
                    SET DS_STATUS = %s,
                        NR_TENTATIVAS = NR_TENTATIVAS + 1,
                        DT_COLETA = %s
                    WHERE DS_PLACA IN ({placeholders})
                """, [status, get_datetime_brasilia()] + plates)
            else:
                cur.execute(f"""
                    UPDATE {full_table}
                    SET DS_STATUS = %s
                    WHERE DS_PLACA IN ({placeholders})
                """, [status] + plates)
            rows_updated = cur.rowcount

        conn.commit()
        logger.info(f"✓ {rows_updated} placas → status '{status}'")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status: {e}")
        raise
    finally:
        cur.close()


@task(name="process_plates", log_prints=True, cache_policy=NONE)
def process_plates(plates: List[PlateRecord], client: PlacaAPIClient) -> Dict:
    """Processa lote de placas com rate limit."""
    logger = get_run_logger()

    results = []
    failed = []
    invalid = {}
    request_timestamps = []

    logger.info(f"Processando {len(plates)} placas ({RATE_LIMIT_PER_MIN} req/min)")

    for i, plate_record in enumerate(plates, 1):
        plate = plate_record.plate
        current_time = time.time()

        # Rate limit: janela de 60s
        request_timestamps = [t for t in request_timestamps if current_time - t < 60]

        if len(request_timestamps) >= RATE_LIMIT_PER_MIN:
            wait_time = 60 - (current_time - request_timestamps[0])
            if wait_time > 0:
                time.sleep(wait_time)
                current_time = time.time()
            request_timestamps.pop(0)

        # Intervalo mínimo entre requests
        if request_timestamps:
            time_since_last = current_time - request_timestamps[-1]
            if time_since_last < 10:
                time.sleep(10 - time_since_last)

        # Consulta API
        vehicle_data = client.query_plate(plate)
        request_timestamps.append(time.time())

        # Processa resultado
        if vehicle_data:
            if vehicle_data.get("status") == "invalid":
                invalid[plate] = vehicle_data.get("reason", "UNKNOWN")
                logger.warning(f"[{plate}] Inválido: {invalid[plate]}")
            elif vehicle_data.get("status") == 429:
                failed.append(plate)
                logger.warning(f"[{plate}] Rate limit (429)")
            else:
                row = transform_plate_to_snowflake_row(plate, vehicle_data)
                results.append(row)
        else:
            failed.append(plate)

        if i % 10 == 0 or i == 1:
            logger.info(f"[{i}/{len(plates)}] Processado: {plate}")

    logger.info(f"✓ {len(results)} sucesso | {len(invalid)} inválidos | {len(failed)} erros")

    return {
        "df": pd.DataFrame(results) if results else pd.DataFrame(),
        "failed": failed,
        "invalid": invalid
    }


@task(name="insert_plate_data", log_prints=True, cache_policy=NONE)
def insert_plate_data(conn, df: pd.DataFrame) -> int:
    """Insere dados de placas no Snowflake."""
    logger = get_run_logger()

    if df.empty:
        return 0

    cur = conn.cursor()
    full_table = f"{DATABASE}.{SCHEMA}.{TABLE_VEHICLE_DETAILS}"

    try:
        columns_list = ", ".join(PLATE_COLUMNS)
        placeholders = ", ".join(["%s"] * len(PLATE_COLUMNS))
        insert_sql = f"INSERT INTO {full_table} ({columns_list}) VALUES ({placeholders})"

        records = [tuple(row[col] for col in PLATE_COLUMNS) for _, row in df.iterrows()]

        cur.executemany(insert_sql, records)
        conn.commit()

        logger.info(f"✓ {cur.rowcount} registros inseridos")
        return cur.rowcount

    except Exception as e:
        logger.error(f"Erro ao inserir: {e}")
        raise
    finally:
        cur.close()


@flow(name="vehicle_details_api_to_snowflake", log_prints=True)
@flow_alerts(
    flow_name="Placa Consulta",
    source="API Placamaster",
    destination="Snowflake (BRONZE)",
    extract_summary=lambda result: {"records_loaded": result.get("inserted", 0)}
)
def main(batch_size: int = BATCH_SIZE):
    """Flow: Consulta API Placamaster e insere no Snowflake."""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("🚘 PLACA: API → SNOWFLAKE")
    logger.info("=" * 80)

    # Proxy
    proxies = load_proxy()
    client = PlacaAPIClient(proxies)

    with snowflake_connection(database=DATABASE, schema=SCHEMA) as conn:
        # Busca pendentes
        plates = get_pending_plates(conn, batch_size)

        # Exemplo pronto para utilizar em debug manual afim de validar alguma placa específica
        # plates = PlateRecord(
        #     plate='TXM1F73',
        #     date=datetime.now()
        # )

        if not plates:
            logger.info("Nenhuma placa pendente")
            return {"inserted": 0}

        # Atualiza para 'P' e registra tentativa
        update_status(conn, [p.plate for p in plates], 'P', update_collection_date=True)

        # Processa
        result = process_plates(plates, client)
        df = result["df"]
        failed = result["failed"]
        invalid = result["invalid"]

        # Insere
        inserted = 0
        if not df.empty:
            inserted = insert_plate_data(conn, df)
            # Atualiza para 'S' (não incrementa tentativas novamente)
            update_status(conn, df['DS_PLACA'].tolist(), 'S')

        # Atualiza inválidos (não incrementa tentativas novamente)
        if invalid:
            update_status(conn, list(invalid.keys()), 'I', invalid)

        # Atualiza erros (não incrementa tentativas novamente)
        if failed:
            update_status(conn, failed, 'E')

        # Resumo
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 80)
        logger.info(f"✅ Concluído: {inserted} inseridos | {len(invalid)} inválidos | {len(failed)} erros")
        logger.info(f"⏱️  Duração: {int(elapsed // 60)}m {int(elapsed % 60)}s")
        logger.info("=" * 80)

        return {"inserted": inserted}


if __name__ == "__main__":
    # main()

    main.from_source(
        source=".",
        entrypoint="flows/vehicle/main_details.py:main"
    ).deploy(
        name="vehicle-details-api-to-snowflake",
        work_pool_name="local-pool",
        schedules=[CronSchedule(cron="*/15 * * * *", timezone="America/Sao_Paulo")],
        tags=["rpa", "api", "snowflake", "bronze"],
        parameters={},
        description="🚘 Placamaster → Snowflake | Consulta detalhes de veículos por placa e carrega no Bronze. Rate limit (5 req/min), bypass Cloudflare.",
        version="2.0.0"
    )
