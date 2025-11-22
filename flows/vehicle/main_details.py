import time
from datetime import datetime
from typing import Optional, List, Dict

import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.blocks.system import Secret
from prefect.cache_policies import NONE
from prefect.client.schemas.schedules import CronSchedule

from flows.vehicle.client import AnyCarAPIClient
from flows.vehicle.schemas import PlateRecord, transform_plate_to_snowflake_row
from shared.connections.postgresql import postgresql_connection
from shared.decorators import flow_alerts
from shared.utils import get_datetime_brasilia

load_dotenv()

# Constantes
BATCH_SIZE = 10
RATE_LIMIT_PER_MIN = 5
SCHEMA = "public"

# Tables
TABLE_PLATE_RAW = "brz_01_placa_raw"
TABLE_VEHICLE_DETAILS = "brz_02_veiculo_detalhe"

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

    try:
        cur.execute(f"""
            SELECT ds_placa, dt_insercao
            FROM {TABLE_PLATE_RAW}
            WHERE ds_status IN ('P', 'E', 'N')
            ORDER BY
                CASE ds_status WHEN 'P' THEN 1 WHEN 'E' THEN 2 WHEN 'N' THEN 3 END,
                dt_insercao DESC
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
def update_status(conn, plates: List[str], status: str, reasons: Optional[Dict[str, str]] = None, update_collection_date: bool = False, commit: bool = False) -> int:
    """Atualiza status de placas."""
    logger = get_run_logger()

    if not plates:
        return 0

    cur = conn.cursor()

    try:
        if reasons:
            # Atualiza com motivo usando executemany (batch único)
            if update_collection_date:
                update_sql = f"""
                    UPDATE {TABLE_PLATE_RAW}
                    SET ds_status = %s, ds_motivo_erro = %s,
                        nr_tentativas = nr_tentativas + 1,
                        dt_coleta = %s
                    WHERE ds_placa = %s
                """
                dt_coleta = get_datetime_brasilia()
                params = [(status, reasons.get(plate, 'UNKNOWN'), dt_coleta, plate) for plate in plates]
            else:
                update_sql = f"""
                    UPDATE {TABLE_PLATE_RAW}
                    SET ds_status = %s, ds_motivo_erro = %s
                    WHERE ds_placa = %s
                """
                params = [(status, reasons.get(plate, 'UNKNOWN'), plate) for plate in plates]

            cur.executemany(update_sql, params)
            rows_updated = cur.rowcount
        else:
            # Atualiza sem motivo (batch único)
            placeholders = ','.join(['%s'] * len(plates))
            if update_collection_date:
                cur.execute(f"""
                    UPDATE {TABLE_PLATE_RAW}
                    SET ds_status = %s,
                        nr_tentativas = nr_tentativas + 1,
                        dt_coleta = %s
                    WHERE ds_placa IN ({placeholders})
                """, [status, get_datetime_brasilia()] + plates)
            else:
                cur.execute(f"""
                    UPDATE {TABLE_PLATE_RAW}
                    SET ds_status = %s
                    WHERE ds_placa IN ({placeholders})
                """, [status] + plates)
            rows_updated = cur.rowcount

        if commit:
            conn.commit()
        logger.info(f"✓ {rows_updated} placas → status '{status}'")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status: {e}")
        raise
    finally:
        cur.close()


@task(name="process_plates", log_prints=True, cache_policy=NONE)
def process_plates(plates: List[PlateRecord], client: AnyCarAPIClient) -> Dict:
    """Processa lote de placas com rate limit."""
    logger = get_run_logger()

    results = []
    failed = []
    invalid = {}
    request_timestamps = []

    logger.info(f"Processando {len(plates)} placas ({RATE_LIMIT_PER_MIN} req/min)")

    for i, plate_record in enumerate(plates, 1):
        plate = plate_record.plate

        # Valida formato ANTES de fazer sleep (otimização)
        if not client.validate_plate(plate):
            invalid[plate] = "INVALID_PLATE_FORMAT"
            logger.warning(f"[{plate}] Formato inválido - pulando sem delay")
            if i % 10 == 0 or i == 1:
                logger.info(f"[{i}/{len(plates)}] Processado: {plate} (inválido)")
            continue

        current_time = time.time()

        # Rate limit: janela de 60s
        request_timestamps = [t for t in request_timestamps if current_time - t < 60]

        # if len(request_timestamps) >= RATE_LIMIT_PER_MIN:
        #     wait_time = 60 - (current_time - request_timestamps[0])
        #     if wait_time > 0:
        #         time.sleep(wait_time)
        #         current_time = time.time()
        #     request_timestamps.pop(0)

        # Intervalo mínimo entre requests (comentado para AnyCar - delay já está no client)
        # if request_timestamps:
        #     time_since_last = current_time - request_timestamps[-1]
        #     if time_since_last < 10:
        #         time.sleep(10 - time_since_last)

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
def insert_plate_data(conn, df: pd.DataFrame, commit: bool = False) -> int:
    """Insere dados de placas no PostgreSQL."""
    logger = get_run_logger()

    if df.empty:
        return 0

    cur = conn.cursor()

    try:
        # Converte nomes de colunas para minúsculas (padrão PostgreSQL)
        columns_lower = [col.lower() for col in PLATE_COLUMNS]
        columns_list = ", ".join(columns_lower)
        placeholders = ", ".join(["%s"] * len(PLATE_COLUMNS))
        insert_sql = f"INSERT INTO {TABLE_VEHICLE_DETAILS} ({columns_list}) VALUES ({placeholders})"

        records = [tuple(row[col] for col in PLATE_COLUMNS) for _, row in df.iterrows()]

        cur.executemany(insert_sql, records)

        if commit:
            conn.commit()

        logger.info(f"✓ {cur.rowcount} registros inseridos")
        return cur.rowcount

    except Exception as e:
        logger.error(f"Erro ao inserir: {e}")
        raise
    finally:
        cur.close()


@flow(name="vehicle_details_api_to_postgresql", log_prints=True)
@flow_alerts(
    flow_name="Placa Consulta",
    source="API AnyCar",
    destination="PostgreSQL (BRONZE)",
    extract_summary=lambda result: {"records_loaded": result.get("inserted", 0)}
)
def main(batch_size: int = BATCH_SIZE):
    """Flow: Consulta API AnyCar e insere no PostgreSQL."""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("🚘 PLACA: API → POSTGRESQL")
    logger.info("=" * 80)

    # Proxy
    proxies = load_proxy()
    client = AnyCarAPIClient(proxies)

    with postgresql_connection(schema=SCHEMA) as conn:
        try:
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

            # Atualiza para 'P' e registra tentativa (com commit imediato para visibilidade em tempo real)
            update_status(conn, [p.plate for p in plates], 'P', update_collection_date=True, commit=True)

            # Processa
            result = process_plates(plates, client)
            df = result["df"]
            failed = result["failed"]
            invalid = result["invalid"]

            # Insere (sem commit)
            inserted = 0
            if not df.empty:
                inserted = insert_plate_data(conn, df)
                # Atualiza para 'S' (não incrementa tentativas novamente, sem commit)
                update_status(conn, df['DS_PLACA'].tolist(), 'S')

            # Atualiza inválidos (não incrementa tentativas novamente, sem commit)
            if invalid:
                update_status(conn, list(invalid.keys()), 'I', invalid)

            # Atualiza erros (não incrementa tentativas novamente, sem commit)
            if failed:
                update_status(conn, failed, 'E')

            # Commit único para todo o lote
            conn.commit()
            logger.info("✓ Transação commitada com sucesso")

            # Resumo
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info("=" * 80)
            logger.info(f"✅ Concluído: {inserted} inseridos | {len(invalid)} inválidos | {len(failed)} erros")
            logger.info(f"⏱️  Duração: {int(elapsed // 60)}m {int(elapsed % 60)}s")
            logger.info("=" * 80)

            # Criar artefato com resumo detalhado
            create_table_artifact(
                key="vehicle-details-summary",
                table={
                    "Métrica": [
                        "✅ Inseridos",
                        "❌ Inválidos",
                        "⚠️ Erros",
                        "📊 Total Processado",
                        "⏱️ Duração"
                    ],
                    "Valor": [
                        str(inserted),
                        str(len(invalid)),
                        str(len(failed)),
                        str(len(plates)),
                        f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                    ]
                },
                description="Resumo da consulta de placas na API Placamaster"
            )

            return {
                "inserted": inserted,
                "invalid": len(invalid),
                "failed": len(failed),
                "total_processed": len(plates),
                "duration_seconds": int(elapsed)
            }

        except Exception as e:
            logger.error(f"❌ Erro durante processamento: {e}")
            conn.rollback()
            logger.warning("⚠️ Rollback executado - nenhuma alteração foi persistida")
            raise


if __name__ == "__main__":
    # execução local
    main()

    # main.from_source(
    #     source=".",
    #     entrypoint="flows/vehicle/main_details.py:main"
    # ).deploy(
    #     name="vehicle-details-api-to-postgresql",
    #     work_pool_name="local-pool",
    #     schedules=[CronSchedule(cron="*/10 * * * *", timezone="America/Sao_Paulo")],
    #     tags=["rpa", "api", "postgresql", "dw_rpa"],
    #     parameters={},
    #     description="🚘 AnyCar API → PostgreSQL | Consulta detalhes de veículos por placa e carrega no Bronze. Rate limit (5 req/min).",
    #     version="4.0.0"
    # )
