"""
Flow: Consolidação de Placas de Múltiplas Fontes

Coleta placas de:
- WPS (PostgreSQL): dm_fluxo_veiculos.staging_area.wpsentradadetalhada
- Luminus GS (SQL Server): LUMINUS_GS.dbo.MidiaBaseEntity
- Luminus NR (SQL Server): LUMINUS_NR.dbo.MidiaBaseEntity
- Luminus NS (SQL Server): LUMINUS_NS.dbo.MidiaBaseEntity

Insere/Atualiza na tabela: bronze.brz_01_placa_raw (PostgreSQL)
"""

from datetime import datetime
from typing import List, Set
import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.cache_policies import NONE
from prefect.client.schemas.schedules import CronSchedule

from shared.connections.postgresql import postgresql_connection
from shared.connections.sqlserver import sqlserver_connection
from shared.decorators import flow_alerts
from shared.utils import get_datetime_brasilia

load_dotenv()

# Constantes
SCHEMA = "public"
TABLE_PLATE_RAW = "brz_01_placa_raw"


@task(name="extract_plates_wps", log_prints=True, cache_policy=NONE)
def extract_plates_wps() -> Set[str]:
    """Extrai placas distintas do WPS (PostgreSQL)."""
    logger = get_run_logger()

    try:
        with postgresql_connection(schema="staging_area", database="dm_fluxo_veiculos") as conn:
            cur = conn.cursor()

            query = """
                    SELECT DISTINCT UPPER(TRIM(REPLACE(placa, '-', ''))) AS ds_placa
                    FROM staging_area."WpsEntradaDetalhada"
                    WHERE placa IS NOT NULL
                      AND TRIM(placa) != ''
                  AND LENGTH(TRIM(REPLACE(placa, '-', ''))) >= 7 \
                    """

            cur.execute(query)
            results = cur.fetchall()

            plates = {row[0] for row in results if row[0]}
            logger.info(f"✓ WPS: {len(plates)} placas extraídas")

            cur.close()
            return plates

    except Exception as e:
        logger.error(f"Erro ao extrair placas WPS: {e}")
        return set()


@task(name="extract_plates_luminus", log_prints=True, cache_policy=NONE)
def extract_plates_luminus(server: str, source_name: str) -> Set[str]:
    """
    Extrai placas distintas do Luminus (SQL Server).

    Args:
        server: IP/hostname do servidor SQL Server
        source_name: Nome da fonte para log (GS, NR, NS)
    """
    logger = get_run_logger()

    try:
        with sqlserver_connection(server=server, database="Luminus") as conn:
            cur = conn.cursor()

            query = """
                    SELECT DISTINCT UPPER(LTRIM(RTRIM(REPLACE(Placa, '-', '')))) AS ds_placa
                    FROM dbo.MidiaBaseEntity
                    WHERE Placa IS NOT NULL
                      AND LTRIM(RTRIM(Placa)) != ''
                  AND LEN(LTRIM(RTRIM(REPLACE(Placa, '-', '')))) >= 7 \
                    """

            cur.execute(query)
            results = cur.fetchall()

            plates = {row[0] for row in results if row[0]}
            logger.info(f"✓ Luminus {source_name}: {len(plates)} placas extraídas")

            cur.close()
            return plates

    except Exception as e:
        logger.error(f"Erro ao extrair placas Luminus {source_name}: {e}")
        return set()


@task(name="aggregate_plates", log_prints=True, cache_policy=NONE)
def aggregate_plates(
        wps_plates: Set[str],
        gs_plates: Set[str],
        nr_plates: Set[str],
        ns_plates: Set[str]
) -> pd.DataFrame:
    """Agrega placas de todas as fontes e remove duplicatas."""
    logger = get_run_logger()

    # União de todas as placas
    all_plates = wps_plates | gs_plates | nr_plates | ns_plates
    # all_plates = wps_plates

    logger.info(f"Total de placas únicas: {len(all_plates)}")
    logger.info(f"  - WPS: {len(wps_plates)}")
    logger.info(f"  - Luminus GS: {len(gs_plates)}")
    logger.info(f"  - Luminus NR: {len(nr_plates)}")
    logger.info(f"  - Luminus NS: {len(ns_plates)}")

    # Cria DataFrame
    df = pd.DataFrame({
        'ds_placa': sorted(list(all_plates)),
        'ds_status': 'N',
        'ds_motivo_erro': None,
        'nr_tentativas': 0,
        'dt_coleta': None,
        'dt_insercao': get_datetime_brasilia()
    })

    return df


@task(name="ensure_table_exists", log_prints=True, cache_policy=NONE)
def ensure_table_exists(conn):
    """Garante que a tabela brz_01_placa_raw existe."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_PLATE_RAW} (
                ds_placa VARCHAR(10) PRIMARY KEY,
                ds_status CHAR(1) NOT NULL DEFAULT 'N',
                ds_motivo_erro VARCHAR(100),
                nr_tentativas INTEGER NOT NULL DEFAULT 0,
                dt_coleta TIMESTAMP,
                dt_insercao TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')
            )
        """
        cur.execute(create_table_sql)

        # Cria índices
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_brz_placa_status ON {TABLE_PLATE_RAW}(ds_status)")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_brz_placa_dt_coleta ON {TABLE_PLATE_RAW}(dt_coleta)")

        conn.commit()
        logger.info(f"✓ Tabela {TABLE_PLATE_RAW} verificada/criada")

    except Exception as e:
        logger.error(f"Erro ao criar tabela: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()


@task(name="get_existing_plates", log_prints=True, cache_policy=NONE)
def get_existing_plates(conn) -> Set[str]:
    """Busca placas já existentes na tabela bronze."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        cur.execute(f"SELECT ds_placa FROM {TABLE_PLATE_RAW}")
        results = cur.fetchall()

        existing = {row[0] for row in results}
        logger.info(f"Placas existentes na base: {len(existing)}")
        return existing

    except Exception as e:
        logger.error(f"Erro ao buscar placas existentes: {e}")
        conn.rollback()  # Limpa transação abortada
        raise
    finally:
        cur.close()


@task(name="insert_new_plates", log_prints=True, cache_policy=NONE)
def insert_new_plates(conn, df: pd.DataFrame, existing_plates: Set[str]) -> int:
    """Insere placas usando COPY (ultra-rápido para grandes volumes)."""
    logger = get_run_logger()

    if df.empty:
        return 0

    # Filtra apenas placas novas
    new_plates_df = df[~df['ds_placa'].isin(existing_plates)].copy()

    if new_plates_df.empty:
        logger.info("Nenhuma placa nova para inserir")
        return 0

    logger.info(f"Preparando {len(new_plates_df)} placas para inserção via COPY...")

    cur = conn.cursor()

    try:
        from io import StringIO

        # Prepara dados para COPY (substitui None por \N para NULL)
        buffer = StringIO()
        for _, row in new_plates_df.iterrows():
            # Converte dt_insercao para string (pode já ser datetime ou string)
            dt_insercao_val = row['dt_insercao']
            if pd.notna(dt_insercao_val):
                if hasattr(dt_insercao_val, 'isoformat'):
                    dt_insercao_str = dt_insercao_val.isoformat()
                else:
                    dt_insercao_str = str(dt_insercao_val)
            else:
                dt_insercao_str = '\\N'

            line = '\t'.join([
                str(row['ds_placa']),
                str(row['ds_status']),
                str(row['ds_motivo_erro']) if pd.notna(row['ds_motivo_erro']) else '\\N',
                str(row['nr_tentativas']),
                str(row['dt_coleta']) if pd.notna(row['dt_coleta']) else '\\N',
                dt_insercao_str
            ])
            buffer.write(line + '\n')

        buffer.seek(0)

        # COPY ultra-rápido (protocolo nativo PostgreSQL)
        logger.info("Executando COPY FROM STDIN...")
        cur.copy_from(
            buffer,
            TABLE_PLATE_RAW,
            columns=['ds_placa', 'ds_status', 'ds_motivo_erro', 'nr_tentativas', 'dt_coleta', 'dt_insercao'],
            sep='\t',
            null='\\N'
        )

        rows_inserted = len(new_plates_df)
        logger.info(f"✓ {rows_inserted} placas inseridas via COPY (ultra-rápido)")
        return rows_inserted

    except Exception as e:
        logger.error(f"Erro ao inserir placas via COPY: {e}")
        raise
    finally:
        cur.close()


@flow(name="vehicle_placa_raw_consolidation", log_prints=True)
@flow_alerts(
    flow_name="Consolidação Placas",
    source="WPS + Luminus (GS/NR/NS)",
    destination="PostgreSQL (BRONZE)",
    extract_summary=lambda result: {"new_plates": result.get("inserted", 0)}
)
def main():
    """Flow: Consolida placas de múltiplas fontes e insere no PostgreSQL."""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("🚗 CONSOLIDAÇÃO DE PLACAS: Múltiplas Fontes → PostgreSQL")
    logger.info("=" * 80)

    try:
        # Extração paralela de todas as fontes
        logger.info("📥 Iniciando extração de placas...")

        wps_plates = extract_plates_wps()
        gs_plates = extract_plates_luminus("172.24.30.222", "GS")
        nr_plates = extract_plates_luminus("172.26.30.243", "NR")
        ns_plates = extract_plates_luminus("172.27.30.241", "NS")

        # Agregação
        logger.info("🔄 Agregando placas...")
        df_plates = aggregate_plates(wps_plates, gs_plates, nr_plates, ns_plates)
        # df_plates = aggregate_plates(wps_plates)

        if df_plates.empty:
            logger.warning("⚠️ Nenhuma placa encontrada nas fontes")
            return {"inserted": 0, "total_sources": 0}

        # Carga no PostgreSQL
        with postgresql_connection(schema=SCHEMA) as conn:
            try:
                # Garante que a tabela existe
                ensure_table_exists(conn)

                # Busca placas já existentes
                existing_plates = get_existing_plates(conn)

                # Insere apenas placas novas usando COPY (ultra-rápido)
                inserted = insert_new_plates(conn, df_plates, existing_plates)

                # Commit
                conn.commit()
                logger.info("✓ Transação commitada com sucesso")

                # Resumo
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info("=" * 80)
                logger.info(f"✅ Concluído:")
                logger.info(f"   • Total de placas únicas: {len(df_plates)}")
                logger.info(f"   • Placas já existentes: {len(existing_plates)}")
                logger.info(f"   • Placas novas inseridas: {inserted}")
                logger.info(f"⏱️  Duração: {int(elapsed // 60)}m {int(elapsed % 60)}s")
                logger.info("=" * 80)

                return {
                    "inserted": inserted,
                    "total_sources": len(df_plates),
                    "existing": len(existing_plates)
                }

            except Exception as e:
                logger.error(f"❌ Erro durante processamento: {e}")
                conn.rollback()
                logger.warning("⚠️ Rollback executado - nenhuma alteração foi persistida")
                raise

    except Exception as e:
        logger.error(f"❌ Erro no flow: {e}")
        raise


if __name__ == "__main__":
    # main()

    main.from_source(
        source=".",
        entrypoint="flows/vehicle/main_plate_raw.py:main"
    ).deploy(
        name="vehicle-plate-raw-consolidation",
        work_pool_name="local-pool",
        schedules=[CronSchedule(cron="0 1 * * *", timezone="America/Sao_Paulo")],
        tags=["rpa", "sql", "postgresql", "dw_rpa"],
        parameters={},
        description="🚗 Consolidação de Placas → PostgreSQL | Extrai placas de WPS (PostgreSQL) e Luminus GS/NR/NS (SQL Server) e consolida na tabela bronze.",
        version="1.0.0"
    )
