"""
Flow: Consolidação de Veículos da Bronze para Tabela Consolidada

Consolida veículos únicos por marca, modelo, ano_fabricacao e ano_modelo.
Elimina duplicatas da tabela BRZ_02_VEICULO_DETALHE e prepara para consulta FIPE.

Fonte: dw_rpa.public.brz_02_veiculo_detalhe
Destino: dw_rpa.public.brz_03_veiculo_consolidado

Status:
- N (Novo): Aguardando consulta FIPE
- P (Processando): Consulta FIPE em andamento
- S (Sucesso): Valor FIPE encontrado e persistido em BRZ_04_VEICULO_FIPE
- E (Erro): Erro temporário, permite retry (NO_MATCH, MULTIPLE_VERSIONS, YEAR_NOT_FOUND, API_ERROR, TIMEOUT)
- I (Inconsistente/Inválido): Erro definitivo, não faz retry
"""

from datetime import datetime
from typing import Set
import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.cache_policies import NONE
from prefect.client.schemas.schedules import CronSchedule

from shared.connections.postgresql import postgresql_connection
from shared.decorators import flow_alerts
from shared.utils import get_datetime_brasilia

load_dotenv()

# Constantes
SCHEMA = "public"
TABLE_VEHICLE_DETAILS = "brz_02_veiculo_detalhe"
TABLE_VEHICLE_CONSOLIDATED = "brz_03_veiculo_consolidado"


@task(name="ensure_consolidated_table_exists", log_prints=True, cache_policy=NONE)
def ensure_consolidated_table_exists(conn):
    """Garante que a tabela brz_03_veiculo_consolidado existe."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_VEHICLE_CONSOLIDATED} (
                ds_marca VARCHAR(100) NOT NULL,
                ds_modelo VARCHAR(100) NOT NULL,
                nr_ano_fabricacao INTEGER NOT NULL,
                nr_ano_modelo INTEGER NOT NULL,
                ds_status CHAR(1) NOT NULL DEFAULT 'N',
                ds_motivo_erro VARCHAR(200),
                dt_insercao TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo'),
                PRIMARY KEY (ds_marca, ds_modelo, nr_ano_fabricacao, nr_ano_modelo)
            )
        """
        cur.execute(create_table_sql)

        # Comentários
        cur.execute(
            f"COMMENT ON TABLE {TABLE_VEHICLE_CONSOLIDATED} IS 'Tabela consolidada de veículos únicos por marca, modelo e ano. Agregação da BRZ_02_VEICULO_DETALHE para eliminação de duplicatas'")
        cur.execute(f"COMMENT ON COLUMN {TABLE_VEHICLE_CONSOLIDATED}.ds_marca IS 'Marca do veículo em uppercase (ex: HONDA, TOYOTA, FIAT)'")
        cur.execute(f"COMMENT ON COLUMN {TABLE_VEHICLE_CONSOLIDATED}.ds_modelo IS 'Modelo do veículo em uppercase (ex: CIVIC, COROLLA, UNO)'")
        cur.execute(f"COMMENT ON COLUMN {TABLE_VEHICLE_CONSOLIDATED}.nr_ano_fabricacao IS 'Ano de fabricação do veículo (formato: YYYY)'")
        cur.execute(f"COMMENT ON COLUMN {TABLE_VEHICLE_CONSOLIDATED}.nr_ano_modelo IS 'Ano do modelo do veículo (formato: YYYY)'")
        cur.execute(f"COMMENT ON COLUMN {TABLE_VEHICLE_CONSOLIDATED}.ds_status IS 'Status de processamento da consulta FIPE: N=Novo, P=Processando, S=Sucesso, E=Erro, I=Inconsistente/Inválido'")
        cur.execute(
            f"COMMENT ON COLUMN {TABLE_VEHICLE_CONSOLIDATED}.ds_motivo_erro IS 'Motivo do erro ao consultar FIPE. Valores: NO_MATCH | MULTIPLE_VERSIONS | YEAR_NOT_FOUND | API_ERROR | TIMEOUT | NULL quando sucesso'")
        cur.execute(f"COMMENT ON COLUMN {TABLE_VEHICLE_CONSOLIDATED}.dt_insercao IS 'Data e hora de inserção do registro (timezone: America/Sao_Paulo UTC-3)'")

        # Índices
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_brz_consolidado_status ON {TABLE_VEHICLE_CONSOLIDATED}(ds_status)")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_brz_consolidado_marca_modelo ON {TABLE_VEHICLE_CONSOLIDATED}(ds_marca, ds_modelo)")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_brz_consolidado_dt_insercao ON {TABLE_VEHICLE_CONSOLIDATED}(dt_insercao)")

        conn.commit()
        logger.info(f"✓ Tabela {TABLE_VEHICLE_CONSOLIDATED} verificada/criada")

    except Exception as e:
        logger.error(f"Erro ao criar tabela: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()


@task(name="extract_new_vehicles", log_prints=True, cache_policy=NONE)
def extract_new_vehicles(conn) -> pd.DataFrame:
    """Extrai veículos únicos da tabela de detalhes."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        query = f"""
            SELECT DISTINCT
                UPPER(TRIM(ds_marca)) AS ds_marca,
                UPPER(TRIM(ds_modelo)) AS ds_modelo,
                nr_ano_fabricacao,
                nr_ano_modelo
            FROM {TABLE_VEHICLE_DETAILS}
            WHERE ds_marca IS NOT NULL
              AND ds_modelo IS NOT NULL
              AND nr_ano_fabricacao IS NOT NULL
              AND nr_ano_modelo IS NOT NULL
              AND TRIM(ds_marca) != ''
              AND TRIM(ds_modelo) != ''
        """

        cur.execute(query)
        results = cur.fetchall()

        if not results:
            logger.info("Nenhum veículo encontrado na tabela de detalhes")
            return pd.DataFrame()

        df = pd.DataFrame(results, columns=['ds_marca', 'ds_modelo', 'nr_ano_fabricacao', 'nr_ano_modelo'])
        logger.info(f"✓ {len(df)} veículos únicos extraídos de {TABLE_VEHICLE_DETAILS}")

        return df

    except Exception as e:
        logger.error(f"Erro ao extrair veículos: {e}")
        raise
    finally:
        cur.close()


@task(name="get_existing_vehicles", log_prints=True, cache_policy=NONE)
def get_existing_vehicles(conn) -> Set[tuple]:
    """Busca veículos já consolidados."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        cur.execute(f"""
            SELECT ds_marca, ds_modelo, nr_ano_fabricacao, nr_ano_modelo
            FROM {TABLE_VEHICLE_CONSOLIDATED}
        """)
        results = cur.fetchall()

        existing = {(row[0], row[1], row[2], row[3]) for row in results}
        logger.info(f"Veículos já consolidados: {len(existing)}")
        return existing

    except Exception as e:
        logger.error(f"Erro ao buscar veículos existentes: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()


@task(name="insert_new_vehicles", log_prints=True, cache_policy=NONE)
def insert_new_vehicles(conn, df: pd.DataFrame, existing_vehicles: Set[tuple]) -> int:
    """Insere novos veículos na tabela consolidada."""
    logger = get_run_logger()

    if df.empty:
        return 0

    # Filtra apenas veículos novos
    df['_key'] = df.apply(lambda row: (row['ds_marca'], row['ds_modelo'], row['nr_ano_fabricacao'], row['nr_ano_modelo']), axis=1)
    new_vehicles_df = df[~df['_key'].isin(existing_vehicles)].copy()
    new_vehicles_df = new_vehicles_df.drop(columns=['_key'])

    if new_vehicles_df.empty:
        logger.info("Nenhum veículo novo para inserir")
        return 0

    logger.info(f"Preparando {len(new_vehicles_df)} veículos para inserção...")

    cur = conn.cursor()

    try:
        # Adiciona colunas de controle
        dt_insercao = get_datetime_brasilia()
        new_vehicles_df['ds_status'] = 'N'
        new_vehicles_df['ds_motivo_erro'] = None
        new_vehicles_df['dt_insercao'] = dt_insercao

        # Insert usando executemany
        insert_sql = f"""
            INSERT INTO {TABLE_VEHICLE_CONSOLIDATED}
            (ds_marca, ds_modelo, nr_ano_fabricacao, nr_ano_modelo, ds_status, ds_motivo_erro, dt_insercao)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        records = [
            (
                row['ds_marca'],
                row['ds_modelo'],
                row['nr_ano_fabricacao'],
                row['nr_ano_modelo'],
                row['ds_status'],
                row['ds_motivo_erro'],
                row['dt_insercao']
            )
            for _, row in new_vehicles_df.iterrows()
        ]

        cur.executemany(insert_sql, records)
        rows_inserted = cur.rowcount

        logger.info(f"✓ {rows_inserted} veículos inseridos")
        return rows_inserted

    except Exception as e:
        logger.error(f"Erro ao inserir veículos: {e}")
        raise
    finally:
        cur.close()


@flow(name="vehicle_consolidation", log_prints=True)
@flow_alerts(
    flow_name="Consolidação Veículos",
    source="PostgreSQL (BRZ_02_VEICULO_DETALHE)",
    destination="PostgreSQL (BRZ_03_VEICULO_CONSOLIDADO)",
    extract_summary=lambda result: {"vehicles_consolidated": result.get("inserted", 0)}
)
def main():
    """Flow: Consolida veículos únicos para consulta FIPE."""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("🚗 CONSOLIDAÇÃO DE VEÍCULOS: BRONZE → CONSOLIDADO")
    logger.info("=" * 80)

    with postgresql_connection(schema=SCHEMA) as conn:
        try:
            # Garante que a tabela existe
            ensure_consolidated_table_exists(conn)

            # Extrai veículos únicos
            logger.info("📥 Extraindo veículos únicos...")
            df_vehicles = extract_new_vehicles(conn)

            if df_vehicles.empty:
                logger.warning("⚠️ Nenhum veículo encontrado para consolidar")
                return {"inserted": 0}

            # Busca veículos já consolidados
            existing_vehicles = get_existing_vehicles(conn)

            # Insere apenas veículos novos
            inserted = insert_new_vehicles(conn, df_vehicles, existing_vehicles)

            # Commit
            conn.commit()
            logger.info("✓ Transação commitada com sucesso")

            # Resumo
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info("=" * 80)
            logger.info(f"✅ Concluído:")
            logger.info(f"   • Total de veículos únicos extraídos: {len(df_vehicles)}")
            logger.info(f"   • Veículos já consolidados: {len(existing_vehicles)}")
            logger.info(f"   • Novos veículos inseridos: {inserted}")
            logger.info(f"⏱️  Duração: {int(elapsed // 60)}m {int(elapsed % 60)}s")
            logger.info("=" * 80)

            return {
                "inserted": inserted,
                "total_unique": len(df_vehicles),
                "existing": len(existing_vehicles)
            }

        except Exception as e:
            logger.error(f"❌ Erro durante processamento: {e}")
            conn.rollback()
            logger.warning("⚠️ Rollback executado - nenhuma alteração foi persistida")
            raise


if __name__ == "__main__":
    # Execução local
    # main()

    main.from_source(
        source=".",
        entrypoint="flows/vehicle/main_consolidation.py:main"
    ).deploy(
        name="vehicle-consolidation",
        work_pool_name="local-pool",
        schedules=[CronSchedule(cron="0 * * * *", timezone="America/Sao_Paulo")],
        tags=["rpa", "sql", "postgresql", "dw_rpa"],
        parameters={},
        description="🚗 Consolidação Veículos → PostgreSQL | Consolida veículos únicos da BRZ_02 para BRZ_03, eliminando duplicatas e preparando para consulta FIPE.",
        version="1.0.0"
    )
