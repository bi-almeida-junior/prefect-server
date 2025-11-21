from datetime import datetime
from typing import Optional, List, Dict

import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.cache_policies import NONE
from prefect.client.schemas.schedules import CronSchedule

from flows.vehicle.client import FipeAPIClient
from flows.vehicle.schemas import VehicleRecord, FipeMetadata, transform_to_snowflake_row
from shared.connections.postgresql import postgresql_connection
from shared.decorators import flow_alerts

load_dotenv()

# Constantes
BATCH_SIZE = 50
SCHEMA = "public"

# Tables
TABLE_VEHICLE_CONSOLIDATED = "brz_03_veiculo_consolidado"
TABLE_VEHICLE_FIPE = "brz_04_veiculo_fipe"


@task(name="get_pending_vehicles", log_prints=True, cache_policy=NONE)
def get_pending_vehicles(conn, batch_size: int) -> List[VehicleRecord]:
    """Busca veículos pendentes (status N, E, P)."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        cur.execute(f"""
            SELECT ds_marca, ds_modelo, nr_ano_fabricacao, nr_ano_modelo
            FROM {TABLE_VEHICLE_CONSOLIDATED}
            WHERE ds_status IN ('P', 'E', 'N')
            ORDER BY
                CASE ds_status WHEN 'P' THEN 1 WHEN 'E' THEN 2 WHEN 'N' THEN 3 END,
                dt_insercao DESC
            LIMIT {batch_size}
        """)
        results = cur.fetchall()

        vehicles = [
            VehicleRecord(
                brand=row[0],
                model=row[1],
                year_manuf=row[2],
                year_model=row[3],
                key=f"{row[0]}|{row[1]}|{row[3]}"
            )
            for row in results
        ]

        logger.info(f"Encontrados {len(vehicles)} veículos pendentes")
        return vehicles

    except Exception as e:
        logger.warning(f"Erro ao buscar veículos: {e}")
        return []
    finally:
        cur.close()


@task(name="update_status", log_prints=True, cache_policy=NONE)
def update_status(conn, vehicles: List[str], status: str, commit: bool = False) -> int:
    """Atualiza status de veículos em lote."""
    logger = get_run_logger()

    if not vehicles:
        return 0

    cur = conn.cursor()

    try:
        # Prepara parâmetros para executemany (batch único)
        update_sql = f"""
            UPDATE {TABLE_VEHICLE_CONSOLIDATED}
            SET ds_status = %s
            WHERE ds_marca = %s AND ds_modelo = %s AND nr_ano_modelo = %s
        """

        params = []
        for key in vehicles:
            brand, model, year_str = key.split('|')
            year = int(year_str)
            params.append((status, brand, model, year))

        cur.executemany(update_sql, params)
        rows_updated = cur.rowcount

        if commit:
            conn.commit()

        logger.info(f"✓ {rows_updated} veículos → status '{status}'")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status: {e}")
        raise
    finally:
        cur.close()


@task(name="query_fipe", log_prints=True, cache_policy=NONE)
def query_fipe(vehicle: VehicleRecord, client: FipeAPIClient, table_code: str) -> Optional[Dict]:
    """Consulta valor FIPE de um veículo."""
    logger = get_run_logger()

    # Valida marca
    brand_code = client.BRAND_CODES.get(vehicle.brand_upper)
    if not brand_code:
        logger.warning(f"[{vehicle.key}] Marca não mapeada: {vehicle.brand}")
        return {"_status": "I", "_motivo": f"Marca '{vehicle.brand}' não mapeada"}

    metadata = FipeMetadata(
        brand_code=brand_code,
        model_code="",
        year_fuel_code=""
    )

    try:
        # 1. Busca modelos
        models = client.get_models(brand_code, table_code)
        if not models:
            return None

        # 2. Busca modelo compatível
        keywords = vehicle.model_upper.split()
        compatible = [m for m in models if all(k in m["Label"].upper() for k in keywords)]

        if not compatible and keywords:
            # Busca por primeira palavra
            compatible = [m for m in models if keywords[0] in m["Label"].upper()]

        if not compatible:
            logger.warning(f"[{vehicle.key}] Modelo não encontrado")
            return {"_status": "I", "_motivo": f"Modelo '{vehicle.model}' não encontrado"}

        found_model = compatible[-1]  # Último = versão mais completa
        model_code = found_model["Value"]
        metadata.model_code = model_code

        # 3. Busca anos
        years = client.get_years(brand_code, model_code, table_code)
        year_str = str(vehicle.year_model)
        compatible_years = []

        for y in years:
            year_value = y["Value"].split("-")[0] if "-" in y["Value"] else y["Label"].split()[0]
            if year_str == year_value:
                compatible_years.append(y)

        # Se não encontrou ano, tenta outras variantes do modelo
        if not compatible_years:
            metadata.original_model = found_model["Label"]
            metadata.available_years = ", ".join([y['Label'] for y in years[:10]])

            for variant in compatible:
                variant_years = client.get_years(brand_code, variant["Value"], table_code)
                for y in variant_years:
                    year_value = y["Value"].split("-")[0] if "-" in y["Value"] else y["Label"].split()[0]
                    if year_str == year_value:
                        found_model = variant
                        model_code = variant["Value"]
                        compatible_years = [y]
                        metadata.alternative_search = True
                        metadata.model_code = model_code
                        break
                if compatible_years:
                    break

        if not compatible_years:
            return None

        found_year = compatible_years[0]
        year_model_code = found_year["Value"]
        metadata.year_fuel_code = year_model_code

        # Extrai ano e combustível
        if "-" in year_model_code:
            clean_year = year_model_code.split("-")[0]
            fuel_code = year_model_code.split("-")[1]
        else:
            clean_year = year_model_code
            fuel_code = "1"

        # 4. Consulta valor
        fipe_data = client.get_value(brand_code, model_code, clean_year, fuel_code, table_code)

        if not fipe_data:
            return None

        fipe_data["_metadata"] = metadata
        return fipe_data

    except Exception as e:
        logger.error(f"[{vehicle.key}] Erro: {e}")
        return None


@task(name="process_vehicles", log_prints=True, cache_policy=NONE)
def process_vehicles(vehicles: List[VehicleRecord], table_code: str) -> Dict:
    """Processa lote de veículos."""
    logger = get_run_logger()
    client = FipeAPIClient()

    results = []
    failed = []
    invalid = {}

    for vehicle in vehicles:
        fipe_data = query_fipe(vehicle, client, table_code)

        if fipe_data:
            if fipe_data.get("_status") == "I":
                invalid[vehicle.key] = fipe_data.get("_motivo", "Erro não especificado")
                continue

            metadata = fipe_data.pop("_metadata")
            row = transform_to_snowflake_row(vehicle, fipe_data, metadata)
            results.append(row)
        else:
            failed.append(vehicle.key)

    logger.info(f"✓ {len(results)} sucesso | {len(invalid)} inválidos | {len(failed)} erros")

    return {
        "df": pd.DataFrame(results) if results else pd.DataFrame(),
        "failed": failed,
        "invalid": invalid
    }


@task(name="insert_fipe_data", log_prints=True, cache_policy=NONE)
def insert_fipe_data(conn, df: pd.DataFrame, commit: bool = False) -> int:
    """Insere dados FIPE no PostgreSQL."""
    logger = get_run_logger()

    if df.empty:
        return 0

    cur = conn.cursor()

    try:
        insert_sql = f"""
        INSERT INTO {TABLE_VEHICLE_FIPE}
            (ds_marca, ds_modelo, nr_ano_modelo, ds_modelo_api, nr_ano_modelo_api, fl_busca_alternativa, ds_modelo_original,
             ds_anos_disponiveis, cd_marca_fipe, cd_modelo_fipe, cd_ano_combustivel, ds_combustivel, ds_sigla_combustivel,
             cd_fipe, vl_fipe, vl_fipe_numerico, ds_mes_referencia, nr_mes_referencia, nr_ano_referencia,
             cd_autenticacao, nr_tipo_veiculo, dt_consulta_fipe)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        records = [tuple(row[col] for col in [
            'DS_MARCA', 'DS_MODELO', 'NR_ANO_MODELO', 'DS_MODELO_API', 'NR_ANO_MODELO_API', 'FL_BUSCA_ALTERNATIVA', 'DS_MODELO_ORIGINAL',
            'DS_ANOS_DISPONIVEIS', 'CD_MARCA_FIPE', 'CD_MODELO_FIPE', 'CD_ANO_COMBUSTIVEL', 'DS_COMBUSTIVEL', 'DS_SIGLA_COMBUSTIVEL',
            'CD_FIPE', 'VL_FIPE', 'VL_FIPE_NUMERICO', 'DS_MES_REFERENCIA', 'NR_MES_REFERENCIA', 'NR_ANO_REFERENCIA',
            'CD_AUTENTICACAO', 'NR_TIPO_VEICULO', 'DT_CONSULTA_FIPE'
        ]) for _, row in df.iterrows()]

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


@flow(name="vehicle_fipe_to_postgresql", log_prints=True)
@flow_alerts(
    flow_name="FIPE Consulta",
    source="API FIPE",
    destination="PostgreSQL (BRONZE)",
    extract_summary=lambda result: {"records_loaded": result.get("inserted", 0)}
)
def main(batch_size: int = BATCH_SIZE):
    """Flow: Consulta FIPE e insere no PostgreSQL."""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("🚗 FIPE: API → POSTGRESQL")
    logger.info("=" * 80)

    # Tabela de referência FIPE
    client = FipeAPIClient()
    table_code = client.get_reference_table()
    logger.info(f"📅 Tabela FIPE: {table_code}")

    with postgresql_connection(schema=SCHEMA) as conn:
        try:
            # Busca pendentes
            vehicles = get_pending_vehicles(conn, batch_size)

            if not vehicles:
                logger.info("Nenhum veículo pendente")
                return {"inserted": 0}

            # Atualiza para 'P' (processando, com commit imediato para visibilidade em tempo real)
            update_status(conn, [v.key for v in vehicles], 'P', commit=True)

            # Processa
            result = process_vehicles(vehicles, table_code)
            df = result["df"]
            failed = result["failed"]
            invalid = result["invalid"]

            # Insere (sem commit)
            inserted = 0
            if not df.empty:
                inserted = insert_fipe_data(conn, df)
                # Atualiza para 'S' (sem commit)
                update_status(conn, df['CHAVE_VEICULO'].tolist(), 'S')

            # Atualiza inválidos (sem commit)
            if invalid:
                update_status(conn, list(invalid.keys()), 'I')

            # Atualiza erros (sem commit)
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

            return {"inserted": inserted}

        except Exception as e:
            logger.error(f"❌ Erro durante processamento: {e}")
            conn.rollback()
            logger.warning("⚠️ Rollback executado - nenhuma alteração foi persistida")
            raise


if __name__ == "__main__":
    # Localmente
    # main()

    main.from_source(
        source=".",
        entrypoint="flows/vehicle/main_fipe.py:main"
    ).deploy(
        name="vehicle-fipe-to-postgresql",
        work_pool_name="local-pool",
        schedules=[CronSchedule(cron="*/15 * * * *", timezone="America/Sao_Paulo")],
        tags=["rpa", "api", "postgresql", "dw_rpa"],
        parameters={},
        description="🚗 FIPE → PostgreSQL | Consulta valores FIPE e carrega no Bronze. Processa veículos em lote com busca inteligente.",
        version="3.0.0"
    )
