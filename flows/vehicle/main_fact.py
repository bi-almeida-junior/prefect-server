from datetime import datetime

from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.artifacts import create_table_artifact
from prefect.cache_policies import NONE
from prefect.client.schemas.schedules import CronSchedule

from shared.connections.postgresql import postgresql_connection
from shared.decorators import flow_alerts

load_dotenv()

# Constantes
SCHEMA = "public"

# Tables
TABLE_VEHICLE_DETAILS = "brz_02_veiculo_detalhe"
TABLE_VEHICLE_FIPE = "brz_04_veiculo_fipe"
TABLE_FACT = "fato_veiculo_fipe"


@task(name="create_fact_table", log_prints=True, cache_policy=NONE)
def create_fact_table(conn) -> bool:
    """Cria a tabela fato se não existir."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        logger.info("Verificando/criando tabela fato_veiculo_fipe...")

        # Cria a tabela
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_FACT} (
                ds_placa VARCHAR(10),
                ds_marca VARCHAR(100),
                ds_modelo VARCHAR(200),
                nr_ano_modelo INTEGER,
                nr_ano_fabricacao INTEGER,
                ds_cor VARCHAR(100),
                ds_modelo_api VARCHAR(200),
                nr_ano_modelo_api INTEGER,
                fl_busca_alternativa BOOLEAN,
                ds_modelo_original VARCHAR(200),
                ds_anos_disponiveis TEXT,
                cd_marca_fipe VARCHAR(50),
                cd_modelo_fipe VARCHAR(50),
                cd_ano_combustivel VARCHAR(50),
                ds_combustivel VARCHAR(50),
                ds_sigla_combustivel VARCHAR(10),
                cd_fipe VARCHAR(50),
                vl_fipe VARCHAR(50),
                vl_fipe_numerico NUMERIC(10,2),
                ds_mes_referencia VARCHAR(50),
                nr_mes_referencia INTEGER,
                nr_ano_referencia INTEGER,
                cd_autenticacao VARCHAR(100),
                nr_tipo_veiculo INTEGER,
                dt_coleta_api TIMESTAMP,
                dt_consulta_fipe TIMESTAMP,
                dt_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Adiciona comentário
        cur.execute(f"""
            COMMENT ON TABLE {TABLE_FACT} IS 'Tabela fato com dados consolidados de veículos (detalhes + FIPE)'
        """)

        # Cria índices
        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_fato_veiculo_placa
            ON {TABLE_FACT}(ds_placa)
        """)

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_fato_veiculo_marca_modelo
            ON {TABLE_FACT}(ds_marca, ds_modelo)
        """)

        conn.commit()
        logger.info("✓ Tabela fato_veiculo_fipe criada/verificada com sucesso")
        return True

    except Exception as e:
        logger.error(f"Erro ao criar tabela: {e}")
        raise
    finally:
        cur.close()


@task(name="truncate_fact_table", log_prints=True, cache_policy=NONE)
def truncate_fact_table(conn) -> bool:
    """Trunca a tabela fato antes de recarregar."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        logger.info("Truncando tabela fato_veiculo_fipe...")
        cur.execute(f"TRUNCATE TABLE {TABLE_FACT}")
        conn.commit()
        logger.info("✓ Tabela truncada com sucesso")
        return True

    except Exception as e:
        logger.error(f"Erro ao truncar tabela: {e}")
        raise
    finally:
        cur.close()


@task(name="insert_fact_data", log_prints=True, cache_policy=NONE)
def insert_fact_data(conn) -> int:
    """Insere dados na tabela fato através do JOIN entre brz_02 e brz_04."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        logger.info("Inserindo dados na tabela fato...")

        cur.execute(f"""
            INSERT INTO {TABLE_FACT} (
                ds_placa,
                ds_marca,
                ds_modelo,
                nr_ano_modelo,
                nr_ano_fabricacao,
                ds_cor,
                ds_modelo_api,
                nr_ano_modelo_api,
                fl_busca_alternativa,
                ds_modelo_original,
                ds_anos_disponiveis,
                cd_marca_fipe,
                cd_modelo_fipe,
                cd_ano_combustivel,
                ds_combustivel,
                ds_sigla_combustivel,
                cd_fipe,
                vl_fipe,
                vl_fipe_numerico,
                ds_mes_referencia,
                nr_mes_referencia,
                nr_ano_referencia,
                cd_autenticacao,
                nr_tipo_veiculo,
                dt_coleta_api,
                dt_consulta_fipe,
                dt_insercao
            )
            SELECT
                d.ds_placa,
                f.ds_marca,
                f.ds_modelo,
                f.nr_ano_modelo,
                d.nr_ano_fabricacao,
                d.ds_cor,
                f.ds_modelo_api,
                f.nr_ano_modelo_api,
                f.fl_busca_alternativa,
                f.ds_modelo_original,
                f.ds_anos_disponiveis,
                f.cd_marca_fipe,
                f.cd_modelo_fipe,
                f.cd_ano_combustivel,
                f.ds_combustivel,
                f.ds_sigla_combustivel,
                f.cd_fipe,
                f.vl_fipe,
                f.vl_fipe_numerico,
                f.ds_mes_referencia,
                f.nr_mes_referencia,
                f.nr_ano_referencia,
                f.cd_autenticacao,
                f.nr_tipo_veiculo,
                d.dt_coleta_api,
                f.dt_consulta_fipe,
                CURRENT_TIMESTAMP AS dt_insercao
            FROM {TABLE_VEHICLE_FIPE} f
            INNER JOIN {TABLE_VEHICLE_DETAILS} d
                ON UPPER(TRIM(f.ds_marca)) = UPPER(TRIM(d.ds_marca))
                AND UPPER(TRIM(f.ds_modelo)) = UPPER(TRIM(d.ds_modelo))
                AND f.nr_ano_modelo = d.nr_ano_modelo
        """)

        rows_inserted = cur.rowcount
        conn.commit()
        logger.info(f"✓ {rows_inserted} registros inseridos na tabela fato")
        return rows_inserted

    except Exception as e:
        logger.error(f"Erro ao inserir dados: {e}")
        raise
    finally:
        cur.close()


@task(name="get_fact_stats", log_prints=True, cache_policy=NONE)
def get_fact_stats(conn) -> dict:
    """Retorna estatísticas da tabela fato."""
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        # Total de registros
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_FACT}")
        total = cur.fetchone()[0]

        # Total de placas únicas
        cur.execute(f"SELECT COUNT(DISTINCT ds_placa) FROM {TABLE_FACT}")
        placas_unicas = cur.fetchone()[0]

        # Total de marcas
        cur.execute(f"SELECT COUNT(DISTINCT ds_marca) FROM {TABLE_FACT}")
        marcas = cur.fetchone()[0]

        # Total de modelos
        cur.execute(f"SELECT COUNT(DISTINCT ds_modelo) FROM {TABLE_FACT}")
        modelos = cur.fetchone()[0]

        stats = {
            "total_registros": total,
            "placas_unicas": placas_unicas,
            "marcas": marcas,
            "modelos": modelos
        }

        logger.info(f"📊 Estatísticas: {stats}")
        return stats

    except Exception as e:
        logger.warning(f"Erro ao obter estatísticas: {e}")
        return {}
    finally:
        cur.close()


@flow(name="vehicle_fact_consolidation", log_prints=True)
@flow_alerts(
    flow_name="Consolidação Fato Veículos",
    source="PostgreSQL (BRONZE)",
    destination="PostgreSQL (FATO)",
    extract_summary=lambda result: {"records_loaded": result.get("inserted", 0)}
)
def main():
    """Flow: Gera tabela fato consolidando dados de detalhes e FIPE."""
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("📊 FATO VEÍCULOS: CONSOLIDAÇÃO")
    logger.info("=" * 80)

    with postgresql_connection(schema=SCHEMA) as conn:
        try:
            # Cria tabela se não existir
            create_fact_table(conn)

            # Trunca tabela
            truncate_fact_table(conn)

            # Insere dados
            inserted = insert_fact_data(conn)

            # Obtém estatísticas
            stats = get_fact_stats(conn)

            # Resumo
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info("=" * 80)
            logger.info(f"✅ Concluído: {inserted} registros inseridos")
            if stats:
                logger.info(f"📊 Placas únicas: {stats.get('placas_unicas', 0)}")
                logger.info(f"📊 Marcas: {stats.get('marcas', 0)}")
                logger.info(f"📊 Modelos: {stats.get('modelos', 0)}")
            logger.info(f"⏱️  Duração: {int(elapsed // 60)}m {int(elapsed % 60)}s")
            logger.info("=" * 80)

            # Criar artefato com resumo detalhado
            create_table_artifact(
                key="vehicle-fact-summary",
                table={
                    "Métrica": [
                        "📊 Total Registros",
                        "🚗 Placas Únicas",
                        "🏭 Marcas",
                        "🚙 Modelos",
                        "⏱️ Duração"
                    ],
                    "Valor": [
                        str(stats.get('total_registros', 0)),
                        str(stats.get('placas_unicas', 0)),
                        str(stats.get('marcas', 0)),
                        str(stats.get('modelos', 0)),
                        f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                    ]
                },
                description="Resumo da consolidação da tabela fato (detalhes + FIPE)"
            )

            return {
                "inserted": inserted,
                "total_registros": stats.get('total_registros', 0),
                "placas_unicas": stats.get('placas_unicas', 0),
                "marcas": stats.get('marcas', 0),
                "modelos": stats.get('modelos', 0),
                "duration_seconds": int(elapsed)
            }

        except Exception as e:
            logger.error(f"❌ Erro durante processamento: {e}")
            conn.rollback()
            logger.warning("⚠️ Rollback executado - nenhuma alteração foi persistida")
            raise


if __name__ == "__main__":
    # execução local
    # main()

    main.from_source(
        source=".",
        entrypoint="flows/vehicle/main_fact.py:main"
    ).deploy(
        name="vehicle-fact-consolidation",
        work_pool_name="local-pool",
        schedules=[CronSchedule(cron="* * * * *", timezone="America/Sao_Paulo")],
        tags=["rpa", "sql", "postgresql", "dw_rpa"],
        parameters={},
        description="📊 Consolidação Fato Veículos | Gera tabela fato consolidando dados de detalhes (brz_02) e FIPE (brz_04). Executa a cada 6 horas.",
        version="1.0.0"
    )
