import os
import csv
import time
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE
from prefect.artifacts import create_table_artifact
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule

# Imports dos módulos de conexão
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.connections.deconve import (  # noqa: E402
    authenticate_deconve, get_units, get_unit_details, get_people_counter_report
)
from shared.connections.snowflake import (  # noqa: E402
    connect_snowflake, create_table_if_not_exists,
    merge_csv_to_snowflake, close_snowflake_connection,
    DECONVE_TABLES_SCHEMAS
)
from shared.alerts import (  # noqa: E402
    send_flow_success_alert, send_flow_error_alert
)

# Carrega variáveis de ambiente
load_dotenv()


def calculate_date_range(days_back: int = 3):
    """
    Calcula range de datas para processamento retroativo

    Args:
        days_back: Número de dias para voltar (padrão: 3)

    Returns:
        Tuple (start_date, end_date) no formato ISO com timezone
    """
    # Data/hora de D-1 (ontem)
    end_datetime = datetime.now() - timedelta(days=1)

    # Data/hora inicial (X dias atrás a partir de D-1)
    start_datetime = end_datetime - timedelta(days=days_back)

    # Formata para ISO com timezone -03:00 (horário de Brasília)
    # Início do dia
    start_date = start_datetime.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S-03:00")

    # Final do dia D-1 (ontem)
    end_date = end_datetime.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S-03:00")

    return start_date, end_date


@task(cache_policy=NO_CACHE)
def get_all_cameras(access_token: str, units_data: Dict[str, Any]) -> List[str]:
    """
    Obtém lista de todos os IDs de câmeras de todas as unidades

    Args:
        access_token: Token de acesso da API
        units_data: Dados das unidades

    Returns:
        Lista de IDs de câmeras
    """
    logger = get_run_logger()

    camera_ids = []
    items = units_data.get('items', [])

    logger.info(f"🎥 Buscando câmeras de {len(items)} unidade(s)...")

    for idx, item in enumerate(items, 1):
        unit_id = item.get('id', '')
        unit_name = item.get('name', '')

        try:
            logger.info(f"  [{idx}/{len(items)}] Buscando câmeras de {unit_name}...")
            unit_details = get_unit_details.fn(access_token, unit_id)
            videos = unit_details.get('videos', [])

            for video in videos:
                camera_id = video.get('id')
                if camera_id:
                    camera_ids.append(camera_id)

            logger.info(f"    ✅ {len(videos)} câmera(s) encontrada(s)")

        except Exception as e:
            logger.error(f"    ❌ Erro ao buscar câmeras da unit {unit_id}: {str(e)}")
            continue

    logger.info(f"✅ Total de câmeras encontradas: {len(camera_ids)}")

    return camera_ids


@task(cache_policy=NO_CACHE)
def fetch_people_counter_data_with_pagination(
        access_token: str,
        video_id: str,
        start_date: str,
        end_date: str,
        group_by: str = "hour",
        limit: int = 1000
) -> List[Dict[str, Any]]:
    """
    Busca dados de contador de pessoas com paginação automática

    Args:
        access_token: Token de acesso
        video_id: ID do vídeo/câmera
        start_date: Data inicial
        end_date: Data final
        group_by: Agrupamento
        limit: Limite por página

    Returns:
        Lista completa de registros
    """
    logger = get_run_logger()

    all_items = []
    skip = 0
    page = 1

    while True:
        try:
            logger.info(f"    📄 Página {page} (skip={skip})...")

            report_data = get_people_counter_report(
                access_token=access_token,
                video_id=video_id,
                start_date=start_date,
                end_date=end_date,
                group_by=group_by,
                limit=limit,
                skip=skip
            )

            items = report_data.get('items', [])
            has_more = report_data.get('has_more', False)
            total = report_data.get('total', 0)

            all_items.extend(items)

            logger.info(f"    ✅ {len(items)} registros na página {page} (total acumulado: {len(all_items)}/{total})")

            if not has_more or len(items) == 0:
                break

            skip += limit
            page += 1

        except Exception as e:
            logger.error(f"    ❌ Erro na página {page}: {str(e)}")
            break

    return all_items


@task(cache_policy=NO_CACHE)
def process_cameras_and_save_csv(
        access_token: str,
        camera_ids: List[str],
        start_date: str,
        end_date: str,
        output_path: str,
        group_by: str = "hour"
) -> Dict[str, Any]:
    """
    Processa todas as câmeras e salva dados de person flow em CSV

    Args:
        access_token: Token de acesso
        camera_ids: Lista de IDs das câmeras
        start_date: Data inicial
        end_date: Data final
        output_path: Caminho do arquivo CSV
        group_by: Agrupamento (hour, day, etc)

    Returns:
        Dict com estatísticas do processamento
    """
    logger = get_run_logger()

    try:
        # Cria diretório de saída se não existir
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Define as colunas do CSV (em UPPER CASE)
        fieldnames = ['ID_CAMERA', 'DT_FLUXO', 'NR_ENTRADA', 'NR_SAIDA', 'DT_CRIACAO']

        # Data/hora de criação do registro (horário de Brasília: UTC-3)
        dt_criacao = (datetime.now() - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

        total_cameras = len(camera_ids)
        total_records = 0

        logger.info(f"💾 Processando {total_cameras} câmera(s)...")
        logger.info(f"📅 Período: {start_date} a {end_date}")

        # Escreve arquivo CSV
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Escreve cabeçalho
            writer.writeheader()

            # Processa cada câmera
            for idx, camera_id in enumerate(camera_ids, 1):
                logger.info(f"\n📹 [{idx}/{total_cameras}] Câmera: {camera_id[:8]}...")

                try:
                    # Busca dados com paginação
                    items = fetch_people_counter_data_with_pagination(
                        access_token=access_token,
                        video_id=camera_id,
                        start_date=start_date,
                        end_date=end_date,
                        group_by=group_by
                    )

                    # Escreve registros no CSV
                    for item in items:
                        created_at = item.get('created_at', '')
                        direction_in = item.get('direction_in', {})
                        direction_out = item.get('direction_out', {})

                        nr_entrada = direction_in.get('total', 0)
                        nr_saida = direction_out.get('total', 0)

                        writer.writerow({
                            'ID_CAMERA': camera_id,
                            'DT_FLUXO': created_at,
                            'NR_ENTRADA': nr_entrada,
                            'NR_SAIDA': nr_saida,
                            'DT_CRIACAO': dt_criacao
                        })

                        total_records += 1

                    logger.info(f"  ✅ {len(items)} registro(s) salvos")

                except Exception as e:
                    logger.error(f"  ❌ Erro ao processar câmera {camera_id}: {str(e)}")
                    # Continua processando as outras câmeras

        file_size = os.path.getsize(output_path)
        file_size_kb = file_size / 1024

        logger.info(f"\n✅ Arquivo CSV criado: {output_path}")
        logger.info(f"📊 {total_records} registro(s) de {total_cameras} câmera(s) salvos ({file_size_kb:.2f} KB)")

        return {
            "status": "success",
            "rows_written": total_records,
            "cameras_processed": total_cameras,
            "file_path": output_path,
            "file_size_bytes": file_size
        }

    except Exception as e:
        logger.error(f"❌ Erro ao processar câmeras: {str(e)}")
        raise


@flow(log_prints=True, name="deconve-person-flow-to-snowflake")
def deconve_person_flow_to_snowflake(
        # Deconve params
        api_key: Optional[str] = None,
        days_back: int = 3,  # Processa últimos 3 dias por padrão (retroativo)
        start_date: Optional[str] = None,  # Permite override manual
        end_date: Optional[str] = None,  # Permite override manual
        group_by: str = "hour",
        # Snowflake params
        snowflake_account: Optional[str] = None,
        snowflake_user: Optional[str] = None,
        snowflake_private_key: Optional[str] = None,
        snowflake_private_key_passphrase: Optional[str] = None,
        snowflake_warehouse: Optional[str] = None,
        snowflake_database: Optional[str] = None,
        snowflake_schema: Optional[str] = None,
        snowflake_role: Optional[str] = None,
        # Alertas
        send_alerts: bool = True,
        alert_group_id: Optional[str] = None
):
    """
    Flow: Deconve API -> Snowflake (DECONVE_PERSON_FLOW) com MERGE

    Estratégia anti-duplicação:
    - Usa MERGE (UPSERT) no Snowflake
    - Chave composta: (ID_CAMERA, DT_FLOW)
    - Se registro já existe: ATUALIZA
    - Se registro não existe: INSERE
    - Permite reprocessamento seguro de períodos retroativos

    Processo:
    1. Calcula range de datas (data atual - X dias)
    2. Autentica na API Deconve
    3. Obtém lista de todas as câmeras
    4. Para cada câmera, busca relatório de person flow (com paginação)
    5. Salva em CSV temporário
    6. Cria tabela no Snowflake se não existir (schema GOLD)
    7. Faz MERGE (UPSERT) do CSV no Snowflake
    8. Remove CSV temporário
    9. Envia alertas
    """
    logger = get_run_logger()
    start_time = time.time()

    logger.info("=" * 80)
    logger.info("🚀 Iniciando flow Deconve API -> Snowflake (Person Flow)")
    logger.info("=" * 80)

    # 1. Carrega configurações
    # Carrega API Key do Deconve via Prefect Block (Secret)
    if not api_key:
        try:
            logger.info("🔐 Carregando DECONVE_API_KEY do Prefect Block...")
            api_key_block = Secret.load("deconve-api-key")
            api_key = api_key_block.get()
            logger.info("✅ API Key carregada do Block 'deconve-api-key'")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao carregar Block 'deconve-api-key': {e}")
            logger.info("Tentando carregar do .env como fallback...")
            api_key = os.getenv("DECONVE_API_KEY")

    # Snowflake (mantém .env para estas variáveis por enquanto)
    snowflake_account = snowflake_account or os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user = snowflake_user or os.getenv("SNOWFLAKE_USER")
    snowflake_private_key = snowflake_private_key or os.getenv("SNOWFLAKE_PRIVATE_KEY")
    snowflake_private_key_passphrase = snowflake_private_key_passphrase or os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    snowflake_warehouse = snowflake_warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_database = snowflake_database or os.getenv("SNOWFLAKE_DATABASE")
    snowflake_role = snowflake_role or os.getenv("SNOWFLAKE_ROLE")

    # Carrega Schema específico do Deconve via Prefect Block (Secret)
    if not snowflake_schema:
        try:
            logger.info("🔐 Carregando DECONVE_SNOWFLAKE_SCHEMA do Prefect Block...")
            schema_block = Secret.load("deconve-snowflake-schema")
            snowflake_schema = schema_block.get()
            logger.info(f"✅ Schema carregado do Block 'deconve-snowflake-schema': {snowflake_schema}")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao carregar Block 'deconve-snowflake-schema': {e}")
            logger.info("Usando fallback: GOLD")
            snowflake_schema = "GOLD"

    # Valida configurações
    if not api_key:
        raise ValueError("Configure o Block 'deconve-api-key' na UI do Prefect ou DECONVE_API_KEY no .env")

    if not all([snowflake_account, snowflake_user, snowflake_private_key,
                snowflake_warehouse, snowflake_database]):
        raise ValueError("Configure credenciais Snowflake no .env")

    # 2. Calcula datas (se não fornecidas manualmente)
    if not start_date or not end_date:
        logger.info(f"📅 Calculando período: últimos {days_back} dias...")
        start_date, end_date = calculate_date_range(days_back)

    logger.info(f"📅 Período: {start_date} a {end_date}")

    # Usa diretório temporário que se auto-exclui (mesmo em caso de erro)
    with tempfile.TemporaryDirectory(prefix="deconve_person_flow_") as temp_dir:
        # Monta caminho completo do arquivo CSV temporário
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"person_flow_{timestamp}.csv"
        output_path = os.path.join(temp_dir, output_filename)

        logger.info(f"📁 Diretório temporário: {temp_dir}")
        logger.info(f"📄 Arquivo temporário: {output_filename}")

        try:
            # 3. Autentica na API
            logger.info("\n" + "=" * 80)
            logger.info("🔐 Autenticando na API Deconve...")
            logger.info("=" * 80)

            token_data = authenticate_deconve(api_key)
            access_token = token_data['access_token']

            # 4. Obtém unidades
            logger.info("\n" + "=" * 80)
            logger.info("📥 Buscando unidades...")
            logger.info("=" * 80)

            units_data = get_units(access_token)
            total_units = units_data.get('total', 0)
            logger.info(f"✅ {total_units} unidade(s) encontrada(s)")

            # 5. Obtém lista de câmeras
            logger.info("\n" + "=" * 80)
            logger.info("🎥 Obtendo lista de câmeras...")
            logger.info("=" * 80)

            camera_ids = get_all_cameras(access_token, units_data)

            if len(camera_ids) == 0:
                logger.warning("⚠️ Nenhuma câmera encontrada")
                return {"status": "no_data", "message": "Nenhuma câmera encontrada"}

            # 6. Processa câmeras e salva CSV
            logger.info("\n" + "=" * 80)
            logger.info("📊 Processando relatórios de person flow...")
            logger.info("=" * 80)

            csv_result = process_cameras_and_save_csv(
                access_token=access_token,
                camera_ids=camera_ids,
                start_date=start_date,
                end_date=end_date,
                output_path=output_path,
                group_by=group_by
            )

            rows_written = csv_result.get('rows_written', 0)
            cameras_processed = csv_result.get('cameras_processed', 0)

            # 7. Conecta Snowflake
            logger.info("\n" + "=" * 80)
            logger.info(f"❄️ Conectando Snowflake: {snowflake_account}")
            logger.info("=" * 80)

            snowflake_conn = connect_snowflake(
                account=snowflake_account,
                user=snowflake_user,
                private_key=snowflake_private_key,
                warehouse=snowflake_warehouse,
                database=snowflake_database,
                schema=snowflake_schema,
                role=snowflake_role,
                private_key_passphrase=snowflake_private_key_passphrase,
                timeout=60
            )

            # 8. Obtém schema da tabela
            table_schema = DECONVE_TABLES_SCHEMAS["person_flow"]
            table_name = table_schema["table_name"]
            primary_keys = table_schema["primary_key"]

            # 9. Cria tabela se não existe
            logger.info(f"🏗️ Criando tabela {table_name} se não existir...")
            create_table_if_not_exists(
                snowflake_conn,
                table_name,
                table_schema["columns"],
                primary_keys
            )

            # 10. Faz MERGE (UPSERT) no Snowflake
            logger.info("\n" + "=" * 80)
            logger.info("🔄 Carregando dados no Snowflake (MERGE - UPSERT)...")
            logger.info("=" * 80)

            merge_result = merge_csv_to_snowflake(
                snowflake_conn,
                table_name,
                output_path,
                primary_keys,
                csv_encoding='utf-8',
                columns=['ID_CAMERA', 'DT_FLUXO', 'NR_ENTRADA', 'NR_SAIDA', 'DT_CRIACAO']
            )

            rows_inserted = merge_result.get('rows_inserted', 0)
            rows_updated = merge_result.get('rows_updated', 0)

            # 11. Fecha conexão Snowflake
            close_snowflake_connection(snowflake_conn)

            # 12. CSV temporário será removido automaticamente ao sair do contexto

            # 13. Resumo
            logger.info("\n" + "=" * 80)
            logger.info("📊 RESUMO")
            logger.info("=" * 80)

            logger.info(f"✅ Registros extraídos da API: {rows_written}")
            logger.info(f"📝 Inseridos no Snowflake: {rows_inserted}")
            logger.info(f"🔄 Atualizados no Snowflake: {rows_updated}")
            logger.info(f"🎥 Câmeras processadas: {cameras_processed}")
            logger.info(f"📅 Período: {start_date} a {end_date}")
            logger.info(f"❄️ Tabela: {snowflake_database}.{snowflake_schema}.{table_name}")

            duration = time.time() - start_time
            logger.info(f"⏱️ Duração: {duration:.1f}s")
            logger.info("=" * 80 + "\n")

            # 14. ARTIFACTS: Visibilidade no Prefect UI
            try:
                # Tabela de resumo
                status_icon = "✅" if rows_written > 0 else "⚠️"

                table_data = [{
                    "Métrica": "Status",
                    "Valor": f"{status_icon} {'Sucesso' if rows_written > 0 else 'Sem dados'}"
                }, {
                    "Métrica": "Registros Extraídos",
                    "Valor": f"{rows_written:,}"
                }, {
                    "Métrica": "Novos Inseridos",
                    "Valor": f"{rows_inserted:,}"
                }, {
                    "Métrica": "Atualizados",
                    "Valor": f"{rows_updated:,}"
                }, {
                    "Métrica": "Câmeras Processadas",
                    "Valor": f"{cameras_processed:,}"
                }, {
                    "Métrica": "Período",
                    "Valor": f"{days_back} dias retroativos"
                }, {
                    "Métrica": "Duração",
                    "Valor": f"{duration:.1f}s"
                }, {
                    "Métrica": "Tabela Snowflake",
                    "Valor": f"{snowflake_database}.{snowflake_schema}.{table_name}"
                }]

                artifact_desc = f"Person Flow: {rows_written:,} registros | {cameras_processed} câmeras"
                create_table_artifact(
                    key="deconve-person-flow-metrics",
                    table=table_data,
                    description=artifact_desc
                )
            except Exception as e:
                logger.warning(f"Erro criando artifact de tabela: {e}")

            # 15. Envia alerta de sucesso
            if send_alerts:
                try:
                    from prefect.context import get_run_context
                    try:
                        context = get_run_context()
                        job_id = str(context.flow_run.id) if hasattr(context, 'flow_run') else None
                    except Exception:
                        job_id = None

                    alert_sent = send_flow_success_alert(
                        flow_name="Extração Deconve Person Flow",
                        source="Deconve API",
                        destination=f"Snowflake - {snowflake_database}.{snowflake_schema}.{table_name}",
                        summary={
                            "records_extracted": rows_written,
                            "records_inserted": rows_inserted,
                            "records_updated": rows_updated,
                            "cameras_processed": cameras_processed,
                            "period": f"{start_date} a {end_date}"
                        },
                        duration_seconds=duration,
                        job_id=job_id,
                        group_id=alert_group_id
                    )

                    if alert_sent:
                        logger.info("✅ Alerta de sucesso enviado")
                    else:
                        logger.warning("⚠️ Falha ao enviar alerta (verifique conexão com API de mensagens)")
                except Exception as alert_error:
                    logger.warning(f"⚠️ Erro ao enviar alerta: {alert_error}")

            return {
                "status": "success",
                "records_extracted": rows_written,
                "records_inserted": rows_inserted,
                "records_updated": rows_updated,
                "cameras_processed": cameras_processed,
                "period": f"{start_date} a {end_date}",
                "duration_seconds": duration
            }

        except Exception as e:
            logger.error(f"❌ Erro: {str(e)}")

            duration = time.time() - start_time

            if send_alerts:
                try:
                    from prefect.context import get_run_context
                    try:
                        context = get_run_context()
                        job_id = str(context.flow_run.id) if hasattr(context, 'flow_run') else None
                    except Exception:
                        job_id = None

                    alert_sent = send_flow_error_alert(
                        flow_name="Extração Deconve Person Flow",
                        source="Deconve API",
                        destination=f"Snowflake - {snowflake_database or 'N/A'}",
                        error_message=str(e),
                        duration_seconds=duration,
                        job_id=job_id,
                        group_id=alert_group_id
                    )

                    if alert_sent:
                        logger.info("✅ Alerta de erro enviado")
                    else:
                        logger.warning("⚠️ Falha ao enviar alerta de erro (verifique conexão com API de mensagens)")
                except Exception as alert_error:
                    logger.warning(f"⚠️ Erro ao enviar alerta de erro: {alert_error}")

            raise
    # Ao sair do with, o diretório temporário é automaticamente excluído


# Deployment do flow
if __name__ == "__main__":
    # Execução local para teste
    # deconve_person_flow_to_snowflake()

    # Para fazer deploy:
    deconve_person_flow_to_snowflake.from_source(
        source=".",
        entrypoint="flows/deconve/deconve_person_flow_to_snowflake.py:deconve_person_flow_to_snowflake"
    ).deploy(
        name="deconve-person-flow-to-snowflake",
        work_pool_name="local-pool",
        schedules=[
            CronSchedule(cron="1 0 * * *", timezone="America/Sao_Paulo")
        ],
        tags=["deconve", "api", "snowflake", "gold", "fact"],
        parameters={
            "days_back": 3  # Processa últimos 3 dias (retroativo)
        },
        description="Pipeline: Deconve API -> Snowflake (DECONVE_PERSON_FLOW) com MERGE",
        version="1.0.0"
    )
