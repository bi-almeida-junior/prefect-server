import os
import re
import time
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE
from prefect.artifacts import create_table_artifact

# Imports dos módulos de conexão
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.connections.sftp import (  # noqa: E402
    connect_sftp, get_latest_file, download_csv_from_sftp,
    normalize_csv_header, close_sftp_connection
)
from shared.connections.snowflake import (  # noqa: E402
    connect_snowflake, create_table_if_not_exists,
    insert_csv_file_replace, close_snowflake_connection,
    SALESFORCE_TABLES_SCHEMAS
)
from shared.alerts import (  # noqa: E402
    send_flow_success_alert, send_flow_error_alert
)

# Carrega variáveis de ambiente
load_dotenv()

# Configurações dos streams Salesforce
SALESFORCE_STREAMS = {
    "resubscribes": {
        "folder": "RESUBSCRIBES",
        "description": "Leads que voltaram a se inscrever"
    },
    "subscribers": {
        "folder": "SUBSCRIBERS",
        "description": "Novos assinantes"
    },
    "unsubscribes": {
        "folder": "UNSUBSCRIBES",
        "description": "Usuários que cancelaram inscrição"
    }
}


def normalize_column_name(text: str) -> str:
    """
    Converte CamelCase para snake_case

    Exemplos:
        SubscriberID -> subscriber_id
        DateUndeliverable -> date_undeliverable
        EmailAddress -> email_address
    """
    # Adiciona underscore antes de letras maiúsculas
    text = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
    text = re.sub('([a-z0-9])([A-Z])', r'\1_\2', text)
    return text.lower()


@task(cache_policy=NO_CACHE)
def process_sftp(
        sftp_client,
        snowflake_conn,
        stream_name: str,
        base_path: str
) -> Dict[str, Any]:
    """
    Processa stream do Salesforce SFTP e carrega no Snowflake

    Processo:
    1. Baixa CSV do SFTP (sem carregar em memória)
    2. Normaliza apenas o cabeçalho (CamelCase -> snake_case)
    3. PUT + COPY INTO direto para Snowflake
    """
    logger = get_run_logger()

    stream_config = SALESFORCE_STREAMS[stream_name]
    remote_folder = f"{base_path}/{stream_config['folder']}"

    logger.info(f"📂 Processando stream: {stream_name} ({stream_config['description']})")

    # 1. Obtém arquivo mais recente
    file_info = get_latest_file(sftp_client, remote_folder)

    if not file_info:
        logger.warning(f"⚠️ Nenhum arquivo encontrado para {stream_name}")
        return {
            "stream_name": stream_name,
            "status": "no_data",
            "rows_loaded": 0,
            "bytes_processed": 0
        }

    # 2. Baixa CSV (sem carregar em memória)
    csv_info = download_csv_from_sftp(sftp_client, file_info["full_path"])

    if csv_info["num_records"] == 0:
        logger.warning(
            f"⚠️ Arquivo vazio: {stream_name} "
            f"(fonte: {file_info['filename']}, modificado: {file_info['modified_datetime']})"
        )
        return {
            "stream_name": stream_name,
            "status": "empty",
            "rows_loaded": 0,
            "bytes_processed": 0
        }

    # 3. Cria mapeamento de colunas (CamelCase -> snake_case)
    import csv
    with open(csv_info["file_path"], 'r', encoding=csv_info["encoding"]) as f:
        reader = csv.reader(f)
        original_columns = next(reader)

    column_mapping = {}
    for col in original_columns:
        if col:
            column_mapping[col] = normalize_column_name(col)

    normalized_columns = list(column_mapping.values())

    logger.info(f"📝 Mapeamento: {len(column_mapping)} colunas")

    # 4. Normaliza cabeçalho do CSV
    logger.info("🔄 Normalizando cabeçalho para snake_case...")
    normalized_csv_path = normalize_csv_header(
        csv_info["file_path"],
        csv_info["encoding"],
        column_mapping
    )

    # 5. Obtém schema da tabela Snowflake
    table_schema = SALESFORCE_TABLES_SCHEMAS[stream_name]
    table_name = table_schema["table_name"]

    # 6. Cria tabela se não existe
    create_table_if_not_exists(
        snowflake_conn,
        table_name,
        table_schema["columns"],
        table_schema["primary_key"]
    )

    # 7. Carrega CSV direto no Snowflake (PUT + COPY INTO)
    logger.info(f"⚡ Carregando {csv_info['num_records']} registros em {table_name}...")

    result = insert_csv_file_replace(
        snowflake_conn,
        table_name,
        normalized_csv_path,
        csv_encoding='utf-8',  # Sempre UTF-8 após normalização
        columns=normalized_columns
    )

    logger.info(
        f"✅ {stream_name}: {result['rows_inserted']} registros carregados "
        f"de {file_info['filename']}"
    )

    # 8. Captura tamanho do arquivo processado
    file_size = os.path.getsize(normalized_csv_path)

    # 9. Limpa arquivo temporário
    try:
        os.unlink(normalized_csv_path)
    except OSError:
        pass

    return {
        "stream_name": stream_name,
        "table_name": table_name,
        "source_file": file_info["filename"],
        "rows_loaded": result['rows_inserted'],
        "bytes_processed": file_size,
        "status": "success"
    }


@flow(log_prints=True, name="salesforce-sftp-to-snowflake")
def salesforce_to_snowflake(  # noqa: C901
        streams_to_process: Optional[list[str]] = None,
        sftp_host: Optional[str] = None,
        sftp_username: Optional[str] = None,
        sftp_private_key: Optional[str] = None,
        sftp_port: int = 22,
        sftp_base_path: str = "Import",
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
    Flow: Salesforce SFTP -> Snowflake

    Estratégia:
    - Baixa CSV sem carregar em memória
    - Normaliza apenas cabeçalho (instantâneo)
    - PUT + COPY INTO bulk load (paralelo)
    - Envia alertas de sucesso/erro
    """
    logger = get_run_logger()
    start_time = time.time()

    logger.info("=" * 80)
    logger.info("🚀 Iniciando flow Salesforce SFTP -> Snowflake")
    logger.info("=" * 80)

    # 1. Carrega configurações
    sftp_host = sftp_host or os.getenv("SFTP_HOST")
    sftp_username = sftp_username or os.getenv("SFTP_USERNAME")
    sftp_private_key = sftp_private_key or os.getenv("SFTP_PRIVATE_KEY")
    sftp_base_path = sftp_base_path or os.getenv("SFTP_BASE_PATH", "Import")

    snowflake_account = snowflake_account or os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user = snowflake_user or os.getenv("SNOWFLAKE_USER")
    snowflake_private_key = snowflake_private_key or os.getenv("SNOWFLAKE_PRIVATE_KEY")
    snowflake_private_key_passphrase = snowflake_private_key_passphrase or os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
    snowflake_warehouse = snowflake_warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_database = snowflake_database or os.getenv("SNOWFLAKE_DATABASE")
    snowflake_schema = snowflake_schema or os.getenv("SNOWFLAKE_SCHEMA", "BRONZE")
    snowflake_role = snowflake_role or os.getenv("SNOWFLAKE_ROLE")

    # Valida configurações
    if not all([sftp_host, sftp_username, sftp_private_key]):
        raise ValueError("Configure SFTP_HOST, SFTP_USERNAME e SFTP_PRIVATE_KEY")

    if not all([snowflake_account, snowflake_user, snowflake_private_key,
                snowflake_warehouse, snowflake_database]):
        raise ValueError("Configure credenciais Snowflake no .env")

    # Determina streams
    if not streams_to_process:
        streams_to_process = list(SALESFORCE_STREAMS.keys())

    logger.info(f"📊 Streams: {', '.join(streams_to_process)}")

    try:
        # 2. Conecta SFTP
        logger.info(f"🔌 Conectando SFTP: {sftp_host}")
        sftp_client, ssh_client = connect_sftp(
            host=sftp_host,
            username=sftp_username,
            private_key=sftp_private_key,
            port=sftp_port,
            timeout=30
        )

        # 3. Conecta Snowflake
        logger.info(f"❄️ Conectando Snowflake: {snowflake_account}")
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

        # 4. Processa cada stream
        results = []

        for stream_name in streams_to_process:
            logger.info(f"\n{'=' * 80}")
            logger.info(f"📥 Stream: {stream_name}")
            logger.info(f"{'=' * 80}")

            stream_result = process_sftp(
                sftp_client,
                snowflake_conn,
                stream_name,
                sftp_base_path
            )

            results.append(stream_result)

        # 5. Fecha conexões
        close_sftp_connection(sftp_client, ssh_client)
        close_snowflake_connection(snowflake_conn)

        # 6. Resumo
        logger.info(f"\n{'=' * 80}")
        logger.info("📊 RESUMO")
        logger.info(f"{'=' * 80}")

        total_rows = sum(r.get("rows_loaded", 0) for r in results)
        total_bytes = sum(r.get("bytes_processed", 0) for r in results)

        for r in results:
            status = "✅" if r["status"] == "success" else "⚠️"
            rows = r.get("rows_loaded", 0)
            bytes_mb = r.get("bytes_processed", 0) / (1024 * 1024)
            logger.info(f"{status} {r['stream_name']}: {rows:_} registros ({bytes_mb:.1f} MB)".replace('_', '.'))

        total_mb = total_bytes / (1024 * 1024)
        duration = time.time() - start_time
        records_per_sec = total_rows / duration if duration > 0 else 0

        logger.info(f"\n📈 TOTAL: {total_rows:_} registros ({total_mb:.1f} MB)".replace('_', '.'))
        logger.info(f"⚡ Performance: {records_per_sec:_.0f} registros/seg".replace('_', '.'))
        logger.info(f"⏱️ Duração: {duration:.1f}s")
        logger.info(f"{'=' * 80}\n")

        # 7. ARTIFACTS: Visibilidade no Prefect UI
        try:
            # Tabela de resumo por stream
            table_data = []
            for r in results:
                status_text = {
                    "success": "✅ Sucesso",
                    "no_data": "⚠️ Sem dados",
                    "empty": "⚠️ Vazio"
                }.get(r["status"], "❌ Erro")

                table_data.append({
                    "Stream": r["stream_name"],
                    "Status": status_text,
                    "Registros": f"{r.get('rows_loaded', 0):,}",
                    "Tamanho (MB)": f"{r.get('bytes_processed', 0) / (1024 * 1024):.2f}",
                    "Tabela Snowflake": r.get("table_name", "N/A"),
                    "Arquivo Fonte": r.get("source_file", "N/A")
                })

            create_table_artifact(
                key="salesforce-stream-results",
                table=table_data,
                description=f"Processamento de {len(results)} streams do Salesforce"
            )
        except Exception as e:
            logger.warning(f"Erro criando artifact de tabela: {e}")

        # 8. Calcula duração e envia alerta de sucesso
        duration = time.time() - start_time

        if send_alerts:
            try:
                # Obtém job_id do contexto do Prefect se disponível
                from prefect.context import get_run_context
                try:
                    context = get_run_context()
                    job_id = str(context.flow_run.id) if hasattr(context, 'flow_run') else None
                except Exception:
                    job_id = None

                send_flow_success_alert(
                    flow_name="Sincronização Salesforce",
                    source="Salesforce SFTP",
                    destination=f"Snowflake - {snowflake_database}",
                    summary={
                        "records_extracted": total_rows,
                        "records_loaded": total_rows,
                        "bytes_processed": total_bytes,
                        "streams_processed": len(results)
                    },
                    duration_seconds=duration,
                    job_id=job_id,
                    group_id=alert_group_id
                )
                logger.info("✅ Alerta de sucesso enviado")
            except Exception as alert_error:
                logger.warning(f"⚠️ Erro ao enviar alerta: {alert_error}")

        return {
            "status": "success",
            "streams_processed": len(results),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "duration_seconds": duration,
            "results": results
        }

    except Exception as e:
        logger.error(f"❌ Erro: {str(e)}")

        # Calcula duração até o erro e envia alerta
        duration = time.time() - start_time

        if send_alerts:
            try:
                # Obtém job_id do contexto do Prefect se disponível
                from prefect.context import get_run_context
                try:
                    context = get_run_context()
                    job_id = str(context.flow_run.id) if hasattr(context, 'flow_run') else None
                except Exception:
                    job_id = None

                # Tenta coletar métricas parciais se houver
                partial_summary = None
                try:
                    if 'results' in locals():
                        partial_rows = sum(r.get("rows_loaded", 0) for r in results)
                        partial_bytes = sum(r.get("bytes_processed", 0) for r in results)
                        partial_summary = {
                            "records_extracted": partial_rows,
                            "records_loaded": partial_rows,
                            "bytes_processed": partial_bytes
                        }
                except Exception:
                    pass

                send_flow_error_alert(
                    flow_name="Sincronização Salesforce",
                    source="Salesforce SFTP",
                    destination=f"Snowflake - {snowflake_database or 'N/A'}",
                    error_message=str(e),
                    duration_seconds=duration,
                    job_id=job_id,
                    partial_summary=partial_summary,
                    group_id=alert_group_id
                )
                logger.info("✅ Alerta de erro enviado")
            except Exception as alert_error:
                logger.warning(f"⚠️ Erro ao enviar alerta de erro: {alert_error}")

        raise


# Deployment do flow
if __name__ == "__main__":
    # Execução local
    # salesforce_to_snowflake()

    # Para fazer deploy:
    salesforce_to_snowflake.from_source(
        source=".",
        entrypoint="flows/salesforce/salesforce_to_snowflake.py:salesforce_to_snowflake"
    ).deploy(
        name="salesforce-sftp-to-snowflake",
        work_pool_name="local-pool",
        # Executa diariamente às 4h da manhã
        cron="0 4 * * *",
        tags=["salesforce", "sftp", "snowflake", "bronze", "dimension"],
        parameters={
            "sftp_base_path": "Import"
        },
        description="Pipeline: Salesforce SFTP -> Snowflake (COPY INTO bulk load)",
        version="1.0.0"
    )
