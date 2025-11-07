import os
import csv
import time
import tempfile
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE
from prefect.artifacts import create_table_artifact, create_markdown_artifact, create_link_artifact
from prefect.blocks.system import Secret

# Imports dos módulos de conexão
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from shared.connections.deconve import (  # noqa: E402
    authenticate_deconve, get_units, get_unit_details, get_video_details
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

# Mapeamento de shoppings para IDs e siglas
SHOPPING_MAPPING = {
    "neumarkt": {"id_shopping": 1, "ds_sigla": "NK"},
    "balneário": {"id_shopping": 2, "ds_sigla": "BS"},
    "balneario": {"id_shopping": 2, "ds_sigla": "BS"},
    "garten": {"id_shopping": 3, "ds_sigla": "GS"},
    "norte": {"id_shopping": 4, "ds_sigla": "NR"},
    "continente": {"id_shopping": 5, "ds_sigla": "CS"},
    "nações": {"id_shopping": 6, "ds_sigla": "NS"},
    "nacoes": {"id_shopping": 6, "ds_sigla": "NS"}
}


def get_shopping_info(shopping_name: str) -> Dict[str, Any]:
    """
    Retorna informações do shopping baseado no nome

    Args:
        shopping_name: Nome do shopping retornado pela API

    Returns:
        Dict com id_shopping (numérico) e ds_sigla, ou None para ambos se não encontrado
    """
    if not shopping_name:
        return {"id_shopping": None, "ds_sigla": None}

    # Normaliza o nome para busca (lowercase)
    normalized_name = shopping_name.lower()

    # Busca por match parcial no nome
    for key, info in SHOPPING_MAPPING.items():
        if key in normalized_name:
            return info

    return {"id_shopping": None, "ds_sigla": None}


@task(cache_policy=NO_CACHE)
def process_units_and_cameras(
        access_token: str,
        units_data: Dict[str, Any],
        output_path: str
) -> Dict[str, Any]:
    """
    Processa unidades e extrai informações de câmeras (dimensão)

    Este processo busca todas as unidades/shoppings e suas respectivas câmeras,
    extraindo metadados como ID, nome, localização (shopping) e outras informações
    cadastrais das câmeras instaladas.

    Args:
        access_token: Token de acesso da API Deconve
        units_data: Dados retornados pela API (items, has_more, total)
        output_path: Caminho do arquivo CSV de saída

    Returns:
        Dict com estatísticas do processamento:
        - status: Status da operação (success/no_data)
        - rows_written: Número de câmeras extraídas
        - units_processed: Número de unidades processadas
        - file_path: Caminho do arquivo CSV gerado
        - file_size_bytes: Tamanho do arquivo em bytes
    """
    logger = get_run_logger()

    try:
        items = units_data.get('items', [])
        total_units = len(items)

        if total_units == 0:
            logger.warning("⚠️ Nenhuma unidade encontrada para processar")
            return {
                "status": "no_data",
                "rows_written": 0,
                "file_path": None
            }

        # Cria diretório de saída se não existir
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Define as colunas do CSV (em UPPER CASE)
        fieldnames = ['ID_CAMERA', 'DS_CAMERA', 'ID_UNIT', 'ID_SHOPPING', 'DS_SIGLA', 'DS_SHOPPING', 'DT_CRIACAO']

        # Data/hora de criação
        dt_criacao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"💾 Processando {total_units} unidade(s) e suas câmeras...")

        total_cameras = 0

        # Escreve arquivo CSV
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Escreve cabeçalho
            writer.writeheader()

            # Processa cada unidade
            for idx, item in enumerate(items, 1):
                unit_id = item.get('id', '')
                shopping_name = item.get('name', '')
                shopping_info = get_shopping_info(shopping_name)

                if not shopping_info['id_shopping']:
                    logger.warning(f"⚠️ Shopping não mapeado: {shopping_name}")

                logger.info(f"📥 [{idx}/{total_units}] Buscando câmeras de {shopping_name}...")

                # Busca detalhes da unit (incluindo vídeos/câmeras)
                try:
                    unit_details = get_unit_details.fn(access_token, unit_id)
                    videos = unit_details.get('videos', [])

                    # Escreve uma linha para cada câmera
                    for video_idx, video in enumerate(videos, 1):
                        camera_id = video.get('id', '')
                        camera_name = ''

                        # Busca nome da câmera
                        try:
                            logger.info(f"  📹 [{video_idx}/{len(videos)}] Buscando detalhes da câmera {camera_id[:8]}...")
                            video_details = get_video_details.fn(access_token, camera_id)
                            camera_name = video_details.get('name', '')
                        except Exception as video_error:
                            logger.warning(f"  ⚠️ Erro ao buscar nome da câmera {camera_id}: {str(video_error)}")
                            # Continua mesmo se não conseguir buscar o nome

                        writer.writerow({
                            'ID_CAMERA': camera_id,
                            'DS_CAMERA': camera_name,
                            'ID_UNIT': unit_id,
                            'ID_SHOPPING': shopping_info['id_shopping'] or '',
                            'DS_SIGLA': shopping_info['ds_sigla'] or '',
                            'DS_SHOPPING': shopping_name,
                            'DT_CRIACAO': dt_criacao
                        })

                        total_cameras += 1

                except Exception as e:
                    logger.error(f"❌ Erro ao buscar câmeras da unit {unit_id}: {str(e)}")
                    # Continua processando as outras units

        file_size = os.path.getsize(output_path)
        file_size_kb = file_size / 1024

        logger.info(f"✅ Arquivo CSV criado: {output_path}")
        logger.info(f"📊 {total_cameras} câmera(s) de {total_units} unidade(s) salvas ({file_size_kb:.2f} KB)")

        return {
            "status": "success",
            "rows_written": total_cameras,
            "units_processed": total_units,
            "file_path": output_path,
            "file_size_bytes": file_size
        }

    except Exception as e:
        logger.error(f"❌ Erro ao processar unidades e câmeras: {str(e)}")
        raise


@flow(log_prints=True, name="deconve-camera-to-snowflake")
def deconve_camera_to_snowflake(
        # Deconve params
        api_key: Optional[str] = None,
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
    Flow: Deconve API -> Snowflake (DECONVE_CAMERA) - Dimensão de Câmeras

    Extrai metadados cadastrais de todas as câmeras instaladas nas unidades/shoppings
    da rede, mantendo uma tabela dimensão atualizada no Snowflake (camada GOLD).

    Estratégia anti-duplicação (MERGE/UPSERT):
    - Chave primária composta: (ID_UNIT, ID_CAMERA)
    - Se câmera já existe: ATUALIZA informações cadastrais
    - Se câmera não existe: INSERE novo registro
    - Permite reprocessamento seguro sem duplicatas

    Processo detalhado:
    1. Autentica na API Deconve usando chave privada
    2. Obtém lista completa de unidades (shoppings)
    3. Para cada unidade:
       - Busca detalhes da unidade
       - Extrai lista de câmeras (vídeos)
       - Busca nome e metadados de cada câmera
       - Mapeia shopping para ID numérico e sigla
    4. Salva dados em CSV temporário (UPPER CASE)
    5. Conecta ao Snowflake (schema GOLD)
    6. Cria tabela DECONVE_CAMERA se não existir
    7. Executa MERGE (UPSERT) do CSV no Snowflake
    8. Remove CSV temporário
    9. Envia alertas de sucesso/erro

    Frequência recomendada:
    - Diária (6h da manhã) - Captura novas câmeras e atualizações cadastrais
    """
    logger = get_run_logger()
    start_time = time.time()

    logger.info("=" * 80)
    logger.info("🚀 Iniciando flow Deconve API -> Snowflake (Dimensão de Câmeras)")
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

    # Usa diretório temporário que se auto-exclui (mesmo em caso de erro)
    with tempfile.TemporaryDirectory(prefix="deconve_camera_") as temp_dir:
        # Monta caminho completo do arquivo CSV temporário
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"camera_{timestamp}.csv"
        output_path = os.path.join(temp_dir, output_filename)

        logger.info(f"📁 Diretório temporário: {temp_dir}")
        logger.info(f"📄 Arquivo temporário: {output_filename}")

        try:
            # 2. Autentica na API
            logger.info("\n" + "=" * 80)
            logger.info("🔐 Autenticando na API Deconve...")
            logger.info("=" * 80)

            token_data = authenticate_deconve(api_key)
            access_token = token_data['access_token']

            # 3. Obtém unidades
            logger.info("\n" + "=" * 80)
            logger.info("📥 Buscando unidades (units)...")
            logger.info("=" * 80)

            units_data = get_units(access_token)

            total_units = units_data.get('total', 0)
            has_more = units_data.get('has_more', False)

            logger.info(f"📊 Total de unidades: {total_units}")
            if has_more:
                logger.warning("⚠️ Há mais unidades disponíveis (paginação necessária)")

            # 4. Processa unidades e busca câmeras, salva CSV
            logger.info("\n" + "=" * 80)
            logger.info("🎥 Processando unidades e buscando câmeras...")
            logger.info("=" * 80)

            csv_result = process_units_and_cameras(access_token, units_data, output_path)

            if csv_result.get('status') == 'no_data':
                logger.warning("⚠️ Nenhuma câmera encontrada")
                return {"status": "no_data", "message": "Nenhuma câmera encontrada"}

            rows_written = csv_result.get('rows_written', 0)
            units_processed = csv_result.get('units_processed', 0)

            # 5. Conecta Snowflake
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

            # 6. Obtém schema da tabela
            table_schema = DECONVE_TABLES_SCHEMAS["camera"]
            table_name = table_schema["table_name"]
            primary_keys = table_schema["primary_key"]

            # 7. Cria tabela se não existe
            logger.info(f"🏗️ Criando tabela {table_name} se não existir...")
            create_table_if_not_exists(
                snowflake_conn,
                table_name,
                table_schema["columns"],
                primary_keys
            )

            # 8. Faz MERGE (UPSERT) no Snowflake
            logger.info("\n" + "=" * 80)
            logger.info("🔄 Carregando dados no Snowflake (MERGE - UPSERT)...")
            logger.info("=" * 80)

            merge_result = merge_csv_to_snowflake(
                snowflake_conn,
                table_name,
                output_path,
                primary_keys,
                csv_encoding='utf-8',
                columns=['ID_CAMERA', 'DS_CAMERA', 'ID_UNIT', 'ID_SHOPPING', 'DS_SIGLA', 'DS_SHOPPING', 'DT_CRIACAO']
            )

            rows_inserted = merge_result.get('rows_inserted', 0)
            rows_updated = merge_result.get('rows_updated', 0)

            # 9. Fecha conexão Snowflake
            close_snowflake_connection(snowflake_conn)

            # 10. CSV temporário será removido automaticamente ao sair do contexto

            # 11. Resumo
            logger.info("\n" + "=" * 80)
            logger.info("📊 RESUMO - Dimensão de Câmeras")
            logger.info("=" * 80)

            logger.info(f"🎥 Total de câmeras extraídas da API: {rows_written}")
            logger.info(f"📝 Novas câmeras inseridas no Snowflake: {rows_inserted}")
            logger.info(f"🔄 Câmeras atualizadas no Snowflake: {rows_updated}")
            logger.info(f"🏬 Unidades/shoppings processados: {units_processed}")
            logger.info(f"❄️ Tabela destino: {snowflake_database}.{snowflake_schema}.{table_name}")

            duration = time.time() - start_time
            logger.info(f"⏱️ Duração total: {duration:.1f}s")
            logger.info("=" * 80 + "\n")

            # 12. ARTIFACTS: Visibilidade no Prefect UI
            try:
                # Artifact 1: Tabela de resumo
                table_data = [{
                    "Métrica": "Câmeras Extraídas",
                    "Valor": f"{rows_written:,}"
                }, {
                    "Métrica": "Novas Inseridas",
                    "Valor": f"{rows_inserted:,}"
                }, {
                    "Métrica": "Atualizadas",
                    "Valor": f"{rows_updated:,}"
                }, {
                    "Métrica": "Unidades Processadas",
                    "Valor": f"{units_processed:,}"
                }, {
                    "Métrica": "Duração",
                    "Valor": f"{duration:.1f}s"
                }]

                create_table_artifact(
                    key="deconve-camera-metrics",
                    table=table_data,
                    description="Métricas da extração de câmeras Deconve"
                )
            except Exception as e:
                logger.warning(f"Erro criando artifact de tabela: {e}")

            # Artifact 2: Markdown com resumo executivo
            try:
                markdown_content = f"""# Deconve API → Snowflake (Câmeras)

## Resumo da Execução

- **Câmeras extraídas**: {rows_written:,}
- **Novas câmeras inseridas**: {rows_inserted:,}
- **Câmeras atualizadas**: {rows_updated:,}
- **Unidades processadas**: {units_processed}
- **Duração**: {duration:.1f}s

## Detalhes

### Tabela Snowflake
- **Database**: `{snowflake_database}`
- **Schema**: `{snowflake_schema}` (GOLD layer)
- **Tabela**: `{table_name}`
- **Chave Primária**: {', '.join(primary_keys)}

### Estratégia
- **Método**: MERGE/UPSERT
- **Tipo**: Dimensão (SCD Type 1)
- **Atualização**: Diária (6h)
"""

                create_markdown_artifact(
                    key="deconve-camera-summary",
                    markdown=markdown_content,
                    description="Resumo executivo da dimensão de câmeras"
                )
            except Exception as e:
                logger.warning(f"Erro criando artifact de markdown: {e}")

            # Artifact 3: Link para Snowflake
            try:
                if snowflake_account:
                    snowflake_url = f"https://app.snowflake.com/{snowflake_account}/"

                    create_link_artifact(
                        key="deconve-camera-snowflake",
                        link=snowflake_url,
                        link_text="Abrir Snowflake Console",
                        description=f"Tabela: {snowflake_database}.{snowflake_schema}.{table_name}"
                    )
            except Exception as e:
                logger.warning(f"Erro criando artifact de link: {e}")

            # 13. Envia alerta de sucesso
            if send_alerts:
                try:
                    from prefect.context import get_run_context
                    try:
                        context = get_run_context()
                        job_id = str(context.flow_run.id) if hasattr(context, 'flow_run') else None
                    except Exception:
                        job_id = None

                    send_flow_success_alert(
                        flow_name="Extração Deconve Câmeras",
                        source="Deconve API",
                        destination=f"Snowflake - {snowflake_database}.{snowflake_schema}.{table_name}",
                        summary={
                            "records_extracted": rows_written,
                            "records_loaded": rows_inserted + rows_updated
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
                "cameras_extracted": rows_written,
                "records_inserted": rows_inserted,
                "records_updated": rows_updated,
                "units_processed": units_processed,
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

                    send_flow_error_alert(
                        flow_name="Extração Deconve Câmeras",
                        source="Deconve API",
                        destination=f"Snowflake - {snowflake_database or 'N/A'}",
                        error_message=str(e),
                        duration_seconds=duration,
                        job_id=job_id,
                        group_id=alert_group_id
                    )
                    logger.info("✅ Alerta de erro enviado")
                except Exception as alert_error:
                    logger.warning(f"⚠️ Erro ao enviar alerta de erro: {alert_error}")

            raise
    # Ao sair do with, o diretório temporário é automaticamente excluído


# Deployment do flow
if __name__ == "__main__":
    # Execução local para teste
    # deconve_camera_to_snowflake()

    # Para fazer deploy:
    deconve_camera_to_snowflake.from_source(
        source=".",
        entrypoint="flows/deconve/deconve_camera_to_snowflake.py:deconve_camera_to_snowflake"
    ).deploy(
        name="deconve-camera-to-snowflake",
        work_pool_name="local-pool",
        # Executa diariamente às 6h da manhã
        cron="0 6 * * *",
        tags=["deconve", "api", "snowflake", "gold", "dimension"],
        description="Pipeline: Deconve API -> Snowflake - Dimensão de Câmeras (DECONVE_CAMERA) | Extrai metadados cadastrais de câmeras instaladas",
        version="1.0.0"
    )
