import sys
import os
import re
import io
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.cache_policies import NONE
from prefect.artifacts import create_table_artifact
from prefect.client.schemas.schedules import CronSchedule

# Imports das conexões compartilhadas
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from shared.connections.s3 import connect_s3, read_csv_from_s3, get_file_metadata
from shared.connections.snowflake import connect_snowflake, close_snowflake_connection
from shared.alerts import send_flow_success_alert, send_flow_error_alert

# Carrega variáveis de ambiente
load_dotenv()

# ====== CONFIGURAÇÕES ======
CHUNK_SIZE = 50
CREATE_OR_REPLACE = False
WINDOW_DAYS = 15

# Pattern para extrair metadados do nome do arquivo
PAT_NAME = re.compile(
    r'^(?P<category>.+?)_'
    r'(?P<start>\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})_'
    r'(?P<end>\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})'
    r'(?:\.csv)?$'
)


def extract_category_from_filename(filename: str) -> str:
    """Extrai categoria do nome do arquivo."""
    basename = filename.split('/')[-1]
    match = re.match(r'^(.+?)_(\d{4}_\d{2}_\d{2})', basename)
    if match:
        return match.group(1)
    parts = basename.replace('.csv', '').split('_')
    if len(parts) >= 2:
        return '_'.join(parts[:2])
    return 'outros'


def parse_name_meta(s3_key: str):
    """Extrai categoria, start_ts, end_ts e filename via nome."""
    base = s3_key.split('/')[-1]
    m = PAT_NAME.match(base)
    if not m:
        return None

    parse = lambda s: datetime.strptime(s, "%Y_%m_%d_%H_%M_%S").replace(tzinfo=timezone.utc)
    return {
        "category": m["category"],
        "file_start_ts": parse(m["start"]),
        "file_end_ts": parse(m["end"]),
        "filename": base,
    }


@task(name="list_s3_files", retries=3, retry_delay_seconds=10, log_prints=True, cache_policy=NONE)
def list_all_files_by_category(s3_client, bucket, prefix):
    """Lista todos os CSVs do S3 e agrupa por categoria."""
    logger = get_run_logger()
    logger.info(f"Listando arquivos: s3://{bucket}/{prefix}")

    files_by_category = defaultdict(list)
    paginator = s3_client.get_paginator('list_objects_v2')

    total_files = 0
    for page_idx, page in enumerate(paginator.paginate(Bucket=bucket, Prefix=prefix), start=1):
        page_files = 0
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('/') and key.endswith('.csv'):
                category = extract_category_from_filename(key)
                files_by_category[category].append(key)
                page_files += 1
                total_files += 1
        logger.info(f"Página {page_idx}: +{page_files} CSVs (acumulado: {total_files})")

    logger.info(f"Total CSVs: {total_files} | Categorias: {len(files_by_category)}")
    return files_by_category


@task(name="create_manifest_table", log_prints=True, cache_policy=NONE)
def create_manifest_table(conn, database, schema):
    """Garante a existência da tabela de manifesto."""
    logger = get_run_logger()
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema};")
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.BESISTEMAS_ARQUIVOS_COLETADOS (
          CATEGORIA       STRING,
          S3_KEY          STRING,
          FILENAME        STRING,
          ETAG            STRING,
          FILE_START_TS   TIMESTAMP_NTZ,
          FILE_END_TS     TIMESTAMP_NTZ,
          LAST_MODIFIED   TIMESTAMP_NTZ,
          ROWS_READ       NUMBER,
          STATUS          STRING,
          PROCESSED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
          PRIMARY KEY (CATEGORIA, S3_KEY)
        );
        """)
        logger.info("✓ Tabela de manifesto garantida")
    finally:
        cur.close()


def upsert_manifest_row(conn, database, schema, row):
    """Insere ou atualiza registro no manifesto."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
        MERGE INTO {database}.{schema}.BESISTEMAS_ARQUIVOS_COLETADOS T
        USING (
          SELECT %s AS CATEGORIA,
                 %s AS S3_KEY,
                 %s AS FILENAME,
                 %s AS ETAG,
                 %s::TIMESTAMP_NTZ AS FILE_START_TS,
                 %s::TIMESTAMP_NTZ AS FILE_END_TS,
                 %s::TIMESTAMP_NTZ AS LAST_MODIFIED,
                 %s::NUMBER        AS ROWS_READ,
                 %s AS STATUS
        ) S
        ON T.CATEGORIA=S.CATEGORIA AND T.S3_KEY=S.S3_KEY
        WHEN MATCHED THEN UPDATE SET
          T.FILENAME      = S.FILENAME,
          T.ETAG          = S.ETAG,
          T.FILE_START_TS = S.FILE_START_TS,
          T.FILE_END_TS   = S.FILE_END_TS,
          T.LAST_MODIFIED = S.LAST_MODIFIED,
          T.ROWS_READ     = S.ROWS_READ,
          T.STATUS        = S.STATUS,
          T.PROCESSED_AT  = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
          (CATEGORIA,S3_KEY,FILENAME,ETAG,FILE_START_TS,FILE_END_TS,LAST_MODIFIED,ROWS_READ,STATUS)
        VALUES
          (S.CATEGORIA,S.S3_KEY,S.FILENAME,S.ETAG,S.FILE_START_TS,S.FILE_END_TS,S.LAST_MODIFIED,S.ROWS_READ,S.STATUS);
        """, (
            row['categoria'],
            row['s3_key'],
            row['filename'],
            row.get('etag'),
            row.get('file_start_ts'),
            row.get('file_end_ts'),
            row.get('last_modified'),
            int(row.get('rows_read', 0) or 0),
            row.get('status', 'success')
        ))
    finally:
        cur.close()


def create_or_replace_table(conn, database, schema, table_name, sample_df):
    """CREATE OR REPLACE tabela."""
    cursor = conn.cursor()
    try:
        columns_def = []
        for col in sample_df.columns:
            col_name = col.upper().replace(' ', '_').replace('-', '_').replace('.', '_')
            columns_def.append(f'"{col_name}" VARCHAR')

        sql = f"""
        CREATE OR REPLACE TABLE {database}.{schema}.{table_name} (
            {', '.join(columns_def)}
        )
        """
        cursor.execute(sql)
    finally:
        cursor.close()


def create_table_if_not_exists(conn, database, schema, table_name, sample_df):
    """Cria tabela se não existir (APPEND)."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{table_name}'
        """)
        exists = cursor.fetchone()[0] > 0

        if exists:
            return

        columns_def = []
        for col in sample_df.columns:
            col_name = col.upper().replace(' ', '_').replace('-', '_').replace('.', '_')
            columns_def.append(f'"{col_name}" VARCHAR')

        sql = f"""
        CREATE TABLE {database}.{schema}.{table_name} (
            {', '.join(columns_def)}
        )
        """
        cursor.execute(sql)
    finally:
        cursor.close()


def load_chunk_to_snowflake(conn, database, schema, table_name, df) -> int:
    """Carrega um chunk via PUT + COPY."""
    cursor = conn.cursor()
    temp_file_path = None
    try:
        df.columns = [c.upper().replace(' ', '_').replace('-', '_').replace('.', '_') for c in df.columns]

        stage_name = f"TEMP_STAGE_{table_name}"
        cursor.execute(f"CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name}")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8', sep='|', quoting=1, lineterminator='\n')
        csv_buffer.seek(0)
        csv_content = csv_buffer.getvalue()

        # Usa diretório temporário do sistema (Linux: /tmp, Windows: C:\Temp)
        import tempfile
        temp_dir = tempfile.gettempdir()

        temp_file_path = os.path.join(temp_dir, f"snowflake_{table_name}_chunk.csv")
        with open(temp_file_path, 'w', encoding='utf-8', newline='') as f:
            f.write(csv_content)

        put_file_path = temp_file_path.replace('\\', '/')
        cursor.execute(f"PUT file://{put_file_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

        copy_sql = f"""
        COPY INTO {database}.{schema}.{table_name}
        FROM @{stage_name}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = '|'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            ENCODING = 'UTF8'
        )
        ON_ERROR = 'CONTINUE'
        """
        cursor.execute(copy_sql)
        copy_data = cursor.fetchall()

        rows_loaded = 0
        if copy_data and len(copy_data[0]) > 3:
            rows_loaded = copy_data[0][3]

        return rows_loaded

    finally:
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
        except Exception:
            pass
        cursor.close()


def get_file_end_ts_or_last_modified(s3_client, bucket, key):
    """Retorna file_end_ts via nome; se não parsear, usa LastModified do S3."""
    meta = parse_name_meta(key)
    if meta and meta.get("file_end_ts"):
        return meta["file_end_ts"]

    metadata = get_file_metadata(s3_client, bucket, key)
    return metadata.get('last_modified')


@task(name="load_manifest_data", log_prints=True, cache_policy=NONE)
def load_manifest_recent_ok_retry(conn, database, schema, category, window_days):
    """Carrega do manifesto (janela) os conjuntos ok e retry."""
    logger = get_run_logger()
    cur = conn.cursor()
    try:
        cur.execute(f"""
        SELECT S3_KEY, STATUS, ETAG
        FROM {database}.{schema}.BESISTEMAS_ARQUIVOS_COLETADOS
        WHERE CATEGORIA = %s
          AND (
                FILE_END_TS   >= DATEADD(day, -{window_days}, CURRENT_DATE())
             OR LAST_MODIFIED >= DATEADD(day, -{window_days}, CURRENT_DATE())
          );
        """, (category,))
        ok = set()
        retry = set()
        rows = cur.fetchall()
        for s3_key, status, etag in rows:
            if status and status.lower() == 'success':
                ok.add(s3_key)
            else:
                retry.add(s3_key)
        logger.info(f"{category}: ok={len(ok)} retry={len(retry)} (janela {window_days}d)")
        return ok, retry
    finally:
        cur.close()


@task(name="filter_recent_files", log_prints=True, cache_policy=NONE)
def filter_candidates_last_n_days(s3_client, bucket, keys, window_days):
    """Filtra keys pela janela móvel usando file_end_ts ou LastModified."""
    logger = get_run_logger()
    threshold = datetime.now(timezone.utc) - timedelta(days=window_days)
    recent = []
    miss = 0
    for k in keys:
        try:
            end_ts = get_file_end_ts_or_last_modified(s3_client, bucket, k)
            if isinstance(end_ts, datetime) and end_ts >= threshold:
                recent.append(k)
        except Exception as e:
            miss += 1
            logger.warning(f"Falha ao obter end_ts de {k}: {e}")
    logger.info(f"Candidatos: {len(keys)} | Recentes(>= {window_days}d): {len(recent)} | Miss: {miss}")
    return recent


def compute_todo_from_manifest(recent_keys, ok_set, retry_set):
    """TODO = (recentes - ok) ∪ (recentes ∩ retry)"""
    set_recent = set(recent_keys)
    todo = (set_recent - ok_set) | (set_recent & retry_set)
    todo_list = sorted(todo)
    return todo_list


@task(name="process_category", retries=2, retry_delay_seconds=30, log_prints=True, timeout_seconds=7200, cache_policy=NONE)
def process_category_chunked(s3_client, conn, database, schema, bucket, category, file_keys):
    """Processa uma categoria em chunks e registra manifesto por arquivo."""
    logger = get_run_logger()
    category_start_time = datetime.now()

    logger.info(f"Processando categoria: {category} | arquivos: {len(file_keys)}")

    if not file_keys:
        logger.info("Nada a processar. Pulando.")
        return 0

    table_name = f"BRZ_BESISTEMAS_{category}".upper().replace('-', '_')

    # Pré-criação/verificação de tabela com um sample
    logger.info("Verificando/criando tabela de destino...")
    sample_df = None
    for sample_key in file_keys:
        try:
            sample_df = read_csv_from_s3(s3_client, bucket, sample_key)
            if len(sample_df) > 0:
                if CREATE_OR_REPLACE:
                    create_or_replace_table(conn, database, schema, table_name, sample_df)
                else:
                    create_table_if_not_exists(conn, database, schema, table_name, sample_df)
                break
        except Exception as e:
            logger.warning(f"Sample falhou para {sample_key}: {e}")
            continue

    if sample_df is None or len(sample_df) == 0:
        logger.error("Nenhum arquivo válido para definir schema. Pulando categoria.")
        return 0

    total_rows_loaded = 0

    # Chunks
    for chunk_start in range(0, len(file_keys), CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, len(file_keys))
        chunk_files = file_keys[chunk_start:chunk_end]

        logger.info(f"CHUNK {chunk_start // CHUNK_SIZE + 1}: Arquivos {chunk_start + 1}-{chunk_end}/{len(file_keys)}")

        dfs = []
        manifest_batch = []

        for i, key in enumerate(chunk_files, 1):
            try:
                if i == 1 or i == len(chunk_files) or i % 10 == 0:
                    logger.info(f"Lendo {i}/{len(chunk_files)}: {key}")

                # Metadados S3
                metadata = get_file_metadata(s3_client, bucket, key)
                etag = metadata.get('etag')
                last_modified = metadata.get('last_modified')

                # Meta do nome
                name_meta = parse_name_meta(key)
                if name_meta is None:
                    base = key.split('/')[-1]
                    name_meta = {
                        "category": category,
                        "file_start_ts": None,
                        "file_end_ts": last_modified,
                        "filename": base
                    }

                df = read_csv_from_s3(s3_client, bucket, key)
                rows_read = int(len(df))

                manifest_batch.append({
                    'categoria': category,
                    's3_key': key,
                    'filename': name_meta['filename'],
                    'etag': etag,
                    'file_start_ts': name_meta['file_start_ts'],
                    'file_end_ts': name_meta['file_end_ts'],
                    'last_modified': last_modified,
                    'rows_read': rows_read,
                    'status': 'success'
                })

                if rows_read > 0:
                    dfs.append(df)
                else:
                    logger.warning(f"{key} sem linhas úteis")

            except Exception as e:
                logger.error(f"Falha lendo {key}: {e}")
                try:
                    metadata = get_file_metadata(s3_client, bucket, key)
                    etag = metadata.get('etag')
                    last_modified = metadata.get('last_modified')
                except Exception:
                    etag, last_modified = None, None
                nm = parse_name_meta(key) or {'filename': key.split('/')[-1], 'file_start_ts': None, 'file_end_ts': None}
                upsert_manifest_row(conn, database, schema, {
                    'categoria': category,
                    's3_key': key,
                    'filename': nm['filename'],
                    'etag': etag,
                    'file_start_ts': nm.get('file_start_ts'),
                    'file_end_ts': nm.get('file_end_ts'),
                    'last_modified': last_modified,
                    'rows_read': 0,
                    'status': 'failed'
                })

        if not dfs:
            logger.warning("Sem DataFrames válidos. Pulando COPY.")
            for rec in manifest_batch:
                if rec['rows_read'] == 0:
                    try:
                        upsert_manifest_row(conn, database, schema, rec)
                    except Exception as e:
                        logger.warning(f"Manifesto não atualizado para {rec['filename']}: {e}")
            continue

        logger.info(f"Concatenando {len(dfs)} DF(s)...")
        chunk_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Linhas={len(chunk_df):,} Cols={len(chunk_df.columns)}")

        logger.info("Enviando para Snowflake...")
        rows = load_chunk_to_snowflake(conn, database, schema, table_name, chunk_df)
        total_rows_loaded += rows
        logger.info(f"✓ Linhas carregadas: {rows:,} | Total acumulado: {total_rows_loaded:,}")

        # Atualiza manifesto
        for rec in manifest_batch:
            try:
                upsert_manifest_row(conn, database, schema, rec)
            except Exception as e:
                logger.warning(f"Manifesto não atualizado para {rec['filename']}: {e}")

        del dfs
        del chunk_df

    duration = datetime.now() - category_start_time
    m, s = divmod(duration.total_seconds(), 60)
    logger.info(f"✓ {category} concluído. Linhas: {total_rows_loaded:,} | Tempo: {int(m)}m {int(s)}s")
    return total_rows_loaded


@flow(name="besistemas_to_snowflake", log_prints=True)
def besistemas_to_snowflake(
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        s3_path: Optional[str] = None,
        snowflake_account: Optional[str] = None,
        snowflake_user: Optional[str] = None,
        snowflake_private_key: Optional[str] = None,
        snowflake_warehouse: Optional[str] = None,
        snowflake_database: Optional[str] = None,
        snowflake_schema: Optional[str] = None,
        snowflake_role: Optional[str] = None,
        window_days: int = WINDOW_DAYS
):
    """
    Flow principal: S3 → Snowflake incremental com controle de manifesto.

    Args:
        aws_access_key_id: AWS Access Key (padrão: .env)
        aws_secret_access_key: AWS Secret Key (padrão: .env)
        aws_region: Região AWS (padrão: .env ou sa-east-1)
        s3_bucket: Nome do bucket S3 (padrão: .env)
        s3_path: Prefixo/pasta no S3 (padrão: .env)
        snowflake_account: Conta Snowflake (padrão: .env)
        snowflake_user: Usuário Snowflake (padrão: .env)
        snowflake_private_key: Chave privada Snowflake (padrão: .env)
        snowflake_warehouse: Warehouse Snowflake (padrão: .env)
        snowflake_database: Database Snowflake (padrão: .env)
        snowflake_schema: Schema Snowflake (padrão: .env)
        snowflake_role: Role Snowflake (padrão: .env)
        window_days: Janela de dias para processar (padrão: 15)
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info(f"BESISTEMAS: S3 → SNOWFLAKE | MODO: {'CREATE OR REPLACE' if CREATE_OR_REPLACE else 'APPEND'} | JANELA: {window_days}d")
    logger.info("=" * 80)

    # Carrega configurações do ambiente
    aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = aws_region or os.getenv("AWS_REGION", "sa-east-1")
    s3_bucket = s3_bucket or os.getenv("BESISTEMAS_S3_BUCKET", "bemall-storage-private")
    s3_path = s3_path or os.getenv("BESISTEMAS_S3_PATH", "ajcorp/export-content/")

    snowflake_account = snowflake_account or os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user = snowflake_user or os.getenv("SNOWFLAKE_USER")
    snowflake_private_key = snowflake_private_key or os.getenv("SNOWFLAKE_PRIVATE_KEY")
    snowflake_warehouse = snowflake_warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_database = snowflake_database or os.getenv("SNOWFLAKE_DATABASE")
    snowflake_schema = snowflake_schema or os.getenv("SNOWFLAKE_SCHEMA")
    snowflake_role = snowflake_role or os.getenv("SNOWFLAKE_ROLE")

    try:
        # Cliente S3
        logger.info("Criando cliente S3...")
        s3 = connect_s3(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        logger.info("✓ S3 pronto")

        # Listagem
        files_by_category = list_all_files_by_category(s3, s3_bucket, s3_path)

        if not files_by_category:
            logger.info("Nenhum arquivo encontrado. Encerrando.")
            return

        # Conexão Snowflake
        conn = connect_snowflake(
            account=snowflake_account,
            user=snowflake_user,
            private_key=snowflake_private_key,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema,
            role=snowflake_role
        )

        # Manifesto
        create_manifest_table(conn, snowflake_database, snowflake_schema)

        results = {}

        for category, all_keys in sorted(files_by_category.items()):
            logger.info(f"--- Categoria: {category} | total keys={len(all_keys)} ---")

            # 1) Candidatos na janela
            recent_keys = filter_candidates_last_n_days(s3, s3_bucket, all_keys, window_days)

            # 2) Manifesto: ok e retry
            ok_set, retry_set = load_manifest_recent_ok_retry(conn, snowflake_database, snowflake_schema, category, window_days)

            # 3) TODO
            todo_keys = compute_todo_from_manifest(recent_keys, ok_set, retry_set)
            logger.info(f"TODO para {category}: {len(todo_keys)} arquivos")

            # 4) Processa
            try:
                rows = process_category_chunked(s3, conn, snowflake_database, snowflake_schema, s3_bucket, category, todo_keys)
                results[category] = {'status': 'success', 'rows': rows, 'todo': len(todo_keys)}
            except Exception as e:
                logger.error(f"Categoria {category}: {e}")
                results[category] = {'status': 'failed', 'error': str(e), 'todo': len(todo_keys)}
                import traceback
                traceback.print_exc()

        close_snowflake_connection(conn)

        # Resumo
        end_time = datetime.now()
        elapsed = end_time - start_time
        h, rem = divmod(elapsed.total_seconds(), 3600)
        m, s = divmod(rem, 60)

        logger.info("=" * 80)
        logger.info("PROCESSO CONCLUÍDO")
        logger.info("=" * 80)
        logger.info(f"Database: {snowflake_database}")
        logger.info(f"Schema:   {snowflake_schema}")
        logger.info(f"Início:   {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Fim:      {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duração:  {int(h)}h {int(m)}m {int(s)}s")

        succ = [k for k, v in results.items() if v['status'] == 'success']
        fail = [k for k, v in results.items() if v['status'] == 'failed']

        logger.info(f"✅ Categorias com sucesso: {len(succ)}")
        total_rows = sum(results[k]['rows'] for k in succ)
        for k in sorted(succ):
            logger.info(f"  • {k}: TODO={results[k]['todo']} | Linhas={results[k]['rows']:,}")

        if fail:
            logger.info(f"❌ Categorias com falha: {len(fail)}")
            for k in fail:
                logger.info(f"  • {k}: TODO={results[k]['todo']} | Erro={results[k]['error'][:120]}")

        # ====== ARTIFACTS: Visibilidade no Prefect UI ======

        # Tabela de resumo por categoria
        try:
            table_data = []
            for category in sorted(results.keys()):
                r = results[category]

                if r['status'] == 'success':
                    rows = r.get('rows', 0)
                    status = "✅ Sucesso" if rows > 0 else "⚠️ Vazio"
                    linhas = f"{rows:,}"
                    erro = "-"
                else:
                    status = "❌ Falha"
                    linhas = "N/A"
                    erro = r.get('error', 'Erro desconhecido')[:100]

                table_data.append({
                    "Categoria": category,
                    "Status": status,
                    "Arquivos": r['todo'],
                    "Linhas": linhas,
                    "Erro": erro,
                    "Tabela": f"BRZ_BESISTEMAS_{category.upper().replace('-', '_')}" if r['status'] == 'success' else "N/A"
                })

            artifact_desc = f"✅ {len(succ)} sucesso | ❌ {len(fail)} falhas | Total: {len(results)}"
            create_table_artifact(
                key="besistemas-category-results",
                table=table_data,
                description=artifact_desc
            )
        except Exception as e:
            logger.warning(f"Erro criando artifact de tabela: {e}")

        # Se houver falhas, envia alerta de erro e levanta exceção
        if fail:
            logger.error(f"❌ Flow com falhas: {len(fail)}/{len(results)} categorias falharam")

            try:
                send_flow_error_alert(
                    flow_name="Besistemas",
                    source="S3",
                    destination="Snowflake",
                    error_message=f"{len(fail)} categorias falharam: {', '.join(fail[:5])}",
                    duration_seconds=elapsed.total_seconds(),
                    partial_summary={
                        "records_loaded": total_rows,
                        "categories_success": len(succ),
                        "categories_failed": len(fail)
                    }
                )
            except Exception as e:
                logger.warning(f"Falha ao enviar alerta de erro: {e}")

            raise Exception(f"Flow falhou: {len(fail)} de {len(results)} categorias falharam")

        # Se tudo OK, envia alerta de sucesso
        try:
            send_flow_success_alert(
                flow_name="Besistemas",
                source="S3",
                destination="Snowflake",
                summary={
                    "records_loaded": total_rows,
                    "streams_processed": len(succ)
                },
                duration_seconds=elapsed.total_seconds()
            )
        except Exception as e:
            logger.warning(f"Falha ao enviar alerta de sucesso: {e}")

    except Exception as e:
        logger.error(f"Erro no flow: {e}")
        import traceback
        traceback.print_exc()

        # Envia alerta de erro
        try:
            elapsed_error = (datetime.now() - start_time).total_seconds()
            send_flow_error_alert(
                flow_name="Besistemas",
                source="S3",
                destination="Snowflake",
                error_message=str(e),
                duration_seconds=elapsed_error
            )
        except Exception as alert_error:
            logger.warning(f"Falha ao enviar alerta de erro: {alert_error}")

        raise


if __name__ == "__main__":
    # Execução local para teste
    # besistemas_to_snowflake()

    # Deployment para execução agendada
    besistemas_to_snowflake.from_source(
        source=".",
        entrypoint="flows/besistemas/besistemas_to_snowflake.py:besistemas_to_snowflake"
    ).deploy(
        name="besistemas-s3-to-snowflake",
        work_pool_name="local-pool",
        schedules=[
            CronSchedule(cron="20 09 * * *", timezone="America/Sao_Paulo")
        ],
        tags=["besistemas", "s3", "snowflake", "bronze", "incremental"],
        parameters={},
        description="Pipeline: Besistemas S3 -> Snowflake",
        version="1.0.0"
    )
