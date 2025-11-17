from typing import List, Dict, Any, Optional
from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import re


# Whitelist de identifiers permitidos
ALLOWED_DATABASES = ['AJ_DATALAKEHOUSE_RPA', 'AJ_DATALAKEHOUSE_MKT']
ALLOWED_SCHEMAS = ['BRONZE', 'GOLD', 'SILVER', 'PUBLIC']


def validate_identifier(value: str, allowed: List[str], param_name: str) -> str:
    """Valida identifier contra whitelist para prevenir SQL injection"""
    if value not in allowed:
        raise ValueError(f"Invalid {param_name}: {value}. Allowed: {allowed}")
    return value


def sanitize_identifier(value: str) -> str:
    """Remove caracteres perigosos de identifiers"""
    if not re.match(r'^[A-Za-z0-9_]+$', value):
        raise ValueError(f"Invalid identifier: {value}. Only alphanumeric and underscore allowed")
    return value


def _load_private_key_bytes(private_key_string: str, passphrase: Optional[str] = None):
    """
    Carrega chave privada para uso com Snowflake

    Args:
        private_key_string: Chave privada em formato PEM (string)
        passphrase: Passphrase opcional se a chave for criptografada

    Returns:
        Bytes da chave privada em formato DER (pkcs8)
    """
    # Converte passphrase para bytes se fornecida
    password_bytes = passphrase.encode() if passphrase else None

    # Carrega a chave privada
    private_key = serialization.load_pem_private_key(
        private_key_string.encode(),
        password=password_bytes,
        backend=default_backend()
    )

    # Converte para formato DER (PKCS8) que o Snowflake aceita
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return private_key_bytes


@task(retries=3, retry_delay_seconds=10)
def connect_snowflake(
        account: str,
        user: str,
        private_key: str,
        warehouse: str,
        database: str,
        schema: str,
        role: Optional[str] = None,
        private_key_passphrase: Optional[str] = None,
        timeout: int = 60
):
    """
    Estabelece conexão com Snowflake usando autenticação por chave privada

    Args:
        account: Nome da conta Snowflake (ex: amdjr.us-east-1)
        user: Usuário Snowflake
        private_key: Chave privada em formato PEM (string)
        warehouse: Warehouse a ser utilizado
        database: Database a ser utilizado
        schema: Schema a ser utilizado
        role: Role opcional (ex: ACCOUNTADMIN)
        private_key_passphrase: Passphrase da chave privada (se criptografada)
        timeout: Timeout de conexão em segundos

    Returns:
        Conexão Snowflake
    """
    logger = get_run_logger()

    try:
        # Valida identifiers
        database = validate_identifier(database, ALLOWED_DATABASES, 'database')
        schema = validate_identifier(schema, ALLOWED_SCHEMAS, 'schema')

        logger.info(f"Conectando ao Snowflake: {account} / {database}.{schema}")

        # Carrega chave privada
        private_key_bytes = _load_private_key_bytes(private_key, private_key_passphrase)

        conn = snowflake.connector.connect(
            account=account,
            user=user,
            private_key=private_key_bytes,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            login_timeout=timeout,
            network_timeout=timeout
        )

        logger.info("✅ Conexão Snowflake estabelecida com sucesso")
        return conn

    except Exception as e:
        logger.error(f"❌ Erro ao conectar Snowflake: {str(e)}")
        raise


@task(cache_policy=NO_CACHE)
def create_table_if_not_exists(
        conn,
        table_name: str,
        columns_schema: Dict[str, str],
        primary_key: Optional[Any] = None
):
    """
    Cria tabela no Snowflake se não existir

    Args:
        conn: Conexão Snowflake
        table_name: Nome da tabela (ex: BRZ_SALESFORCE_SUBSCRIBER)
        columns_schema: Dicionário {nome_coluna: tipo_dados}
        primary_key: Chave primária (string para chave simples ou lista para chave composta)

    Example:
        # Chave simples
        primary_key = "subscriber_id"

        # Chave composta
        primary_key = ["id_camera", "dt_fluxo"]

        columns_schema = {
            "subscriber_id": "VARCHAR(255)",
            "date_joined": "VARCHAR(255)",
            "email_address": "VARCHAR(500)",
            "loaded_at": "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
        }
    """
    logger = get_run_logger()

    try:
        # Obtém o schema atual da conexão
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_SCHEMA()")
        current_schema = cursor.fetchone()[0]
        cursor.close()

        # Monta DDL com schema explícito
        full_table_name = f"{current_schema}.{table_name}"
        columns_ddl = ", ".join([f'"{col}" {dtype}' for col, dtype in columns_schema.items()])

        if primary_key:
            # Trata chave primária: pode ser string ou lista (chave composta)
            if isinstance(primary_key, list):
                # Chave composta: múltiplas colunas
                pk_columns = ", ".join([f'"{pk}"' for pk in primary_key])
                columns_ddl += f', PRIMARY KEY ({pk_columns})'
            else:
                # Chave simples: uma coluna
                columns_ddl += f', PRIMARY KEY ("{primary_key}")'

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {columns_ddl}
        )
        """

        logger.info(f"📋 Criando tabela: {full_table_name}")

        cursor = conn.cursor()
        cursor.execute(ddl)
        cursor.close()

        logger.info(f"✅ Tabela {full_table_name} verificada/criada com sucesso")

    except Exception as e:
        logger.error(f"❌ Erro ao criar tabela {table_name}: {str(e)}")
        raise


@task(cache_policy=NO_CACHE)
def truncate_table(conn, table_name: str):
    """
    Trunca (apaga todos os registros) de uma tabela Snowflake

    Args:
        conn: Conexão Snowflake
        table_name: Nome da tabela
    """
    logger = get_run_logger()

    try:
        logger.info(f"Truncando tabela {table_name}...")

        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.close()

        logger.info(f"✅ Tabela {table_name} truncada com sucesso")

    except Exception as e:
        logger.error(f"❌ Erro ao truncar tabela {table_name}: {str(e)}")
        raise


@task(cache_policy=NO_CACHE)
def insert_csv_file_replace(
        conn,
        table_name: str,
        csv_file_path: str,
        csv_encoding: str = 'utf-8',
        columns: Optional[List[str]] = None
):
    """
    Insere CSV direto no Snowflake usando COPY INTO (otimizado)

    Processo:
    1. TRUNCATE table
    2. PUT CSV existente para stage interno
    3. COPY INTO table FROM stage (paralelo)
    4. REMOVE arquivo do stage

    Args:
        conn: Conexão Snowflake
        table_name: Nome da tabela
        csv_file_path: Caminho do arquivo CSV local
        csv_encoding: Encoding do CSV (utf-8, utf-16, etc.)
        columns: Lista de colunas (opcional, lê do CSV se não fornecido)
    """
    logger = get_run_logger()
    import os
    import csv

    try:
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {csv_file_path}")

        file_size_mb = os.path.getsize(csv_file_path) / (1024 * 1024)
        logger.info(f"🚀 Carregando CSV ({file_size_mb:.2f} MB) em {table_name} usando COPY INTO...")

        # 1. TRUNCATE
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        logger.info(f"🗑️ Tabela {table_name} truncada")

        # 2. Lê colunas do CSV se não fornecidas
        if not columns:
            with open(csv_file_path, 'r', encoding=csv_encoding) as f:
                reader = csv.reader(f)
                columns = next(reader)
            logger.info(f"📋 {len(columns)} colunas detectadas no CSV")

        # 3. PUT para stage interno
        stage_name = f"@%{table_name}"
        file_name = os.path.basename(csv_file_path)

        logger.info(f"⬆️ Enviando CSV para stage interno {stage_name}...")

        # Converte path para formato Windows
        put_path = csv_file_path.replace('\\', '/')
        put_sql = f"PUT 'file://{put_path}' {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cursor.execute(put_sql)
        logger.info("✅ Arquivo enviado para stage")

        # 4. COPY INTO (carrega tudo em paralelo)
        logger.info("⚡ Executando COPY INTO (bulk load paralelo)...")

        quoted_columns = [f'"{col}"' for col in columns]

        # Define encoding no FILE_FORMAT
        encoding_param = "UTF16" if "utf-16" in csv_encoding.lower() else "UTF8"

        copy_sql = f"""
        COPY INTO {table_name} ({', '.join(quoted_columns)})
        FROM {stage_name}/{file_name}.gz
        FILE_FORMAT = (
            TYPE = 'CSV'
            ENCODING = '{encoding_param}'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            NULL_IF = ('NULL', 'null', '')
            EMPTY_FIELD_AS_NULL = TRUE
            COMPRESSION = 'AUTO'
        )
        ON_ERROR = 'ABORT_STATEMENT'
        """

        cursor.execute(copy_sql)
        result = cursor.fetchone()

        # Resultado: [file, status, rows_parsed, rows_loaded, ...]
        rows_loaded = result[3] if result else 0

        logger.info(f"✅ {rows_loaded} registros carregados via COPY INTO")

        # 5. REMOVE arquivo do stage (limpeza)
        logger.info("🧹 Removendo arquivo do stage...")
        remove_sql = f"REMOVE {stage_name}/{file_name}.gz"
        cursor.execute(remove_sql)

        cursor.close()
        conn.commit()

        logger.info(f"🎉 Carga completa: {rows_loaded} registros em {table_name}")
        return {"rows_inserted": rows_loaded}

    except Exception as e:
        logger.error(f"❌ Erro ao carregar CSV em {table_name}: {str(e)}")
        conn.rollback()
        raise


@task(cache_policy=NO_CACHE)
def execute_query(conn, query: str) -> List[Dict[str, Any]]:
    """
    Executa query SQL e retorna resultados

    Args:
        conn: Conexão Snowflake
        query: Query SQL a ser executada

    Returns:
        Lista de dicionários com os resultados
    """
    logger = get_run_logger()

    try:
        logger.info(f"Executando query: {query[:100]}...")

        cursor = conn.cursor(DictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        logger.info(f"✅ Query executada com sucesso: {len(results)} linha(s) retornada(s)")

        return results

    except Exception as e:
        logger.error(f"❌ Erro ao executar query: {str(e)}")
        raise


@task(cache_policy=NO_CACHE)
def close_snowflake_connection(conn):
    """
    Fecha conexão Snowflake de forma segura

    Args:
        conn: Conexão Snowflake
    """
    logger = get_run_logger()

    try:
        if conn:
            conn.close()
        logger.info("✅ Conexão Snowflake fechada com sucesso")
    except Exception as e:
        logger.warning(f"⚠️ Aviso ao fechar conexão: {str(e)}")


# Schemas das tabelas com prefixo BRZ_SALESFORCE_
# Nomes em snake_case (normalizados de CamelCase)
SALESFORCE_TABLES_SCHEMAS = {
    "resubscribes": {
        "table_name": "BRZ_SALESFORCE_RESUBSCRIBER",
        "primary_key": "id_lead",
        "columns": {
            "id_lead": "VARCHAR(255)",
            "dt_registro": "TIMESTAMP",
            "loaded_at": "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
        }
    },
    "subscribers": {
        "table_name": "BRZ_SALESFORCE_SUBSCRIBER",
        "primary_key": "subscriber_id",
        "columns": {
            "subscriber_id": "VARCHAR(255)",
            "date_undeliverable": "VARCHAR(255)",
            "date_joined": "VARCHAR(255)",
            "date_unsubscribed": "VARCHAR(255)",
            "domain": "VARCHAR(500)",
            "email_address": "VARCHAR(500)",
            "bounce_count": "VARCHAR(50)",
            "subscriber_key": "VARCHAR(255)",
            "subscriber_type": "VARCHAR(100)",
            "status": "VARCHAR(100)",
            "locale": "VARCHAR(50)",
            "loaded_at": "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
        }
    },
    "unsubscribes": {
        "table_name": "BRZ_SALESFORCE_UNSUBSCRIBER",
        "primary_key": "subscriber_id",
        "columns": {
            "subscriber_id": "VARCHAR(255)",
            "date_unsubscribed": "VARCHAR(255)",
            "loaded_at": "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
        }
    }
}


# Schemas das tabelas Deconve (GOLD layer)
DECONVE_TABLES_SCHEMAS = {
    "camera": {
        "table_name": "DECONVE_DIM_CAMERA",
        "primary_key": ["ID_UNIT", "ID_CAMERA"],
        "columns": {
            "ID_CAMERA": "VARCHAR(255)",
            "DS_CAMERA": "VARCHAR(500)",
            "ID_UNIT": "VARCHAR(255)",
            "ID_SHOPPING": "INTEGER",
            "DS_SIGLA": "VARCHAR(10)",
            "DS_SHOPPING": "VARCHAR(255)",
            "DT_CRIACAO": "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
        }
    },
    "person_flow": {
        "table_name": "DECONVE_FATO_FLUXO_PESSOA",
        "primary_key": ["ID_CAMERA", "DT_FLUXO"],
        "columns": {
            "ID_CAMERA": "VARCHAR(255)",
            "DT_FLUXO": "TIMESTAMP_NTZ",
            "NR_ENTRADA": "INTEGER",
            "NR_SAIDA": "INTEGER",
            "DT_CRIACAO": "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"
        }
    }
}


@task(cache_policy=NO_CACHE)
def merge_csv_to_snowflake(
        conn,
        table_name: str,
        csv_file_path: str,
        primary_keys: List[str],
        csv_encoding: str = 'utf-8',
        columns: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Carrega CSV no Snowflake usando MERGE (UPSERT) para evitar duplicatas

    Estratégia:
    1. Cria tabela staging temporária
    2. Carrega CSV na staging (PUT + COPY INTO)
    3. Faz MERGE da staging para a tabela final usando primary_keys
    4. Remove staging

    Args:
        conn: Conexão Snowflake
        table_name: Nome da tabela de destino
        csv_file_path: Caminho do arquivo CSV local
        primary_keys: Lista de colunas que formam a chave primária (pode ser composta)
        csv_encoding: Encoding do CSV (utf-8, utf-16, etc.)
        columns: Lista de colunas (opcional, lê do CSV se não fornecido)

    Returns:
        Dict com estatísticas (rows_inserted, rows_updated)
    """
    logger = get_run_logger()
    import os
    import csv as csv_module
    import time

    try:
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {csv_file_path}")

        file_size_mb = os.path.getsize(csv_file_path) / (1024 * 1024)
        logger.info(f"🚀 Carregando CSV ({file_size_mb:.2f} MB) em {table_name} usando MERGE (UPSERT)...")

        cursor = conn.cursor()

        # 1. Lê colunas do CSV se não fornecidas
        if not columns:
            with open(csv_file_path, 'r', encoding=csv_encoding) as f:
                reader = csv_module.reader(f)
                columns = next(reader)
            logger.info(f"📋 {len(columns)} colunas detectadas no CSV")

        # 2. Cria tabela staging temporária (clone da estrutura da tabela principal)
        staging_table = f"{table_name}_STAGING_{int(time.time())}"
        logger.info(f"🏗️ Criando tabela staging: {staging_table}")

        create_staging_sql = f"CREATE TEMPORARY TABLE {staging_table} LIKE {table_name}"
        cursor.execute(create_staging_sql)
        logger.info(f"✅ Tabela staging criada")

        # 3. PUT arquivo no stage da tabela staging
        stage_name = f"@%{staging_table}"
        file_name = os.path.basename(csv_file_path)

        logger.info(f"⬆️ Enviando CSV para stage staging {stage_name}...")
        put_path = csv_file_path.replace('\\', '/')
        put_sql = f"PUT 'file://{put_path}' {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cursor.execute(put_sql)
        logger.info("✅ Arquivo enviado para stage")

        # 4. COPY INTO staging
        logger.info("⚡ Carregando dados na staging...")

        quoted_columns = [f'"{col}"' for col in columns]
        encoding_param = "UTF16" if "utf-16" in csv_encoding.lower() else "UTF8"

        copy_sql = f"""
        COPY INTO {staging_table} ({', '.join(quoted_columns)})
        FROM {stage_name}/{file_name}.gz
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            ENCODING = '{encoding_param}'
            FIELD_DELIMITER = ','
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        )
        ON_ERROR = 'CONTINUE'
        """

        cursor.execute(copy_sql)
        copy_result = cursor.fetchone()
        rows_loaded = copy_result[1] if copy_result else 0

        logger.info(f"✅ {rows_loaded} linhas carregadas na staging")

        # 5. Monta condição de MATCH usando as chaves primárias
        match_conditions = " AND ".join([f'target."{pk}" = source."{pk}"' for pk in primary_keys])

        # 6. Monta UPDATE SET (todas as colunas exceto as chaves primárias)
        update_columns = [col for col in columns if col not in primary_keys]
        update_set = ", ".join([f'target."{col}" = source."{col}"' for col in update_columns])

        # 7. Monta INSERT (todas as colunas)
        insert_columns = ", ".join([f'"{col}"' for col in columns])
        insert_values = ", ".join([f'source."{col}"' for col in columns])

        # 8. Executa MERGE
        logger.info("🔄 Executando MERGE (UPSERT)...")

        merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING {staging_table} AS source
        ON {match_conditions}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """

        cursor.execute(merge_sql)
        merge_result = cursor.fetchone()

        rows_inserted = merge_result[0] if merge_result else 0
        rows_updated = merge_result[1] if merge_result else 0

        logger.info(f"✅ MERGE concluído:")
        logger.info(f"   📝 Inseridos: {rows_inserted}")
        logger.info(f"   🔄 Atualizados: {rows_updated}")

        # 9. Remove staging (o stage interno é automaticamente removido junto)
        cursor.execute(f"DROP TABLE {staging_table}")
        logger.info(f"🗑️ Staging removida")

        # Nota: Não precisa fazer REMOVE do stage pois tabelas TEMPORARY
        # limpam automaticamente seus stages internos ao serem dropadas

        cursor.close()

        return {
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "total_rows_processed": rows_loaded
        }

    except Exception as e:
        logger.error(f"❌ Erro ao fazer MERGE: {str(e)}")
        raise
