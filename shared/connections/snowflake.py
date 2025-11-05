from typing import List, Dict, Any, Optional
from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


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
            network_timeout=timeout,
            insecure_mode=True  # Desabilita validação SSL (necessário em alguns ambientes corporativos/Docker)
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
        primary_key: Optional[str] = None
):
    """
    Cria tabela no Snowflake se não existir

    Args:
        conn: Conexão Snowflake
        table_name: Nome da tabela (ex: BRZ_SALESFORCE_SUBSCRIBER)
        columns_schema: Dicionário {nome_coluna: tipo_dados}
        primary_key: Nome da coluna chave primária (opcional)

    Example:
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
