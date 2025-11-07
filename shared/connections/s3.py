import os
import io
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pandas as pd
from prefect import get_run_logger


def connect_s3(
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        region_name: str = None,
        max_retries: int = 5
):
    """
    Cria cliente S3 com autenticação por access key.

    Args:
        aws_access_key_id: AWS Access Key ID
        aws_secret_access_key: AWS Secret Access Key
        region_name: Região AWS (padrão: sa-east-1)
        max_retries: Número máximo de tentativas de retry

    Returns:
        boto3.client: Cliente S3 autenticado
    """
    logger = get_run_logger()

    # Carrega credenciais do ambiente se não fornecidas
    aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
    region_name = region_name or os.getenv("AWS_REGION", "sa-east-1")

    if not aws_access_key_id or not aws_secret_access_key:
        raise ValueError("Credenciais AWS não fornecidas. Configure AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY")

    logger.info(f"Conectando ao S3 na região {region_name}...")

    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )

    s3_client = session.client(
        "s3",
        config=Config(retries={"max_attempts": max_retries, "mode": "standard"})
    )

    logger.info("✓ Conexão S3 estabelecida")
    return s3_client


def list_files(
        s3_client,
        bucket: str,
        prefix: str = "",
        suffix: str = None
) -> list:
    """
    Lista arquivos em um bucket S3.

    Args:
        s3_client: Cliente S3 boto3
        bucket: Nome do bucket
        prefix: Prefixo do caminho (pasta)
        suffix: Extensão dos arquivos (ex: '.csv')

    Returns:
        list: Lista de dicionários com metadados dos arquivos
    """
    logger = get_run_logger()
    logger.info(f"Listando arquivos: s3://{bucket}/{prefix}")

    files = []
    paginator = s3_client.get_paginator('list_objects_v2')

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']

                # Ignora diretórios
                if key.endswith('/'):
                    continue

                # Filtra por sufixo se especificado
                if suffix and not key.endswith(suffix):
                    continue

                files.append({
                    'key': key,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'etag': obj.get('ETag', '').strip('"')
                })

        logger.info(f"✓ Encontrados {len(files)} arquivo(s)")
        return files

    except ClientError as e:
        logger.error(f"Erro ao listar arquivos: {e}")
        raise


def read_csv_from_s3(
        s3_client,
        bucket: str,
        key: str,
        encoding: str = 'utf-8',
        **kwargs
) -> pd.DataFrame:
    """
    Lê arquivo CSV do S3 para DataFrame.

    Args:
        s3_client: Cliente S3 boto3
        bucket: Nome do bucket
        key: Chave (caminho) do arquivo
        encoding: Encoding do arquivo (padrão: utf-8)
        **kwargs: Argumentos adicionais para pd.read_csv

    Returns:
        pd.DataFrame: DataFrame com os dados do CSV
    """
    logger = get_run_logger()

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()

        # Configurações padrão para leitura robusta
        default_kwargs = {
            'encoding': encoding,
            'low_memory': False,
            'on_bad_lines': 'skip'
        }
        default_kwargs.update(kwargs)

        df = pd.read_csv(io.BytesIO(content), **default_kwargs)

        logger.info(f"✓ CSV lido: {len(df)} linhas, {len(df.columns)} colunas")
        return df

    except ClientError as e:
        logger.error(f"Erro ao ler CSV {key}: {e}")
        raise
    except Exception as e:
        logger.error(f"Erro ao processar CSV {key}: {e}")
        raise


def get_file_metadata(
        s3_client,
        bucket: str,
        key: str
) -> dict:
    """
    Obtém metadados de um arquivo S3.

    Args:
        s3_client: Cliente S3 boto3
        bucket: Nome do bucket
        key: Chave (caminho) do arquivo

    Returns:
        dict: Metadados do arquivo (ETag, LastModified, ContentLength, etc.)
    """
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return {
            'etag': response.get('ETag', '').strip('"'),
            'last_modified': response.get('LastModified'),
            'content_length': response.get('ContentLength'),
            'content_type': response.get('ContentType'),
            'metadata': response.get('Metadata', {})
        }
    except ClientError as e:
        logger = get_run_logger()
        logger.error(f"Erro ao obter metadados de {key}: {e}")
        raise


def upload_file(
        s3_client,
        file_path: str,
        bucket: str,
        key: str,
        extra_args: dict = None
) -> bool:
    """
    Faz upload de arquivo local para S3.

    Args:
        s3_client: Cliente S3 boto3
        file_path: Caminho do arquivo local
        bucket: Nome do bucket
        key: Chave (caminho) de destino no S3
        extra_args: Argumentos extras (ACL, metadata, etc.)

    Returns:
        bool: True se sucesso
    """
    logger = get_run_logger()

    try:
        s3_client.upload_file(file_path, bucket, key, ExtraArgs=extra_args or {})
        logger.info(f"✓ Upload concluído: {key}")
        return True
    except ClientError as e:
        logger.error(f"Erro no upload de {file_path}: {e}")
        raise
