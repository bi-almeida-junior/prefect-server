import io
import os
import csv
from datetime import datetime
from typing import List, Optional, Dict, Any
import paramiko
from paramiko import RSAKey, Ed25519Key
from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE


@task(retries=3, retry_delay_seconds=10)
def connect_sftp(
        host: str,
        username: str,
        private_key: Optional[str] = None,
        private_key_path: Optional[str] = None,
        passphrase: Optional[str] = None,
        port: int = 22,
        timeout: int = 30
):
    """
    Estabelece conexão SFTP usando chave privada (string ou arquivo)

    Args:
        host: Hostname ou IP do servidor SFTP
        username: Usuário SFTP
        private_key: Chave privada como string (formato OpenSSH)
        private_key_path: Caminho para arquivo de chave privada
        passphrase: Passphrase da chave privada (opcional)
        port: Porta do servidor SFTP (padrão: 22)
        timeout: Timeout de conexão em segundos

    Returns:
        Tuple (sftp_client, ssh_client)
    """
    logger = get_run_logger()

    try:
        # Inicializa SSH client
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Carrega chave privada
        pkey = _load_private_key(private_key, private_key_path, passphrase, logger)

        # Conecta via SSH
        logger.info(f"Conectando ao servidor SFTP: {host}:{port}")
        ssh_client.connect(
            hostname=host,
            port=port,
            username=username,
            pkey=pkey,
            timeout=timeout
        )

        # Abre sessão SFTP
        sftp_client = ssh_client.open_sftp()
        logger.info(f"✅ Conexão SFTP estabelecida com sucesso em {host}")

        return sftp_client, ssh_client

    except Exception as e:
        logger.error(f"❌ Erro ao conectar SFTP: {str(e)}")
        raise Exception(f"Falha na conexão SFTP: {str(e)}") from e


def _load_private_key(
        private_key: Optional[str],
        private_key_path: Optional[str],
        passphrase: Optional[str],
        logger
):
    """
    Carrega chave privada de string ou arquivo
    Aceita formato OpenSSH (RSA, Ed25519)
    """
    # Opção 1: Chave como string (RECOMENDADO)
    if private_key:
        logger.info("Carregando chave privada da string fornecida")
        return _load_key_from_string(private_key, passphrase)

    # Opção 2: Chave de arquivo
    elif private_key_path:
        logger.info(f"Carregando chave privada do arquivo: {private_key_path}")

        if not os.path.exists(private_key_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {private_key_path}")

        return _load_key_from_file(private_key_path, passphrase)

    else:
        raise ValueError(
            "Configure 'private_key' (string) ou 'private_key_path' (arquivo)"
        )


def _load_key_from_string(key_string: str, passphrase: Optional[str] = None):
    """Carrega chave de uma string"""
    key_types = [(RSAKey, "RSA"), (Ed25519Key, "Ed25519")]

    for key_class, key_type in key_types:
        try:
            key_file = io.StringIO(key_string)
            return key_class.from_private_key(key_file, password=passphrase)
        except Exception:
            continue

    raise Exception(
        "Não foi possível carregar a chave. "
        "Certifique-se de usar formato OpenSSH (não .ppk). "
        "Suporte: RSA, Ed25519. "
        "Converta com: puttygen sua_chave.ppk -O private-openssh -o chave_openssh"
    )


def _load_key_from_file(key_path: str, passphrase: Optional[str] = None):
    """Carrega chave de um arquivo"""
    key_types = [(RSAKey, "RSA"), (Ed25519Key, "Ed25519")]

    for key_class, key_type in key_types:
        try:
            return key_class.from_private_key_file(key_path, password=passphrase)
        except Exception:
            continue

    raise Exception(
        f"Não foi possível carregar a chave do arquivo {key_path}. "
        "Certifique-se de usar formato OpenSSH (não .ppk). "
        "Suporte: RSA, Ed25519. "
        "Converta com: puttygen sua_chave.ppk -O private-openssh -o chave_openssh"
    )


@task(cache_policy=NO_CACHE)
def list_csv_files(sftp_client, remote_folder: str) -> List[Dict[str, Any]]:
    """
    Lista todos os arquivos CSV de uma pasta SFTP com seus metadados

    Args:
        sftp_client: Cliente SFTP conectado
        remote_folder: Pasta remota para listar

    Returns:
        Lista de dicionários com informações dos arquivos CSV
    """
    logger = get_run_logger()

    try:
        # Lista todos os arquivos da pasta
        files = sftp_client.listdir_attr(remote_folder)

        # Filtra apenas arquivos .csv
        csv_files = [
            {
                "filename": f.filename,
                "full_path": f"{remote_folder}/{f.filename}",
                "modified_timestamp": f.st_mtime,
                "modified_datetime": datetime.fromtimestamp(f.st_mtime).isoformat(),
                "size_bytes": f.st_size
            }
            for f in files
            if f.filename.endswith('.csv') and not f.filename.startswith('.')
        ]

        if not csv_files:
            logger.warning(f"⚠️ Nenhum arquivo CSV encontrado em {remote_folder}")
            return []

        # Ordena por data de modificação (mais recente primeiro)
        csv_files.sort(key=lambda x: x["modified_timestamp"], reverse=True)

        logger.info(f"✅ {len(csv_files)} arquivo(s) CSV encontrado(s) em {remote_folder}")

        return csv_files

    except Exception as e:
        logger.error(f"❌ Erro ao listar arquivos em {remote_folder}: {str(e)}")
        raise


@task(cache_policy=NO_CACHE)
def get_latest_file(sftp_client, remote_folder: str) -> Optional[Dict[str, Any]]:
    """
    Retorna o arquivo CSV mais recente (por data de modificação) de uma pasta SFTP

    Args:
        sftp_client: Cliente SFTP conectado
        remote_folder: Pasta remota para buscar

    Returns:
        Dicionário com informações do arquivo mais recente ou None
    """
    logger = get_run_logger()

    csv_files = list_csv_files.fn(sftp_client, remote_folder)

    if not csv_files:
        return None

    latest_file = csv_files[0]
    logger.info(
        f"✅ Arquivo mais recente: {latest_file['filename']} "
        f"(modificado em {latest_file['modified_datetime']})"
    )

    return latest_file


def _detect_encoding(file_path: str, logger) -> str:
    """
    Detecta o encoding de um arquivo tentando os formatos mais comuns
    """
    # Lista de encodings para tentar, em ordem de prioridade
    encodings = [
        'utf-16',      # UTF-16 com BOM (comum em arquivos do Windows/Salesforce)
        'utf-16-le',   # UTF-16 Little Endian
        'utf-16-be',   # UTF-16 Big Endian
        'utf-8',       # UTF-8
        'utf-8-sig',   # UTF-8 com BOM
        'latin-1',     # ISO-8859-1
        'cp1252',      # Windows-1252
    ]

    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                # Tenta ler as primeiras 1000 linhas para validar
                for _ in range(1000):
                    line = f.readline()
                    if not line:
                        break
                # Se chegou aqui, o encoding funciona
                return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception:
            continue

    # Fallback para utf-8 com tratamento de erros
    logger.warning("⚠️ Não foi possível detectar encoding, usando UTF-8 com ignore de erros")
    return 'utf-8'


@task(cache_policy=NO_CACHE)
def download_csv_from_sftp(sftp_client, remote_file_path: str) -> Dict[str, Any]:
    """
    Baixa arquivo CSV do SFTP (sem carregar em memória)

    Args:
        sftp_client: Cliente SFTP conectado
        remote_file_path: Caminho completo do arquivo remoto

    Returns:
        Dict com file_path, encoding, source_file, extracted_at
    """
    logger = get_run_logger()

    try:
        # Verifica tamanho do arquivo
        file_attrs = sftp_client.stat(remote_file_path)
        file_size_mb = file_attrs.st_size / (1024 * 1024)
        logger.info(f"📥 Baixando arquivo: {remote_file_path} ({file_size_mb:.2f} MB)")

        # Cria arquivo temporário (modo binário para download)
        import tempfile
        temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv')
        temp_file_path = temp_file.name
        temp_file.close()

        # Download otimizado com prefetch
        sftp_client.get(remote_file_path, temp_file_path)

        # Detecta encoding
        encoding = _detect_encoding(temp_file_path, logger)
        logger.info(f"📄 Encoding detectado: {encoding}")

        # Conta linhas (rápido, só lê sem processar)
        with open(temp_file_path, 'r', encoding=encoding) as f:
            num_lines = sum(1 for _ in f) - 1  # -1 para o cabeçalho

        logger.info(f"✅ Download concluído: {num_lines:_} registros ({file_size_mb:.2f} MB)".replace('_', '.'))

        return {
            "file_path": temp_file_path,
            "encoding": encoding,
            "source_file": os.path.basename(remote_file_path),
            "extracted_at": datetime.now().isoformat(),
            "num_records": num_lines
        }

    except Exception as e:
        logger.error(f"❌ Erro ao baixar CSV {remote_file_path}: {str(e)}")
        raise


def normalize_csv_header(
        csv_file_path: str,
        encoding: str,
        column_mapping: Dict[str, str]
) -> str:
    """
    Normaliza apenas o cabeçalho (primeira linha) do CSV

    Args:
        csv_file_path: Caminho do arquivo CSV
        encoding: Encoding do arquivo
        column_mapping: Mapeamento {original: normalizado}

    Returns:
        Caminho do novo arquivo com cabeçalho normalizado
    """
    import tempfile

    # Cria arquivo temporário para o CSV normalizado
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv', encoding='utf-8', newline='')
    temp_path = temp_file.name

    with open(csv_file_path, 'r', encoding=encoding) as input_file:
        reader = csv.reader(input_file)

        # Lê e normaliza o cabeçalho
        header = next(reader)
        normalized_header = [column_mapping.get(col, col) for col in header]

        # Escreve cabeçalho normalizado
        writer = csv.writer(temp_file)
        writer.writerow(normalized_header)

        # Copia o resto das linhas sem modificação
        for row in reader:
            writer.writerow(row)

    temp_file.close()

    # Remove arquivo original
    os.unlink(csv_file_path)

    return temp_path


@task(cache_policy=NO_CACHE)
def close_sftp_connection(sftp_client, ssh_client):
    """
    Fecha conexão SFTP e SSH de forma segura

    Args:
        sftp_client: Cliente SFTP conectado
        ssh_client: Cliente SSH conectado
    """
    logger = get_run_logger()

    try:
        if sftp_client:
            sftp_client.close()
        if ssh_client:
            ssh_client.close()
        logger.info("✅ Conexão SFTP fechada com sucesso")
    except Exception as e:
        logger.warning(f"⚠️ Aviso ao fechar conexão: {str(e)}")
