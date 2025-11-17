import sys
import os
import re
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from curl_cffi import requests as curl_requests
import pandas as pd
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from prefect.cache_policies import NONE
from prefect.artifacts import create_table_artifact
from prefect.client.schemas.schedules import CronSchedule
from prefect.blocks.system import Secret

# Imports das conexões compartilhadas
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from shared.connections.snowflake import connect_snowflake, close_snowflake_connection
from shared.alerts import send_flow_success_alert, send_flow_error_alert

# Carrega variáveis de ambiente
load_dotenv()

# ====== CONFIGURAÇÕES ======
BATCH_SIZE = 5  # Quantidade de placas por lote (ajustar conforme necessidade)
RATE_LIMIT = 5  # Requisições por minuto (limite da API)
API_URL = "https://placamaster.com/api/consulta-gratuita"


# ====== SCRIPT DDL PARA ADICIONAR CAMPO DS_MOTIVO_ERRO ======
# Execute este script manualmente no Snowflake antes de usar status 'I':
#
# ALTER TABLE AJ_DATALAKEHOUSE_RPA.BRONZE.BRZ_01_PLACA_RAW
# ADD COLUMN DS_MOTIVO_ERRO VARCHAR(200) DEFAULT NULL;
#
# STATUS 'I' (Inválido) - Usado EXCLUSIVAMENTE para casos irrecuperáveis:
#   - 404: Placa não encontrada na base Placamaster
#   - 403: Acesso bloqueado permanentemente
#   - 500+: Erros de servidor persistentes
#   - EMPTY_DATA: API retorna success=true mas sem marca/modelo
# ================================================================

# ESTRATÉGIA:
# - Busca 250 placas por execução
# - Rate limit interno aguarda automaticamente (5 req/min)
# - 250 placas = ~50 minutos de processamento
# - Executar a cada 1 hora = eficiente e seguro
# - Uma única conexão Snowflake + INSERT direto = otimizado


@task(name="load_proxy_settings", log_prints=True, cache_policy=NONE)
def load_proxy_settings() -> Optional[Dict[str, str]]:
    """
    Carrega configurações de proxy do Prefect Blocks UMA ÚNICA VEZ.

    Esta função evita carregar secrets repetidamente (antes carregava 500x por execução!).
    Agora carrega apenas 1x e reutiliza para todas as 250 requisições.

    Returns:
        Dict com proxies {'http': '...', 'https': '...'} ou None se falhar
    """
    logger = get_run_logger()
    try:
        logger.info("🔐 Carregando configurações de proxy do Prefect Blocks...")

        proxy_http_block = Secret.load("dataimpulse-proxy-http")
        proxy_https_block = Secret.load("dataimpulse-proxy-https")

        proxies = {
            'http': proxy_http_block.get(),
            'https': proxy_https_block.get()
        }

        logger.info("✓ Proxy carregado com sucesso (será reutilizado para todas as requisições)")
        return proxies

    except Exception as e:
        logger.warning(f"⚠ Erro ao carregar proxy blocks: {e}")
        logger.warning("Continuando SEM proxy (requisições podem ser bloqueadas pelo Cloudflare)")
        return None


@task(name="get_pending_plates", log_prints=True, cache_policy=NONE)
def get_pending_plates(conn, database, schema, batch_size: int = 250) -> List[Dict[str, Any]]:
    """
    Busca registros com DS_STATUS = 'P', 'E' ou 'N' da tabela BRZ_01_PLACA_RAW.

    Lógica:
    1. Busca até BATCH_SIZE registros com DS_STATUS IN ('P', 'E', 'N')
    2. Prioriza 'P' (processando/retry imediato), depois 'E' (erro/retry), depois 'N' (novos)
    3. Ordena por DT_INSERCAO DESC (mais novos primeiro)
    4. Retorna lista com placas e seus IDs

    Status:
    - P: Processando (prioridade máxima - retry imediato)
    - E: Erro (prioridade alta - retry)
    - N: Novo (prioridade normal)
    """
    logger = get_run_logger()
    cur = conn.cursor()

    try:
        logger.info(f"Buscando até {batch_size} placas pendentes (prioridade: 'P' > 'E' > 'N') de BRZ_01_PLACA_RAW...")
        cur.execute(f"""
            SELECT DS_PLACA, DT_INSERCAO, DS_STATUS
            FROM {database}.{schema}.BRZ_01_PLACA_RAW
            WHERE DS_STATUS IN ('P', 'E', 'N')
            ORDER BY
                CASE DS_STATUS
                    WHEN 'P' THEN 1  -- Prioridade 1: Placas processando (retry imediato)
                    WHEN 'E' THEN 2  -- Prioridade 2: Placas com erro (retry)
                    WHEN 'N' THEN 3  -- Prioridade 3: Placas novas
                END,
                DT_INSERCAO DESC
            LIMIT {batch_size}
        """)
        results = cur.fetchall()

        plates = [{"plate": row[0], "date": row[1]} for row in results]

        # Conta quantas são de cada status
        processing_count = sum(1 for row in results if row[2] == 'P')
        retry_count = sum(1 for row in results if row[2] == 'E')
        new_count = sum(1 for row in results if row[2] == 'N')

        logger.info(f"Encontradas {len(plates)} placas: {processing_count} processando + {retry_count} retry(s) + {new_count} nova(s)")

        return plates

    except Exception as e:
        logger.warning(f"Erro ao buscar placas pendentes: {e}")
        logger.info("Tabela BRZ_01_PLACA_RAW pode não existir ainda. Retornando lista vazia.")
        return []
    finally:
        cur.close()


@task(name="update_status_processing", log_prints=True, cache_policy=NONE)
def update_status_processing(conn, database, schema, plates: List[str]) -> int:
    """
    Atualiza DS_STATUS de 'N' ou 'E' para 'P' nas placas selecionadas.

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        plates: Lista de placas a atualizar

    Returns:
        Número de registros atualizados
    """
    logger = get_run_logger()

    if not plates:
        logger.info("Nenhuma placa para atualizar status")
        return 0

    cur = conn.cursor()

    try:
        logger.info(f"Atualizando status 'N'/'E' → 'P' para {len(plates)} placas...")

        # Usa parâmetros preparados (segurança contra SQL injection)
        placeholders = ','.join(['%s'] * len(plates))
        update_sql = f"""
            UPDATE {database}.{schema}.BRZ_01_PLACA_RAW
            SET DS_STATUS = 'P'
            WHERE DS_PLACA IN ({placeholders})
              AND (DS_STATUS = 'N' OR DS_STATUS = 'E')
        """

        cur.execute(update_sql, plates)
        rows_updated = cur.rowcount

        # Commit explícito
        conn.commit()

        logger.info(f"✓ {rows_updated} registros atualizados para status 'P'")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status para 'P': {e}")
        raise
    finally:
        cur.close()


def validate_and_normalize_plate(plate: str) -> Optional[str]:
    """
    Valida e normaliza placa para o formato aceito pela API.

    Formatos válidos:
    - Antigo: ABC1234 (3 letras + 4 números)
    - Mercosul: ABC1D23 (3 letras + 1 número + 1 letra + 2 números)

    Returns:
        Placa normalizada (sem hífen) ou None se inválida
    """
    if not plate:
        return None

    # Remove espaços, hífens e converte para maiúsculo
    plate_clean = plate.replace("-", "").replace(" ", "").upper().strip()

    # Padrão Antigo: 3 letras + 4 números (ABC1234)
    pattern_old = r'^[A-Z]{3}\d{4}$'

    # Padrão Mercosul: 3 letras + 1 número + 1 letra + 2 números (ABC1D23)
    pattern_mercosul = r'^[A-Z]{3}\d[A-Z]\d{2}$'

    if re.match(pattern_old, plate_clean) or re.match(pattern_mercosul, plate_clean):
        return plate_clean
    else:
        return None


@task(name="query_plate_api", retries=0, log_prints=True, cache_policy=NONE)
def query_plate_api(plate: str, proxies: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
    """
    Consulta dados de uma placa na API usando curl_cffi (bypassa Cloudflare).

    Args:
        plate: Placa do veículo
        proxies: Dict com configurações de proxy {'http': '...', 'https': '...'}
                 Se None, faz requisição sem proxy

    Retorna:
    - Dict com dados do veículo se sucesso
    - {"status": 429} se rate limit (reprocessável)
    - {"status": "invalid", "reason": "..."} se erro irrecuperável (404, 403, 500+, EMPTY_DATA)
    - None para outros erros (reprocessável)
    """
    logger = get_run_logger()
    try:
        # plate_no_dash = plate.replace("-", "")

        # ✅ VALIDAÇÃO E NORMALIZAÇÃO DA PLACA
        plate_normalized = validate_and_normalize_plate(plate)

        if not plate_normalized:
            logger.warning(f"[{plate}] ⚠ Formato de placa inválido (não é ABC1234 ou ABC1D23)")
            return {"status": "invalid", "reason": "INVALID_PLATE_FORMAT"}

        # Headers para parecer requisição legítima
        # headers = {
        #     'Content-Type': 'application/json',
        #     'Accept': 'application/json, text/plain, */*',
        #     'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        #     'Origin': 'https://placamaster.com',
        #     'Referer': 'https://placamaster.com/',
        # }
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        json_data = {"placa": plate_normalized}

        # Usa curl_cffi com TLS fingerprint do Chrome 110 (Windows)
        # Isto bypassa Cloudflare, DataDome e outras proteções anti-bot
        response = curl_requests.post(
            API_URL,
            json=json_data,
            headers=headers,
            proxies=proxies,  # Usa proxy passado como parâmetro (carregado UMA VEZ no início)
            impersonate="chrome110",  # Simula Chrome 110 real no Windows
            timeout=30
        )

        # ===== DETECÇÃO DE CASOS IRRECUPERÁVEIS (STATUS 'I') =====

        # CASO 1: 404 - Placa não encontrada na base Placamaster
        if response.status_code == 404:
            logger.warning(f"[{plate}] ⚠ 404 - Placa não encontrada (IRRECUPERÁVEL)")
            return {"status": "invalid", "reason": "404_NOT_FOUND"}

        # CASO 2: 403 - Bloqueio permanente
        if response.status_code == 403:
            logger.warning(f"[{plate}] ⚠ 403 - Acesso bloqueado permanentemente (IRRECUPERÁVEL)")
            return {"status": "invalid", "reason": "403_FORBIDDEN"}

        # CASO 3: 500+ - Erros de servidor persistentes
        if response.status_code >= 500:
            logger.warning(f"[{plate}] ⚠ {response.status_code} - Erro de servidor (IRRECUPERÁVEL)")
            return {"status": "invalid", "reason": f"{response.status_code}_SERVER_ERROR"}

        # ===== PROCESSAMENTO NORMAL =====

        if response.status_code == 200:
            data = response.json()

            if data.get("success") and data.get("data"):
                vehicle_data = data.get("data")
                # Valida se tem dados essenciais (marca ou modelo)
                marca = vehicle_data.get("marca")
                modelo = vehicle_data.get("modelo")

                if marca or modelo:
                    logger.info(f"[{plate}] ✓ Dados válidos: {marca} {modelo}")
                    return vehicle_data
                else:
                    # CASO 4: Dados vazios (success=true mas sem marca/modelo)
                    logger.warning(f"[{plate}] ⚠ API retornou success mas SEM marca/modelo (IRRECUPERÁVEL)")
                    return {"status": "invalid", "reason": "EMPTY_DATA"}
            else:
                logger.warning(f"[{plate}] API retornou success={data.get('success')}, data presente={data.get('data') is not None}")
                return None

        # Rate limit (429) - REPROCESSÁVEL
        elif response.status_code == 429:
            logger.warning(f"[{plate}] Rate limit (429)")
            return {"status": 429}

        # Outros erros - REPROCESSÁVEL
        else:
            logger.warning(f"[{plate}] Status code inesperado: {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"Erro ao consultar {plate}: {e}")
        return None


@task(name="process_plate_batch", log_prints=True, cache_policy=NONE)
def process_plate_batch(plates_info: List[Dict[str, Any]], proxies: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Processa um lote de placas respeitando rate limit de 5 req/min.

    Args:
        plates_info: Lista de informações das placas
        proxies: Configurações de proxy (carregado UMA VEZ na função principal)

    Retorna:
    - df: DataFrame com placas processadas com sucesso
    - failed_plates: Lista de placas com erro temporário (status E)
    - invalid_plates: Dict com placas inválidas e seus motivos (status I)
    """
    logger = get_run_logger()

    # ✅ CONFIGURAÇÃO
    REQUESTS_PER_WINDOW = 5
    WINDOW_DURATION = 60
    SAFETY_MARGIN = 3
    MIN_INTERVAL = 10
    MAX_RETRIES = 5
    RETRY_BACKOFF = 15

    results = []
    request_timestamps = []
    last_request_time = None
    total_429 = 0
    total_retries = 0
    failed_plates = []
    invalid_plates = {}  # Dict: {placa: motivo}

    logger.info(f"Processando {len(plates_info)} placas (5 req/min, max {MAX_RETRIES} retries)")

    for i, plate_info in enumerate(plates_info, 1):
        plate = plate_info["plate"]
        plate_date = plate_info["date"]
        current_time = time.time()

        # REGRA 1: Intervalo mínimo
        if last_request_time is not None:
            time_since_last = current_time - last_request_time
            if time_since_last < MIN_INTERVAL:
                time.sleep(MIN_INTERVAL - time_since_last)
                current_time = time.time()

        # REGRA 2: Limpa janela
        request_timestamps = [t for t in request_timestamps if current_time - t < WINDOW_DURATION]

        # REGRA 3: Controla janela
        if len(request_timestamps) >= REQUESTS_PER_WINDOW:
            first_request_time = request_timestamps[0]
            wait_time = WINDOW_DURATION + SAFETY_MARGIN - (current_time - first_request_time)

            if wait_time > 0:
                time.sleep(wait_time)
                current_time = time.time()

            request_timestamps.pop(0)

        # ✅ LOG COMPACTO - A cada 10 placas
        if i % 10 == 0 or i == 1 or i == len(plates_info):
            logger.info(f"[{i}/{len(plates_info)}] Processando: {plate}")

        # Requisição com retry
        attempts = 0
        vehicle_data = None

        while attempts < MAX_RETRIES:
            request_time = time.time()
            vehicle_data = query_plate_api(plate, proxies)  # Passa proxy carregado UMA VEZ
            request_timestamps.append(request_time)
            last_request_time = request_time

            # ===== DETECÇÃO DE CASO IRRECUPERÁVEL (STATUS 'I') =====
            if vehicle_data and vehicle_data.get("status") == "invalid":
                reason = vehicle_data.get("reason", "UNKNOWN")
                invalid_plates[plate] = reason
                logger.warning(f"[{plate}] ⚠ IRRECUPERÁVEL: {reason}")
                vehicle_data = None
                break  # Não faz retry para casos inválidos

            # ===== PROCESSAMENTO DE RATE LIMIT (429) =====
            elif vehicle_data and vehicle_data.get("status") == 429:
                attempts += 1
                total_429 += 1
                total_retries += 1

                if attempts < MAX_RETRIES:
                    backoff_time = RETRY_BACKOFF + (attempts * 5)
                    logger.warning(f"[{plate}] 429 - Retry {attempts}/{MAX_RETRIES} em {backoff_time}s")
                    time.sleep(backoff_time)
                    current_time = time.time()
                    request_timestamps = [t for t in request_timestamps if current_time - t < WINDOW_DURATION]
                else:
                    logger.error(f"[{plate}] FALHOU após {MAX_RETRIES} tentativas (429)")
                    failed_plates.append(plate)
                    vehicle_data = None
                    break

            # ===== SUCESSO OU OUTRO ERRO =====
            else:
                if attempts > 0:
                    logger.info(f"[{plate}] ✓ Sucesso após {attempts} retry(s)")
                break

        # ===== PROCESSA RESULTADO =====
        # Sucesso: adiciona aos resultados
        if vehicle_data and vehicle_data.get("status") != 429 and vehicle_data.get("status") != "invalid":
            color = vehicle_data.get("cor")
            results.append({
                "DS_PLACA": plate,
                "DS_MARCA": vehicle_data.get("marca"),
                "DS_MODELO": vehicle_data.get("modelo"),
                "NR_ANO_FABRICACAO": vehicle_data.get("ano_fabricacao"),
                "NR_ANO_MODELO": vehicle_data.get("ano_modelo"),
                "DS_COR": color.upper() if color else None,
                "DT_COLETA_API": (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S'),
            })
        # Falha temporária: adiciona à lista de erros
        elif not vehicle_data:
            # Só adiciona se não estiver em invalid_plates nem em failed_plates
            if plate not in invalid_plates and plate not in failed_plates:
                failed_plates.append(plate)
                logger.warning(f"[{plate}] API não retornou dados válidos (erro temporário)")

    # ✅ RESUMO FINAL OBJETIVO
    logger.info("=" * 60)
    logger.info(f"RESULTADO: {len(results)}/{len(plates_info)} placas processadas com sucesso")
    if total_429 > 0:
        logger.warning(f"Total 429 recebidos: {total_429} | Retries executados: {total_retries}")
    if failed_plates:
        logger.warning(f"Erros temporários ({len(failed_plates)}): {', '.join(failed_plates)}")
    if invalid_plates:
        logger.warning(f"⚠ Placas INVÁLIDAS ({len(invalid_plates)}): {', '.join(invalid_plates.keys())}")
    logger.info("=" * 60)

    return {
        "df": pd.DataFrame(results) if results else pd.DataFrame(),
        "failed_plates": failed_plates,
        "invalid_plates": invalid_plates  # Novo retorno
    }


@task(name="update_status_error", log_prints=True, cache_policy=NONE)
def update_status_error(conn, database, schema, plates: List[str]) -> int:
    """
    Atualiza DS_STATUS de 'P' para 'E' nas placas que falharam.

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        plates: Lista de placas que falharam

    Returns:
        Número de registros atualizados
    """
    logger = get_run_logger()

    if not plates:
        return 0

    cur = conn.cursor()

    try:
        logger.info(f"Atualizando status 'P' → 'E' para {len(plates)} placas com erro...")

        # Usa parâmetros preparados (segurança contra SQL injection)
        placeholders = ','.join(['%s'] * len(plates))
        update_sql = f"""
            UPDATE {database}.{schema}.BRZ_01_PLACA_RAW
            SET DS_STATUS = 'E'
            WHERE DS_PLACA IN ({placeholders})
              AND DS_STATUS = 'P'
        """

        cur.execute(update_sql, plates)
        rows_updated = cur.rowcount

        # Commit explícito
        conn.commit()

        logger.info(f"✓ {rows_updated} registros atualizados para status 'E' (erro)")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status para 'E': {e}")
        raise
    finally:
        cur.close()


@task(name="update_status_invalid", log_prints=True, cache_policy=NONE)
def update_status_invalid(conn, database, schema, plates_with_reasons: Dict[str, str]) -> int:
    """
    Atualiza DS_STATUS de 'P' para 'I' (Inválido) nas placas com erros irrecuperáveis.

    CASOS EXCLUSIVOS para status 'I':
    - 404: Placa não encontrada na base Placamaster
    - 403: Acesso bloqueado permanentemente
    - 500+: Erros de servidor persistentes (500, 502, 503, 504)
    - EMPTY_DATA: API retorna success=true mas sem marca/modelo

    Args:
        conn: Conexão Snowflake
        database: Database
        schema: Schema
        plates_with_reasons: Dict com placa como chave e motivo como valor
                           Exemplo: {"ABC1234": "404_NOT_FOUND", "XYZ5678": "EMPTY_DATA"}

    Returns:
        Número de registros atualizados
    """
    logger = get_run_logger()

    if not plates_with_reasons:
        return 0

    cur = conn.cursor()

    try:
        logger.warning(f"⚠ Marcando {len(plates_with_reasons)} placas como INVÁLIDAS (status 'I')")

        rows_updated = 0

        # Atualiza cada placa individualmente para poder setar o motivo específico
        for plate, reason in plates_with_reasons.items():
            update_sql = f"""
                UPDATE {database}.{schema}.BRZ_01_PLACA_RAW
                SET DS_STATUS = 'I',
                    DS_MOTIVO_ERRO = %s
                WHERE DS_PLACA = %s
                  AND DS_STATUS = 'P'
            """

            cur.execute(update_sql, (reason, plate))
            rows_updated += cur.rowcount

            logger.warning(f"  └─ [{plate}] Motivo: {reason}")

        # Commit explícito
        conn.commit()

        logger.warning(f"✓ {rows_updated} registros marcados como status 'I' (inválido/irrecuperável)")
        return rows_updated

    except Exception as e:
        logger.error(f"Erro ao atualizar status para 'I': {e}")
        raise
    finally:
        cur.close()


@task(name="insert_data_snowflake", log_prints=True, cache_policy=NONE)
def insert_data_snowflake(conn, database, schema, df: pd.DataFrame) -> Dict[str, Any]:
    """
    Insere dados no Snowflake e atualiza status na BRZ_01_PLACA_RAW.

    Returns:
        Dict com contadores de sucesso e erro
    """
    logger = get_run_logger()

    if df.empty:
        logger.info("Nenhum dado para inserir")
        return {"success": 0, "error": 0}

    cursor = conn.cursor()

    try:
        logger.info(f"Inserindo {len(df)} registros em {database}.{schema}.BRZ_02_VEICULO_DETALHE...")

        # INSERT direto (controle de duplicatas via DS_STATUS na origem)
        insert_sql = f"""
        INSERT INTO {database}.{schema}.BRZ_02_VEICULO_DETALHE
            (DS_PLACA, DS_MARCA, DS_MODELO, NR_ANO_FABRICACAO, NR_ANO_MODELO, DS_COR, DT_COLETA_API)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Prepara registros para batch insert
        records = [
            (
                row['DS_PLACA'],
                row['DS_MARCA'],
                row['DS_MODELO'],
                row['NR_ANO_FABRICACAO'],
                row['NR_ANO_MODELO'],
                row['DS_COR'],
                row['DT_COLETA_API']
            )
            for _, row in df.iterrows()
        ]

        success_plates = df['DS_PLACA'].tolist()

        # Batch insert (otimizado para até 250 registros)
        cursor.executemany(insert_sql, records)
        rows_inserted = cursor.rowcount

        # Commit explícito
        conn.commit()

        logger.info(f"✓ INSERT concluído: {rows_inserted} registros inseridos")

        # Atualiza status 'P' → 'S' usando parâmetros preparados
        if success_plates:
            placeholders = ','.join(['%s'] * len(success_plates))
            update_success_sql = f"""
                UPDATE {database}.{schema}.BRZ_01_PLACA_RAW
                SET DS_STATUS = 'S'
                WHERE DS_PLACA IN ({placeholders})
                  AND DS_STATUS = 'P'
            """
            cursor.execute(update_success_sql, success_plates)
            conn.commit()
            logger.info(f"✓ {len(success_plates)} placas atualizadas para status 'S' (sucesso)")

        return {"success": rows_inserted, "error": len(df) - rows_inserted}

    except Exception as e:
        logger.error(f"Erro ao inserir dados: {e}")
        raise
    finally:
        cursor.close()


@flow(name="vehicle_details_api_to_snowflake", log_prints=True)
def vehicle_details_api_to_snowflake(
        snowflake_account: Optional[str] = None,
        snowflake_user: Optional[str] = None,
        snowflake_private_key: Optional[str] = None,
        snowflake_warehouse: Optional[str] = None,
        snowflake_role: Optional[str] = None,
        batch_size: int = BATCH_SIZE
):
    """
    Flow principal: Consulta API de placas e insere no Snowflake.

    Args:
        snowflake_account: Conta Snowflake (padrão: .env)
        snowflake_user: Usuário Snowflake (padrão: .env)
        snowflake_private_key: Chave privada Snowflake (padrão: .env)
        snowflake_warehouse: Warehouse Snowflake (padrão: .env)
        snowflake_role: Role Snowflake (padrão: .env)
        batch_size: Tamanho do lote de processamento
    """
    logger = get_run_logger()
    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("PLACA CONSULTA: API → SNOWFLAKE")
    logger.info("=" * 80)

    # Carrega configurações do ambiente
    snowflake_account = snowflake_account or os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user = snowflake_user or os.getenv("SNOWFLAKE_USER")
    snowflake_private_key = snowflake_private_key or os.getenv("SNOWFLAKE_PRIVATE_KEY")
    snowflake_warehouse = snowflake_warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_role = snowflake_role or os.getenv("SNOWFLAKE_ROLE")

    # Databases e schemas
    dest_database = "AJ_DATALAKEHOUSE_RPA"
    dest_schema = "BRONZE"

    conn = None  # Inicializa conexão como None
    try:
        # Conexão Snowflake
        conn = connect_snowflake(
            account=snowflake_account,
            user=snowflake_user,
            private_key=snowflake_private_key,
            warehouse=snowflake_warehouse,
            database=dest_database,
            schema=dest_schema,
            role=snowflake_role
        )

        # Busca placas pendentes (DS_STATUS = 'N')
        plates_info = get_pending_plates(conn, dest_database, dest_schema, batch_size)

        if not plates_info:
            logger.info("Nenhuma placa pendente para processar. Encerrando.")
            return

        # Atualiza status para 'P' (processando)
        plates_list = [p["plate"] for p in plates_info]
        update_status_processing(conn, dest_database, dest_schema, plates_list)

        # ✅ OTIMIZAÇÃO: Carrega proxy UMA ÚNICA VEZ (antes carregava 500x!)
        proxies = load_proxy_settings()

        # Processa em lotes
        total_processed = 0
        total_inserted = 0
        total_errors = 0

        for batch_start in range(0, len(plates_info), batch_size):
            batch_end = min(batch_start + batch_size, len(plates_info))
            batch = plates_info[batch_start:batch_end]

            logger.info(f"Lote {batch_start // batch_size + 1}: Processando placas {batch_start + 1}-{batch_end}/{len(plates_info)}")

            # Processa lote (passa proxy carregado UMA VEZ)
            result = process_plate_batch(batch, proxies)
            df = result["df"]
            failed_plates = result["failed_plates"]
            invalid_plates = result.get("invalid_plates", {})  # Novo campo

            # Insere no Snowflake e atualiza status 'S' para sucesso
            if not df.empty:
                insert_result = insert_data_snowflake(conn, dest_database, dest_schema, df)
                total_inserted += insert_result["success"]
                total_errors += insert_result["error"]

            # Atualiza status 'I' para placas INVÁLIDAS (irrecuperáveis)
            if invalid_plates:
                update_status_invalid(conn, dest_database, dest_schema, invalid_plates)
                total_errors += len(invalid_plates)

            # Atualiza status 'E' para placas que falharam na API (temporário)
            if failed_plates:
                update_status_error(conn, dest_database, dest_schema, failed_plates)
                total_errors += len(failed_plates)

            total_processed += len(batch)

        # Resumo
        end_time = datetime.now()
        elapsed = end_time - start_time
        m, s = divmod(elapsed.total_seconds(), 70)

        logger.info("=" * 80)
        logger.info("PROCESSO CONCLUÍDO")
        logger.info("=" * 80)
        logger.info(f"Database: {dest_database}")
        logger.info(f"Schema:   {dest_schema}")
        logger.info(f"Tabela:   BRZ_02_VEICULO_DETALHE")
        logger.info(f"Início:   {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Fim:      {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duração:  {int(m)}m {int(s)}s")
        logger.info(f"Placas processadas: {total_processed}")
        logger.info(f"Registros inseridos: {total_inserted}")
        logger.info(f"Erros encontrados: {total_errors}")

        # Artifact
        try:
            create_table_artifact(
                key="plates-results",
                table=[{
                    "Métrica": "Placas Processadas",
                    "Valor": total_processed
                }, {
                    "Métrica": "Registros Inseridos",
                    "Valor": total_inserted
                }, {
                    "Métrica": "Erros",
                    "Valor": total_errors
                }, {
                    "Métrica": "Duração (min)",
                    "Valor": f"{int(m)}m {int(s)}s"
                }],
                description=f"✅ {total_inserted} registros inseridos | {total_errors} erros"
            )
        except Exception as e:
            logger.warning(f"Erro criando artifact: {e}")

        # Alerta de sucesso
        try:
            send_flow_success_alert(
                flow_name="Placa Consulta",
                source="API Placamaster",
                destination="Snowflake",
                summary={
                    "records_loaded": total_inserted,
                    "plates_processed": total_processed,
                    "errors": total_errors
                },
                duration_seconds=elapsed.total_seconds()
            )
        except Exception as e:
            logger.warning(f"Falha ao enviar alerta de sucesso: {e}")

    except Exception as e:
        logger.error(f"Erro no flow: {e}")
        import traceback
        traceback.print_exc()

        # Alerta de erro
        try:
            elapsed_error = (datetime.now() - start_time).total_seconds()
            send_flow_error_alert(
                flow_name="Placa Consulta",
                source="API Placamaster",
                destination="Snowflake",
                error_message=str(e),
                duration_seconds=elapsed_error
            )
        except Exception as alert_error:
            logger.warning(f"Falha ao enviar alerta de erro: {alert_error}")

        raise

    finally:
        # Garante que a conexão seja fechada mesmo em caso de erro
        if conn is not None:
            try:
                close_snowflake_connection(conn)
                logger.info("✓ Conexão Snowflake fechada com sucesso")
            except Exception as close_error:
                logger.warning(f"Erro ao fechar conexão Snowflake: {close_error}")


if __name__ == "__main__":
    # Execução local para teste
    # vehicle_details_api_to_snowflake()

    # Deployment para execução agendada
    vehicle_details_api_to_snowflake.from_source(
        source=".",
        entrypoint="flows/vehicle/vehicle_details_api_to_snowflake.py:vehicle_details_api_to_snowflake"
    ).deploy(
        name="vehicle-details-api-to-snowflake",
        work_pool_name="local-pool",
        schedules=[
            CronSchedule(cron="0 * * * *", timezone="America/Sao_Paulo")
        ],
        tags=["rpa", "api", "snowflake", "bronze"],
        parameters={},
        description="🚘 Integração API → Snowflake | Consulta detalhes de veículos por placa (marca, modelo, ano, cor) e carrega na Bronze. Processa até 250 placas/hora com rate limit (5 req/min), retry automático, bypass Cloudflare, proxy DataImpulse e gestão de status (N/P/E/S/I).",
        version="1.0.0"
    )
