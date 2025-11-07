import time
from typing import Dict, Any
import requests
from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NONE as NO_CACHE


@task(retries=3, retry_delay_seconds=10)
def authenticate_deconve(api_key: str) -> Dict[str, Any]:
    """
    Autentica na API Deconve e retorna token de acesso

    Args:
        api_key: API Key do Deconve

    Returns:
        Dict com access_token, token_type e expires_in
    """
    logger = get_run_logger()

    try:
        auth_url = "https://auth.deconve.com/v1/token/"

        logger.info("🔐 Autenticando na API Deconve...")

        response = requests.post(
            auth_url,
            json={"api_key": api_key},
            timeout=30
        )

        response.raise_for_status()

        token_data = response.json()

        logger.info(f"✅ Token obtido com sucesso (expira em {token_data['expires_in']}s)")

        # Adiciona timestamp de quando o token foi obtido
        token_data['obtained_at'] = time.time()

        return token_data

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erro ao autenticar: {str(e)}")
        raise Exception(f"Falha na autenticação Deconve: {str(e)}") from e


@task(cache_policy=NO_CACHE)
def get_units(access_token: str) -> Dict[str, Any]:
    """
    Obtém lista de unidades (units) da API Deconve

    Args:
        access_token: Token de acesso obtido na autenticação

    Returns:
        Dict com items (lista de unidades), has_more e total
    """
    logger = get_run_logger()

    try:
        api_url = "https://api.deconve.com/v1/units/"

        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        logger.info("📡 Buscando unidades da API Deconve...")

        response = requests.get(
            api_url,
            headers=headers,
            timeout=30
        )

        response.raise_for_status()

        units_data = response.json()

        total_units = units_data.get('total', 0)
        logger.info(f"✅ {total_units} unidade(s) encontrada(s)")

        return units_data

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erro ao buscar unidades: {str(e)}")
        raise Exception(f"Falha ao buscar unidades Deconve: {str(e)}") from e


@task(cache_policy=NO_CACHE, retries=2, retry_delay_seconds=5)
def get_unit_details(access_token: str, unit_id: str) -> Dict[str, Any]:
    """
    Obtém detalhes de uma unidade específica, incluindo vídeos/câmeras

    Args:
        access_token: Token de acesso obtido na autenticação
        unit_id: ID da unidade

    Returns:
        Dict com detalhes da unidade incluindo lista de vídeos
    """
    logger = get_run_logger()

    try:
        api_url = f"https://api.deconve.com/v1/units/{unit_id}"

        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        response = requests.get(
            api_url,
            headers=headers,
            timeout=30
        )

        response.raise_for_status()

        unit_details = response.json()

        videos_count = len(unit_details.get('videos', []))
        logger.info(f"✅ Unit {unit_id}: {videos_count} vídeo(s)/câmera(s) encontrado(s)")

        return unit_details

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erro ao buscar detalhes da unit {unit_id}: {str(e)}")
        raise Exception(f"Falha ao buscar detalhes da unit {unit_id}: {str(e)}") from e


@task(cache_policy=NO_CACHE, retries=2, retry_delay_seconds=3)
def get_video_details(access_token: str, video_id: str) -> Dict[str, Any]:
    """
    Obtém detalhes de um vídeo/câmera específico

    Args:
        access_token: Token de acesso obtido na autenticação
        video_id: ID do vídeo/câmera

    Returns:
        Dict com detalhes do vídeo incluindo name, url, etc
    """
    logger = get_run_logger()

    try:
        api_url = f"https://api.deconve.com/v1/videos/{video_id}"

        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        response = requests.get(
            api_url,
            headers=headers,
            timeout=30
        )

        response.raise_for_status()

        video_details = response.json()

        return video_details

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erro ao buscar detalhes do vídeo {video_id}: {str(e)}")
        raise Exception(f"Falha ao buscar detalhes do vídeo {video_id}: {str(e)}") from e


def get_people_counter_report(
        access_token: str,
        video_id: str,
        start_date: str,
        end_date: str,
        group_by: str = "hour",
        limit: int = 1000,
        skip: int = 0
) -> Dict[str, Any]:
    """
    Obtém relatório de contador de pessoas para um vídeo/câmera

    Args:
        access_token: Token de acesso obtido na autenticação
        video_id: ID do vídeo/câmera
        start_date: Data/hora inicial (formato ISO: 2025-11-05T00:00:00-03:00)
        end_date: Data/hora final (formato ISO: 2025-11-05T23:59:59-03:00)
        group_by: Agrupamento (hour, day, etc)
        limit: Limite de registros por página
        skip: Número de registros para pular (paginação)

    Returns:
        Dict com items (lista de registros), has_more e total
    """
    logger = get_run_logger()

    try:
        api_url = "https://api.deconve.com/v1/peoplecounter/reports/"

        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        params = {
            "video_id": video_id,
            "start_date": start_date,
            "end_date": end_date,
            "group_by": group_by,
            "limit": limit,
            "skip": skip
        }

        response = requests.get(
            api_url,
            headers=headers,
            params=params,
            timeout=60
        )

        response.raise_for_status()

        report_data = response.json()

        return report_data

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erro ao buscar relatório de fluxo: {str(e)}")
        raise Exception(f"Falha ao buscar relatório de fluxo: {str(e)}") from e


def is_token_expired(token_data: Dict[str, Any], buffer_seconds: int = 60) -> bool:
    """
    Verifica se o token está expirado ou próximo de expirar

    Args:
        token_data: Dados do token retornados pela autenticação
        buffer_seconds: Segundos de buffer antes da expiração (padrão: 60s)

    Returns:
        True se o token está expirado ou próximo de expirar
    """
    if 'obtained_at' not in token_data or 'expires_in' not in token_data:
        return True

    elapsed_time = time.time() - token_data['obtained_at']
    time_until_expiry = token_data['expires_in'] - elapsed_time

    return time_until_expiry <= buffer_seconds
