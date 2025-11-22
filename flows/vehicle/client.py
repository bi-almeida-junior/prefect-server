import re
import time
import random
from typing import Optional, List, Dict, Any

import requests
from curl_cffi import requests as curl_requests
from prefect import get_run_logger


class FipeAPIClient:
    """Cliente para consultar a API da Tabela FIPE."""

    BASE_URL = "https://veiculos.fipe.org.br/api/veiculos"
    TIMEOUT = 30
    REQUEST_DELAY = 1.0

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://veiculos.fipe.org.br',
        'Referer': 'https://veiculos.fipe.org.br/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin'
    }

    # Mapeamento de marcas para códigos FIPE
    BRAND_CODES = {
        "ACURA": "1", "AGRALE": "2", "ALFA ROMEO": "3", "AUDI": "6", "BMW": "7",
        "BYD": "238", "CAOA CHERY": "245", "CHERY": "245", "CHEVROLET": "23", "GM": "23",
        "CITROËN": "13", "CITROEN": "13", "FIAT": "21", "FORD": "22", "HONDA": "25",
        "HYUNDAI": "26", "JEEP": "29", "KIA": "31", "MERCEDES-BENZ": "39", "MERCEDES": "39",
        "MITSUBISHI": "41", "NISSAN": "43", "PEUGEOT": "44", "PORSCHE": "47", "RAM": "185",
        "RENAULT": "48", "TOYOTA": "56", "VOLKSWAGEN": "59", "VW": "59", "VOLVO": "58"
    }

    def __init__(self):
        self.logger = get_run_logger()

    def get_reference_table(self) -> str:
        """Busca código da tabela de referência FIPE atual."""
        try:
            response = requests.post(f"{self.BASE_URL}/ConsultarTabelaDeReferencia", headers=self.HEADERS, timeout=self.TIMEOUT)
            time.sleep(self.REQUEST_DELAY)

            if response.status_code == 200:
                tables = response.json()
                if tables:
                    return str(tables[0]["Codigo"])

            return "327"  # Fallback

        except Exception:
            return "327"

    def get_models(self, brand_code: str, table_code: str) -> List[Dict]:
        """Consulta modelos de uma marca."""
        payload = {
            "codigoTipoVeiculo": "1",
            "codigoTabelaReferencia": table_code,
            "codigoMarca": brand_code
        }

        response = requests.post(f"{self.BASE_URL}/ConsultarModelos", data=payload, headers=self.HEADERS, timeout=self.TIMEOUT)
        time.sleep(self.REQUEST_DELAY)

        if response.status_code == 200:
            return response.json().get("Modelos", [])
        return []

    def get_years(self, brand_code: str, model_code: str, table_code: str) -> List[Dict]:
        """Consulta anos disponíveis de um modelo."""
        payload = {
            "codigoTipoVeiculo": "1",
            "codigoTabelaReferencia": table_code,
            "codigoMarca": brand_code,
            "codigoModelo": model_code
        }

        response = requests.post(f"{self.BASE_URL}/ConsultarAnoModelo", data=payload, headers=self.HEADERS, timeout=self.TIMEOUT)
        time.sleep(self.REQUEST_DELAY)

        if response.status_code == 200:
            return response.json()
        return []

    def get_value(self, brand_code: str, model_code: str, year: str, fuel_code: str, table_code: str) -> Optional[Dict]:
        """Consulta valor FIPE de um veículo."""
        payload = {
            "codigoTabelaReferencia": table_code,
            "codigoMarca": brand_code,
            "codigoModelo": model_code,
            "codigoTipoVeiculo": "1",
            "anoModelo": year,
            "codigoTipoCombustivel": fuel_code,
            "tipoVeiculo": "carro",
            "modeloCodigoExterno": "",
            "tipoConsulta": "tradicional"
        }

        response = requests.post(f"{self.BASE_URL}/ConsultarValorComTodosParametros", data=payload, headers=self.HEADERS, timeout=self.TIMEOUT)
        time.sleep(self.REQUEST_DELAY)

        result = response.json()

        # Verifica erro da API
        if "codigo" in result and result["codigo"] == "2":
            return None

        return result


class AnyCarAPIClient:
    """Cliente para consultar a API AnyCar com retry robusto."""

    BASE_URL = "https://api-v2.anycar.com.br/site/test-drive"
    HOME_URL = "https://anycar.com.br"
    TIMEOUT = 30
    MAX_RETRIES = 5
    RETRY_BACKOFF = 15

    def __init__(self, proxies: Optional[Dict[str, str]] = None):
        self.proxies = proxies
        self.logger = get_run_logger()
        self.session = None
        self._establish_session()

    def validate_plate(self, plate: str) -> Optional[str]:
        """Valida e normaliza placa."""
        if not plate:
            return None

        plate_clean = plate.replace("-", "").replace(" ", "").upper().strip()

        # Padrão Antigo: ABC1234
        pattern_old = r'^[A-Z]{3}\d{4}$'
        # Padrão Mercosul: ABC1D23
        pattern_mercosul = r'^[A-Z]{3}\d[A-Z]\d{2}$'

        if re.match(pattern_old, plate_clean) or re.match(pattern_mercosul, plate_clean):
            return plate_clean
        return None

    def _format_plate_with_hyphen(self, plate: str) -> str:
        """Formata placa com hífen (ABC-1234 ou ABC-1D23)."""
        if len(plate) == 7:
            return f"{plate[:3]}-{plate[3:]}"
        return plate

    def _establish_session(self):
        """Estabelece sessão visitando a home page para obter cookies."""
        try:
            self.session = curl_requests.Session()
            # Visita home page para obter cookies de sessão
            self.session.get(
                self.HOME_URL,
                proxies=self.proxies,
                impersonate="chrome124",
                timeout=15
            )
            # Delay humano após visitar home
            time.sleep(random.uniform(1, 2))
            self.logger.info("✓ Sessão estabelecida com AnyCar")
        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao estabelecer sessão: {e}")
            self.session = curl_requests.Session()

    def _query_once(self, plate_normalized: str) -> Optional[Dict[str, Any]]:
        """Executa uma única requisição à API AnyCar."""
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Origin': 'https://anycar.com.br',
            'Referer': 'https://anycar.com.br/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        }

        try:
            # Adiciona hífen na placa
            plate_with_hyphen = self._format_plate_with_hyphen(plate_normalized)

            # POST para obter ID (usando session)
            response_post = self.session.post(
                self.BASE_URL,
                json={"placa": plate_with_hyphen},
                headers=headers,
                proxies=self.proxies,
                impersonate="chrome124",
                timeout=self.TIMEOUT
            )

            # Casos irrecuperáveis
            if response_post.status_code == 400:
                return {"status": "invalid", "reason": "400_INVALID_DATA"}
            if response_post.status_code == 404:
                return {"status": "invalid", "reason": "404_NOT_FOUND"}
            if response_post.status_code == 403:
                return {"status": "invalid", "reason": "403_FORBIDDEN"}
            if response_post.status_code >= 500:
                return {"status": "invalid", "reason": f"{response_post.status_code}_SERVER_ERROR"}

            # Rate limit
            if response_post.status_code == 429:
                return {"status": 429}

            # Sucesso no POST - extrai ID
            if response_post.status_code == 200:
                data_post = response_post.json()
                if not data_post.get("status") or not data_post.get("id"):
                    self.logger.info(response_post.json())
                    return {"status": "invalid", "reason": "NO_ID_RETURNED"}

                vehicle_id = data_post["id"]

                # Aguarda delay randômico entre 1-3 segundos antes do GET
                time.sleep(random.uniform(1, 3))

                # GET com o ID (usando session)
                response_get = self.session.get(
                    f"{self.BASE_URL}/{vehicle_id}",
                    headers=headers,
                    proxies=self.proxies,
                    impersonate="chrome124",
                    timeout=self.TIMEOUT
                )

                if response_get.status_code == 200:
                    data_get = response_get.json()
                    if data_get.get("status") and data_get.get("data"):
                        vehicle_data = data_get["data"]

                        # Verifica se encontrou dados
                        if not vehicle_data.get("encontrado"):
                            return {"status": "invalid", "reason": "NOT_FOUND_IN_DATABASE"}

                        # Transforma para o formato esperado
                        return {
                            "marca": vehicle_data.get("marca"),
                            "modelo": vehicle_data.get("modelo"),
                            "ano": vehicle_data.get("anoFabricacao"),
                            "anoModelo": vehicle_data.get("anoModelo"),
                            "cor": vehicle_data.get("cor")
                        }
                    return {"status": "invalid", "reason": "EMPTY_DATA"}

                # Erros no GET
                if response_get.status_code == 429:
                    return {"status": 429}
                if response_get.status_code >= 500:
                    return {"status": "invalid", "reason": f"GET_{response_get.status_code}_SERVER_ERROR"}

            return None

        except Exception as e:
            self.logger.error(f"Erro na requisição: {e}")
            return None

    def query_plate(self, plate: str) -> Optional[Dict[str, Any]]:
        """
        Consulta dados de uma placa com retry robusto para 429.

        Returns:
            - Dict com dados do veículo se sucesso
            - {"status": "invalid", "reason": "..."} se inválido (não retenta)
            - None para erro após todas as tentativas
        """
        plate_normalized = self.validate_plate(plate)

        if not plate_normalized:
            self.logger.warning(f"[{plate}] Formato inválido")
            return {"status": "invalid", "reason": "INVALID_PLATE_FORMAT"}

        # Retry robusto
        attempts = 0
        while attempts < self.MAX_RETRIES:
            result = self._query_once(plate_normalized)

            # Caso inválido: retorna imediatamente (não retenta)
            if result and result.get("status") == "invalid":
                return result

            # Rate limit 429: retenta com backoff
            if result and result.get("status") == 429:
                attempts += 1
                if attempts < self.MAX_RETRIES:
                    backoff_time = self.RETRY_BACKOFF + (attempts * 5)
                    self.logger.warning(f"[{plate}] 429 - Retry {attempts}/{self.MAX_RETRIES} em {backoff_time}s")
                    time.sleep(backoff_time)
                else:
                    self.logger.error(f"[{plate}] FALHOU após {self.MAX_RETRIES} tentativas (429)")
                    return None
            # Sucesso ou outro erro: retorna
            else:
                if attempts > 0:
                    self.logger.info(f"[{plate}] ✓ Sucesso após {attempts} retry(s)")
                return result

        return None


class PlacaAPIClient:
    """Cliente para consultar a API Placamaster com retry robusto."""

    BASE_URL = "https://placamaster.com/api/consulta-gratuita"
    TIMEOUT = 30
    MAX_RETRIES = 5
    RETRY_BACKOFF = 15

    def __init__(self, proxies: Optional[Dict[str, str]] = None):
        self.proxies = proxies
        self.logger = get_run_logger()

    def validate_plate(self, plate: str) -> Optional[str]:
        """Valida e normaliza placa."""
        if not plate:
            return None

        plate_clean = plate.replace("-", "").replace(" ", "").upper().strip()

        # Padrão Antigo: ABC1234
        pattern_old = r'^[A-Z]{3}\d{4}$'
        # Padrão Mercosul: ABC1D23
        pattern_mercosul = r'^[A-Z]{3}\d[A-Z]\d{2}$'

        if re.match(pattern_old, plate_clean) or re.match(pattern_mercosul, plate_clean):
            return plate_clean
        return None

    def _query_once(self, plate_normalized: str) -> Optional[Dict[str, Any]]:
        """Executa uma única requisição à API."""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
        }

        try:
            response = curl_requests.post(self.BASE_URL, json={"placa": plate_normalized}, headers=headers, proxies=self.proxies, impersonate="chrome124", timeout=self.TIMEOUT)

            # Casos irrecuperáveis
            if response.status_code == 400:
                return {"status": "invalid", "reason": response.json().get("error", "400_INVALID_DATA")}
            if response.status_code == 404:
                return {"status": "invalid", "reason": "404_NOT_FOUND"}
            if response.status_code == 403:
                return {"status": "invalid", "reason": "403_FORBIDDEN"}
            if response.status_code >= 500:
                return {"status": "invalid", "reason": f"{response.status_code}_SERVER_ERROR"}

            # Sucesso
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data"):
                    vehicle_data = data["data"]
                    if vehicle_data.get("marca") or vehicle_data.get("modelo"):
                        return vehicle_data
                    else:
                        return {"status": "invalid", "reason": "EMPTY_DATA"}
                return None

            # Rate limit
            if response.status_code == 429:
                return {"status": 429}

            return None

        except Exception as e:
            self.logger.error(f"Erro na requisição: {e}")
            return None

    def query_plate(self, plate: str) -> Optional[Dict[str, Any]]:
        """
        Consulta dados de uma placa com retry robusto para 429.

        Returns:
            - Dict com dados do veículo se sucesso
            - {"status": "invalid", "reason": "..."} se inválido (não retenta)
            - None para erro após todas as tentativas
        """
        plate_normalized = self.validate_plate(plate)

        if not plate_normalized:
            self.logger.warning(f"[{plate}] Formato inválido")
            return {"status": "invalid", "reason": "INVALID_PLATE_FORMAT"}

        # Retry robusto
        attempts = 0
        while attempts < self.MAX_RETRIES:
            result = self._query_once(plate_normalized)

            # Caso inválido: retorna imediatamente (não retenta)
            if result and result.get("status") == "invalid":
                return result

            # Rate limit 429: retenta com backoff
            if result and result.get("status") == 429:
                attempts += 1
                if attempts < self.MAX_RETRIES:
                    backoff_time = self.RETRY_BACKOFF + (attempts * 5)
                    self.logger.warning(f"[{plate}] 429 - Retry {attempts}/{self.MAX_RETRIES} em {backoff_time}s")
                    time.sleep(backoff_time)
                else:
                    self.logger.error(f"[{plate}] FALHOU após {self.MAX_RETRIES} tentativas (429)")
                    return None
            # Sucesso ou outro erro: retorna
            else:
                if attempts > 0:
                    self.logger.info(f"[{plate}] ✓ Sucesso após {attempts} retry(s)")
                return result

        return None
