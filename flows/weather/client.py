from typing import Optional, Dict, Any
import urllib3
import requests
from prefect import get_run_logger

# Desabilita warnings de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class WeatherAPIClient:
    """Cliente para consultar a API HGBrasil Weather."""

    BASE_URL = "https://api.hgbrasil.com/weather"
    TIMEOUT = 10  # segundos

    def __init__(self, api_key: str):
        """
        Inicializa o cliente da API.

        Args:
            api_key: Chave de autenticação da API HGBrasil
        """
        self.api_key = api_key
        self.logger = get_run_logger()

    def fetch_weather(self, city_name: str) -> Optional[Dict[str, Any]]:
        """
        Busca dados climáticos de uma cidade.

        Args:
            city_name: Nome da cidade no formato "Cidade,UF" (ex: "Blumenau,SC")

        Returns:
            Dict com resposta da API ou None em caso de erro
        """
        try:
            self.logger.info(f"🌐 Consultando API para {city_name}...")

            params = {
                'key': self.api_key,
                'city_name': city_name
            }

            response = requests.get(self.BASE_URL, params=params, verify=True, timeout=self.TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"   ✓ Resposta recebida para {city_name}")
                return data
            else:
                self.logger.error(f"   ❌ HTTP {response.status_code} para {city_name}")
                return None

        except requests.exceptions.Timeout:
            self.logger.error(f"   ❌ Timeout ao consultar {city_name}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"   ❌ Erro de requisição para {city_name}: {type(e).__name__}")
            return None
        except Exception as e:
            self.logger.error(f"   ❌ Erro inesperado ao consultar {city_name}: {type(e).__name__}")
            return None
