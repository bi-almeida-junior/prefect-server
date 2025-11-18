from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, field_validator
from shared.utils import get_datetime_brasilia


class ForecastDay(BaseModel):
    """Modelo para um dia de previsão."""

    date: str
    full_date: str
    weekday: str
    max: int
    min: int
    humidity: int
    cloudiness: int
    rain: float
    rain_probability: int
    wind_speedy: str
    sunrise: str
    sunset: str
    moon_phase: str
    description: str
    condition: str

    class Config:
        populate_by_name = True


class WeatherResults(BaseModel):
    """Modelo para os resultados da API."""

    city: str
    temp: float
    humidity: int
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    forecast: List[ForecastDay]

    @field_validator('latitude', mode='before')
    @classmethod
    def validate_latitude(cls, v, info):
        """Aceita 'latitude' ou 'lat', converte float para string."""
        if v is not None:
            return str(v)
        lat = info.data.get('lat')
        return str(lat) if lat is not None else None

    @field_validator('longitude', mode='before')
    @classmethod
    def validate_longitude(cls, v, info):
        """Aceita 'longitude' ou 'lon', converte float para string."""
        if v is not None:
            return str(v)
        lon = info.data.get('lon')
        return str(lon) if lon is not None else None


class WeatherAPIResponse(BaseModel):
    """Modelo para resposta completa da API HGBrasil."""

    valid_key: bool
    results: WeatherResults

    def validate_key(self) -> None:
        """Valida se a API key é válida."""
        if not self.valid_key:
            raise ValueError("API key inválida")


def parse_api_response(raw_data: dict) -> WeatherAPIResponse:
    """
    Valida resposta da API usando Pydantic.

    Args:
        raw_data: Dados brutos da API

    Returns:
        Objeto WeatherAPIResponse validado

    Raises:
        ValueError: Se dados são inválidos
    """
    try:
        response = WeatherAPIResponse(**raw_data)
        response.validate_key()
        return response
    except Exception as e:
        raise ValueError(f"Erro ao validar resposta da API: {e}") from e


def transform_to_snowflake_row(cidade_id: int, results: WeatherResults, forecast_day: ForecastDay) -> Dict[str, Any]:
    """
    Transforma dados da API para formato Snowflake.

    Args:
        cidade_id: ID da cidade
        results: Resultados da API (dados atuais)
        forecast_day: Dia de previsão

    Returns:
        Dict pronto para inserção no Snowflake
    """
    forecast_date = datetime.strptime(forecast_day.full_date, '%d/%m/%Y').date()

    return {
        'ID_CIDADE': cidade_id,
        'NR_LATITUDE': results.latitude,
        'NR_LONGITUDE': results.longitude,
        'NR_TEMPERATURA_ATUAL': results.temp,
        'NR_UMIDADE_ATUAL': results.humidity,
        'DT_PREVISAO': forecast_date,
        'DS_DATA_FORMATADA': forecast_day.date,
        'DS_DATA_COMPLETA': forecast_day.full_date,
        'DS_DIA_SEMANA': forecast_day.weekday,
        'NR_TEMP_MAXIMA': forecast_day.max,
        'NR_TEMP_MINIMA': forecast_day.min,
        'NR_UMIDADE': forecast_day.humidity,
        'NR_NEBULOSIDADE': forecast_day.cloudiness,
        'NR_CHUVA_MM': forecast_day.rain,
        'NR_PROB_CHUVA': forecast_day.rain_probability,
        'DS_VENTO_VELOCIDADE': forecast_day.wind_speedy,
        'DS_HORARIO_NASCER_SOL': forecast_day.sunrise,
        'DS_HORARIO_POR_SOL': forecast_day.sunset,
        'DS_FASE_LUA': forecast_day.moon_phase,
        'DS_DESCRICAO_TEMPO': forecast_day.description,
        'DS_CONDICAO_TEMPO': forecast_day.condition,
        'DT_COLETA_API': get_datetime_brasilia()
    }
