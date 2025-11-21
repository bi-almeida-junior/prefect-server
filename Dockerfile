FROM prefecthq/prefect:3-latest

WORKDIR /opt/prefect

# Instalar dependências do sistema para ODBC (SQL Server)
# Nota: A imagem base prefecthq/prefect:3-latest é Debian/Ubuntu, mesmo rodando em Oracle Linux host
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    apt-transport-https \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y \
    unixodbc \
    unixodbc-dev \
    msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements.txt primeiro (melhor uso de cache do Docker)
COPY requirements.txt .

# Instalar dependências do Python de forma otimizada
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache/pip/*

# Copiar o código da aplicação
COPY . .

# Definir variáveis de ambiente padrão
ENV PYTHONPATH=/opt/prefect