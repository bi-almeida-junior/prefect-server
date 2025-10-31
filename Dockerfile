FROM prefecthq/prefect:3-latest

WORKDIR /opt/prefect

# Copiar requirements.txt primeiro (melhor uso de cache do Docker)
COPY requirements.txt .

# Instalar dependências do Python de forma otimizada
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache/pip/*

# Copiar o código da aplicação
COPY . .

# Definir variáveis de ambiente padrão
ENV PYTHONPATH=/opt/prefect