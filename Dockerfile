FROM prefecthq/prefect:3-latest

WORKDIR /opt/prefect

# Instalar dependências do sistema para ODBC (SQL Server) - Oracle Linux/RHEL
RUN yum install -y curl \
    && curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo \
    && yum install -y \
    unixODBC \
    unixODBC-devel \
    && ACCEPT_EULA=Y yum install -y msodbcsql18 \
    && yum clean all \
    && rm -rf /var/cache/yum

# Copiar requirements.txt primeiro (melhor uso de cache do Docker)
COPY requirements.txt .

# Instalar dependências do Python de forma otimizada
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache/pip/*

# Copiar o código da aplicação
COPY . .

# Definir variáveis de ambiente padrão
ENV PYTHONPATH=/opt/prefect