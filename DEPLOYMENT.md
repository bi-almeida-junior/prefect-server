# Documentação de Deploy Automático

Este documento descreve como configurar e utilizar o sistema de deploy automático para o Prefect Server.

## Pré-requisitos do Servidor de Destino

Antes de configurar o GitHub Actions, certifique-se de que o servidor de destino (10.60.10.190) tem todas as dependências instaladas.

### Instalação Automática (Recomendado)

Execute o script de instalação no servidor de destino:

```bash
# SSH no servidor
ssh root@10.60.10.190

# Baixar o script (ou transferir via rsync/scp)
curl -O https://raw.githubusercontent.com/bi-almeida-junior/prefect-server/main/setup-server.sh

# Dar permissão de execução
chmod +x setup-server.sh

# Executar (como usuário normal com sudo, NÃO como root)
./setup-server.sh
```

### Instalação Manual

Se preferir instalar manualmente:

#### 1. Instalar Docker

```bash
# Fedora
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# RHEL/CentOS/Rocky Linux
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Iniciar Docker
sudo systemctl enable docker
sudo systemctl start docker

# Adicionar usuário ao grupo docker
sudo usermod -aG docker $USER
newgrp docker
```

#### 2. Instalar Docker Compose (standalone)

```bash
# Baixar Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.24.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# Dar permissão de execução
sudo chmod +x /usr/local/bin/docker-compose

# Verificar instalação
docker-compose --version
```

#### 3. Criar diretório do projeto

```bash
sudo mkdir -p /opt/prefect-server
sudo chown $USER:$USER /opt/prefect-server
```

#### 4. Verificar portas necessárias

```bash
# Verificar se as portas estão disponíveis
sudo ss -tulpn | grep -E ':(4200|5432|6379) '
```

#### 5. Configurar Firewall (se necessário)

```bash
# Abrir porta 4200 para o Prefect UI
sudo firewall-cmd --permanent --add-port=4200/tcp
sudo firewall-cmd --reload
```

## Configuração do GitHub Actions

### 1. Configurar Secrets no GitHub

Acesse o repositório no GitHub e vá para **Settings > Secrets and variables > Actions** e adicione os seguintes secrets:

#### Secrets Obrigatórios:

- **VM_HOST**: IP ou hostname da VM de destino
  - Valor: `10.60.10.190`

- **VM_USER**: Usuário para acessar a VM via SSH
  - Exemplo: `root` ou seu usuário específico

- **VM_PASSWORD**: Senha do usuário para acessar a VM
  - Use a senha do usuário configurado na VM
  - IMPORTANTE: Mantenha esta senha segura e não compartilhe

### 2. Criar arquivo .env na VM

O arquivo `.env` não é versionado por segurança. Você precisa criá-lo manualmente na VM:

```bash
# Na VM (10.60.10.190)
cd /opt/prefect-server

# Criar arquivo .env com as variáveis necessárias
cat > .env << 'EOF'
POSTGRES_USER=prefect
POSTGRES_PASSWORD=sua_senha_segura
POSTGRES_DB=prefect
PREFECT_UI_API_URL=http://10.60.10.190:9000/api
PREFECT_API_PORT=9000
EOF
```

## Como Funciona o Deploy Automático

### Trigger do Workflow

O workflow é acionado automaticamente quando há um `push` na branch `main`.

### Fluxo de Execução

1. **Checkout do Código**: Faz o checkout do repositório

2. **Detecção de Mudanças**: Identifica arquivos Python modificados no diretório `flows/`

3. **Deploy dos Arquivos**:
   - Sincroniza todos os arquivos para `/opt/prefect-server` na VM
   - Exclui arquivos desnecessários (`.git`, `__pycache__`, etc.)

4. **Restart dos Containers**:
   - Para os containers Docker existentes
   - Reconstrói e inicia os containers com as novas alterações

5. **Aguarda Inicialização**: Espera 30 segundos para os containers ficarem prontos

6. **Deploy dos Flows**:
   - Executa `docker exec -it prefect-client python /opt/prefect/<flow_file>` para cada arquivo modificado
   - Registra os flows no Prefect Server

### Exemplo de Uso

Quando você adicionar um novo flow:

```bash
# Criar novo arquivo de flow
mkdir -p flows/api
cat > flows/api/api_xpto.py << 'EOF'
from prefect import flow

@flow
def api_xpto():
    print("Executando API XPTO")

if __name__ == "__main__":
    api_xpto()
EOF

# Commit e push
git add flows/api/api_xpto.py
git commit -m "feat: adiciona flow API XPTO"
git push origin main
```

O GitHub Actions automaticamente:
1. Detectará a mudança em `flows/api/api_xpto.py`
2. Fará deploy para a VM
3. Executará: `docker exec -it prefect-client python /opt/prefect/flows/api/api_xpto.py`

## Scripts Auxiliares

### deploy_flow.sh

Deploy manual de um flow específico (executar na VM):

```bash
chmod +x deploy_flow.sh
./deploy_flow.sh flows/salesforce/salesforce_to_snowflake.py
```

### deploy_all_flows.sh

Deploy manual de todos os flows (executar na VM):

```bash
chmod +x deploy_all_flows.sh
./deploy_all_flows.sh
```

## Monitoramento

### Ver logs do workflow

Acesse: **Actions** no repositório do GitHub

### Ver logs dos containers na VM

```bash
# SSH na VM
ssh usuario@10.60.10.190

cd /opt/prefect-server

# Ver logs de todos os containers
docker compose logs -f

# Ver logs de um container específico
docker compose logs -f prefect-client
docker compose logs -f prefect-api
```

### Verificar status dos containers

```bash
docker compose ps
```

### Acessar o Prefect UI

Acesse: http://10.60.10.190:9000

## Troubleshooting

### Erro de Conexão SSH

Se o GitHub Actions não conseguir conectar na VM:

1. Verifique se o secret `VM_PASSWORD` está correto (senha do usuário)
2. Verifique se o secret `VM_USER` está correto (usuário da VM)
3. Verifique se o SSH está habilitado na VM
4. Verifique o firewall da VM (porta 22 deve estar aberta)
5. Teste a conexão manualmente: `ssh usuario@10.60.10.190`

### Erro ao Deploy do Flow

Se um flow falhar no deploy:

1. SSH na VM e execute manualmente:
   ```bash
   docker exec -it prefect-client python /opt/prefect/flows/seu_flow.py
   ```

2. Verifique os logs do container:
   ```bash
   docker compose logs prefect-client
   ```

### Containers não iniciam

Verifique se o arquivo `.env` existe e tem as variáveis corretas:

```bash
cat /opt/prefect-server/.env
```

## Segurança

- **Nunca** commite o arquivo `.env` no repositório
- **Nunca** commite senhas ou credenciais no código
- Use senhas fortes para:
  - Banco de dados PostgreSQL
  - Usuário SSH da VM
  - Qualquer outra credencial do sistema
- Mantenha os secrets do GitHub seguros e atualizados:
  - `VM_PASSWORD` deve ser uma senha forte
  - Rotacione as senhas periodicamente
  - Limite o acesso aos secrets apenas para pessoas autorizadas
- Configure o firewall da VM para aceitar conexões SSH apenas de IPs confiáveis (opcional, mas recomendado)
- Considere usar autenticação por chave SSH ao invés de senha para maior segurança (requer modificação do workflow)

## Estrutura de Diretórios na VM

```
/opt/prefect-server/
├── .env                           # Variáveis de ambiente (não versionado)
├── docker-compose.yml             # Configuração dos containers
├── Dockerfile                     # Build do container customizado
├── requirements.txt               # Dependências Python
├── flows/                         # Diretório de flows
│   ├── salesforce/
│   │   └── salesforce_to_snowflake.py
│   └── api/
│       └── api_xpto.py
├── shared/                        # Código compartilhado
│   ├── connections/
│   └── alerts.py
├── deploy_flow.sh                 # Script de deploy manual
└── deploy_all_flows.sh            # Script de deploy em lote
```