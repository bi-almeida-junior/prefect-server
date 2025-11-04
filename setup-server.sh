#!/bin/bash
# ==============================================================================
# Script de Instalação de Dependências do Servidor Prefect
# ==============================================================================
# Este script instala todas as dependências necessárias no servidor de destino
# para executar o Prefect Server com Docker
#
# Sistema Operacional: RHEL/CentOS/Rocky Linux/Fedora
# Execute como root ou com sudo
# ==============================================================================

set -e  # Parar em caso de erro

echo "===================================="
echo "Instalação de Dependências - Prefect Server"
echo "===================================="
echo ""

# Detectar distribuição
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
    VERSION=$VERSION_ID
    echo "Sistema detectado: $PRETTY_NAME"
else
    echo "Não foi possível detectar o sistema operacional"
    exit 1
fi

# Função para instalar Docker
install_docker() {
    echo ""
    echo ">>> Instalando Docker..."

    if command -v docker &> /dev/null; then
        echo "Docker já está instalado: $(docker --version)"
        return 0
    fi

    case $OS in
        "fedora")
            sudo dnf -y install dnf-plugins-core
            sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo
            sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            ;;
        "rhel"|"centos"|"rocky")
            sudo yum install -y yum-utils
            sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
            sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            ;;
        "ubuntu"|"debian")
            sudo apt-get update
            sudo apt-get install -y ca-certificates curl gnupg
            sudo install -m 0755 -d /etc/apt/keyrings
            curl -fsSL https://download.docker.com/linux/$OS/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
            sudo chmod a+r /etc/apt/keyrings/docker.gpg
            echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/$OS $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            sudo apt-get update
            sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
            ;;
        *)
            echo "Sistema operacional não suportado para instalação automática: $OS"
            echo "Por favor, instale o Docker manualmente: https://docs.docker.com/engine/install/"
            exit 1
            ;;
    esac

    # Habilitar e iniciar Docker
    sudo systemctl enable docker
    sudo systemctl start docker

    # Adicionar usuário atual ao grupo docker
    sudo usermod -aG docker $USER

    echo "Docker instalado com sucesso!"
    docker --version
}

# Função para instalar Docker Compose (standalone)
install_docker_compose() {
    echo ""
    echo ">>> Instalando Docker Compose (standalone)..."

    if command -v docker-compose &> /dev/null; then
        echo "Docker Compose já está instalado: $(docker-compose --version)"
        return 0
    fi

    # Verificar se docker compose plugin existe
    if docker compose version &> /dev/null; then
        echo "Docker Compose Plugin já está instalado"
        # Criar symlink para docker-compose
        if [ ! -f /usr/local/bin/docker-compose ]; then
            echo "Criando symlink para compatibilidade..."
            sudo ln -s /usr/libexec/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose 2>/dev/null || true
        fi
        return 0
    fi

    # Instalar Docker Compose standalone
    echo "Baixando Docker Compose v2.24.5..."
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.24.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

    sudo chmod +x /usr/local/bin/docker-compose

    echo "Docker Compose instalado com sucesso!"
    docker-compose --version
}

# Função para instalar Python e pip
install_python() {
    echo ""
    echo ">>> Verificando Python..."

    if command -v python3 &> /dev/null; then
        echo "Python já está instalado: $(python3 --version)"
    else
        echo "Instalando Python..."
        case $OS in
            "fedora")
                sudo dnf install -y python3 python3-pip
                ;;
            "rhel"|"centos"|"rocky")
                sudo yum install -y python3 python3-pip
                ;;
            "ubuntu"|"debian")
                sudo apt-get install -y python3 python3-pip
                ;;
        esac
    fi
}

# Função para criar diretório do projeto
setup_project_directory() {
    echo ""
    echo ">>> Configurando diretório do projeto..."

    PROJECT_DIR="/opt/prefect-server"

    if [ ! -d "$PROJECT_DIR" ]; then
        echo "Criando diretório: $PROJECT_DIR"
        sudo mkdir -p $PROJECT_DIR
        sudo chown $USER:$USER $PROJECT_DIR
    else
        echo "Diretório já existe: $PROJECT_DIR"
    fi
}

# Função para verificar portas
check_ports() {
    echo ""
    echo ">>> Verificando portas necessárias..."

    PORTS=(4200 5432 6379)
    for port in "${PORTS[@]}"; do
        if sudo ss -tulpn | grep ":$port " > /dev/null; then
            echo "AVISO: Porta $port está em uso!"
            sudo ss -tulpn | grep ":$port "
        else
            echo "Porta $port está disponível"
        fi
    done
}

# Função para verificar firewall
check_firewall() {
    echo ""
    echo ">>> Verificando firewall..."

    if command -v firewall-cmd &> /dev/null; then
        if sudo firewall-cmd --state &> /dev/null; then
            echo "Firewall está ativo"
            echo "Considerando abrir a porta 4200 para o Prefect UI..."
            read -p "Deseja abrir a porta 4200 no firewall? (y/n) " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                sudo firewall-cmd --permanent --add-port=4200/tcp
                sudo firewall-cmd --reload
                echo "Porta 4200 aberta no firewall"
            fi
        else
            echo "Firewall não está ativo"
        fi
    else
        echo "Firewall não detectado (firewalld não instalado)"
    fi
}

# Função para criar arquivo .env de exemplo
create_env_example() {
    echo ""
    echo ">>> Criando arquivo .env de exemplo..."

    PROJECT_DIR="/opt/prefect-server"

    if [ ! -f "$PROJECT_DIR/.env" ]; then
        cat > "$PROJECT_DIR/.env.example" << 'EOF'
# ==============================================================================
# Prefect Configuration
# ==============================================================================

# URL da API para uso interno (worker/client dentro do Docker)
PREFECT_API_URL=http://localhost:4200/api

# URL da API para o navegador (UI) - ALTERE para o IP/domínio do servidor
PREFECT_UI_API_URL=http://10.60.10.190:4200/api

# Porta externa do Prefect API (padrão: 4200)
PREFECT_API_PORT=4200

# ==============================================================================
# PostgreSQL Configuration
# ==============================================================================

POSTGRES_USER=prefect
POSTGRES_PASSWORD=MUDE_ESTA_SENHA
POSTGRES_DB=prefect
EOF
        echo "Arquivo .env.example criado em: $PROJECT_DIR/.env.example"
        echo ""
        echo "IMPORTANTE: Copie e edite o arquivo .env:"
        echo "  cp $PROJECT_DIR/.env.example $PROJECT_DIR/.env"
        echo "  nano $PROJECT_DIR/.env"
    else
        echo "Arquivo .env já existe"
    fi
}

# Função principal
main() {
    echo ""
    echo "Iniciando instalação das dependências..."
    echo ""

    # Verificar se está rodando como root
    if [ "$EUID" -eq 0 ]; then
        echo "AVISO: Não execute este script como root diretamente."
        echo "Execute como usuário normal que tenha acesso ao sudo."
        exit 1
    fi

    # Verificar sudo
    if ! sudo -v; then
        echo "ERRO: Você precisa de permissões sudo para executar este script."
        exit 1
    fi

    # Instalar dependências
    install_docker
    install_docker_compose
    install_python
    setup_project_directory
    check_ports
    check_firewall
    create_env_example

    echo ""
    echo "===================================="
    echo "Instalação Concluída!"
    echo "===================================="
    echo ""
    echo "Próximos passos:"
    echo "  1. Configure o arquivo .env:"
    echo "     cp /opt/prefect-server/.env.example /opt/prefect-server/.env"
    echo "     nano /opt/prefect-server/.env"
    echo ""
    echo "  2. Faça logout e login novamente para aplicar as permissões do Docker"
    echo "     (ou execute: newgrp docker)"
    echo ""
    echo "  3. Teste o Docker:"
    echo "     docker run hello-world"
    echo ""
    echo "  4. O deploy via GitHub Actions está pronto para ser usado!"
    echo ""
}

# Executar
main