-- ====================================================================
-- SCRIPT DDL - TABELAS DE CLIMA
-- ====================================================================
-- Database: AJ_DATALAKEHOUSE_RPA
-- Schema: BRONZE
--
-- ESTRATÉGIA DE DADOS:
--
-- 1. BRZ_CLIMA_TEMPO (Histórico - APPEND ONLY)
--    - Armazena APENAS o primeiro registro (condições atuais no momento da coleta)
--    - Executado a cada hora = 24 registros/dia/cidade
--    - Nunca sobrescreve, sempre INSERT
--    - Dados reais observados
--
-- 2. BRZ_CLIMA_TEMPO_PREVISAO (Previsão - FULL REFRESH)
--    - Armazena os 15 dias de previsão futura
--    - TRUNCATE + INSERT a cada execução
--    - Sempre tem a previsão mais atualizada
--    - Dados previstos pela API
-- ====================================================================

USE DATABASE AJ_DATALAKEHOUSE_RPA;
USE SCHEMA BRONZE;

-- ====================================================================
-- TABELA 1: BRZ_CLIMA_TEMPO (Clima Atual - Histórico)
-- ====================================================================
-- Esta tabela já existe conforme especificação fornecida
-- Mantida para referência e documentação

CREATE OR REPLACE TABLE BRZ_CLIMA_TEMPO (
    ID_CIDADE                NUMBER(10),                                 -- FK para DIM_CIDADE
    NR_LATITUDE              VARCHAR(20),                                -- Latitude da cidade
    NR_LONGITUDE             VARCHAR(20),                                -- Longitude da cidade
    NR_TEMPERATURA_ATUAL     NUMBER(5,2),                                -- Temperatura atual em °C
    NR_UMIDADE_ATUAL         NUMBER(5,2),                                -- Umidade atual em %
    DT_PREVISAO              DATE,                                       -- Data da previsão
    DS_DATA_FORMATADA        VARCHAR(10),                                -- Data no formato DD/MM
    DS_DATA_COMPLETA         VARCHAR(10),                                -- Data no formato DD/MM/YYYY
    DS_DIA_SEMANA            VARCHAR(10),                                -- Dia da semana abreviado
    NR_TEMP_MAXIMA           NUMBER(5,2),                                -- Temperatura máxima em °C
    NR_TEMP_MINIMA           NUMBER(5,2),                                -- Temperatura mínima em °C
    NR_UMIDADE               NUMBER(5,2),                                -- Umidade prevista em %
    NR_NEBULOSIDADE          NUMBER(5,2),                                -- Nebulosidade em %
    NR_CHUVA_MM              NUMBER(10,2),                               -- Volume de chuva em mm
    NR_PROB_CHUVA            NUMBER(5,2),                                -- Probabilidade de chuva em %
    DS_VENTO_VELOCIDADE      VARCHAR(20),                                -- Velocidade do vento
    DS_HORARIO_NASCER_SOL    VARCHAR(10),                                -- Horário do nascer do sol
    DS_HORARIO_POR_SOL       VARCHAR(10),                                -- Horário do pôr do sol
    DS_FASE_LUA              VARCHAR(50),                                -- Fase da lua
    DS_DESCRICAO_TEMPO       VARCHAR(100),                               -- Descrição do tempo
    DS_CONDICAO_TEMPO        VARCHAR(50),                                -- Condição do tempo (código)
    DT_COLETA_API            TIMESTAMP_NTZ,                              -- Data/hora da coleta da API
    DT_INSERCAO              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()   -- Data/hora de inserção
);

-- ====================================================================
-- TABELA 2: BRZ_CLIMA_TEMPO_PREVISAO (Previsão 15 dias - Full Refresh)
-- ====================================================================
-- Esta tabela armazena a previsão dos próximos 15 dias
-- Toda execução faz TRUNCATE + INSERT para manter dados sempre atualizados

CREATE OR REPLACE TABLE BRZ_CLIMA_TEMPO_PREVISAO (
    ID_CIDADE                NUMBER(10),                                 -- FK para DIM_CIDADE
    NR_LATITUDE              VARCHAR(20),                                -- Latitude da cidade
    NR_LONGITUDE             VARCHAR(20),                                -- Longitude da cidade
    NR_TEMPERATURA_ATUAL     NUMBER(5,2),                                -- Temperatura atual em °C (referência)
    NR_UMIDADE_ATUAL         NUMBER(5,2),                                -- Umidade atual em % (referência)
    DT_PREVISAO              DATE,                                       -- Data da previsão
    DS_DATA_FORMATADA        VARCHAR(10),                                -- Data no formato DD/MM
    DS_DATA_COMPLETA         VARCHAR(10),                                -- Data no formato DD/MM/YYYY
    DS_DIA_SEMANA            VARCHAR(10),                                -- Dia da semana abreviado
    NR_TEMP_MAXIMA           NUMBER(5,2),                                -- Temperatura máxima em °C
    NR_TEMP_MINIMA           NUMBER(5,2),                                -- Temperatura mínima em °C
    NR_UMIDADE               NUMBER(5,2),                                -- Umidade prevista em %
    NR_NEBULOSIDADE          NUMBER(5,2),                                -- Nebulosidade em %
    NR_CHUVA_MM              NUMBER(10,2),                               -- Volume de chuva em mm
    NR_PROB_CHUVA            NUMBER(5,2),                                -- Probabilidade de chuva em %
    DS_VENTO_VELOCIDADE      VARCHAR(20),                                -- Velocidade do vento
    DS_HORARIO_NASCER_SOL    VARCHAR(10),                                -- Horário do nascer do sol
    DS_HORARIO_POR_SOL       VARCHAR(10),                                -- Horário do pôr do sol
    DS_FASE_LUA              VARCHAR(50),                                -- Fase da lua
    DS_DESCRICAO_TEMPO       VARCHAR(100),                               -- Descrição do tempo
    DS_CONDICAO_TEMPO        VARCHAR(50),                                -- Condição do tempo (código)
    DT_COLETA_API            TIMESTAMP_NTZ,                              -- Data/hora da coleta da API
    DT_INSERCAO              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()   -- Data/hora de inserção
);

-- ====================================================================
-- COMENTÁRIOS DAS TABELAS
-- ====================================================================

COMMENT ON TABLE BRZ_CLIMA_TEMPO IS
'Tabela histórica de clima atual. Armazena condições climáticas reais observadas no momento da coleta (a cada hora). APPEND ONLY - nunca sobrescreve dados. Gera 24 registros/dia/cidade.';

COMMENT ON TABLE BRZ_CLIMA_TEMPO_PREVISAO IS
'Tabela de previsão climática (15 dias). FULL REFRESH a cada execução (TRUNCATE + INSERT). Sempre contém a previsão mais atualizada disponível na API.';

-- ====================================================================
-- REFERÊNCIA: DIM_CIDADE
-- ====================================================================
-- A tabela DIM_CIDADE contém os IDs usados como FK:
-- (1, 'Blumenau')
-- (2, 'Balneário Camboriú')
-- (3, 'Joinville')
-- (4, 'São José')
-- (5, 'Criciúma')
-- ====================================================================

-- ====================================================================
-- EXEMPLO DE USO
-- ====================================================================

-- Ver clima atual das últimas 24 horas de Blumenau:
-- SELECT * FROM BRZ_CLIMA_TEMPO
-- WHERE ID_CIDADE = 1
-- ORDER BY DT_COLETA_API DESC
-- LIMIT 24;

-- Ver previsão dos próximos dias de todas as cidades:
-- SELECT
--     ID_CIDADE,
--     DT_PREVISAO,
--     DS_DIA_SEMANA,
--     NR_TEMP_MINIMA,
--     NR_TEMP_MAXIMA,
--     NR_PROB_CHUVA,
--     DS_DESCRICAO_TEMPO
-- FROM BRZ_CLIMA_TEMPO_PREVISAO
-- ORDER BY ID_CIDADE, DT_PREVISAO;

-- Comparar previsão vs realidade (o que foi previsto vs o que aconteceu):
-- SELECT
--     p.ID_CIDADE,
--     p.DT_PREVISAO,
--     p.NR_TEMP_MAXIMA AS PREV_MAX,
--     r.NR_TEMP_MAXIMA AS REAL_MAX,
--     p.NR_PROB_CHUVA AS PREV_CHUVA,
--     r.NR_CHUVA_MM AS REAL_CHUVA
-- FROM BRZ_CLIMA_TEMPO_PREVISAO p
-- LEFT JOIN BRZ_CLIMA_TEMPO r
--     ON p.ID_CIDADE = r.ID_CIDADE
--     AND p.DT_PREVISAO = r.DT_PREVISAO
-- WHERE p.DT_PREVISAO < CURRENT_DATE()
-- ORDER BY p.ID_CIDADE, p.DT_PREVISAO;

-- ====================================================================