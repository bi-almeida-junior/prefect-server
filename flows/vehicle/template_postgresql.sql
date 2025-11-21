-- ==============================================================================
-- Placas distintas BRZ_01_PLACA_RAW
-- ==============================================================================

-- Criação da tabela BRZ_01_PLACA_RAW no PostgreSQL
CREATE TABLE IF NOT EXISTS brz_01_placa_raw (
    ds_placa VARCHAR(10) PRIMARY KEY,
    ds_status CHAR(1) NOT NULL DEFAULT 'N',
    ds_motivo_erro VARCHAR(100),
    nr_tentativas INTEGER NOT NULL DEFAULT 0,
    dt_coleta TIMESTAMP,
    dt_insercao TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')
);

-- Comentários na tabela e colunas
COMMENT ON TABLE brz_01_placa_raw IS 'Dimensão de placas consolidadas de múltiplas fontes (WPS, Luminus GS/NR/NS)';
COMMENT ON COLUMN brz_01_placa_raw.ds_placa IS 'Placa do veículo (formato sem hífen, uppercase)';
COMMENT ON COLUMN brz_01_placa_raw.ds_status IS 'Status de processamento da placa: N=Novo, P=Processando, S=Sucesso, E=Erro, I=Inconsistente/Inválido';
COMMENT ON COLUMN brz_01_placa_raw.ds_motivo_erro IS 'Motivo do erro/invalidação. Preenchido apenas quando DS_STATUS IN (E,I). Valores possíveis: 400-Requisição inválida | 404-Placa não encontrada | 403-Acesso bloqueado/Cloudflare | 429-Rate limit excedido | 500-Erro servidor API | TIMEOUT-Timeout na requisição | EMPTY_DATA-API retornou dados vazios | INVALID_FORMAT-Formato de placa inválido | NULL quando sucesso';
COMMENT ON COLUMN brz_01_placa_raw.nr_tentativas IS 'Número de tentativas de processamento realizadas. Incrementado a cada retry em caso de erro temporário (status E)';
COMMENT ON COLUMN brz_01_placa_raw.dt_coleta IS 'Data e hora da última tentativa de coleta/processamento da placa';
COMMENT ON COLUMN brz_01_placa_raw.dt_insercao IS 'Data e hora de inserção do registro na dimensão';

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_brz_placa_status ON brz_01_placa_raw(ds_status);
CREATE INDEX IF NOT EXISTS idx_brz_placa_dt_coleta ON brz_01_placa_raw(dt_coleta);


-- ==============================================================================
-- Detalhes dos veículos BRZ_02_VEICULO_DETALHE
-- ==============================================================================

-- Criação da tabela BRZ_02_VEICULO_DETALHE no PostgreSQL
CREATE TABLE IF NOT EXISTS brz_02_veiculo_detalhe (
    ds_placa VARCHAR(20),
    ds_marca VARCHAR(100),
    ds_modelo VARCHAR(100),
    nr_ano_fabricacao INTEGER,
    nr_ano_modelo INTEGER,
    ds_cor VARCHAR(50),
    dt_coleta_api TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dt_insercao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Comentários na tabela e colunas
COMMENT ON TABLE brz_02_veiculo_detalhe IS 'Tabela Bronze com dados brutos de veículos coletados via RPA';
COMMENT ON COLUMN brz_02_veiculo_detalhe.ds_placa IS 'Placa do veículo (sem transformação)';
COMMENT ON COLUMN brz_02_veiculo_detalhe.ds_marca IS 'Marca do veículo';
COMMENT ON COLUMN brz_02_veiculo_detalhe.ds_modelo IS 'Modelo do veículo';
COMMENT ON COLUMN brz_02_veiculo_detalhe.nr_ano_fabricacao IS 'Ano de fabricação do veículo';
COMMENT ON COLUMN brz_02_veiculo_detalhe.nr_ano_modelo IS 'Ano do modelo do veículo';
COMMENT ON COLUMN brz_02_veiculo_detalhe.ds_cor IS 'Cor do veículo';
COMMENT ON COLUMN brz_02_veiculo_detalhe.dt_coleta_api IS 'Data e hora da coleta do dado pela api';
COMMENT ON COLUMN brz_02_veiculo_detalhe.dt_insercao IS 'Data e hora de inserção do registro';

-- Índice para busca por placa
CREATE INDEX IF NOT EXISTS idx_brz_veiculo_placa ON brz_02_veiculo_detalhe(ds_placa);
CREATE INDEX IF NOT EXISTS idx_brz_veiculo_dt_coleta ON brz_02_veiculo_detalhe(dt_coleta_api);


-- ==============================================================================
-- Detalhes dos veículos BRZ_04_VEICULO_FIPE
-- ==============================================================================

-- Criação da tabela BRZ_04_VEICULO_FIPE no PostgreSQL
CREATE TABLE IF NOT EXISTS brz_04_veiculo_fipe (
    ds_marca VARCHAR(100),
    ds_modelo VARCHAR(200),
    nr_ano_modelo INTEGER,
    ds_modelo_api VARCHAR(200),
    nr_ano_modelo_api INTEGER,
    fl_busca_alternativa BOOLEAN,
    ds_modelo_original VARCHAR(200),
    ds_anos_disponiveis VARCHAR(500),
    cd_marca_fipe VARCHAR(10),
    cd_modelo_fipe VARCHAR(10),
    cd_ano_combustivel VARCHAR(20),
    ds_combustivel VARCHAR(50),
    ds_sigla_combustivel VARCHAR(10),
    cd_fipe VARCHAR(20),
    vl_fipe VARCHAR(50),
    vl_fipe_numerico NUMERIC(15,2),
    ds_mes_referencia VARCHAR(50),
    nr_mes_referencia INTEGER,
    nr_ano_referencia INTEGER,
    cd_autenticacao VARCHAR(50),
    nr_tipo_veiculo INTEGER,
    dt_consulta_fipe TIMESTAMP,
    dt_insercao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Comentários na tabela e colunas
COMMENT ON TABLE brz_04_veiculo_fipe IS 'Tabela Bronze com dados de valores FIPE dos veículos consultados via API';
COMMENT ON COLUMN brz_04_veiculo_fipe.ds_marca IS 'Marca do veículo retornada pela FIPE';
COMMENT ON COLUMN brz_04_veiculo_fipe.ds_modelo IS 'Modelo completo do veículo retornado pela FIPE';
COMMENT ON COLUMN brz_04_veiculo_fipe.nr_ano_modelo IS 'Ano do modelo do veículo retornado pela FIPE';
COMMENT ON COLUMN brz_04_veiculo_fipe.ds_modelo_api IS 'Modelo do veículo conforme enviado na requisição da API';
COMMENT ON COLUMN brz_04_veiculo_fipe.nr_ano_modelo_api IS 'Ano do modelo conforme enviado na requisição da API';
COMMENT ON COLUMN brz_04_veiculo_fipe.fl_busca_alternativa IS 'Indica se foi necessária busca por modelo/ano alternativo';
COMMENT ON COLUMN brz_04_veiculo_fipe.ds_modelo_original IS 'Modelo original buscado antes de usar alternativa';
COMMENT ON COLUMN brz_04_veiculo_fipe.ds_anos_disponiveis IS 'Lista de anos disponíveis encontrados para o modelo original';
COMMENT ON COLUMN brz_04_veiculo_fipe.cd_marca_fipe IS 'Código da marca na tabela FIPE (ex: 25 para Honda)';
COMMENT ON COLUMN brz_04_veiculo_fipe.cd_modelo_fipe IS 'Código do modelo na tabela FIPE (ex: 6779)';
COMMENT ON COLUMN brz_04_veiculo_fipe.cd_ano_combustivel IS 'Código do ano-combustível usado na consulta (ex: 2016-5)';
COMMENT ON COLUMN brz_04_veiculo_fipe.ds_combustivel IS 'Tipo de combustível (ex: Flex, Gasolina, Diesel)';
COMMENT ON COLUMN brz_04_veiculo_fipe.ds_sigla_combustivel IS 'Sigla do combustível (ex: F, G, D)';
COMMENT ON COLUMN brz_04_veiculo_fipe.cd_fipe IS 'Código FIPE do veículo';
COMMENT ON COLUMN brz_04_veiculo_fipe.vl_fipe IS 'Valor FIPE do veículo (formato: R$ 99.999,00)';
COMMENT ON COLUMN brz_04_veiculo_fipe.vl_fipe_numerico IS 'Valor FIPE convertido para número';
COMMENT ON COLUMN brz_04_veiculo_fipe.ds_mes_referencia IS 'Mês de referência da tabela FIPE (ex: novembro de 2025)';
COMMENT ON COLUMN brz_04_veiculo_fipe.nr_mes_referencia IS 'Número do mês de referência (1-12, extraído de ds_mes_referencia)';
COMMENT ON COLUMN brz_04_veiculo_fipe.nr_ano_referencia IS 'Ano de referência (extraído de ds_mes_referencia)';
COMMENT ON COLUMN brz_04_veiculo_fipe.cd_autenticacao IS 'Código de autenticação da consulta FIPE';
COMMENT ON COLUMN brz_04_veiculo_fipe.nr_tipo_veiculo IS 'Tipo do veículo (1=Carro, 2=Moto, 3=Caminhão)';
COMMENT ON COLUMN brz_04_veiculo_fipe.dt_consulta_fipe IS 'Data e hora da consulta na API FIPE';
COMMENT ON COLUMN brz_04_veiculo_fipe.dt_insercao IS 'Data e hora de inserção do registro';

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_brz_fipe_marca_modelo ON brz_04_veiculo_fipe(ds_marca, ds_modelo);
CREATE INDEX IF NOT EXISTS idx_brz_fipe_dt_consulta ON brz_04_veiculo_fipe(dt_consulta_fipe);


-- ==============================================================================
-- Valores dos veículos FATO_VEICULO_FIPE
-- ==============================================================================

CREATE TABLE IF NOT EXISTS fato_veiculo_fipe AS
SELECT
    d.ds_placa,
    f.ds_marca,
    f.ds_modelo,
    f.nr_ano_modelo,
    d.nr_ano_fabricacao,
    d.ds_cor,
    f.ds_modelo_api,
    f.nr_ano_modelo_api,
    f.fl_busca_alternativa,
    f.ds_modelo_original,
    f.ds_anos_disponiveis,
    f.cd_marca_fipe,
    f.cd_modelo_fipe,
    f.cd_ano_combustivel,
    f.ds_combustivel,
    f.ds_sigla_combustivel,
    f.cd_fipe,
    f.vl_fipe,
    f.vl_fipe_numerico,
    f.ds_mes_referencia,
    f.nr_mes_referencia,
    f.nr_ano_referencia,
    f.cd_autenticacao,
    f.nr_tipo_veiculo,
    d.dt_coleta_api,
    f.dt_consulta_fipe,
    CURRENT_TIMESTAMP AS dt_insercao
FROM brz_04_veiculo_fipe f
INNER JOIN brz_02_veiculo_detalhe d
    ON UPPER(TRIM(f.ds_marca)) = UPPER(TRIM(d.ds_marca))
    AND UPPER(TRIM(f.ds_modelo)) = UPPER(TRIM(d.ds_modelo))
    AND f.nr_ano_modelo = d.nr_ano_modelo;

-- Comentários
COMMENT ON TABLE fato_veiculo_fipe IS 'Tabela fato com dados consolidados de veículos (detalhes + FIPE)';

-- Índices
CREATE INDEX IF NOT EXISTS idx_fato_veiculo_placa ON fato_veiculo_fipe(ds_placa);
CREATE INDEX IF NOT EXISTS idx_fato_veiculo_marca_modelo ON fato_veiculo_fipe(ds_marca, ds_modelo);
