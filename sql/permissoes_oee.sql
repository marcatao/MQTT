/*
    permissoes_oee.sql - permissões do Integrador MQTT nas tabelas do portal OEE.

    1) dbo.dispositivos_iot: cadastro dos dispositivos (SELECT, INSERT, UPDATE).
    2) Apontamentos reais pelo IoT (docs/apontamentos_reais.md, app/core/apontamentos_oee.py): SELECT em maquinas,
       dispositivos_iot, legendas e ordens_producao; SELECT, INSERT, UPDATE e DELETE em apontamentos_estado; UPDATE
       em maquinas (situação e OP atual) e ordens_producao (status); SELECT em calendarios e calendario_dias
       (fora de turno, 96; só se as tabelas existirem). O SELECT na IOT001 vem de sql/permissoes_iot001.sql.

    O integrador cadastra/atualiza os dispositivos (codigo = device_id) a cada diagnóstico recebido e renova
    ultimo_sinal_em com as leituras (ver app/core/oee_dispositivos.py). Ele usa o MESMO login do SQL Server da
    gravação na IOT001 ("mqtt") e acessa a tabela pelo nome em 3 partes [OEE].[dbo].[dispositivos_iot].

    Este script NÃO cria nem altera a tabela (ela é do portal OEE, criada pelas migrações do Laravel). Só garante o
    usuário do banco para o login "mqtt" e o GRANT mínimo: SELECT, INSERT, UPDATE. É idempotente.

    Atenção ao nome do banco: o padrão do integrador é OEE ([oee] banco = "OEE" no config.toml), mas o portal pode
    usar outro (o .env.example do OEE usa DB_DATABASE=OEE_Alumbra). Se for outro, troque o nome no USE abaixo e no
    config.toml do integrador ([oee] banco).

    Como rodar
    ----------
    1. Conecte como administrador (sysadmin, ou dono do banco do OEE). NÃO rode com o login "mqtt".
    2. O login "mqtt" já deve existir no servidor (o mesmo de sql/permissoes_iot001.sql).
    3. Exemplo com sqlcmd e autenticação do Windows:
           sqlcmd -S TERRA\SOLO -E -i sql\permissoes_oee.sql
    4. Sem este script o integrador funciona normalmente: só não cadastra os dispositivos no portal (um INFO no
       log: "Portal OEE não encontrado neste SQL Server ... cadastro de dispositivos desligado.").
*/
USE [OEE];
GO

IF OBJECT_ID(N'dbo.dispositivos_iot', N'U') IS NULL
    RAISERROR(N'A tabela dbo.dispositivos_iot não existe neste banco. Rode as migrações do portal OEE antes (este script não cria tabela).', 16, 1);
GO

-- 1) Usuário do banco para o login "mqtt" (só se o login existir no servidor) -----------------------------------
IF SUSER_ID(N'mqtt') IS NOT NULL AND DATABASE_PRINCIPAL_ID(N'mqtt') IS NULL
BEGIN
    CREATE USER [mqtt] FOR LOGIN [mqtt] WITH DEFAULT_SCHEMA = [dbo];
    PRINT 'Usuário [mqtt] criado no banco do OEE.';
END
GO

-- 2) Permissão mínima: o integrador lê, insere e atualiza (GRANT é idempotente) ---------------------------------
IF DATABASE_PRINCIPAL_ID(N'mqtt') IS NOT NULL AND OBJECT_ID(N'dbo.dispositivos_iot', N'U') IS NOT NULL
BEGIN
    GRANT SELECT, INSERT, UPDATE ON OBJECT::dbo.dispositivos_iot TO [mqtt];
    PRINT 'GRANT SELECT, INSERT, UPDATE em dbo.dispositivos_iot para [mqtt].';
END
ELSE
    PRINT 'AVISO: usuário [mqtt] ou tabela dbo.dispositivos_iot ausente; nada foi concedido.';
GO

-- 3) Apontamentos reais pelo IoT (GRANT é idempotente; cada tabela só se existir) ------------------------------
IF DATABASE_PRINCIPAL_ID(N'mqtt') IS NOT NULL
BEGIN
    IF OBJECT_ID(N'dbo.maquinas', N'U') IS NOT NULL
    BEGIN
        GRANT SELECT, UPDATE ON OBJECT::dbo.maquinas TO [mqtt];
        PRINT 'GRANT SELECT, UPDATE em dbo.maquinas para [mqtt].';
    END
    ELSE PRINT 'AVISO: tabela dbo.maquinas ausente; nada foi concedido nela.';

    IF OBJECT_ID(N'dbo.legendas', N'U') IS NOT NULL
    BEGIN
        GRANT SELECT ON OBJECT::dbo.legendas TO [mqtt];
        PRINT 'GRANT SELECT em dbo.legendas para [mqtt].';
    END
    ELSE PRINT 'AVISO: tabela dbo.legendas ausente; nada foi concedido nela.';

    IF OBJECT_ID(N'dbo.ordens_producao', N'U') IS NOT NULL
    BEGIN
        GRANT SELECT, UPDATE ON OBJECT::dbo.ordens_producao TO [mqtt];
        PRINT 'GRANT SELECT, UPDATE em dbo.ordens_producao para [mqtt].';
    END
    ELSE PRINT 'AVISO: tabela dbo.ordens_producao ausente; nada foi concedido nela.';

    IF OBJECT_ID(N'dbo.apontamentos_estado', N'U') IS NOT NULL
    BEGIN
        GRANT SELECT, INSERT, UPDATE, DELETE ON OBJECT::dbo.apontamentos_estado TO [mqtt];
        PRINT 'GRANT SELECT, INSERT, UPDATE, DELETE em dbo.apontamentos_estado para [mqtt].';
    END
    ELSE PRINT 'AVISO: tabela dbo.apontamentos_estado ausente; nada foi concedido nela.';

    -- Fora de turno (96): calendário da máquina (opcional; sem ele o integrador usa só os dias da semana)
    IF OBJECT_ID(N'dbo.calendarios', N'U') IS NOT NULL
    BEGIN
        GRANT SELECT ON OBJECT::dbo.calendarios TO [mqtt];
        PRINT 'GRANT SELECT em dbo.calendarios para [mqtt].';
    END
    ELSE PRINT 'AVISO: tabela dbo.calendarios ausente (portal sem calendários); nada foi concedido nela.';

    IF OBJECT_ID(N'dbo.calendario_dias', N'U') IS NOT NULL
    BEGIN
        GRANT SELECT ON OBJECT::dbo.calendario_dias TO [mqtt];
        PRINT 'GRANT SELECT em dbo.calendario_dias para [mqtt].';
    END
    ELSE PRINT 'AVISO: tabela dbo.calendario_dias ausente (portal sem calendários); nada foi concedido nela.';
END
ELSE
    PRINT 'AVISO: usuário [mqtt] ausente no banco do OEE; nada foi concedido para os apontamentos reais.';
GO

-- Conferência: deve listar INSERT, SELECT e UPDATE para [mqtt].
SELECT pr.name AS usuario, pe.permission_name, pe.state_desc
FROM sys.database_permissions AS pe
JOIN sys.database_principals AS pr ON pr.principal_id = pe.grantee_principal_id
WHERE pe.major_id = OBJECT_ID(N'dbo.dispositivos_iot') AND pr.name = N'mqtt'
ORDER BY pe.permission_name;
GO

-- Conferência dos apontamentos reais: permissões do [mqtt] nas tabelas usadas.
SELECT OBJECT_NAME(pe.major_id) AS tabela, pe.permission_name, pe.state_desc
FROM sys.database_permissions AS pe
JOIN sys.database_principals AS pr ON pr.principal_id = pe.grantee_principal_id
WHERE pr.name = N'mqtt'
  AND pe.major_id IN (OBJECT_ID(N'dbo.maquinas'), OBJECT_ID(N'dbo.dispositivos_iot'), OBJECT_ID(N'dbo.legendas'),
                      OBJECT_ID(N'dbo.ordens_producao'), OBJECT_ID(N'dbo.apontamentos_estado'),
                      OBJECT_ID(N'dbo.calendarios'), OBJECT_ID(N'dbo.calendario_dias'))
ORDER BY tabela, pe.permission_name;
GO
