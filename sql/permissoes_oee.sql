/*
    permissoes_oee.sql - permissões do Integrador MQTT na tabela dbo.dispositivos_iot do portal OEE.

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

-- Conferência: deve listar INSERT, SELECT e UPDATE para [mqtt].
SELECT pr.name AS usuario, pe.permission_name, pe.state_desc
FROM sys.database_permissions AS pe
JOIN sys.database_principals AS pr ON pr.principal_id = pe.grantee_principal_id
WHERE pe.major_id = OBJECT_ID(N'dbo.dispositivos_iot') AND pr.name = N'mqtt'
ORDER BY pe.permission_name;
GO
