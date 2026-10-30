/*
    permissoes_iot001.sql - permissões do app "MQTT SQL Server" na tabela dbo.IOT001.

    A tabela dbo.IOT001 JÁ EXISTE (definida pelo usuário). Este script NÃO cria nem altera a tabela: só garante o
    usuário do banco para o login "mqtt" e o GRANT mínimo (INSERT, SELECT). É idempotente.

    Ordem de execução
    -----------------
    1. Conecte no servidor TERRA\SOLO como dbo do banco MQTT ou como sysadmin (SSMS, Azure Data Studio ou sqlcmd).
       NÃO rode com o login "mqtt": ele não tem (nem deve ter) permissão de DDL.
    2. Pré-requisitos, feitos uma vez pelo DBA, fora deste script (a senha nunca vai para o repositório):
         - o login "mqtt" existe no servidor, com autenticação do SQL Server:
               CREATE LOGIN [mqtt] WITH PASSWORD = '<digite aqui, não salve>', CHECK_POLICY = ON;
         - o servidor está em modo de autenticação misto (SQL Server e Windows).
    3. Rode este script (o USE abaixo garante o banco MQTT). Exemplo com sqlcmd e autenticação do Windows:
           sqlcmd -S TERRA\SOLO -E -d MQTT -i sql\permissoes_iot001.sql
    4. No app, use "Testar conexão": deve mostrar que a tabela existe e que há permissão de INSERT.
*/
USE [MQTT];
GO

IF OBJECT_ID(N'dbo.IOT001', N'U') IS NULL
    RAISERROR(N'A tabela dbo.IOT001 não existe no banco MQTT. Crie-a antes (este script não cria tabela).', 16, 1);
GO

-- 1) Usuário do banco para o login "mqtt" (só se o login existir no servidor) -----------------------------------
IF SUSER_ID(N'mqtt') IS NOT NULL AND DATABASE_PRINCIPAL_ID(N'mqtt') IS NULL
BEGIN
    CREATE USER [mqtt] FOR LOGIN [mqtt] WITH DEFAULT_SCHEMA = [dbo];
    PRINT 'Usuário [mqtt] criado no banco MQTT.';
END
GO

-- 2) Permissão mínima: o app só insere e lê (GRANT é idempotente) -----------------------------------------------
IF DATABASE_PRINCIPAL_ID(N'mqtt') IS NOT NULL AND OBJECT_ID(N'dbo.IOT001', N'U') IS NOT NULL
BEGIN
    GRANT INSERT, SELECT ON OBJECT::dbo.IOT001 TO [mqtt];
    PRINT 'GRANT INSERT, SELECT em dbo.IOT001 para [mqtt].';
END
ELSE
    PRINT 'AVISO: usuário [mqtt] ou tabela dbo.IOT001 ausente; nada foi concedido.';
GO

/*
    SUGESTÃO (não executada; decisão do DBA/usuário): índice para consultas por dispositivo e período.
    Avalie antes o volume e os índices que já existem na tabela.

    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID(N'dbo.IOT001') AND name = N'IX_IOT001_device_id_ts_unix')
        CREATE NONCLUSTERED INDEX IX_IOT001_device_id_ts_unix
            ON dbo.IOT001 (device_id, ts_unix)
            INCLUDE (chd1_value, chd2_value, chd3_value);
*/
