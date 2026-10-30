/*
    criar_tabela.sql - tabela de leituras do app "MQTT SQL Server" (formato longo: uma linha por campo medido).

    Idempotente: pode rodar quantas vezes quiser; só cria o que faltar.

    Ordem de execução
    -----------------
    1. Conecte no servidor TERRA\SOLO como dbo do banco MQTT ou como sysadmin
       (SSMS, Azure Data Studio ou sqlcmd). NÃO rode com o login "mqtt": ele não tem (nem deve ter) DDL.
    2. Pré-requisitos, feitos uma vez pelo DBA, fora deste script (a senha nunca vai para o repositório):
         - o banco MQTT existe:            CREATE DATABASE [MQTT];
         - o login "mqtt" existe no servidor, com autenticação do SQL Server:
               CREATE LOGIN [mqtt] WITH PASSWORD = '<digite aqui, não salve>', CHECK_POLICY = ON;
         - o servidor está em modo de autenticação misto (SQL Server e Windows).
    3. Rode este script no banco MQTT (o USE abaixo garante isso). Exemplo com sqlcmd e autenticação do Windows:
           sqlcmd -S TERRA\SOLO -E -d MQTT -i sql\criar_tabela.sql
    4. No app, use "Testar conexão": deve mostrar que a tabela existe e que há permissão de INSERT.
*/
USE [MQTT];
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- 1) Tabela -----------------------------------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.LeiturasMqtt', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.LeiturasMqtt (
        Id              BIGINT IDENTITY(1,1) NOT NULL,
        RecebidoEm      DATETIME2(3)   NOT NULL,  -- quando o app recebeu a mensagem (hora local)
        Topico          NVARCHAR(255)  NOT NULL,
        Dispositivo     NVARCHAR(128)  NULL,
        DataHoraLeitura DATETIME2(3)   NULL,      -- carimbo de tempo vindo no payload, se houver
        Campo           NVARCHAR(128)  NOT NULL,  -- nome do canal/campo (JSON aninhado vira "a.b.c")
        ValorNumerico   FLOAT          NULL,
        ValorTexto      NVARCHAR(4000) NULL,
        CONSTRAINT PK_LeiturasMqtt PRIMARY KEY CLUSTERED (Id)
    );
    PRINT 'Tabela dbo.LeiturasMqtt criada.';
END
ELSE
    PRINT 'Tabela dbo.LeiturasMqtt já existe.';
GO

-- 2) Índices ----------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'dbo.LeiturasMqtt') AND name = N'IX_LeiturasMqtt_RecebidoEm')
BEGIN
    CREATE NONCLUSTERED INDEX IX_LeiturasMqtt_RecebidoEm
        ON dbo.LeiturasMqtt (RecebidoEm);
    PRINT 'Índice IX_LeiturasMqtt_RecebidoEm criado.';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'dbo.LeiturasMqtt')
                 AND name = N'IX_LeiturasMqtt_Dispositivo_Campo_DataHora')
BEGIN
    CREATE NONCLUSTERED INDEX IX_LeiturasMqtt_Dispositivo_Campo_DataHora
        ON dbo.LeiturasMqtt (Dispositivo, Campo, DataHoraLeitura)
        INCLUDE (ValorNumerico);
    PRINT 'Índice IX_LeiturasMqtt_Dispositivo_Campo_DataHora criado.';
END
GO

-- 3) Usuário do banco para o login "mqtt" (só se o login existir no servidor) -----------------------------------
IF SUSER_ID(N'mqtt') IS NOT NULL AND DATABASE_PRINCIPAL_ID(N'mqtt') IS NULL
BEGIN
    CREATE USER [mqtt] FOR LOGIN [mqtt] WITH DEFAULT_SCHEMA = [dbo];
    PRINT 'Usuário [mqtt] criado no banco MQTT.';
END
GO

-- 4) Permissão mínima: o app só insere e lê (GRANT é idempotente) -----------------------------------------------
IF DATABASE_PRINCIPAL_ID(N'mqtt') IS NOT NULL
BEGIN
    GRANT INSERT, SELECT ON OBJECT::dbo.LeiturasMqtt TO [mqtt];
    PRINT 'GRANT INSERT, SELECT em dbo.LeiturasMqtt para [mqtt].';
END
ELSE
    PRINT 'AVISO: login/usuário [mqtt] não existe; crie o login (passo 2) e rode este script de novo.';
GO
