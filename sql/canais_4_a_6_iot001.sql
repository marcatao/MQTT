/*
    canais_4_a_6_iot001.sql - acrescenta as colunas chd4_value, chd5_value e chd6_value na dbo.IOT001.

    Decidido com o usuário em 11/11/2026: os NOVUS DigiRail têm 6 canais digitais; até aqui só chd1..chd3 tinham
    coluna e os canais 4 a 6 ficavam só no payload_json. Este script só ACRESCENTA colunas que aceitam NULL:
    nenhuma linha é apagada ou modificada, e as linhas antigas ficam com NULL nas colunas novas.

    É idempotente: cada coluna só é criada se ainda não existir (COL_LENGTH ... IS NULL). Pode rodar de novo.

    O integrador não precisa ser parado: ele detecta as colunas da tabela a cada conexão com o banco
    (INFORMATION_SCHEMA). Antes do script, grava só chd1..chd3; depois, na próxima conexão (ou ao reabrir o
    programa), passa a gravar também chd4..chd6. O "Testar conexão" mostra se as colunas já existem.

    Como rodar
    ----------
    1. Conecte no servidor TERRA\SOLO como dbo do banco MQTT ou como sysadmin (SSMS, Azure Data Studio ou sqlcmd).
       NÃO rode com o login "mqtt": ele só tem INSERT e SELECT (sem permissão de alterar a tabela).
    2. Rode este script (o USE abaixo garante o banco MQTT). Exemplo com sqlcmd e autenticação do Windows:
           sqlcmd -S TERRA\SOLO -E -d MQTT -i sql\canais_4_a_6_iot001.sql
    3. Confira o resultado da consulta no final (3 linhas, decimal(18,4), aceita NULL).

    Não é preciso GRANT novo: o GRANT INSERT, SELECT de sql/permissoes_iot001.sql vale para a tabela inteira,
    inclusive colunas novas.
*/
USE [MQTT];
GO

IF OBJECT_ID(N'dbo.IOT001', N'U') IS NULL
    RAISERROR(N'A tabela dbo.IOT001 não existe no banco MQTT. Este script não cria a tabela.', 16, 1);
GO

IF OBJECT_ID(N'dbo.IOT001', N'U') IS NOT NULL AND COL_LENGTH(N'dbo.IOT001', N'chd4_value') IS NULL
BEGIN
    ALTER TABLE [dbo].[IOT001] ADD [chd4_value] decimal(18, 4) NULL;
    PRINT 'Coluna chd4_value criada.';
END
ELSE
    PRINT 'Coluna chd4_value já existe (nada a fazer).';
GO

IF OBJECT_ID(N'dbo.IOT001', N'U') IS NOT NULL AND COL_LENGTH(N'dbo.IOT001', N'chd5_value') IS NULL
BEGIN
    ALTER TABLE [dbo].[IOT001] ADD [chd5_value] decimal(18, 4) NULL;
    PRINT 'Coluna chd5_value criada.';
END
ELSE
    PRINT 'Coluna chd5_value já existe (nada a fazer).';
GO

IF OBJECT_ID(N'dbo.IOT001', N'U') IS NOT NULL AND COL_LENGTH(N'dbo.IOT001', N'chd6_value') IS NULL
BEGIN
    ALTER TABLE [dbo].[IOT001] ADD [chd6_value] decimal(18, 4) NULL;
    PRINT 'Coluna chd6_value criada.';
END
ELSE
    PRINT 'Coluna chd6_value já existe (nada a fazer).';
GO

-- Conferência: devem aparecer 3 linhas (chd4_value, chd5_value, chd6_value), decimal(18,4), IS_NULLABLE = YES.
SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = N'dbo' AND TABLE_NAME = N'IOT001'
  AND COLUMN_NAME IN (N'chd4_value', N'chd5_value', N'chd6_value')
ORDER BY COLUMN_NAME;
GO
