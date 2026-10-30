/*
    Acrescenta à dbo.IOT001 a coluna calculada [data_hora_local]: o mesmo instante de [data_hora_utc], no fuso de
    Brasília (America/Sao_Paulo; no Windows/SQL Server o nome é 'E. South America Standard Time').

    - [data_hora_utc] continua como está: ts_unix é epoch em UTC e a coluna mostra UTC (3 h à frente de Brasília).
    - AT TIME ZONE usa as regras de fuso do Windows do servidor, então acompanha qualquer mudança futura de horário
      de verão sem alterar este script. Requer SQL Server 2016 ou mais novo.
    - A coluna não é PERSISTED (AT TIME ZONE não é determinístico); é calculada na consulta e não ocupa espaço.
    - A conta em dias + segundos evita o CONVERT(int, ts_unix), que estoura em 19/01/2038.
    - O app não precisa de mudança: ele não insere colunas calculadas.

    Como rodar: uma vez, como dbo/sysadmin, no banco MQTT. Pode ser rodado de novo sem efeito.
        sqlcmd -S TERRA\SOLO -E -d MQTT -i sql\data_hora_local_iot001.sql
*/
USE [MQTT];
GO

IF COL_LENGTH(N'dbo.IOT001', N'data_hora_local') IS NULL
BEGIN
    ALTER TABLE dbo.IOT001 ADD [data_hora_local] AS
        CONVERT(datetime2(0),
            (DATEADD(SECOND, CONVERT(int, [ts_unix] % 86400),
                DATEADD(DAY, CONVERT(int, [ts_unix] / 86400), CONVERT(datetime2(0), '1970-01-01')))
                AT TIME ZONE 'UTC')
            AT TIME ZONE 'E. South America Standard Time');
    PRINT 'Coluna dbo.IOT001.data_hora_local criada.';
END
ELSE
    PRINT 'Coluna dbo.IOT001.data_hora_local já existe.';
GO

-- Conferência: data_hora_local deve ficar 3 h atrás de data_hora_utc.
SELECT TOP (5) id, ts_unix, data_hora_utc, data_hora_local, recebido_em
FROM dbo.IOT001
ORDER BY id DESC;
GO
