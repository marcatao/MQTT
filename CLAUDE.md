# Integrador Alumbra MQTT + SQL + Mosquitto (orquestrador do projeto)

Este arquivo é a memória permanente do projeto. Ele é carregado em toda sessão do Claude Code, e quem lê é o
**orquestrador**: a sessão principal, que entende o objetivo, divide o trabalho, delega aos agentes especialistas e
confere o resultado antes de entregar.

## Objetivo do sistema

Aplicativo **desktop para Windows**, escrito em **Python**, que:

1. Conecta em um broker **MQTT** e assina um tópico.
2. Recebe dados enviados por dispositivos em **JSON** ou em formato **tabular** (linhas delimitadas, tipo CSV).
3. **Formata** esses dados (valida, normaliza tipos, datas e nomes de campos).
4. **Salva** os registros em uma tabela no **SQL Server interno** do usuário.

### Interface gráfica (requisitos mínimos)

- Campos para preencher: **BROKER**, **PORTA** e **TÓPICO**.
- Botão **Conectar** (vira **Desconectar** quando conectado).
- Caixa de **logs** mostrando o estado da conexão e o que está sendo tratado (mensagem recebida, formato detectado,
  registros gravados, erros).
- A tela precisa ser agradável: visual limpo, moderno e responsivo, sem travar enquanto chegam mensagens.

## Agentes especialistas (`.claude/agents/`)

| Agente | Quando delegar |
| --- | --- |
| `python-gui` | Tudo que é tela: layout, estilo, tema, componentes, caixa de logs, ícones, empacotamento do `.exe`. |
| `mqtt-python` | Conexão MQTT, assinatura, reconexão, parsing de JSON/tabular, fila de processamento, pipeline e `bridge.py`. |
| `sqlserver` | Conexão com o SQL Server, teste de conexão, tabela e scripts em `sql/`, gravação em lote e erros do banco. |

Regras do orquestrador:

- Antes de delegar, passe ao agente o objetivo concreto, os arquivos envolvidos e as decisões já tomadas abaixo.
- Trabalho de tela e de dados que não dependem um do outro pode ir em paralelo.
- A **fronteira** entre os dois é a interface `core ↔ gui` descrita em "Arquitetura": o agente de GUI não abre
  conexão MQTT nem SQL, e o agente MQTT não importa nada de Qt fora do adaptador de sinais.
- Confira o que voltou (leia o diff, rode os testes) antes de dizer ao usuário que está pronto.
- Se uma decisão nova for tomada com o usuário, **atualize este arquivo** para que ela não se perca.

## Decisões técnicas

- **Python 3.12+**, Windows 10/11 como alvo. Desenvolvimento pode ser testado em Linux, mas a entrega é Windows.
- **GUI:** PySide6 (Qt 6), com folha de estilo (QSS) própria e suporte a tema claro/escuro.
- **MQTT:** `paho-mqtt` 2.x (`CallbackAPIVersion.VERSION2`), rede em thread própria (`loop_start`), reconexão
  automática com backoff.
- **JSON:** `orjson` (mais rápido que `json`); fallback para `json` se não estiver instalado.
- **Tabular:** módulo `csv` da biblioteca padrão, com detecção de delimitador (`;`, `,`, `\t`, `|`).
- **SQL Server:** `pyodbc` + "ODBC Driver 18 for SQL Server", `fast_executemany = True`, gravação **em lotes**
  (por quantidade ou por tempo, o que vier primeiro), sempre com parâmetros (nunca concatenar SQL).
- **Configuração:** arquivo `config.toml` (lido com `tomllib`) para SQL Server e opções; os campos da tela lembram o
  último valor usado. Senhas não ficam em texto puro no repositório (preferir autenticação do Windows ou `keyring`).
- **Logs:** módulo `logging`, com um handler que envia para a caixa de logs da tela e outro para arquivo rotativo.
- **Empacotamento:** PyInstaller gerando um `.exe` para Windows.

## Arquitetura

```
app/
  main.py            # ponto de entrada: cria QApplication e a janela
  gui/               # (agente python-gui) janela, widgets, estilo QSS, ícones
  core/              # (agente mqtt-python) sem dependência de Qt
    mqtt_client.py   # conexão, assinatura, reconexão
    parser.py        # detecta JSON x tabular e normaliza em registros (list[dict])
    sql_conexao.py   # (agente sqlserver) string de conexão, teste, tradução de erros
    sink_sqlserver.py# fila + gravação em lote via pyodbc (usa sql_conexao)
    pipeline.py      # liga mqtt → parser → sink, expõe eventos
  bridge.py          # adaptador: eventos do core viram Signals do Qt
tests/
```

Fluxo: thread do paho recebe a mensagem → coloca numa `queue.Queue` → thread de processamento faz parsing e grava em
lote → eventos (status, log, contadores) chegam à GUI **só por Signals do Qt**. Nunca atualizar widget fora da
thread principal; nunca bloquear a thread da GUI com rede ou banco.

## Valores padrão (broker Mosquitto da Alumbra)

Vindos da configuração que o usuário já usa (nó MQTT do Node-RED). A tela abre com eles preenchidos:

| Campo | Valor |
| --- | --- |
| BROKER | `192.168.100.162` |
| PORTA | `1883` |
| TÓPICO | `NOVUS/Teste_Alumbra/#` (dispositivos NOVUS publicando abaixo desse prefixo) |
| QoS | `2` |
| Saída | auto-detecção: objeto JSON, texto (inclusive tabular) ou binário |

A tela também tem um seletor de **QoS** (0, 1, 2). Os padrões ficam em `app/config.py` e podem ser sobrescritos em
`config.toml` (modelo em `config.example.toml`); o último valor digitado na tela vale mais que ambos (`QSettings`).

## SQL Server do usuário

| Campo | Valor |
| --- | --- |
| Servidor | `TERRA\SOLO` (instância nomeada) |
| Autenticação | SQL Server |
| Usuário | `mqtt` |
| Banco | `MQTT` |
| Criptografar | Opcional (`Encrypt=Optional`) |
| Confiar no certificado | não |

**A senha não fica no repositório** (nem em código, testes, logs ou commits). O usuário digita na tela; com
"Lembrar senha" marcado ela vai para o Gerenciador de Credenciais do Windows via `keyring`
(serviço `"MQTT SQL Server"`, usuário = login do SQL).

A tela tem uma seção **Banco de dados** com: Servidor, Autenticação (Windows / SQL Server), Usuário, Senha,
Lembrar senha, Banco, Criptografar (Opcional / Obrigatório / Estrito), Confiar no certificado, e o botão
**Testar conexão**.

## Tabela no SQL Server: `dbo.IOT001` (definida pelo usuário, já existe no banco `MQTT`)

**Formato largo: uma linha por leitura** (uma mensagem JSON com uma leitura = uma linha; uma lista de leituras =
várias linhas). O app não cria nem altera esta tabela; só insere.

```sql
CREATE TABLE [dbo].[IOT001](
    [id]            bigint IDENTITY(1,1) NOT NULL,   -- PK_IOT001; nunca inserir
    [pid]           bigint NOT NULL,
    [device_id]     varchar(100) NOT NULL,
    [ts_unix]       bigint NOT NULL,                 -- epoch em segundos (UTC)
    [data_hora_utc] AS (dateadd(second, CONVERT(int, ts_unix), CONVERT(datetime2(0), '1970-01-01'))),  -- calculada; nunca inserir
    [chd1_value]    decimal(18, 4) NULL,
    [chd2_value]    decimal(18, 4) NULL,
    [chd3_value]    decimal(18, 4) NULL,
    [payload_json]  nvarchar(max) NULL,              -- payload original, como chegou
    [recebido_em]   datetime2(3) NOT NULL            -- DEFAULT sysutcdatetime(): UTC
    -- [data_hora_local] (calculada, opcional): data_hora_utc em America/Sao_Paulo; ver sql/data_hora_local_iot001.sql
);
```

**Fuso:** tudo é gravado em UTC (`ts_unix`, `data_hora_utc`, `recebido_em`). A hora de Brasília é só para exibição,
pela coluna calculada `data_hora_local` (`AT TIME ZONE 'E. South America Standard Time'`), decidida com o usuário
em 30/10/2026. O app não muda por causa disso.

Regras de mapeamento payload → linha:

- Colunas inseridas: `pid, device_id, ts_unix, chd1_value, chd2_value, chd3_value, payload_json, recebido_em`.
- `recebido_em` em **UTC** (mesmo critério do DEFAULT da tabela), hora em que o app recebeu a mensagem.
- `device_id` é `varchar` (não Unicode): enviar como `SQL_VARCHAR`; se faltar no payload, usar o dispositivo deduzido do
  tópico (nível após o prefixo da assinatura).
- `ts_unix` em segundos: converter de ms/µs/ISO 8601 se vier assim. `pid`, `device_id` e `ts_unix` são `NOT NULL`:
  linha sem algum deles não é inventada; vai para a contingência com o motivo no log.
- `chdN_value`: aceitar `chdN_value`, `chdN.value` (JSON aninhado) ou `chdN`; `decimal(18,4)` — arredondar para 4
  casas, valor fora da faixa vai como NULL com aviso.
- **Payload real do NOVUS** (tópico `NOVUS/Teste_Alumbra/events`, uma mensagem a cada ~10 s), confirmado em 30/10/2026:

  ```json
  {"pid":51387408,"device_id":"Teste_Alumbra","channels":{"timestamp":1793386748,"chd1_value":67,"chd2_value":119,"chd3_value":0}}
  ```

  `timestamp` e `chdN_value` vêm **aninhados em `channels`**; `timestamp` é epoch em segundos UTC. O mapeador procura
  cada chave primeiro no nível de cima e depois nos objetos aninhados (pelo último segmento do nome). As chaves aceitas para cada coluna
  ficam na seção `[mapeamento]` do `config.toml` (`MapeamentoConfig` em `app/config.py`); os formatos aceitos estão
  documentados no topo de `app/core/mapeamento.py`.
- Leitura incompleta não conta como gravada: conta como erro e vai para a contingência com o motivo.
- Permissões para o usuário `mqtt`: `sql/permissoes_iot001.sql` (rodar como dbo/sysadmin).

## Build

`.github/workflows/build-windows.yml` roda os testes e gera o `.exe` (PyInstaller via `build_exe.bat`) em
`windows-latest` a cada push e publica `MQTT-SQLServer-windows.zip` na página **Releases** (tag `v0.1.<n>`).

**Entrega ao usuário (sempre):** depois de cada push que gera versão, espere o build terminar, baixe o `.zip` da
release (`https://github.com/marcatao/MQTT/releases/download/v0.1.<n>/MQTT-SQLServer-windows.zip`) para o scratchpad,
confira que contém `MQTT-SQLServer.exe` e **envie o arquivo na conversa** (SendUserFile, `display: attach`), junto
com o link da release. O usuário não deve precisar procurar o `.exe` no GitHub.

Sem `[sqlserver]` configurado, o app roda do mesmo jeito: mostra os registros no log e guarda em `contingencia/`
(JSON Lines) para gravar depois.

## Contrato core ↔ gui (`app/bridge.py`)

```python
class PipelineBridge(QObject):
    status_alterado = Signal(str, str)      # (estado, detalhe); estado: "desconectado" | "conectando" | "conectado" | "erro"
    log = Signal(str, str, str)             # (hora "HH:MM:SS", nivel "INFO" | "AVISO" | "ERRO", mensagem)
    contadores = Signal(int, int, int)      # (mensagens recebidas, registros gravados, erros), no máx. ~5x por segundo
    teste_sql_concluido = Signal(bool, str) # (sucesso, mensagem multilinha em português)
    proxima_gravacao = Signal(float)        # epoch (s) do próximo ciclo de gravação; emitido ao conectar e a cada ciclo
    ultima_gravacao_ok = Signal(str)        # ISO 8601 local "AAAA-MM-DDTHH:MM:SS" do último INSERT com sucesso no SQL
    ultima_leitura = Signal(str)            # ISO 8601 local da última mensagem MQTT recebida (para o vigia)

    def __init__(self, config: Config, parent=None): ...
    def definir_sql(self, sql: SqlServerConfig) -> None: ...   # a GUI chama antes de conectar e quando o usuário altera
    def testar_sql(self, sql: SqlServerConfig) -> None: ...    # não bloqueia; responde em teste_sql_concluido
    def conectar(self, broker: str, porta: int, topico: str, qos: int) -> None: ...  # não bloqueia
    def desconectar(self) -> None: ...                                              # não bloqueia
    def encerrar(self, timeout: float = 5.0) -> None: ...  # ao fechar a janela: para, grava o último lote, desconecta
```

Os sinais são emitidos de outras threads; o Qt entrega na thread da GUI (conexão enfileirada). A GUI só usa isso.

Funções do SQL (`app/core/sql_conexao.py`, agente `sqlserver`), usadas pelo bridge e pelo sink:

```python
def montar_connection_string(sql: SqlServerConfig, driver: str | None = None) -> str: ...
def mascarar(connection_string: str) -> str: ...          # PWD=*** para logs
def conectar(sql: SqlServerConfig) -> "pyodbc.Connection": ...  # importa pyodbc só aqui (import preguiçoso)
def testar_conexao(sql: SqlServerConfig) -> tuple[bool, str]: ...  # bloqueante; o bridge roda em thread
def traduzir_erro(exc: Exception) -> str: ...
def validar_tabela(nome: str) -> tuple[str, str]: ...      # (schema, tabela); ValueError se inválido
def tabela_citada(nome: str) -> str: ...                   # "[dbo].[IOT001]"
def normalizar_criptografia(valor: str) -> str: ...        # aceita também Opcional/Obrigatório/Estrito
class DriverOdbcAusente(RuntimeError): ...
```

`testar_conexao` devolve `False` também quando conecta mas falta a tabela ou a permissão de INSERT; a tela mostra a
mensagem inteira, não só o booleano.

## Operação contínua (decidido com o usuário em 03/11/2026)

- **Amostragem de 1 em 1 minuto** (`[gravacao] intervalo_gravacao_segundos = 60`; 0 = grava tudo): os ciclos são
  alinhados ao relógio (hh:mm:00). Em cada ciclo grava-se, **por `device_id`, a leitura completa mais recente** recebida
  naquele minuto; as demais são descartadas (não são erro). Leitura incompleta continua indo na hora para a
  contingência. Um log por ciclo resume: gravadas, recebidas e descartadas pela amostragem.
- **Caixa ATIVIDADE**: mostra contagem regressiva até a próxima gravação e dia/hora da última gravação com sucesso no
  SQL Server (contingência não conta como sucesso). É **limpa todo dia à meia-noite** (hora local); o arquivo
  `logs/` continua com tudo.
- **Vigia (watchdog)**: com o usuário conectado, se passar **10 min** (`[vigia] minutos = 10`) sem nenhuma leitura
  recebida, ou sem gravação com sucesso no SQL (só quando o SQL está configurado), o app **se reinicia sozinho** e
  reconecta automaticamente (`--auto-conectar`). O motivo vai para o log em arquivo. A contagem começa no momento da
  conexão. Para gravar no banco depois de reiniciar, a senha precisa estar salva ("Lembrar senha"); sem isso a tela
  avisa ao conectar.

- **Conexão automática ao abrir** (decidido em 03/11/2026): toda vez que o programa abre, ele testa o banco e conecta
  no broker sozinho, sem clique (`[inicio] conectar_automaticamente = true`). Sem senha salva, conecta o MQTT e avisa
  que as leituras vão para a contingência até a senha ser salva.

- **Aba Mosquitto** (decidido em 03/11/2026): o cartão de atividade vira abas. A aba **Atividade** fica exatamente
  como era. A aba **Mosquitto** é um console que, ao abrir o programa, executa `mosquitto -v` na pasta
  `C:\Program Files\mosquitto168` (mostrando o prompt `C:\Program Files\mosquitto168>mosquitto -v`) e exibe a saída ao
  vivo. O processo é filho do app (QProcess, só na GUI): para ao fechar o programa e volta junto no reinício do vigia.
  Pasta, comando e início automático configuráveis em `[mosquitto]`. O usuário abre o Mosquitto manualmente (não é
  serviço do Windows), então o padrão é iniciar automaticamente.
- **Nome do app** (03/11/2026): título exibido "Integrador Alumbra MQTT + SQL + Mosquitto". Os identificadores internos
  continuam (`QSettings("Alumbra", "MQTT SQL Server")`, serviço do keyring, `MQTT-SQLServer.exe`) para não perder a
  senha e os valores salvos.

## Pendências conhecidas

- Reprocessar os arquivos de `contingencia/` (`.jsonl`) no SQL Server quando o banco voltar.
- Possível duplicação de um lote se o `commit` chegar ao servidor e a resposta se perder (exigiria id de lote ou
  chave única — decidir com o usuário).

## Estado atual

- v0.1.3 validada pelo usuário em produção (NOVUS → dbo.IOT001). v0.1.4: amostragem de 1 min, contagem regressiva, limpeza diária e vigia. v0.1.5: conexão automática ao abrir. v0.1.6: abas Atividade/Mosquitto e título Integrador Alumbra.

## Idioma

Conversa, textos da interface, logs, comentários e commits em **português do Brasil**.
