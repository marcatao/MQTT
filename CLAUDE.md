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
  (padrão: gravação imediata — grava assim que a fila esvazia, até `lote_max` por lote), sempre com parâmetros
  (nunca concatenar SQL).
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
    [chd4_value]    decimal(18, 4) NULL,             -- chd4..chd6 acrescentadas por sql/canais_4_a_6_iot001.sql (11/11/2026)
    [chd5_value]    decimal(18, 4) NULL,
    [chd6_value]    decimal(18, 4) NULL,
    [payload_json]  nvarchar(max) NULL,              -- payload original, como chegou
    [recebido_em]   datetime2(3) NOT NULL            -- DEFAULT sysutcdatetime(): UTC
    -- [data_hora_local] (calculada, opcional): data_hora_utc em America/Sao_Paulo; ver sql/data_hora_local_iot001.sql
);
```

**Fuso:** tudo é gravado em UTC (`ts_unix`, `data_hora_utc`, `recebido_em`). A hora de Brasília é só para exibição,
pela coluna calculada `data_hora_local` (`AT TIME ZONE 'E. South America Standard Time'`), decidida com o usuário
em 30/10/2026. O app não muda por causa disso.

Regras de mapeamento payload → linha:

- Colunas inseridas: `pid, device_id, ts_unix, chd1_value … chd6_value, payload_json, recebido_em` (chd4–6 só se a
  coluna existir na tabela; canal não enviado = NULL).
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

**Histórico de versões (sempre, pedido do usuário em 05/11/2026):** a cada release publicada, acrescente uma linha
no topo da tabela "Histórico de versões" do `README.md` (versão, data dd/mm/aaaa, principais melhorias em linguagem do
usuário). Se o build falhar e pular um número, diga isso na linha seguinte. Mudança só no README não gera release
(`paths-ignore`).

Sem `[sqlserver]` configurado, o app roda do mesmo jeito: mostra os registros no log e guarda em `contingencia/`
(JSON Lines) para gravar depois.

## Contrato core ↔ gui (`app/bridge.py`)

```python
class PipelineBridge(QObject):
    status_alterado = Signal(str, str)      # (estado, detalhe); estado: "desconectado" | "conectando" | "conectado" | "erro"
    log = Signal(str, str, str)             # (hora "HH:MM:SS", nivel "INFO" | "AVISO" | "ERRO", mensagem)
    contadores = Signal(int, int, int)      # (mensagens recebidas, registros gravados, erros), no máx. ~5x por segundo
    teste_sql_concluido = Signal(bool, str) # (sucesso, mensagem multilinha em português)
    proxima_gravacao = Signal(float)        # epoch (s) do próximo ciclo; só com amostragem (> 0); no padrão (0) não é emitido
    ultima_gravacao_ok = Signal(str)        # ISO 8601 local "AAAA-MM-DDTHH:MM:SS" do último INSERT com sucesso no SQL
    ultima_leitura = Signal(str)            # ISO 8601 local da última mensagem MQTT recebida (para o vigia)
    dispositivo_visto = Signal(str, str, str)  # (device_id, tópico, ISO local) só de leitura de contagem (channels); máx. 1 a cada 5 s por device
    erro_registrado = Signal(str)           # JSON de UM erro (ver "Erros detalhados"); 1 sinal para cada +1 no contador de erros
    canais_vistos = Signal(str, str)        # (device_id, JSON lista ["chd1","chd2","chd3"]) quando o conjunto de canais muda
    diagnostico_dispositivo = Signal(str, str)  # (device_id, JSON do objeto "diag" do DigiRail: sn, fw_v, curr/avg_rssi, mqtt_queue, ipv4...)
    resposta_comando = Signal(str, str, bool, str)  # (device_id, comando "reset_counters"|"diag", sucesso, mensagem em português)
    apontamentos_atualizados = Signal(str)  # JSON de cada ciclo dos apontamentos reais (formato em app/core/apontamentos_oee.py)
    recalculo_concluido = Signal(int, str, bool, str)  # (maquina_id, dia "AAAA-MM-DD", sucesso, mensagem em português)

    def __init__(self, config: Config, parent=None): ...
    def definir_sql(self, sql: SqlServerConfig) -> None: ...   # a GUI chama antes de conectar e quando o usuário altera
    def testar_sql(self, sql: SqlServerConfig) -> None: ...    # não bloqueia; responde em teste_sql_concluido
    def conectar(self, broker: str, porta: int, topico: str, qos: int) -> None: ...  # não bloqueia
    def desconectar(self) -> None: ...                                              # não bloqueia
    def encerrar(self, timeout: float = 5.0) -> None: ...  # ao fechar a janela: para, grava o último lote, desconecta
    def zerar_contadores(self, device_id: str, canais: list[int]) -> None: ...  # publica reset_counters; não bloqueia
    def pedir_diagnostico(self, device_id: str) -> None: ...                   # publica diag; não bloqueia
    # Tópicos por dispositivo (v0.1.11): chaves "events", "config", "ack_config", "command", "ack_command"
    def topicos_do_dispositivo(self, device_id: str) -> dict[str, str]: ...    # efetivos: salvos ou padrão do modelo
    def salvar_topicos_dispositivo(self, device_id: str, topicos: dict[str, str]) -> None: ...  # grava e aplica já
    def restaurar_topicos_padrao(self, device_id: str) -> None: ...            # apaga os salvos desse device
    def topicos_ouvidos(self) -> list[str]: ...    # filtros assinados agora (assinatura do usuário + extras)
    def topico_coberto(self, topico: str) -> bool: ...  # o integrador recebe mensagens desse tópico?
    def definir_periodicidade_apontamentos(self, minutos: float) -> None: ...  # apontamentos reais; não bloqueia
    def executar_apontamentos_agora(self) -> None: ...                       # não bloqueia
    def recalcular_dia_apontamentos(self, maquina_id: int, dia: str) -> None: ...  # dia "AAAA-MM-DD"; não bloqueia
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

- **Amostragem (opcional desde 04/11/2026; padrão agora é gravação imediata, ver abaixo)** (`[gravacao]
  intervalo_gravacao_segundos = 60` só se configurado; 0 = grava tudo, padrão): os ciclos são
  alinhados ao relógio (hh:mm:00). Desde a v0.1.10 a amostragem é **pelo minuto da leitura (`ts_unix`)**: em cada
  ciclo grava-se, por `device_id`, a leitura completa mais recente de cada minuto de `ts_unix` já encerrado
  (inclusive os reenviados da memória do dispositivo); as demais são descartadas (não são erro). Leitura incompleta continua indo na hora para a
  contingência. Um log por ciclo resume: gravadas, recebidas e descartadas pela amostragem.
- **Caixa ATIVIDADE**: mostra contagem regressiva (desde 04/11/2026: até o reinício automático) e dia/hora da última gravação com sucesso no
  SQL Server (contingência não conta como sucesso). É **limpa todo dia à meia-noite** (hora local); o arquivo
  `logs/` continua com tudo.
- **Vigia (watchdog)**: com o usuário conectado, se passar **60 min** (`[vigia] minutos = 60`; eram 10 até 04/11/2026) sem nenhuma leitura
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

- **Aba Clientes** (decidido em 03/11/2026): terceira aba, lista os dispositivos que enviam dados, com **Nome**,
  **IP**, **MAC**, **Último envio**, tópico, status (conectado/desconectado) e quantidade de mensagens. O MQTT não
  traz IP/MAC: o **IP e o client id** vêm da saída do `mosquitto -v` da aba Mosquitto ("New client connected from
  IP:porta as ID", "Received PUBLISH from ID (... 'tópico' ...)", "Client ID disconnected"); o **MAC** vem da tabela
  ARP do Windows (`arp -a`, só para IPs da mesma rede local); o **nome** é o `device_id` do payload (via
  `dispositivo_visto`), ligado ao client id pelo tópico. Clientes que só assinam (o próprio app, `mosquitto_sub`) aparecem
  marcados como "assinante" e ficam ocultos por padrão (pelo comportamento: assinou e nunca publicou). "Mensagens" conta
  as linhas PUBLISH do `mosquitto -v`. Tamanho padrão da janela: 1440×900 (limitado à tela). Sem o Mosquitto na aba, a lista mostra só nome, tópico e último
  envio.
- **Erros detalhados** (decidido em 03/11/2026): clicar no card **ERROS** abre uma janela com a lista de erros
  registrados (data/hora, categoria, mensagem, tópico, detalhe e trecho do payload), com filtro, cópia e exportação
  CSV. Cada erro que soma 1 no contador gera exatamente um registro. O core grava os registros também em
  `logs/erros_AAAAMMDD.jsonl`, e a janela lê o arquivo do dia ao abrir, para não perder a lista num reinício. Formato
  do JSON: `{"quando": ISO local, "categoria": "payload"|"sql"|"contingencia"|"mqtt"|"fila"|"comando"|"apontamentos", "mensagem": str,
  "topico": str|null, "detalhe": str|null, "payload": str|null (máx. 2000 caracteres)}`.
  Broker MQTT (categoria `"mqtt"`): **um erro por queda (incidente)**, não um por tentativa de reconexão. Conexão
  perdida sem o usuário pedir ("Conexão com o broker IP:porta perdida", detalhe com o motivo e a hora da última
  mensagem recebida) ou primeira tentativa de conexão que falha (inacessível ou CONNACK recusado, motivo no detalhe)
  abre o incidente e soma 1; as tentativas seguintes da mesma queda só vão para o log. Ao reconectar, o incidente
  fecha com um INFO "Broker voltou após 3 min 12 s" (não é erro); uma queda nova soma outro erro. Desconectar pelo
  usuário, encerrar ou reinício do vigia não geram erro. Cada SUBACK recusado soma 1 erro, com o tópico.

## Dispositivo cliente: NOVUS DigiRail IoT

Manual em `docs/Manual_DigiRail_IoT.pdf` (enviado pelo usuário em 04/11/2026); referência técnica do MQTT em
`docs/digirail_iot_mqtt.md`, anexada ao agente `mqtt-python`. `pid` é o código do modelo, não número de pacote.
Eventos (`events`) e respostas (`ack/command`, `ack/config`) **não são leituras**. Recomendado no NXperience até as
lacunas L1/L2 serem corrigidas: QoS 1, "Reter Dados" desligado, diagnóstico periódico desligado.

### Correções e comandos do DigiRail (decidido com o usuário em 04/11/2026)

- **Classificação das mensagens** (L1/L2/L9): `channels` → leitura (vai para a IOT001); `events` → evento (não é
  leitura, não grava, não é erro; só log resumido); `reported`/`desired`/respostas em `ack/*` → resposta de comando
  (não é leitura nem erro; `diag` e `reset_counters` viram sinais). Dispositivo do tópico no padrão NOVUS é o nível
  `{id}` de `NOVUS/{id}/...`.
- **Amostragem pelo minuto da leitura** (L3): substitui a regra anterior. Grava-se 1 linha por `device_id` por minuto
  de `ts_unix` (a mais recente daquele minuto). Leituras reenviadas da memória circular depois de uma queda viram uma
  linha por minuto, sem buracos. O ciclo continua a cada minuto do relógio e grava todos os minutos de `ts_unix` já
  encerrados.
- **Sem duplicatas** (L4/L5): o integrador não grava duas vezes o mesmo `(device_id, minuto de ts_unix)` nem o mesmo
  `(device_id, ts_unix)` (memória das últimas 24 h; mensagens retidas e reenvios QoS 1 caem aqui). Índice único no
  banco fica como opção futura.
- **Comandos ao dispositivo** (primeira vez que o integrador publica): tópico de comando `NOVUS/{id}/command`, resposta
  em `NOVUS/{id}/ack/command` (modelos configuráveis em `[dispositivos]`), QoS 1, `timestamp` = agora.
  - **Zerar contadores** por canal: `{"timestamp": T, "desired": {"reset_counters": {"reset_chd2": 1}}}`. Só funciona
    nos canais com "reset por MQTT" habilitado no NXperience; a resposta traz `error` (0 = ok). Sem resposta em 15 s
    → falha "sem resposta do dispositivo". Falha registra erro categoria `"comando"`.
  - **Diagnóstico**: `{"timestamp": T, "desired": {"diag": {}}}` ao ver um dispositivo pela primeira vez em cada
    conexão e a cada `[dispositivos] diagnostico_minutos = 3` (era 30 até 11/11/2026; 0 desliga). Preenche na aba Clientes: nº de série,
    firmware, sinal Wi-Fi (RSSI), leituras guardadas no aparelho (`mqtt_queue`) e IP.
- **Aba Clientes**: colunas novas Nº série, Firmware, Wi-Fi e Fila; em cada linha, botão **Zerar contadores** que abre
  a escolha dos canais (chd1–chd6, individual ou todos), pede confirmação e mostra o resultado.

### Tópicos por dispositivo e tópicos não ouvidos (decidido com o usuário em 04/11/2026)

Motivo: o NXperience do `Teste_Alumbra` grava o "Tópico de confirmação de comando" como `NOÏUS/Teste_Alumbra/ack/command`
(o aparelho troca o V sozinho e não deixa corrigir); o diagnóstico e as respostas de Zerar contadores iam para um tópico
que o integrador não ouvia.

- **Tópicos por dispositivo**: cada device tem 5 tópicos (`events`, `config`, `ack_config`, `command`, `ack_command`),
  pré-preenchidos com o padrão de `[dispositivos]` (`NOVUS/{id}/...`), editáveis na aba Clientes (diálogo "Tópicos…")
  e salvos em `dispositivos.json` na pasta do programa (`pasta_base()`, sobrevive a atualizações do `.zip`). O core usa
  esses tópicos para publicar comandos, assina os que a assinatura do usuário não cobre (refaz ao reconectar e ao
  salvar) e reconhece as respostas e o dispositivo por eles. Texto do tópico é guardado exatamente como digitado/colado
  (UTF-8), inclusive caracteres como `Ï`.
- **Tópicos não ouvidos**: a GUI lê as linhas `Received PUBLISH from ID (... 'tópico' ...)` do `mosquitto -v` e, quando
  um cliente publica num tópico que `topico_coberto()` diz que não é ouvido, registra um AVISO na Atividade (1 vez por
  tópico por sessão), marca a linha do cliente na aba Clientes e lista o tópico no diálogo "Tópicos…" com botão para
  usá-lo num dos 5 campos (ex.: "Usar como confirmação de comando").
- **Sem resposta**: a coluna Diagnóstico mostra "sem resposta" (com a hora e o tópico usado na dica) quando o pedido de
  diagnóstico — manual ou automático — não é respondido no tempo; o core emite `resposta_comando(device, "diag", False,
  msg)` também para o automático, só em caso de falha.

### Gravação imediata, reinício em 1 h e endereço do Mosquitto (decidido com o usuário em 04/11/2026)

- **Sem amostragem**: o padrão passa a `intervalo_gravacao_segundos = 0`. Cada leitura completa é gravada no SQL
  Server **assim que chega** (lote só quando várias chegam juntas, como no reenvio da memória do dispositivo). A
  proteção contra duplicatas `(device_id, ts_unix)` continua. A amostragem por minuto fica disponível só via
  `config.toml` (não é mais o padrão).
- **Vigia de 1 hora**: `[vigia] minutos = 60`. Se em 1 h não houver nenhuma gravação com sucesso (ou nenhuma leitura,
  quando o SQL não está configurado), o programa se reinicia e reconecta sozinho.
- **Contador da aba Atividade**: o chip "Próxima gravação" sai; no lugar entra "Reinício automático em mm:ss se nada for
  gravado", que recomeça a cada gravação com sucesso. O chip "Última gravação" continua.
- **Endereço Mosquitto** (aba Mosquitto): campo com o caminho da pasta do Mosquitto, pré-preenchido com `[mosquitto]
  pasta`, botão "…" ao lado para escolher a pasta (ou o `mosquitto.exe`), editável com o Mosquitto parado; ao Iniciar,
  usa o caminho do campo (prompt `<pasta>>mosquitto -v`). Salvo em `QSettings` (vale mais que o `config.toml`) e usado
  também no reinício automático.

### Canais 4–6, diagnóstico de 3 min, último envio e cadastro no OEE (decidido com o usuário em 11/11/2026)

- **Canais 4 a 6**: script `sql/canais_4_a_6_iot001.sql` (idempotente) acrescenta `chd4_value`, `chd5_value`,
  `chd6_value` `decimal(18,4) NULL`. O integrador grava os canais que vierem e deixa NULL os que não vierem. Antes do
  script ser rodado, o integrador detecta as colunas existentes (INFORMATION_SCHEMA) e insere só as que existem.
- **Diagnóstico a cada 3 min**: `[dispositivos] diagnostico_minutos = 3`.
- **Último envio** (aba Clientes) e `dispositivo_visto` só mudam com **leituras de contagem** (`channels`); diagnóstico,
  respostas de comando e eventos não atualizam. As linhas PUBLISH do `mosquitto -v` não mexem mais no último envio
  (continuam valendo para IP, client id, mensagens e tópicos não ouvidos).
- **Cadastro no portal OEE** (repositório `marcatao/OEE`, Laravel): se no mesmo SQL Server existir o banco do OEE
  (`[oee] banco = "OEE"`, configurável) com a tabela `dbo.dispositivos_iot` e o login tiver permissão, cada diagnóstico
  recebido faz **upsert** por `codigo = device_id`:
  - novo: `codigo`, `nome` (= device_id), `tipo = 'contador'`, `protocolo = 'mqtt'`, `endereco` (IP do diag),
    `topico` (tópico de dados), `ultimo_sinal_em` (UTC), `observacao` (resumo do diag: nº série, firmware, Wi-Fi, fila,
    hora), `ativo = 1`, `unidade` (`[oee] unidade = "manutencao"`), `criado_em`/`atualizado_em` UTC, `*_por_id` NULL.
  - existente: atualiza `endereco`, `topico`, `ultimo_sinal_em`, `observacao`, `atualizado_em` — **nunca `maquina_id`**,
    e também não mexe em `nome`, `tipo`, `ativo`, `unidade`, `porta` nem auditoria de criação (são do portal).
  - `ultimo_sinal_em` também é renovado com as leituras, no máx. 1 vez por minuto por device.
  - Sem o banco/tabela/permissão: ignora em silêncio (1 INFO no log), não é erro. Permissões: `sql/permissoes_oee.sql`.
  - Estrutura da tabela vem de `database/migrations/2026_11_06_000002_criar_dispositivos_iot.php` do OEE (+ `unidade`).
    O integrador **não cria nem altera** tabelas do OEE.

### Apontamentos reais pelo IoT (decidido com o usuário em 12/11/2026)

Especificação completa em `docs/apontamentos_reais.md` (fonte da verdade; código em `app/core/apontamentos_oee.py`).

- Nova aba **Apontamentos reais** ao lado de Clientes. A cada **3 min** (periodicidade editável na tela, salva em
  `QSettings`; padrão em `[apontamentos] minutos`), o integrador lê as máquinas ativas com IoT de
  `OEE.dbo.maquinas` (dispositivo + canal de produção, `micro_parada_s`, `macro_parada_s`), as leituras da
  `MQTT.dbo.IOT001` e grava a timeline em `OEE.dbo.apontamentos_estado`.
- **Intervalo = tempo sem o contador do canal aumentar** (não o tempo entre mensagens). `< micro` → legenda **2**
  Produção Normal; `micro ≤ intervalo < macro` → **99** Microparada; `≥ macro` → **98** Macro parada. A parada começa
  na última produção.
- Apontamentos únicos: o mesmo estado cresce num só apontamento (o último em aberto); segue as regras do portal (não
  passa de um dia local, o seguinte começa 1 s depois do anterior). Marcados com `observacao` `[IoT] ...`.
- As legendas 2, 98 e 99 **já existem no portal**; o integrador não cria legendas (falta = aviso, não aponta).
- **O IoT prevalece sobre o apontamento manual** (aparado ou excluído, com aviso no log). O objetivo é dizer se a
  máquina trabalhou ou parou; depois o operador troca 98/99 pelo motivo (Setup, Manutenção...) e o integrador
  **mantém** a legenda do operador enquanto o trecho continuar parado.
- **Sem leitura do aparelho** (desligado/fora da rede, nenhuma linha na IOT001 por mais de 120 s): legenda **97 Sem
  apontamento IOT** (decidido em 12/11/2026). O intervalo entre produções conta só o tempo com leitura. Se o aparelho
  voltar e mandar dados retroativos para a IOT001, o trecho 97 é **corrigido** com os dados reais (até 7 dias atrás).
- **Fora de turno** (13/11/2026): pelo calendário (`calendarios`/`calendario_dias`) e horário de trabalho da máquina
  (`horarios_semana` ou `dias_semana` + `jornada_*`), parada (98/99/97) fora do turno vira **96 Fora de turno**; dentro
  continua 98. Ex.: turno 05:00, parada 00:00–05:22 → 96 00:00–04:59:59 + 98 05:00–05:22.
- **Diagnóstico antes de apontar** (13/11/2026): cada ciclo manda `diag` aos dispositivos; sem resposta, a parada em
  aberto da máquina vira **97**.
- **Recalcular** (13/11/2026): botão por máquina na aba, refaz a timeline desde o dia escolhido (até 7 dias), avisando
  que os apontamentos manuais desse período serão apagados.
- Primeira execução: a partir de **hoje 00:00** (hora de Brasília); depois reconcilia de ontem 00:00 até agora.
- Atualiza a máquina como o portal (`SituacaoMaquina`: situação, desde, legenda, OP) e o status das OPs afetadas
  (`StatusOrdemProducao`). Permissões em `sql/permissoes_oee.sql`.

## Pendências conhecidas

- Índice único `(device_id, ts_unix)` na IOT001 (opcional, decidir com o usuário).
- Lacunas do DigiRail ainda abertas (seção 7 de `docs/digirail_iot_mqtt.md`): L6 vigia × intervalo de envio (agora só se o envio passar de 60 min); L7 JSON inválido do firmware (parcial, confirmar com captura real); L11 entradas analógicas e canais remotos (chr1–chr8) só no `payload_json` (chd4–chd6 gravados desde 11/11/2026). L1–L5, L8–L10 resolvidas na v0.1.10 (L5 só em memória: some ao reiniciar o programa).

- Reprocessar os arquivos de `contingencia/` (`.jsonl`) no SQL Server quando o banco voltar.
- Possível duplicação de um lote se o `commit` chegar ao servidor e a resposta se perder (exigiria id de lote ou
  chave única — decidir com o usuário).

## Estado atual

- v0.1.3 validada pelo usuário em produção (NOVUS → dbo.IOT001). v0.1.4: amostragem de 1 min, contagem regressiva, limpeza diária e vigia. v0.1.5: conexão automática ao abrir. v0.1.6: abas Atividade/Mosquitto e título Integrador Alumbra. v0.1.7: aba Clientes e janela de erros (card ERROS). v0.1.8: queda do broker conta como erro (1 por queda). v0.1.10: DigiRail (classificação, amostragem por ts_unix, sem duplicatas), diagnóstico e Zerar contadores na aba Clientes. v0.1.11: tópicos por dispositivo (dispositivos.json), aviso de tópicos não ouvidos e diagnóstico "sem resposta". v0.1.13 (a v0.1.12 falhou no build): gravação imediata (sem amostragem), vigia de 1 h com contador, Endereço Mosquitto salvo. v0.1.14: primeira release da `main` (README novo, programa igual à v0.1.13). v0.1.15: canais 4–6, diagnóstico de 3 min, último envio só com contagem, cadastro no portal OEE. v0.1.16: aba Apontamentos reais (timeline no portal OEE a partir da IOT001). v0.1.17: legenda 97 Sem apontamento IOT e correção retroativa.

## Branches

- **Só a `main`** (decidido com o usuário em 05/11/2026). O antigo branch de trabalho foi apagado depois de a `main`
  receber todo o conteúdo dele. Todo trabalho é commitado e enviado direto na `main`; não crie outros branches sem o
  usuário pedir.
- Push na `main` com mudança de código gera release (workflow `build-windows.yml`, que só roda para a `main`); só
  `.md` e `.claude/` não geram. Por isso, envie código pronto e testado de uma vez, não em pedaços.
- Commits com a identidade `Claude <noreply@anthropic.com>` (o GitHub mostra os outros como "Unverified").

## Idioma

Conversa, textos da interface, logs, comentários e commits em **português do Brasil**.
