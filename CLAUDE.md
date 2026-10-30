# MQTT → SQL Server (orquestrador do projeto)

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
| `mqtt-python` | Conexão MQTT, assinatura, reconexão, parsing de JSON/tabular, fila de processamento, gravação no SQL Server e desempenho. |

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
    sink_sqlserver.py# fila + gravação em lote via pyodbc
    pipeline.py      # liga mqtt → parser → sink, expõe eventos
  bridge.py          # adaptador: eventos do core viram Signals do Qt
tests/
```

Fluxo: thread do paho recebe a mensagem → coloca numa `queue.Queue` → thread de processamento faz parsing e grava em
lote → eventos (status, log, contadores) chegam à GUI **só por Signals do Qt**. Nunca atualizar widget fora da
thread principal; nunca bloquear a thread da GUI com rede ou banco.

## Estado atual

- O repositório ainda contém um protótipo antigo em **Node.js** (`package.json`, `src/`). Ele foi só um teste de
  conexão e **será substituído** pelo app em Python; não evolua esse código.

## Idioma

Conversa, textos da interface, logs, comentários e commits em **português do Brasil**.
