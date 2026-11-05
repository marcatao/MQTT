# Integrador Alumbra MQTT + SQL + Mosquitto

Iniciativa da Alumbra para **gerenciar os dispositivos IoT de um lugar só**. O integrador é um aplicativo desktop para
Windows que:

- **captura todas as leituras** que os dispositivos publicam pelo protocolo **MQTT**;
- **grava cada leitura** num banco **SQL Server**, assim que ela chega;
- **traz o broker Mosquitto acoplado**: o próprio programa inicia o Mosquitto e mostra o que acontece nele;
- **acompanha e comanda os dispositivos**: status, sinal Wi-Fi, firmware, fila de envio, tópicos e zerar contadores.

Em vez de manter um prompt com o Mosquitto, um fluxo no Node-RED e consultas soltas no banco, tudo fica numa janela
só, que abre, conecta e começa a gravar sozinha.

```
 Dispositivos IoT ──MQTT──▶ Mosquitto (aba Mosquitto) ──▶ Integrador ──▶ SQL Server
 (NOVUS DigiRail)  ◀──comandos (diagnóstico, zerar contadores)──┘
```

## O que o integrador faz

| Área | O que oferece |
| --- | --- |
| **Broker MQTT** | Conecta no broker, assina o tópico (curingas `+` e `#`), escolhe o QoS e reconecta sozinho. |
| **Banco de dados** | Conexão com o SQL Server (autenticação do Windows ou do SQL), botão **Testar conexão** e senha guardada no Gerenciador de Credenciais do Windows. |
| **Gravação** | Cada leitura completa vai para o banco assim que chega. Rajadas, como o reenvio da memória do dispositivo depois de uma queda, viram um único lote. A mesma leitura nunca é gravada duas vezes. |
| **Aba Atividade** | Log ao vivo do que foi recebido e gravado, a última gravação com sucesso e a contagem regressiva do reinício automático. É limpa todo dia à meia-noite. |
| **Aba Mosquitto** | Console com o `mosquitto -v` rodando dentro do programa, campo **Endereço Mosquitto** (salvo) e linha de comando para `mosquitto_sub`, `mosquitto_pub` e outros. |
| **Aba Clientes** | Lista dos dispositivos: nome, IP, MAC, último envio, Wi-Fi, fila no aparelho, nº de série, firmware e diagnóstico. Também os botões **Zerar contadores** (por canal) e **Tópicos…** (tópicos de cada aparelho). |
| **Erros** | O card **ERROS** abre a lista detalhada (payload, banco, broker, comandos), com filtro, busca e exportação CSV. Cada queda do broker conta um erro. |
| **Operação contínua** | Abre já conectado. Se passar 1 hora sem gravar, o programa se reinicia e reconecta sozinho. Sem banco, as leituras ficam guardadas em `contingencia/`. |

## Dispositivos suportados

Os clientes atuais são **NOVUS DigiRail IoT** (firmware V1.2x). O manual está em `docs/Manual_DigiRail_IoT.pdf` e
o resumo técnico do MQTT em `docs/digirail_iot_mqtt.md`. O integrador:

- separa **leituras** (`channels`), **eventos** (`events`) e **respostas** (`ack/command`, `ack/config`): só as leituras vão para o banco;
- pede o **diagnóstico** ao aparelho (Wi-Fi, firmware, nº de série, fila) ao conectar e a cada 30 min;
- envia **zerar contadores** por canal (`chd1`–`chd6`), desde que o canal tenha "reset por MQTT" habilitado no NXperience;
- aceita **tópicos próprios por aparelho**, quando o aparelho usa tópicos diferentes do padrão `NOVUS/{id}/...`. Os tópicos ficam salvos em `dispositivos.json`.

Outros dispositivos que publiquem JSON ou texto tabular (CSV) também funcionam: as chaves aceitas para cada coluna
são configuráveis na seção `[mapeamento]`.

## Instalação (usuário)

1. Baixe o `MQTT-SQLServer-windows.zip` da versão mais recente em **Releases** deste repositório.
2. Descompacte numa pasta, por exemplo `C:\IntegradorAlumbra`, e abra `MQTT-SQLServer.exe`. Não precisa instalar Python.
3. Requisitos no computador:
   - Windows 10 ou 11;
   - **ODBC Driver 18 for SQL Server** (ou o 17);
   - Mosquitto instalado, se o broker roda nesta máquina.
4. Na primeira vez:
   - confira os campos do broker e do banco;
   - digite a senha e marque **Lembrar senha neste computador**;
   - clique em **Testar conexão**.

   Das próximas vezes, o programa conecta sozinho.

Para atualizar, descompacte o `.zip` novo por cima da pasta. Não se perdem:
- a configuração, que fica em `config.toml`, `dispositivos.json` e nas preferências do Windows;
- a senha;
- os arquivos `logs/` e `contingencia/`.

### Arquivos que o programa cria na pasta dele

| Arquivo / pasta | Conteúdo |
| --- | --- |
| `config.toml` | Configuração opcional. Copie de `config.example.toml`; sem ele, valem os padrões. |
| `dispositivos.json` | Tópicos salvos de cada dispositivo. |
| `logs/` | Log completo (arquivo rotativo) e `erros_AAAAMMDD.jsonl`, com os erros detalhados do dia. |
| `contingencia/` | Leituras que não puderam ir para o banco, em JSON Lines, para gravar depois. |

## Banco de dados

O integrador **não cria nem altera** tabelas; só insere. A tabela de leituras (padrão `dbo.IOT001`) tem uma linha por
leitura:

| Coluna | Conteúdo |
| --- | --- |
| `pid` | Código do modelo do dispositivo. |
| `device_id` | Identificação do dispositivo. |
| `ts_unix` | Hora da leitura, em segundos UTC. |
| `chd1_value`…`chd3_value` | Valores dos canais. |
| `payload_json` | Mensagem original. |
| `recebido_em` | Hora em que o integrador recebeu, em UTC. |

Scripts em `sql/`, para rodar uma vez como administrador do banco:

- `permissoes_iot001.sql`: dá ao usuário do integrador permissão de `INSERT` e `SELECT` na tabela.
- `data_hora_local_iot001.sql`: acrescenta a coluna calculada `data_hora_local`, que é a hora da leitura no fuso de Brasília.

## Configuração (`config.toml`)

Todas as opções estão comentadas em `config.example.toml`. As principais seções:

| Seção | Para quê |
| --- | --- |
| `[mqtt]` | Broker, porta, tópico, QoS, usuário e senha do broker. |
| `[sqlserver]` | Servidor, banco, tabela, autenticação e criptografia. A senha **não** vai aqui: use "Lembrar senha". |
| `[gravacao]` | Gravação imediata (padrão) ou amostragem por minuto, tamanho do lote, pastas de logs e contingência. |
| `[mapeamento]` | Nomes de chaves aceitos para cada coluna da tabela. |
| `[vigia]` | Minutos sem gravar antes do reinício automático (padrão 60; 0 desliga). |
| `[inicio]` | Conectar sozinho ao abrir (padrão: sim). |
| `[mosquitto]` | Pasta e comando do Mosquitto e início automático. O campo da aba Mosquitto vale mais que o arquivo. |
| `[dispositivos]` | Modelos de tópico (`NOVUS/{id}/...`), intervalo do diagnóstico e tempo limite dos comandos. |

Os campos digitados na tela valem mais que o `config.toml`, que vale mais que os padrões do programa.

## Desenvolvimento

Requer Python 3.12 ou mais novo.

```sh
python -m venv .venv
.venv\Scripts\activate                      # Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt
python -m app.main                          # abre o integrador
python -m pytest -q                         # testes (no Linux sem tela: QT_QPA_PLATFORM=offscreen)
build_exe.bat                               # gera dist\MQTT-SQLServer\MQTT-SQLServer.exe (Windows)
```

A cada push, o GitHub Actions (`.github/workflows/build-windows.yml`) roda os testes no Windows, gera o `.exe` e
publica o `.zip` em **Releases**.

### Estrutura

```
app/
  main.py            ponto de entrada
  config.py          leitura do config.toml e padrões
  bridge.py          ponte entre o núcleo e a tela (sinais do Qt)
  core/              núcleo, sem Qt: MQTT, interpretação das mensagens, gravação no SQL Server
    mqtt_client.py   conexão, assinatura, reconexão e publicação
    parser.py        detecção de JSON, tabular, texto e binário
    mapeamento.py    mensagem → linhas da tabela; separa leituras, eventos e respostas
    digirail.py      protocolo do NOVUS DigiRail (comandos, diagnóstico, eventos)
    topicos.py       tópicos por dispositivo (dispositivos.json)
    pipeline.py      fila, gravação, duplicatas, comandos e erros
    sink_sqlserver.py  gravação em lote e contingência
    sql_conexao.py   conexão, teste e tradução de erros do SQL Server
    erros.py         registro dos erros detalhados
  gui/               tela em PySide6: janela, abas Atividade/Mosquitto/Clientes, erros, tópicos, vigia
sql/                 scripts para o banco
docs/                manual do DigiRail e referência MQTT
tests/               testes automatizados (pytest)
```

O `CLAUDE.md` guarda as decisões do projeto e o combinado entre núcleo e tela. Ele é a referência para quem for
evoluir o integrador.
