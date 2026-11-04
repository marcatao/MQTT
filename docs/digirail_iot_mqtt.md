# NOVUS DigiRail IoT: referência MQTT para o integrador

Referência técnica do dispositivo que publica no broker Mosquitto da Alumbra, extraída do manual
**"DigiRail IoT, Manual de Instruções V1.2x D"** (firmware a partir da V1.2x), arquivo
`docs/Manual_DigiRail_IoT.pdf` (109 páginas). Os números de página citados são os do rodapé do manual ("NN/109").

Convenções deste documento:

- Tudo o que não tem página citada e não está marcado é **dedução** a partir do manual, e vem indicado como tal.
- **(não consta no manual)** marca informação que o manual não traz. Nesses casos não há garantia sobre o
  comportamento do firmware; é preciso confirmar com uma captura real (`mosquitto_sub -v -t 'NOVUS/#'`).
- O manual tem erros tipográficos nos exemplos. Eles foram mantidos e apontados onde importam (seção 3.8).

---

## 1. Resumo do dispositivo

**Modelos** (Tabela 1, p. 7; Tabela 11, p. 45):

| Modelo | Entradas digitais | Entradas analógicas | Saídas digitais | USB | RS485 | Ethernet | Wi-Fi |
| --- | --- | --- | --- | --- | --- | --- | --- |
| DigiRail IoT – WRL | 6 | 2 | 2 | 1 | 1 | – | 1 (802.11 b/g/n 2,4 GHz) |
| DigiRail IoT – ETH | 6 | 2 | 2 | 1 | 1 | 1 (10/100 Mbps) | – |

- **Entradas digitais** `chd1`…`chd6` (p. 11, 39, 45): sensor PNP, NPN ou contato seco. Cada uma opera em modo
  **Contador** (conta bordas) ou **Evento** (registra a borda com hora). O capítulo 6 chama o modo Evento de
  "Registro" (p. 15–16). No Anexo 3, `counting_m`: 0 = não configurado, 1 = Contador, 2 = Evento (p. 106).
  Até 3 kHz (PNP/NPN) ou < 10 Hz (contato seco) (p. 45).
- **Entradas analógicas** `ch1`, `ch2` (p. 45): 0–5 V, 0–10 V, 0–20 mA ou 4–20 mA, 15 bits, exatidão de 0,15 % F.E.
- **Canais remotos** `chr1`…`chr8` (p. 40, 45): valores lidos por Modbus-RTU de escravos na RS485 (com a RS485 em
  modo mestre), de até 64 bits.
- **Saídas digitais** `out1`, `out2` (p. 45): NPN, até 700 mA. Podem ser comandadas por MQTT (seção 5).
- **Interfaces** (p. 13–14, 45): USB-C (configuração e diagnóstico, não isolada), RS485 (Modbus-RTU gateway, mestre
  ou escravo), Ethernet **ou** Wi-Fi, Modbus-TCP (até 3 clientes, p. 25) e MQTT.
- **Memória circular** (buffer de registros) de 925 a 7096 registros, conforme os canais habilitados (Tabelas 12 e
  13, p. 46). Ver seção 4.6.
- **Relógio interno** (RTC) com pilha CR2032 (duração típica de 5 anos, p. 45), ajuste por NTP opcional (p. 33).
- **LEDs** (Tabela 3, p. 12): Status, conexão com a rede e **conexão com o broker MQTT** (aceso = conectado,
  piscando = transmitindo, apagado = desabilitado ou falha).

**O que ele publica por MQTT** (p. 15, 92):

1. **Dados de canal** (`channels`): periódicos, no intervalo de registro.
2. **Eventos** (`events`): borda em canal digital no modo Evento.
3. **Respostas de configuração** (`reported` no tópico Config Ack).
4. **Respostas de comando** (`reported` no tópico Command Ack), inclusive o **diagnóstico periódico**
   (`diag` + `logs`) se a opção estiver ligada (p. 37).

O manual não traz um "status online/offline" publicado pelo dispositivo nem Last Will (não consta no manual).

---

## 2. Tópicos

### 2.1 Os cinco tópicos (p. 15, 36–37, 92)

| Variável (Anexo 3) | Nome no NXperience | Direção para o dispositivo | Conteúdo |
| --- | --- | --- | --- |
| **Device data** | Tópico de publicação de dados periódicos e eventos | publica | `channels` e `events` |
| **Config** | Tópico para receber configurações | assina | `desired` de configuração (enviado pela aplicação) |
| **Config Ack** | Tópico de confirmação de configuração | publica | `reported` de configuração; também a configuração atual depois que uma configuração é aplicada (p. 15) |
| **Command** | Tópico para receber comandos | assina | `desired` de comando (enviado pela aplicação) |
| **Command Ack** | Tópico de confirmação de comando | publica | `reported` de comando; diagnóstico periódico (p. 37) |

### 2.2 Padrão de nomes

Exemplo do manual para broker genérico (Tabela 5, p. 15), igual ao padrão fixo de AWS e NOVUS Cloud
(Tabelas 48 e 51, p. 105–106), em que `{id}` é o ID do dispositivo informado na configuração:

```
NOVUS/{id}/events          Device data   (publica)
NOVUS/{id}/config          Config        (assina)
NOVUS/{id}/ack/config      Config Ack    (publica)
NOVUS/{id}/command         Command       (assina)
NOVUS/{id}/ack/command     Command Ack   (publica)
```

Significado de cada nível (dedução a partir das tabelas):

- `NOVUS`: prefixo fixo do fabricante nas nuvens AWS e NOVUS Cloud; no broker genérico é só convenção.
- `{id}`: identificador do dispositivo.
- `events` / `config` / `command` / `ack/config` / `ack/command`: tipo do canal de mensagens. Repare que os acks têm
  **dois níveis** (`ack/...`).

Para **broker genérico**, os cinco tópicos são **texto livre** (Tabela 52, p. 106: "podem ser definidos de modo
arbitrário"). O manual recomenda que Device data, Config Ack e Command Ack sejam só publicados pelo dispositivo e
que Config e Command sejam só publicados pela aplicação de gerenciamento. Cada tópico ocupa 30 registradores
Modbus (p. 72–77). Para o Device ID o manual diz "cada registrador equivale a dois caracteres" (p. 71), o que daria
60 caracteres por tópico; o limite do tópico não está escrito de forma explícita (não consta no manual).
Google IoT e Azure usam outros padrões (Tabelas 49 e 50, p. 105) que não se aplicam ao nosso Mosquitto.

### 2.3 Comparação com a produção (`NOVUS/Teste_Alumbra/events`)

- O tópico de produção segue exatamente o padrão `NOVUS/{id}/events` com `{id} = Teste_Alumbra`.
- O `device_id` do payload também é `"Teste_Alumbra"`. O `device_id` vem do campo **"ID do Dispositivo"** da aba
  Protocolo MQTT do NXperience (p. 15, 36, 92), não do "Nome do Dispositivo" (campo `title`, até 20 caracteres,
  p. 33). O Device ID ocupa 10 registradores de 2 caracteres, ou seja, **até 20 caracteres** (p. 71).
- **De onde vem "Teste_Alumbra" no tópico:** é o ID do Dispositivo configurado no NXperience. Se a nuvem escolhida
  for AWS ou NOVUS Cloud, o tópico é montado com esse ID (`NOVUS/{id}/...`, p. 105–106). Se for "Geral"
  (broker genérico, nosso caso, já que o broker é o Mosquitto local), os tópicos foram digitados em "Tópicos →
  Editar" (p. 37), provavelmente seguindo o exemplo da Tabela 5. O manual não diz se o NXperience preenche os
  tópicos do broker genérico sozinho a partir do ID (não consta no manual). Confirme no NXperience os cinco tópicos
  realmente gravados.
- **Outros tópicos que o dispositivo publica e que caem na nossa assinatura `NOVUS/Teste_Alumbra/#`** (se os
  tópicos seguirem a Tabela 5):
  - `NOVUS/Teste_Alumbra/ack/command`: respostas de comando e, **se "Publicar diagnóstico periodicamente" estiver
    habilitado, duas mensagens (`diag` e `logs`) a cada inicialização do dispositivo e todo dia à meia-noite**
    (p. 37). O fuso dessa meia-noite não consta no manual.
  - `NOVUS/Teste_Alumbra/ack/config`: respostas de configuração e a configuração atual depois que o NXperience (ou
    outra aplicação) aplica uma configuração (p. 15).
  - `NOVUS/Teste_Alumbra/events` também recebe **eventos** (`events`), não só `channels` (p. 16, 93).
  - `NOVUS/Teste_Alumbra/config` e `NOVUS/Teste_Alumbra/command`: o dispositivo **não** publica nesses tópicos,
    mas qualquer outra aplicação (Node-RED, `mosquitto_pub`) que mande configuração ou comando publica ali, e o
    integrador também recebe essas mensagens.
- Hoje o integrador trata todas essas mensagens como "leitura" e algumas viram linhas erradas no banco ou erros
  falsos (ver seção 7, itens L1 e L2).

---

## 3. Formato JSON das mensagens

Todas as mensagens são JSON. Todos os timestamps são **Unix UTC (GMT 0)** (p. 15, 93).

### 3.1 Campos comuns: `pid` e `device_id` (p. 15, 92)

- `pid`: **identificador do modelo do produto** (Tabela 6, p. 15; Tabela 21, p. 92). É **constante** para o modelo:
  não é número de pacote, sequência nem número de série.

  | Modelo | PID |
  | --- | --- |
  | DigiRail IoT ETH | 51584019 |
  | DigiRail IoT WRL | 51518482 |

  O `pid` de produção, **51387408**, **não está nas tabelas**. O próprio manual usa 51387408 em alguns exemplos
  (`"pid_id":51387408` no diagnóstico, p. 20; respostas de Gateway RS485 e Get Diagnostic, p. 101), sem explicar.
  Qual modelo ou variante corresponde a 51387408 não consta no manual. O número de série (`sn`) e a versão de
  firmware (`fw_v`) aparecem no diagnóstico (seção 3.5).
- `device_id`: identificador definido pelo usuário no campo "Device ID" (ID do Dispositivo) do NXperience (p. 15),
  até 20 caracteres (p. 71).

### 3.2 Dados de canal: `channels` (p. 15–16, 93)

Publicados periodicamente no tópico Device data:

```json
{
  "pid": 51584019,
  "device_id": "device0",
  "channels": {
    "timestamp": 1585819219,
    "chd1_value": 0, "chd2_value": 0, "chd3_value": 0,
    "chd4_value": 0, "chd5_value": 0, "chd6_value": 0,
    "ch1_user_range": 2, "ch2_user_range": -19991,
    "chr1_value": 0, "chr2_value": 0, "chr3_value": 0, "chr4_value": 0,
    "chr5_value": 0, "chr6_value": 0, "chr7_value": 0, "chr8_value": 0
  }
}
```

(Exemplo da p. 15–16; o do Anexo 3, p. 93, tem só `chd1`–`chd6` e `ch1_user_range: 2.17`, `ch2_user_range: 2.2`.)

| Chave | Significado (p. 16) |
| --- | --- |
| `channels.timestamp` | Unix UTC, em **segundos inteiros**, do **momento em que a leitura foi feita** (não do envio). |
| `chdX_value` (X = 1…6) | Canal digital X. Em modo **Contador**: valor do contador. Em modo **Registro/Evento**: nível lógico (0/1) no momento. **Canal desabilitado não aparece no JSON.** |
| `chX_user_range` (X = 1, 2) | Entrada analógica X na faixa configurada pelo usuário (número com casas decimais conforme `decimal_point`, p. 107). Canal desabilitado não aparece. |
| `chrX_value` (X = 1…8) | Último valor lido do escravo Modbus do canal remoto X. Canal desabilitado não aparece. (A nota da p. 16 diz `chrX_user_range`, mas o exemplo usa `chrX_value`.) |

Outras informações sobre os contadores:

- Faixa do contador: 0 a 4.294.967.295 (p. 19, comando Set Counters). Cabe no `decimal(18,4)` da `dbo.IOT001`.
- O contador pode voltar a zero por **overflow**, **calendário** (zeramento periódico diário/semanal/mensal) ou
  **protocolo** (comando MQTT/Modbus), conforme `reset_m` e `chd_periodic_reset` (p. 39, 97, 106–107). Quem
  calcular diferenças entre leituras precisa tratar essa volta a zero.
- No modo Evento, a contagem só entra no JSON se "Adicionar contagem em canais configurados como Evento"
  (`add_counter_on_events`) estiver habilitado (p. 39, 96).

**Comparação com a produção:**

```json
{"pid":51387408,"device_id":"Teste_Alumbra","channels":{"timestamp":1793386748,"chd1_value":67,"chd2_value":119,"chd3_value":0}}
```

- Estrutura idêntica à do manual. `timestamp` 1793386748 = 2026-10-30 18:59:08 UTC.
- Como canal desabilitado não aparece (p. 16), a produção tem **só `chd1`–`chd3` habilitados**; `chd4`–`chd6`,
  analógicas e remotos estão desabilitados (dedução).
- `chd1 = 67` e `chd2 = 119` são maiores que 1, então esses canais estão em modo **Contador** (dedução). `chd3 = 0`
  pode ser contador zerado ou nível lógico.
- Hoje o mapeamento pega `chd1_value`…`chd3_value` dentro de `channels`. Isso está correto para este payload.

### 3.3 Eventos: `events` (p. 16, 93)

Gerados quando um canal digital no modo Evento detecta a borda configurada. Publicados no **mesmo tópico** dos
dados (Device data):

```json
{
  "pid": 51584019,
  "device_id": "device0",
  "events": {
    "chd1": {
      "timestamp": 1585819219.685,
      "edge": 1
    }
  }
}
```

- `timestamp`: Unix UTC **com milissegundos na parte fracionária** (double) (p. 16, 93).
- `edge`: 1 = borda de subida, 0 = borda de descida (p. 16).
- O exemplo do manual tem vírgula sobrando depois de `"edge":1,`, o que é **JSON inválido** (p. 16 e 93). Ver 3.8.
- O manual não diz se uma mensagem pode trazer mais de um canal dentro de `events`, nem se o evento leva o valor do
  contador quando `add_counter_on_events = 1` (não consta no manual).

### 3.4 Respostas de configuração (Config Ack) (p. 94–99)

Formato genérico da resposta (p. 94). Repare que esse exemplo **não tem `pid`**:

```json
{
  "device_id": "device0",
  "timestamp": 1585819219,
  "reported": { "<item>": { "error": 0, "...": "..." } }
}
```

Os exemplos de cada item (p. 96–99) trazem `"pid"`. O `timestamp` da resposta é **o mesmo do pedido** (não é hora
de leitura). Exemplo, NTP (p. 98):

```json
{"pid":51584019,"device_id":"digirail_iot","timestamp":1585819219,
 "reported":{"ntp":{"error":0,"enable":1,"diff_to_update":5,"host":"time.google.com"}}}
```

### 3.5 Respostas de comando (Command Ack) e diagnóstico (p. 17–24, 99–105)

Estrutura comum: `pid`, `device_id`, `timestamp` (**o mesmo do comando recebido**), `reported.<comando>` com
`error`. Exemplos:

- **Output** (p. 17, 99): `"reported":{"output":{"error":0,"out1":1,"out2":1}}`
- **Reset counters** (p. 18, 100): `"reported":{"reset_counters":{"error":0,"reset_chd1":0,...,"reset_chd6":0}}`
- **Set counters** (p. 19, 100): `"reported":{"set_counters":{"error":0,"set_chd1":0,"set_chd2":6500,...}}`
- **Gateway RS485** (p. 22, 101): `"reported":{"gateway_485":{"error":0,"mb_buffer":"02 03 14 ..."}}`; o exemplo
  usa `"timestamp":15`, e o `mb_buffer` volta vazio em timeout do escravo.
- **Reset diagnostic** (p. 22, 103): `"reported":{"reset_diag":{"error":0,"reset_watchdog_counter":0,...}}`
- **Get diagnostic** (p. 20–21, 101–102):

```json
{
  "pid": 51584019, "device_id": "device0", "timestamp": 1585819219,
  "reported": { "diag": {
    "error": 0, "title": "Pci v2", "location": "home",
    "curr_timestamp": 1589326517, "cfg_timestamp": 1589311676,
    "fw_v": "1.23", "mqtt_queue": 1, "sn": "00000001",
    "curr_rssi": "55", "min_rssi": "-46", "max_rssi": "-87", "avg_rssi": "-54",
    "ipv4": [192,168,0,23], "gateway": [192,168,0,1], "mask": [255,255,255,0],
    "dns1": [192,168,0,1], "dns2": [8,8,8,8]
  } }
}
```

  | Campo | Significado (p. 21) |
  | --- | --- |
  | `title`, `location` | Nome e localização configurados no NXperience. |
  | `curr_timestamp` | Hora atual do relógio do dispositivo (Unix UTC). |
  | `cfg_timestamp` | Hora da última configuração aplicada (Unix UTC). |
  | `fw_v` | Versão do firmware. |
  | `mqtt_queue` | **Número de registros pendentes de envio por MQTT** (fila da memória circular). |
  | `sn` | Número de série. |
  | `curr_rssi` (%), `min_rssi`/`max_rssi`/`avg_rssi` (dBm) | Qualidade do Wi-Fi (texto, não número). |
  | `ipv4`, `gateway`, `mask`, `dns1`, `dns2` | Rede, como lista de 4 inteiros. |

  Com "Publicar diagnóstico periodicamente" habilitado, a resposta inclui também (p. 20–21, 102):
  `log_counters` (`pwr_on`, `pwr_sw_reset`, `net_disconnected`, `wifi_prov_error`, `dhcp_error`, `dns_error_1`,
  `dns_error_2`, `cfg_updated`, `fw_updated`) e `watchdog_counters` (`analog`, `data_storage`, `record_storage`,
  `digital`, `modbus`, `record_periodic`, `mqtt`, `network`, com valores em texto).
  Um dos exemplos usa **`"pid_id"`** em vez de `"pid"` (p. 20); o outro (p. 102) usa `"pid"`.

### 3.6 Logs (p. 23–24, 103–105)

- **`logs`**: últimos **50** eventos, cada um com `ts` (Unix) e `id` (código numérico):
  `"reported":{"logs":{"error":0,"events":[{"ts":1638193059,"id":9},{"ts":1638193055,"id":10}, ...]}}`
- **`logs_parsed`**: últimos **30** eventos, com descrição em vez de código:
  `"events":[{"ts":1638193059,"mqtt":"connected"},{"ts":1636468024,"net":"connected"}, ...]`
  (o exemplo da p. 104 vem sem `pid`).

Códigos (Tabela 9, p. 24; Tabela 47, p. 104–105):

| Código | logs_parsed | Descrição |
| --- | --- | --- |
| 0 | `pwr: on` | Inicialização padrão |
| 1 | `pwr: sw_reset` | Inicialização por reset de software |
| 2 | `pwr: wdt_reset` | Inicialização por watchdog interno |
| 3 | `pwr: lvd_reset` | Inicialização por falha na alimentação |
| 4 / 5 | `net: connected` / `disconnected` | Conectado / desconectado da rede (Wi-Fi ou Ethernet) |
| 6 | `wifi: prov_error` | SSID ou senha do Wi-Fi incorretos |
| 7 | `dhcp: error` | Erro de DHCP |
| 8 | `sntp: error` | Erro de SNTP |
| 9 / 10 | `mqtt: connected` / `disconnected` | Conectado / desconectado do broker |
| 11 / 12 | `mqtt: sub_error` / `pub_error` | Erro de inscrição / publicação |
| 13 / 14 | `mqtt: alter_int` / `default_int` | Passou para o intervalo de publicação alternativo / voltou ao padrão |
| 15–17 | `dns: error_1..3` | Erro interno de DNS (fases 1 a 3) |
| 18 | `mem: init_error` | Erro ao iniciar a memória circular (recuperado) |
| 19 | `mem: not_init` | Memória circular não inicializada |
| 20 | `mem: read_error` | Falha de leitura da memória circular |
| 21 | `cfg: updated` | Configuração atualizada |
| 22 | `fw: updated` | Firmware atualizado |
| 24 | `mqtt: conn_error` | Erro na conexão com o broker |
| 25 | `mqtt: conn_recover` | Tentativa de recuperar a conexão com o broker |

(O código 23 não aparece nas tabelas.)

### 3.7 Pedidos (`desired`), publicados pela aplicação, não pelo dispositivo (p. 17, 94)

```json
{"timestamp": 1585819219, "desired": {"<item ou comando>": { ... }}}
```

Não têm `pid` nem `device_id`. O integrador só os vê se alguém publicar em Config/Command dentro da assinatura.

### 3.8 Como distinguir o tipo de mensagem (dedução a partir das seções acima)

| Tipo | Chave de primeiro nível que identifica | Tópico (padrão da Tabela 5) |
| --- | --- | --- |
| Dados de canal | `channels` (objeto) | `.../events` |
| Evento | `events` (objeto) | `.../events` |
| Resposta de configuração | `reported` + item de configuração (`rtc`, `device`, `chdX`, `chX`, `chrX`, `chd_periodic_reset`, `eth`, `wifi`, `ntp`, `modbus_tcp`, `rs485`) | `.../ack/config` |
| Resposta de comando / diagnóstico / logs | `reported` + `output`, `reset_counters`, `set_counters`, `gateway_485`, `diag`, `reset_diag`, `logs`, `logs_parsed` | `.../ack/command` |
| Pedido da aplicação | `desired` | `.../config` ou `.../command` |

Erros nos exemplos do manual que podem ser só tipográficos ou podem refletir o firmware (confirmar com captura
real):

- vírgula sobrando antes de `}` nos eventos (p. 16, 93) e em `log_counters` (p. 21);
- `;` no lugar de `,` em `"error":0; "mb_buffer"` (p. 22);
- `"pid_id"` no lugar de `"pid"` (p. 20); `"pid": 51584019` sem vírgula (p. 23);
- `" reported "` com espaços na resposta Modbus-TCP (p. 99) e `"alter_pub_interval "` com espaço (p. 96).

---

## 4. Comportamento de envio

### 4.1 Periodicidade (p. 40–41, 96, 106)

- **Intervalo de Registros** (segundos): é o intervalo de gravação na memória circular **e** o de publicação dos
  dados periódicos quando o MQTT está habilitado (p. 40). Via MQTT: `device.pub_interval`, de 1 a 65535 s (p. 106).
  Em produção, cerca de 10 s (CLAUDE.md).
- **Troca automática para o Intervalo de Registros Alternativo** (`alter_pub_interval_enable`): quando a **fila de
  envio MQTT passa de 10 % da capacidade**, o intervalo de registros muda para o alternativo; volta ao normal quando
  a conexão é restabelecida e a fila fica abaixo de 10 % (p. 40–41). `alter_pub_interval`: de 60 a 65535 s (p. 106).
  As trocas ficam no log (códigos 13 e 14).
- Eventos: publicados quando ocorrem (p. 93).
- Diagnóstico periódico (se habilitado): ao inicializar e todo dia à meia-noite, duas publicações (`diag` e `logs`)
  no tópico Command Ack (p. 37).

### 4.2 QoS e retain (p. 36; registradores p. 68 e 72)

- **QoS de publicação: 0 ou 1** (o dispositivo não usa QoS 2). Registrador `HR_CS_MQTT_QOS`, de 0 a 1 (p. 68).
- **Reter Dados**: liga a flag *retain* nas publicações ("nem todas as plataformas suportam", p. 36; registrador
  `HR_CS_MQTT_TOPIC_RETAIN`, p. 72). O manual não diz se vale para todos os tópicos ou só para Device data (não
  consta no manual).
- O integrador assina com QoS 2; o broker entrega com o **menor** entre o QoS da publicação e o da assinatura, ou
  seja, no máximo QoS 1 para mensagens deste dispositivo (regra do MQTT, não consta no manual).

### 4.3 Versão do protocolo

- Capítulo 6: "compatível com o protocolo MQTT, versões **3.1 e 3.1.1**" (p. 15).
- Anexo 3: "Broker MQTT genérico (**versão 5.0 ou superior**)" (p. 92).
- As duas frases se contradizem. O Mosquitto aceita 3.1, 3.1.1 e 5.0, então a contradição não atrapalha.

### 4.4 Keepalive, Last Will, clean session e client id

**Não constam no manual**: keepalive, Last Will (LWT), clean session, client id e intervalo ou backoff de
reconexão. Na prática, o client id aparece na saída do `mosquitto -v` ("New client connected from IP:porta as ID"),
que a aba Clientes já lê. A desconexão do dispositivo só é registrada no log interno dele (códigos 10, 24 e 25),
que pode ser lido pelo comando `logs` ou `logs_parsed`.

### 4.5 Autenticação e TLS (p. 36–37, 71)

- **Usuário/Senha do Broker**: até 64 caracteres cada; **vazio = conexão anônima** (p. 36).
- **Segurança** (p. 37): Nenhuma; **TLS V1.2 – CA Somente**; **TLS V1.2 – Self Signed** (exige também o certificado
  do cliente e a chave privada). Arquivos `.pem` ou `.der`. O registrador `HR_CS_MQTT_SECURITY` lista também
  "TLS V1.2 – Server signed" (valor 1, p. 71).
- A porta é livre ("Porta de Serviço", p. 36).

### 4.6 Quando a conexão cai: memória circular e reenvio

O que o manual diz:

- O dispositivo grava cada registro periódico (todos os canais habilitados) e cada evento numa **memória circular**
  (p. 86).
- Existe uma **fila de envio MQTT**: `mqtt_queue` = "número de registros pendentes de envio por MQTT" (p. 21);
  "Fila MQTT: número de registros aguardando publicação" (p. 43); registrador `HR_STATUS_OF_RECORDS` (p. 26). O
  NXperience mostra também "Última atualização: dia e hora do último pacote publicado com sucesso" (p. 43).
- Com a conexão instável, a fila cresce. Acima de 10 % da capacidade, o dispositivo pode espaçar os registros
  (intervalo alternativo) e volta ao normal quando a conexão é restaurada e a fila cai abaixo de 10 % (p. 40–41).
  Ou seja, **os registros acumulados durante a queda são publicados depois**.
- O `timestamp` de `channels` é o **momento da leitura** (p. 16), então os registros atrasados chegam com o
  **timestamp antigo**.
- **Capacidade** (Tabela 12, p. 46): com 3 canais digitais e nenhuma analógica (a configuração aparente da
  produção, ver 3.2) são **3361 registros**. Com 1 registro a cada 10 s, isso dá cerca de 9 h 20 min de queda
  coberta, e 10 % (cerca de 336 registros) é atingido em cerca de 56 min (cálculo a partir da tabela). Com 6
  digitais e 2 analógicas são 1824; os canais remotos reduzem a capacidade (Tabela 13, p. 46).
- Memória "circular": quando enche, os registros novos passam a ocupar as posições dos mais antigos (exemplos 1 a 3,
  p. 89).

O que **não consta no manual**:

- a **ordem** do reenvio (FIFO do mais antigo para o mais novo?) e a **taxa** (rajada ou um por intervalo);
- se os dados ao vivo são publicados no meio do reenvio ou só depois dele;
- se registros **ainda não enviados** são sobrescritos quando a memória dá a volta (perda) ou se a gravação para;
- se o reenvio usa o QoS configurado e se há deduplicação.

Consequências para o integrador (detalhadas na seção 7): depois de qualquer queda do broker (inclusive o
reinício feito pelo vigia, que derruba junto o Mosquitto filho do app), chega uma **rajada de leituras com
timestamps antigos**. A amostragem atual, que agrupa pela hora de recebimento, descarta quase todas.

### 4.7 Relógio

- Timestamps em UTC (p. 15). Campo **GMT** no NXperience ("fuso do local", p. 33) e NTP opcional com "diferença para
  atualizar" (p. 33, 98). O NXperience acerta o relógio do dispositivo com a hora do Windows ao enviar a
  configuração (p. 33).
- Se o relógio do dispositivo estiver errado (sem NTP, pilha fraca), o `ts_unix` gravado fica errado. O manual não
  diz o que acontece com o timestamp quando o RTC perde a hora (não consta no manual).

---

## 5. Comandos e configuração via MQTT

### 5.1 Como enviar (p. 17, 94–95, 99)

1. Publicar no tópico **Command** (`NOVUS/Teste_Alumbra/command`) ou **Config** (`NOVUS/Teste_Alumbra/config`):
   ```json
   {"timestamp": <inteiro>, "desired": {"<item>": { ...pares a alterar... }}}
   ```
   O `timestamp` identifica o pedido e volta igual na resposta (use-o como id de correlação).
2. Assinar **Command Ack** / **Config Ack** e esperar o `reported` com o mesmo `timestamp`.
3. Para **consultar** sem alterar, mandar o item vazio: `{"timestamp":..., "desired":{"rtc":{}}}` (p. 95).
   Consultas podem ter vários itens vazios; **alterações**, só um item por mensagem (p. 94–95).

### 5.2 Erros (p. 17–18, 94–95)

| `error` | Significado |
| --- | --- |
| 0 | Êxito |
| 1 | Estrutura correta, mas parâmetro fora da faixa |
| 2 | Estrutura correta, mas parâmetro desconhecido |

- Item não reconhecido: o `reported` traz só `error`.
- Erro ao aplicar: o `reported` traz os valores **atuais** e o primeiro erro; nada do item é aplicado (tudo ou
  nada por item).
- **Sem resposta nenhuma**: JSON malformado, ou faltando `timestamp`, `desired` ou o item (p. 18). O integrador
  precisaria de timeout.

### 5.3 Comandos (tópico Command) (p. 17–24, 99–105)

| Comando | `desired` | Uso | Pré-requisito |
| --- | --- | --- | --- |
| `output` | `{"out1":1,"out2":0}` ou `{}` para ler | Liga/desliga ou lê as saídas | – |
| `reset_counters` | `{"reset_chd2":1}` | Zera contadores | Canal com zeramento por MQTT habilitado (p. 18) |
| `set_counters` | `{"set_chd2":6500}` (0…4.294.967.295) | Ajusta contadores | "Permitir ajuste no valor da contagem" (p. 19, 39) |
| `diag` | `{}` | Diagnóstico (seção 3.5) | – |
| `reset_diag` | `{"reset_watchdog_counter":1,"reset_logs_counter":1,"reset_diag_rssi":1}` | Zera contadores de diagnóstico e RSSI | – |
| `logs` | `{}` | Últimos 50 eventos (códigos) | – |
| `logs_parsed` | `{}` | Últimos 30 eventos (texto) | – |
| `gateway_485` | `{"mb_buffer":"02 03 00 00 00 0A C5 FE"}` | Envia bytes Modbus-RTU pela RS485 e devolve a resposta | RS485 em modo Gateway |

### 5.4 Itens de configuração (tópico Config) (Tabela 7, p. 16; p. 96–99; variáveis p. 106–109)

`rtc` (year 2016–2080, month, day 1–28, hour, minute, sec), `device` (`title` ≤ 20, `location` ≤ 40,
`pub_interval` 1–65535 s, `alter_pub_interval_enable`, `alter_pub_interval` 60–65535 s, `add_counter_on_events`),
`chd1`…`chd6` (`enable`, `counting_m`, `type`, `edge`, `debounce`, `reset_m`, `debounce_enable`),
`chd_periodic_reset`, `ch1`/`ch2` (`enable`, `sensor_type`, `range_min`, `range_max`, `decimal_point`),
`chr1`…`chr8`, `eth`, `wifi` (a senha não volta na resposta), `ntp` (`enable`, `diff_to_update`, `host`),
`modbus_tcp`, `rs485`. A configuração do MQTT em si (broker, tópicos, QoS) **não** aparece entre os itens
configuráveis por MQTT (não consta no manual).

### 5.5 O que o integrador poderia fazer (hoje ele só lê)

Só leitura e sem efeito no processo:

- **`diag`**: IP, número de série, firmware, `mqtt_queue` (registros pendentes), relógio do dispositivo
  (`curr_timestamp`), RSSI, contadores de reinício e de queda de rede.
- **`logs_parsed`** / **`logs`**: histórico de conexão do lado do dispositivo (quedas de rede ou de broker,
  reinícios, erros de memória).
- **`output` com `{}`**: estado das saídas.
- **Consultar itens de configuração** (`{"device":{}}`, `{"chd1":{}}`, `{"ntp":{}}`, `{"rtc":{}}`): saber o
  `pub_interval` real, o modo dos canais, se o NTP está ativo.

Com efeito no processo (exigem decisão do usuário e cuidado):

- `rtc` / `ntp`: acertar o relógio.
- `reset_counters` / `set_counters`: mexe nos valores que vão para o banco.
- `output`: aciona as saídas físicas.
- `device.pub_interval`: muda a cadência de publicação.

Publicar exige que o cliente do integrador tenha permissão de escrita nesses tópicos no Mosquitto (ACL, se houver).
Desde 04/11/2026 o integrador publica `diag` e `reset_counters` (seção 7, "O que pode ser aproveitado").

---

## 6. Configuração no NXperience para o nosso Mosquitto (192.168.100.162:1883)

Campos da aba **Comunicação → Protocolo MQTT** (p. 36–37) e afins. "Recomendado" é sugestão nossa, a partir das
seções 4 e 7, não do manual.

| Campo (página) | Valor recomendado | Motivo |
| --- | --- | --- |
| Habilitar MQTT (p. 36) | Sim | – |
| Nuvem (p. 36) | **Geral** (plataforma genérica) | Mosquitto local; permite editar todos os parâmetros. |
| URL ou IP do Broker (p. 36) | `192.168.100.162` | IP do PC onde roda o Mosquitto (até 60 caracteres). |
| Porta de Serviço (p. 36) | `1883` | Sem TLS. |
| Usuário / Senha do Broker (p. 36) | Vazios (anônimo), a menos que o Mosquitto exija usuário | Vazio = anônimo. A configuração de autenticação do Mosquitto não consta no manual. |
| QoS (p. 36) | **1** | Entrega garantida até o broker; duplicatas possíveis (ver L5). |
| Reter Dados (p. 36) | **Desligado** | Mensagem retida volta a cada nova assinatura do integrador e duplica linha (ver L4). |
| ID do Dispositivo (p. 36) | `Teste_Alumbra` (até 20 caracteres; um por dispositivo) | Vira o `device_id` gravado. |
| Tópicos → Editar (p. 37) | Device data `NOVUS/Teste_Alumbra/events`; Config Ack `NOVUS/Teste_Alumbra/ack/config`; Command Ack `NOVUS/Teste_Alumbra/ack/command`; Config `NOVUS/Teste_Alumbra/config`; Command `NOVUS/Teste_Alumbra/command` | Padrão da Tabela 5. Com vários dispositivos, trocar só o `{id}`. |
| Segurança (p. 37) | Nenhuma | Rede local, porta 1883. |
| Publicar diagnóstico periodicamente (p. 37) | **Desligado até o integrador classificar as mensagens** (L2); depois, ligado | Hoje vira linha falsa no banco ou erro; depois de corrigido, é a melhor fonte de IP, SN, firmware e fila. |
| Intervalo de Registros (p. 40) | 10 a 30 s (hoje cerca de 10 s) | Com amostragem de 1 min, menos de 1 leitura por minuto deixa minutos sem dado. Um valor maior aumenta a cobertura da memória (3361 × intervalo). |
| Troca automática para intervalo alternativo (p. 40) | Pode ficar ligada, com **Intervalo alternativo bem abaixo de 10 min** (ex.: 60–120 s) | Acima de 10 min sem leitura o vigia reinicia o app (ver L6). |
| Relógio: GMT (p. 33) | -03:00 (Brasília) | Os timestamps continuam UTC (p. 15); o GMT parece servir ao zeramento por calendário e à meia-noite do diagnóstico (dedução; não consta no manual). |
| Servidor NTP (p. 33) | Habilitado, com um servidor alcançável pela rede da planta | Evita `ts_unix` errado. Qual servidor usar numa rede sem internet não consta no manual. |
| Canais digitais (p. 39) | Como já estão (Contador em `chd1`–`chd3`) | A `dbo.IOT001` só tem `chd1`–`chd3`. |

Fora do manual: o Mosquitto precisa escutar na interface de rede (não só em `localhost`) e aceitar o dispositivo
(anônimo ou com usuário). Isso depende da versão e do `mosquitto.conf` (não consta no manual). Como a produção já
recebe `NOVUS/Teste_Alumbra/events`, isso hoje está atendido.

---

## 7. Análise de lacunas no integrador (em ordem de prioridade)

Base: `app/core/mapeamento.py`, `parser.py`, `pipeline.py`, `mqtt_client.py`, `app/config.py` e `CLAUDE.md` na
versão v0.1.8. Os comportamentos marcados com **[simulado]** foram verificados passando os exemplos do manual por
`mapear()` com a assinatura `NOVUS/Teste_Alumbra/#` (sem alterar código):

| Mensagem (exemplo do manual) | Resultado de `mapear()` hoje |
| --- | --- |
| `channels` de produção | completa, chd = 67/119/0 (correto) |
| `events.chd1` (`timestamp` 1793386750.685) | **completa**, `ts_unix` 1793386750, chd1–3 = **NULL**, aviso "objeto sem 'value'" |
| `reported.diag` com `timestamp` válido | **completa**, chd1–3 = NULL |
| `reported.logs` / `reported.set_counters` | **completa**, chd1–3 = NULL, `ts_unix` = timestamp do pedido |
| `reported.gateway_485` com `timestamp` 15 | incompleta: "ts inválido" → **erro + contingência** |
| Config Ack sem `pid` (p. 94) / diag com `pid_id` (p. 20) | incompleta: "sem pid" → **erro + contingência** |
| `desired` em `.../command` | incompleta: "sem pid", `device_id` deduzido = **"command"** → **erro + contingência** |
| Evento com vírgula sobrando (como no manual) | "JSON inválido" → **erro + contingência** |
| `channels` só com analógica (sem chd1–3) | completa, chd1–3 = NULL |

### L1 (crítica): eventos (`events`) viram leitura com canais NULL e podem apagar a leitura do minuto

- **Estado (04/11/2026): resolvida.** `classificar()` em `app/core/mapeamento.py`: `events` não vira linha, não entra na amostragem e não é erro; vira o log INFO "Evento chd1 (borda de subida) em Teste_Alumbra às 14:31:05" (limitado a 20 logs por segundo). Testes em `tests/test_digirail.py`.

- **Problema:** um evento tem `pid`, `device_id` e `timestamp` (aninhado em `events.chdX`), então o mapeador o trata
  como leitura **completa** com `chd1..3 = NULL` [simulado]. A chave `chd1` da configuração casa com `events.chd1`
  (um objeto sem `value`), o que gera ainda um aviso a cada evento. Com amostragem (padrão), `_Amostragem` guarda
  por `device_id` a linha de **maior `recebido_em`**: se um evento chegar depois da última leitura `channels` do
  minuto, **a linha gravada no minuto é a do evento, toda NULL**, e a leitura real é contada como "descartada pela
  amostragem". Sem amostragem, cada evento vira uma linha extra de NULLs na `dbo.IOT001`.
- **Impacto:** alto quando algum canal estiver no modo Evento (os valores 67/119 sugerem que hoje está tudo em
  Contador, então o risco aparece na primeira reconfiguração). Dado errado no banco, sem nenhum erro visível.
- **Sugestão:** classificar a mensagem antes de mapear (tabela 3.8). Só `channels` (ou payloads genéricos sem
  `events`/`reported`/`desired`, para não quebrar outros dispositivos) vira linha da `IOT001`. `events` não entra
  na amostragem nem na tabela: log INFO "Evento chd1 borda de subida às …" e, se o usuário quiser guardar, uma
  tabela própria (decisão com o usuário, agente sqlserver). Restringir a busca dos canais a `channels` quando ele
  existir, ou tirar `chdN` puro das chaves padrão quando o valor for objeto sem `value`.
- **Arquivos:** `app/core/mapeamento.py` (classificação e `ResultadoMapa` com tipo), `app/core/pipeline.py`
  (roteamento: só `channels` vai para `_Amostragem`/sink), `app/config.py` (opção de chaves de envelope, ex.
  `[mapeamento] ignorar = ["events", "reported", "desired"]`), testes em `tests/test_mapeamento.py` e
  `tests/test_amostragem.py`.

### L2 (crítica): respostas de comando/configuração e diagnóstico periódico viram linha NULL ou erro falso

- **Estado (04/11/2026): resolvida.** `reported`/`desired` e tudo em `.../ack/command` e `.../ack/config` (inclusive JSON ilegível, vazio ou binário) é resposta: não é leitura nem erro e não dispara `dispositivo_visto`. `reported.diag` sai no sinal `diagnostico_dispositivo`; `reported.reset_counters` fecha o comando pendente (sinal `resposta_comando`). Pedidos `desired` de terceiros são ignorados em silêncio.

- **Problema:** com a assinatura `NOVUS/Teste_Alumbra/#`, chegam também `.../ack/command` e `.../ack/config`.
  - `reported.diag`, `reported.logs`, `set_counters`, `reset_counters`, `output` → linha **completa** com canais
    NULL e `ts_unix` = timestamp **do pedido**, não de uma leitura [simulado]. Com amostragem, pode substituir a
    leitura do minuto (mesmo mecanismo de L1).
  - Respostas com `timestamp` pequeno (o manual usa 15 no Gateway), Config Ack sem `pid` (p. 94), `logs_parsed` sem
    `pid` (p. 104) e diag com `pid_id` (p. 20) → **erro "payload" + contingência**: somam no card ERROS e enchem
    `contingencia/` com mensagens que nunca deveriam ir para o banco.
  - Pedidos `desired` publicados por terceiros em `.../command` ou `.../config` → erro "sem pid", e
    `dispositivo_do_topico` devolve **"command"** como device [simulado].
  - Se "Publicar diagnóstico periodicamente" estiver ligado: **2 mensagens a cada inicialização e todo dia à
    meia-noite** (p. 37). Isso dá pelo menos 1 linha falsa ou 1–2 erros por dia, justamente no minuto 00:00, perto
    da limpeza diária da caixa ATIVIDADE.
- **Impacto:** alto (linhas falsas no SQL, contador de erros inflado, contingência poluída). Também dispara
  `dispositivo_visto` com o tópico `ack/command`, o que pode trocar o tópico exibido na aba Clientes.
- **Sugestão:** a mesma classificação de L1. `reported`/`desired` nunca viram linha da `IOT001` e não contam erro;
  vão para o log (DEBUG ou INFO resumido) e, no caso de `diag`, alimentam o aproveitamento A1. Como alternativa
  mínima, a tela poderia sugerir assinar `NOVUS/Teste_Alumbra/events` (ou `NOVUS/+/events`), mas isso perde o
  diagnóstico; a classificação pelo payload é melhor e não depende de como os tópicos foram digitados.
- **Arquivos:** `app/core/mapeamento.py`, `app/core/pipeline.py` (não chamar `_erro("payload", ...)` para tipos
  conhecidos não-leitura; não chamar `_avisar_dispositivos` com tópico de ack, ou passar o tipo), `CLAUDE.md`
  (documentar a regra), testes.

### L3 (crítica): reenvio da memória circular é descartado pela amostragem

- **Estado (04/11/2026): resolvida.** A amostragem agrupa por `(device_id, minuto de ts_unix)` e fica com a leitura de maior `ts_unix` de cada minuto. A cada ciclo (hh:mm:00) grava todos os minutos encerrados, em ordem cronológica, inclusive os reenviados da memória; o minuto corrente espera. O log do ciclo informa "+ N recuperadas da memória do dispositivo" (minutos anteriores ao ciclo anterior). Ao desconectar/encerrar, grava todos os pendentes.

- **Problema:** depois de uma queda (broker fora, rede fora, **reinício do vigia, que reinicia junto o Mosquitto
  filho do app**), o dispositivo publica os registros pendentes com o **timestamp original** (seção 4.6). A
  `_Amostragem` agrupa pela janela de **recebimento** (`recebido_em`) e guarda só a última por `device_id`: uma
  rajada de N registros antigos que chega num mesmo minuto vira **1 linha**, e as N−1 vão como "descartadas pela
  amostragem". A lacuna no banco nunca é preenchida, embora o dispositivo tenha guardado os dados. Se o reenvio não
  for em ordem cronológica (não consta no manual), a linha que sobra pode até ser **antiga**, gravada como se fosse
  a do minuto atual.
- O mesmo acontece com mensagens enfileiradas pelo broker para a sessão persistente do integrador
  (`clean_session=False`) quando só o integrador fica fora do ar.
- **Impacto:** alto para a completude histórica (até cerca de 9 h de dados recuperáveis em produção, seção 4.6).
- **Sugestão:** amostrar por **minuto do `ts_unix`** (minuto da leitura), não pelo minuto de recebimento. Chave
  `(device_id, floor(ts_unix/intervalo))`; dentro do balde, vence o maior `ts_unix` (empate: último recebido). Na
  virada do ciclo, gravar todos os baldes de minutos **já fechados**, inclusive os antigos que chegaram atrasados;
  o balde do minuto corrente espera a virada. Para não regravar um minuto já gravado (reenvio duplicado), guardar
  em memória os últimos `(device_id, minuto)` gravados e/ou depender da chave única de L5. Ajustar o log do ciclo
  ("gravadas X, sendo Y atrasadas da memória do dispositivo"). Mensagens com `ts_unix` no **futuro** (maior que
  agora + 2 min) indicam relógio errado (L8).
- **Arquivos:** `app/core/pipeline.py` (`_Amostragem`, `_ciclo`), `tests/test_amostragem.py`,
  `tests/test_operacao_continua.py`, `CLAUDE.md` (seção "Operação contínua": a regra passa a ser "por minuto da
  leitura").

### L4 (alta): mensagens retidas (`retain`) duplicam linhas a cada reconexão

- **Estado (04/11/2026): resolvida pela deduplicação (L5).** A mensagem retida reentregue a cada reconexão tem o mesmo `(device_id, ts_unix)` e conta como "duplicada ignorada" (não é erro). Não foi criada a opção `ignorar_retidas`. Limite: a memória não sobrevive a um reinício do programa, então a última retida pode ser regravada uma vez depois de reiniciar (o índice único no banco resolveria).

- **Problema:** se "Reter Dados" estiver ligado no dispositivo (p. 36), o broker guarda a última mensagem de cada
  tópico e a reenvia **a cada SUBSCRIBE**. O `ClienteMqtt` reassina em todo `on_connect` (correto), então a cada
  reconexão a última leitura (e a última resposta de diag) volta. O pipeline só acrescenta "(retida)" ao log
  (`pipeline.py`, `extra = " (retida)"`) e grava. Sem amostragem, isso gera linha **duplicada**; com amostragem,
  uma leitura velha entra na janela atual (ver L3).
- **Impacto:** médio. Fácil de evitar na configuração (seção 6: Reter Dados desligado), mas o integrador não se
  protege.
- **Sugestão:** ignorar mensagens com `retain=1` para gravação (contar, logar "retida ignorada") ou só aceitar se
  `(device_id, ts_unix)` ainda não foi visto. Opção em `config.py` (`[mqtt] ignorar_retidas = true`).
- **Arquivos:** `app/core/pipeline.py` (`_processar`), `app/config.py`, testes.

### L5 (alta): duplicatas com QoS 1 e sem chave única no banco

- **Estado (04/11/2026): resolvida em memória; índice único no banco pendente (decisão do usuário).** `_MemoriaGravadas` (`app/core/pipeline.py`): 24 h, até 200 mil chaves, vale entre conexões enquanto o programa está aberto. Não grava de novo o mesmo `(device_id, ts_unix)` e, com amostragem, o mesmo `(device_id, minuto)`. Com `intervalo_gravacao_segundos = 0`, só a regra `(device_id, ts_unix)` vale (grava todas as leituras distintas).

- **Problema:** o dispositivo publica com QoS 0 ou 1 (p. 36). Com QoS 1, o MQTT entrega "pelo menos uma vez", então
  uma leitura pode chegar duas vezes, sobretudo em reconexões e no reenvio da memória. Sem amostragem (intervalo 0),
  ambas são inseridas. Com amostragem por recebimento, a duplicata pode cair em outro minuto e ser gravada de novo.
  Somado à pendência "duplicação de lote se o commit chegar e a resposta se perder" (CLAUDE.md), não existe defesa
  no banco.
- **Atenção:** `pid` é o **código do modelo** (constante), não um id de pacote, e não serve para deduplicar. A
  chave natural de uma leitura do DigiRail é **`(device_id, ts_unix)`** (dedução: timestamp em segundos da leitura,
  um registro por intervalo).
- **Sugestão:** deduplicar em memória por `(device_id, ts_unix)` na janela e, no banco, propor ao usuário um índice
  único filtrado ou `IGNORE_DUP_KEY` em `(device_id, ts_unix)` (decisão do usuário; agente sqlserver; a tabela é do
  usuário e o app não a altera). Isso resolveria também a pendência do commit perdido.
- **Arquivos:** `app/core/pipeline.py`, `app/core/sink_sqlserver.py` (tratar a violação de chave única como
  "já gravado", não como erro), `sql/` (script proposto), `CLAUDE.md` (pendências).

### L6 (média): vigia de 10 min contra intervalos longos do dispositivo, e reinício que causa nova queda

- **Estado (04/11/2026): pendente.** Ajuda indireta: o diagnóstico automático mostra `mqtt_queue` na aba Clientes.

- **Problema:** o vigia reinicia o app depois de 10 min sem leitura. O dispositivo aceita `pub_interval` até
  65535 s e `alter_pub_interval` de 60 a 65535 s (p. 106); o exemplo do manual usa 600 s (p. 96), exatamente 10 min.
  Se o intervalo normal ou o alternativo for maior ou igual a 10 min, o vigia reinicia o app sem necessidade. O
  reinício derruba o Mosquitto (processo filho), o dispositivo perde a conexão, a fila cresce, passa de 10 % e o
  dispositivo pode entrar (ou continuar) no intervalo alternativo longo. É um ciclo que se realimenta.
- **Impacto:** médio. Hoje, com cerca de 10 s, não ocorre. Ocorre se alguém ligar o intervalo alternativo com
  valor alto.
- **Sugestão:** documentar no `config.example.toml`/`CLAUDE.md` que `[vigia] minutos` deve ser maior que o maior
  intervalo do dispositivo (normal e alternativo) com folga. Com A1, ler `pub_interval`/`alter_pub_interval` via
  consulta `{"device":{}}` ou observar o log `alter_int` e avisar se o vigia estiver curto. Considerar diag ou
  qualquer mensagem do dispositivo como sinal de vida (isso já acontece: `ultima_leitura` conta qualquer mensagem).
- **Arquivos:** `app/config.py` (`VigiaConfig`, comentário/validação), GUI/bridge só para exibir aviso,
  `CLAUDE.md`.

### L7 (média): JSON levemente inválido nos exemplos do manual

- **Estado (04/11/2026): parcial.** Nos tópicos `ack/*`, JSON inválido vira resposta ilegível (AVISO, não é erro). No tópico de dados, evento com vírgula sobrando continua "JSON inválido" (erro); falta captura real para decidir o fallback tolerante.

- **Problema:** os exemplos de evento e de `log_counters` têm vírgula sobrando (p. 16, 21, 93), e o do Gateway tem
  `;` (p. 22). Se o firmware realmente publicar assim, o `orjson`/`json` rejeita: "JSON inválido" → erro +
  contingência [simulado]. Pode ser só erro de digitação do manual.
- **Impacto:** médio, mas incerto. Só se confirma com uma captura real de um evento e de um diag.
- **Sugestão:** primeiro capturar (`mosquitto_sub -v -t 'NOVUS/#'`) com um canal em modo Evento e com diag
  periódico ligado. Se confirmado, um fallback tolerante só para esse caso: remover `,` antes de `}`/`]` fora de
  strings e tentar de novo, registrando um AVISO.
- **Arquivos:** `app/core/parser.py` (`_json_loads`), `app/core/mapeamento.py`, testes.

### L8 (média): relógio do dispositivo não é verificado

- **Estado (04/11/2026): resolvida (parte do futuro).** Leitura com `ts_unix` mais de 5 min à frente do relógio do PC é gravada, com um AVISO uma vez por dispositivo (enquanto o programa está aberto). Ela vai no próximo ciclo, sem esperar o minuto dela chegar. Atraso não gera aviso, porque o reenvio da memória é normal.

- **Problema:** `ts_unix` vem do RTC do dispositivo (p. 16). Sem NTP, ou com a pilha fraca, as leituras são gravadas
  com horário errado, e `data_hora_utc` e a amostragem por `ts_unix` (L3) ficam errados também. Hoje o mapeador só
  rejeita fora de 2000–3000.
- **Sugestão:** comparar `ts_unix` com `recebido_em`. **Futuro** com mais de 2 min de diferença = relógio adiantado
  (AVISO, uma vez por hora por device). Atraso grande só é suspeito se não houver reenvio em curso
  (`mqtt_queue = 0` no diag, A1). Com A1, comparar `curr_timestamp` do diag com a hora do PC.
- **Arquivos:** `app/core/pipeline.py` ou `app/core/mapeamento.py` (aviso), `CLAUDE.md`.

### L9 (baixa): `device_id` deduzido do tópico pega o nível errado no padrão NOVUS

- **Estado (04/11/2026): resolvida.** `dispositivo_novus()` em `app/core/parser.py`: o `{id}` é o nível antes de `events`/`config`/`command`/`ack/config`/`ack/command` no fim do tópico. Os outros tópicos seguem a regra do curinga.

- **Problema:** `dispositivo_do_topico` devolve o nível que casa com o primeiro curinga. Com
  `NOVUS/Teste_Alumbra/#`, para `NOVUS/Teste_Alumbra/events` ele devolve **"events"** (e "ack" ou "command" nos
  outros). Só é usado quando falta `device_id` no payload, o que pelo manual só acontece em pedidos `desired`, mas
  nesses casos a contingência registra o device errado.
- **Sugestão:** reconhecer o padrão `NOVUS/{id}/(events|config|command|ack/config|ack/command)` e usar `{id}`, ou
  usar o último nível fixo antes do curinga quando o nível do curinga for um desses nomes. Com L2 resolvido, o caso
  praticamente some.
- **Arquivos:** `app/core/parser.py` (`dispositivo_do_topico`), testes.

### L10 (baixa): semântica de `pid` documentada errado na configuração

- **Estado (04/11/2026): resolvida.** As chaves alternativas de `pid` passaram a ser `pid`, `pid_id` (exemplo do manual, p. 20), `product_id` e `id_produto`. Saíram `packet_id`, `id_pacote`, `seq` e `sequencia`. Comentários em `app/config.py`, `config.example.toml` e `mapeamento.py`.

- **Problema:** `MapeamentoConfig.pid` aceita `packet_id`, `id_pacote`, `seq`, `sequencia`, o que sugere "número de
  pacote". No DigiRail, `pid` é o **código do modelo** (p. 15). O valor de produção (51387408) nem está na tabela
  de modelos. Risco: alguém usar `pid` como chave ou contador de sequência.
- **Sugestão:** documentar em `mapeamento.py`/`CLAUDE.md` que, para o DigiRail, `pid` é constante por modelo. Manter
  os aliases para outros dispositivos.
- **Arquivos:** `app/core/mapeamento.py` (docstring), `app/config.py` (comentário), `CLAUDE.md`.

### L11 (baixa): canais além de `chd1`–`chd3` só ficam no `payload_json`

- **Estado (04/11/2026): pendente.** O sinal `canais_vistos` informa à GUI os canais presentes em `channels` (chdN, chN, chrN), mas não há aviso nem coluna nova.

- **Problema:** `chd4`–`chd6`, `ch1_user_range`/`ch2_user_range` e `chr1`–`chr8` (p. 15–16) não têm coluna. Se o
  usuário habilitar a analógica e desabilitar `chd1`–`chd3`, a linha sai **completa com os três canais NULL** sem
  aviso [simulado]. O dado fica só em `payload_json`, que pode ser lido com `OPENJSON`.
- **Sugestão:** AVISO (uma vez por device) quando `channels` tiver chaves de canal que não estão mapeadas, ou quando
  nenhum `chd1..3` vier. Colunas novas só com decisão do usuário (a tabela é dele).
- **Arquivos:** `app/core/mapeamento.py`, `CLAUDE.md`.

### O que pode ser aproveitado

Estado em 04/11/2026: **A1** feito (sinal `diagnostico_dispositivo`, inclusive o diagnóstico periódico publicado pelo
próprio aparelho); **A3** feito (`diag` pedido ao ver o dispositivo pela 1ª vez em cada conexão e a cada
`[dispositivos] diagnostico_minutos`); **A6** feito (log do ciclo com as recuperadas). Também feito: comando
`reset_counters` (zerar contadores por canal, sinal `resposta_comando`, falha = erro categoria `"comando"`). A2, A4 e
A5 continuam pendentes.

| # | Ideia | Fonte no manual | Esforço / dependência |
| --- | --- | --- | --- |
| A1 | **Ler o diagnóstico** (`reported.diag`) que já chega no Command Ack quando o periódico está ligado: preencher na aba Clientes **IP** (`ipv4`, dispensa `arp`/`mosquitto -v` para este dispositivo), **número de série**, **firmware**, RSSI, **`mqtt_queue`** (mostrar "N registros pendentes no dispositivo", que explica rajadas atrasadas, L3) e diferença de relógio (`curr_timestamp`, L8). Novo sinal no bridge, ex. `diagnostico_dispositivo = Signal(str, str)` (device_id, JSON). | p. 20–21, 37, 101–102 | Médio; depende de L2. Contrato core ↔ gui novo (orquestrador e agente python-gui). |
| A2 | **`logs_parsed`**: mostrar as quedas de broker vistas pelo dispositivo (códigos 9, 10, 24, 25) junto com os incidentes "mqtt" do app, e os reinícios (0–3) na janela de erros como informação. | p. 23–24, 104–105 | Baixo, depois de L2. |
| A3 | **Pedir `diag` ao conectar** (publicar `{"timestamp":<agora>,"desired":{"diag":{}}}` em `.../command`), sem esperar a meia-noite. Seria o primeiro *publish* do integrador. | p. 20, 101 | Decisão do usuário (o app passa a escrever no broker); tópico de comando configurável. |
| A4 | **Consultar a configuração** (`{"device":{}}`, `{"chd1":{}}`) para conferir `pub_interval`, intervalo alternativo e modo dos canais, e avisar se o vigia estiver curto (L6) ou se um canal estiver em modo Evento (L1). | p. 95–96 | Depende de A3. |
| A5 | **Guardar eventos** de borda com milissegundos em tabela própria (ex. `dbo.IOT001_EVENTOS`). | p. 16, 93 | Decisão do usuário e agente sqlserver. |
| A6 | **Detectar reenvio**: com L3, contar no log do ciclo quantas leituras chegaram atrasadas e de quando, e mostrar na ATIVIDADE "recuperadas da memória do dispositivo". | p. 21, 40–41, 43 | Baixo, junto com L3. |

### Ordem sugerida de implementação

(Itens 1 e 2 e parte do 3 e do 4 feitos em 04/11/2026; ver o estado em cada lacuna.)

1. L1 + L2 (classificação de mensagens; uma mudança única no mapeador e no pipeline).
2. L3 (amostragem por minuto da leitura) + L4 (retidas) + dedup em memória de L5.
3. Captura real para L7; ajustes de L8, L9, L10 e L11.
4. Aproveitamentos A1, A2, A6; A3, A4 e A5 só depois de decisão do usuário. Chave única no banco (L5) também
   depende do usuário.
