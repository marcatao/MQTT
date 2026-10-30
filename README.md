# MQTT

Cliente MQTT em Node.js com [mqtt.js](https://github.com/mqttjs/MQTT.js): um publisher, um subscriber e um broker
local ([aedes](https://github.com/moscajs/aedes)) para testar sem servidor externo.

## Estrutura

- `src/config.js`: lê a configuração das variáveis de ambiente.
- `src/publisher.js`: publica uma mensagem, ou uma leitura simulada (JSON) a cada N segundos.
- `src/subscriber.js`: assina um tópico (aceita curingas `+` e `#`) e imprime as mensagens.
- `src/broker.js`: broker MQTT local na porta 1883 (só para desenvolvimento).

## Instalação

Requer Node.js 22 ou mais novo.

```sh
npm install
cp .env.example .env   # opcional: ajuste broker, usuário, senha, tópico e QoS
```

Sem `.env`, o Node avisa "`.env not found`" e usa os padrões (`mqtt://localhost:1883`, tópico `marcatron/teste`, QoS 1).

## Uso

Em três terminais:

```sh
npm run broker                         # broker local
npm run sub -- 'marcatron/#'           # assina tudo abaixo de marcatron/
npm run pub -- "olá mundo"             # publica uma vez
```

Outras formas do publisher:

```sh
npm run pub -- --interval 5                       # leitura simulada a cada 5 s, até Ctrl+C
npm run pub -- --topic marcatron/status --retain "online"   # mensagem retida
```

Para usar um broker externo, troque `MQTT_URL` no `.env` (ex.: `mqtts://broker.exemplo.com:8883` ou
`wss://broker.exemplo.com/mqtt`) e preencha `MQTT_USERNAME` e `MQTT_PASSWORD` se ele exigir.
