// Publica uma mensagem e sai, ou publica uma leitura simulada a cada N segundos.
// Uso: npm run pub -- "mensagem"        publica uma vez em MQTT_TOPIC
//      npm run pub -- --interval 5      publica JSON a cada 5 s até Ctrl+C
//      opções: --topic <t>  --retain
import { parseArgs } from 'node:util';
import mqtt from 'mqtt';
import { config, connectOptions } from './config.js';

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    topic: { type: 'string', default: config.topic },
    interval: { type: 'string' },
    retain: { type: 'boolean', default: false },
  },
});

const client = await mqtt.connectAsync(config.url, connectOptions('pub'));
const opts = { qos: config.qos, retain: values.retain };

if (values.interval) {
  const ms = Number(values.interval) * 1000;
  let seq = 0;
  const publicar = async () => {
    const leitura = { seq: ++seq, temperatura: +(20 + Math.random() * 10).toFixed(1), ts: new Date().toISOString() };
    await client.publishAsync(values.topic, JSON.stringify(leitura), opts);
    console.log(`→ ${values.topic}: ${JSON.stringify(leitura)}`);
  };
  await publicar();
  const timer = setInterval(publicar, ms);
  process.on('SIGINT', async () => {
    clearInterval(timer);
    await client.endAsync();
    process.exit(0);
  });
} else {
  const mensagem = positionals.join(' ') || 'Olá do Marcatron!';
  await client.publishAsync(values.topic, mensagem, opts);
  console.log(`→ ${values.topic}: ${mensagem}`);
  await client.endAsync();
}
