// Assina um tópico e imprime cada mensagem recebida.
// Uso: npm run sub -- [tópico]   (aceita curingas, ex.: marcatron/#)
import mqtt from 'mqtt';
import { config, connectOptions } from './config.js';

const topic = process.argv[2] || config.topic;
const client = mqtt.connect(config.url, connectOptions('sub'));

client.on('connect', async () => {
  console.log(`Conectado a ${config.url}`);
  await client.subscribeAsync(topic, { qos: config.qos });
  console.log(`Assinando "${topic}" (QoS ${config.qos}). Ctrl+C para sair.`);
});

client.on('message', (t, payload, packet) => {
  const hora = new Date().toLocaleTimeString('pt-BR');
  const retida = packet.retain ? ' [retida]' : '';
  console.log(`[${hora}] ${t}${retida}: ${payload.toString()}`);
});

client.on('reconnect', () => console.log('Reconectando...'));
client.on('error', (err) => console.error('Erro:', err.message));

process.on('SIGINT', () => client.end(() => process.exit(0)));
