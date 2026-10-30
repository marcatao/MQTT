// Broker MQTT local (aedes) para testar sem depender de um servidor externo.
import { createServer } from 'node:net';
import { Aedes } from 'aedes';

const port = Number(process.env.BROKER_PORT || 1883);
const aedes = await Aedes.createBroker();
const server = createServer(aedes.handle);

aedes.on('client', (c) => console.log(`+ cliente ${c.id}`));
aedes.on('clientDisconnect', (c) => console.log(`- cliente ${c.id}`));

server.listen(port, () => console.log(`Broker MQTT ouvindo em mqtt://localhost:${port}`));

process.on('SIGINT', () => {
  server.close();
  aedes.close(() => process.exit(0));
});
