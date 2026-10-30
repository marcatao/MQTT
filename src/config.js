// Lê a configuração das variáveis de ambiente (ou do arquivo .env).
export const config = {
  url: process.env.MQTT_URL || 'mqtt://localhost:1883',
  username: process.env.MQTT_USERNAME || undefined,
  password: process.env.MQTT_PASSWORD || undefined,
  topic: process.env.MQTT_TOPIC || 'marcatron/teste',
  qos: Number(process.env.MQTT_QOS ?? 1),
};

export function connectOptions(clientPrefix) {
  return {
    clientId: `${clientPrefix}-${Math.random().toString(16).slice(2, 10)}`,
    username: config.username,
    password: config.password,
    reconnectPeriod: 2000,
    connectTimeout: 10_000,
  };
}
