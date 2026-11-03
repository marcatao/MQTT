"""Cliente MQTT (paho-mqtt 2.x): conexão, assinatura, reconexão automática e mensagens de log em português.

O callback ``on_message`` do paho só repassa (``ao_mensagem``) e retorna: quem chama deve apenas enfileirar.

Eventos de conexão (``ao_evento(tipo, motivo)``), para o pipeline contar erros por incidente:
- ``"conectado"``: CONNACK aceito;
- ``"perdida"``: conexão caiu sem o usuário pedir (estava conectado);
- ``"falha_conexao"``: tentativa de conexão falhou (broker inacessível ou CONNACK recusado);
- ``"assinatura_recusada"``: SUBACK com falha (motivo = código devolvido pelo broker).
Nada é avisado quando a desconexão foi pedida (``parar``). O cliente avisa todas as tentativas; quem decide
"um erro por queda" é o pipeline.
"""
from __future__ import annotations

import socket
import ssl
import uuid
from typing import Callable

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode

Logar = Callable[[str, str], None]           # (nivel, mensagem)
Status = Callable[[str, str], None]          # (estado, detalhe)
AoMensagem = Callable[[str, bytes, int, bool], None]  # (topico, payload, qos, retida)
AoEvento = Callable[[str, str], None]        # (tipo, motivo): ver docstring do módulo

# CONNACK (MQTT 3.1.1 mapeado pelo paho para os códigos do MQTT 5) → mensagem clara
_MOTIVOS_CONNACK = {
    128: "erro não especificado",
    129: "pacote malformado",
    130: "erro de protocolo",
    131: "erro específico da implementação",
    132: "versão do protocolo MQTT não suportada pelo broker",
    133: "identificador do cliente (client_id) rejeitado",
    134: "usuário ou senha inválidos",
    135: "não autorizado (confira usuário/senha e as permissões no broker)",
    136: "servidor indisponível",
    137: "servidor ocupado",
    138: "cliente banido",
    140: "método de autenticação inválido",
    149: "pacote grande demais",
    151: "cota excedida",
    156: "use outro servidor",
    157: "o servidor mudou de endereço",
    159: "taxa de conexões excedida",
}

# DISCONNECT (o paho converte as quedas de rede do MQTT 3.1.1 nesses códigos) → mensagem clara
_MOTIVOS_DESCONEXAO = {
    0: "desconexão normal",
    128: "conexão de rede interrompida (broker fechou ou rede caiu)",
    139: "o broker está sendo desligado",
    141: "tempo de keepalive esgotado (broker não respondeu)",
    142: "sessão assumida por outro cliente com o mesmo client_id",
}


def validar_topico_assinatura(topico: str) -> str:
    """Valida um filtro de tópico para SUBSCRIBE e devolve-o sem espaços nas pontas."""
    t = (topico or "").strip()
    if not t:
        raise ValueError("Informe o tópico.")
    if "\x00" in t:
        raise ValueError("Tópico inválido: contém caractere nulo.")
    if len(t.encode("utf-8")) > 65535:
        raise ValueError("Tópico longo demais.")
    niveis = t.split("/")
    for i, nivel in enumerate(niveis):
        if "#" in nivel and (nivel != "#" or i != len(niveis) - 1):
            raise ValueError("Tópico inválido: '#' só pode ser o último nível (ex.: NOVUS/Teste_Alumbra/#).")
        if "+" in nivel and nivel != "+":
            raise ValueError("Tópico inválido: '+' precisa ocupar um nível inteiro (ex.: NOVUS/+/dados).")
    return t


def validar_parametros(broker: str, porta: int, topico: str, qos: int) -> tuple[str, int, str, int]:
    b = (broker or "").strip()
    if not b:
        raise ValueError("Informe o endereço do broker.")
    try:
        p = int(porta)
    except (TypeError, ValueError):
        raise ValueError(f"Porta inválida: {porta!r}.") from None
    if not 1 <= p <= 65535:
        raise ValueError(f"Porta inválida: {p} (use 1 a 65535).")
    try:
        q = int(qos)
    except (TypeError, ValueError):
        raise ValueError(f"QoS inválido: {qos!r}.") from None
    if q not in (0, 1, 2):
        raise ValueError(f"QoS inválido: {q} (use 0, 1 ou 2).")
    return b, p, validar_topico_assinatura(topico), q


def client_id_padrao() -> str:
    """Estável por máquina (sessão persistente funciona entre reconexões) e único entre máquinas."""
    try:
        host = socket.gethostname() or "pc"
    except OSError:  # pragma: no cover
        host = "pc"
    base = "".join(c for c in host if c.isalnum() or c in "-_")[:40] or "pc"
    return f"mqtt-sqlserver-{base}"


def _texto_motivo(reason_code, tabela: dict[int, str] = _MOTIVOS_CONNACK) -> str:
    valor = getattr(reason_code, "value", reason_code)
    try:
        valor = int(valor)
    except (TypeError, ValueError):
        return str(reason_code)
    return tabela.get(valor, f"{reason_code} (código {valor})")


def _texto_desconexao(reason_code) -> str:
    return _texto_motivo(reason_code, {**_MOTIVOS_CONNACK, **_MOTIVOS_DESCONEXAO})


class ClienteMqtt:
    """Embrulha o ``paho.mqtt.client.Client`` com reconexão (1 s até 60 s) e reassinatura a cada conexão."""

    def __init__(
        self,
        broker: str,
        porta: int,
        topico: str,
        qos: int,
        ao_mensagem: AoMensagem,
        ao_status: Status,
        logar: Logar,
        usuario: str = "",
        senha: str = "",
        client_id: str | None = None,
        sessao_persistente: bool = True,
        keepalive: int = 30,
        tls: bool | None = None,
        ao_evento: AoEvento | None = None,
    ) -> None:
        self.broker, self.porta, self.topico, self.qos = validar_parametros(broker, porta, topico, qos)
        self._ao_mensagem = ao_mensagem
        self._ao_status = ao_status
        self._logar = logar
        self._ao_evento = ao_evento
        self._keepalive = keepalive
        self._parando = False
        self._conectado = False
        self.endereco = f"{self.broker}:{self.porta}"

        cid = client_id or client_id_padrao()
        if not sessao_persistente:
            cid = f"{cid}-{uuid.uuid4().hex[:6]}"
        self._cliente = mqtt.Client(
            CallbackAPIVersion.VERSION2,
            client_id=cid,
            clean_session=not sessao_persistente,
            protocol=mqtt.MQTTv311,
            reconnect_on_failure=True,
        )
        self._cliente.reconnect_delay_set(min_delay=1, max_delay=60)
        self._cliente.max_inflight_messages_set(100)
        self._cliente.max_queued_messages_set(0)
        if usuario:
            self._cliente.username_pw_set(usuario, senha or None)
        if tls if tls is not None else self.porta == 8883:
            self._cliente.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        self._cliente.on_connect = self._on_connect
        self._cliente.on_connect_fail = self._on_connect_fail
        self._cliente.on_disconnect = self._on_disconnect
        self._cliente.on_subscribe = self._on_subscribe
        self._cliente.on_message = self._on_message

    @property
    def conectado(self) -> bool:
        return self._conectado

    # -- controle -----------------------------------------------------------------------------------

    def iniciar(self) -> None:
        """Não bloqueia: a conexão (e as reconexões) acontecem na thread de rede do paho."""
        self._parando = False
        self._ao_status("conectando", f"Conectando a {self.endereco}…")
        self._logar("INFO", f"Conectando a {self.endereco}…")
        self._cliente.connect_async(self.broker, self.porta, keepalive=self._keepalive)
        self._cliente.loop_start()

    def parar(self) -> None:
        """Desconecta e para a thread de rede (bloqueia até ela terminar; chame fora da GUI)."""
        self._parando = True
        try:
            self._cliente.disconnect()
        except Exception:  # noqa: BLE001
            pass
        self._cliente.loop_stop()
        self._conectado = False

    # -- callbacks do paho (thread de rede) ----------------------------------------------------------

    def _evento(self, tipo: str, motivo: str = "") -> None:
        if self._ao_evento is None or self._parando:
            return
        try:
            self._ao_evento(tipo, motivo)
        except Exception:  # noqa: BLE001 - nunca derruba a thread de rede do paho
            pass

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code.is_failure:
            self._conectado = False
            motivo = _texto_motivo(reason_code)
            self._logar("ERRO", f"Broker {self.endereco} recusou a conexão: {motivo}")
            self._ao_status("erro", f"Conexão recusada: {motivo}")
            self._evento("falha_conexao", f"Broker recusou a conexão: {motivo}")
            return
        self._conectado = True
        sessao = " (sessão anterior retomada)" if getattr(flags, "session_present", False) else ""
        self._logar("INFO", f"Conectado a {self.endereco}{sessao}")
        self._ao_status("conectado", f"Conectado a {self.endereco}")
        self._evento("conectado")
        resultado, _mid = client.subscribe(self.topico, qos=self.qos)
        if resultado != MQTTErrorCode.MQTT_ERR_SUCCESS:
            self._logar("ERRO", f"Falha ao pedir assinatura de {self.topico}: {mqtt.error_string(resultado)}")

    def _on_connect_fail(self, client, userdata) -> None:
        if self._parando:
            return
        self._logar("AVISO", f"Não foi possível conectar a {self.endereco} (broker inacessível?). "
                             "Tentando de novo automaticamente…")
        self._ao_status("conectando", f"Tentando conectar a {self.endereco}…")
        self._evento("falha_conexao", "Broker inacessível (sem resposta, porta fechada ou rede fora do ar)")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        estava = self._conectado
        self._conectado = False
        if self._parando:
            self._logar("INFO", f"Desconectado de {self.endereco}")
            return
        if estava:
            motivo = _texto_desconexao(reason_code)
            self._logar("AVISO", f"Conexão com {self.endereco} perdida ({motivo}). Reconectando…")
            self._ao_status("conectando", f"Reconectando a {self.endereco}…")
            self._evento("perdida", motivo)

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties) -> None:
        for rc in reason_codes:
            if rc.is_failure:
                self._logar("ERRO", f"Broker recusou a assinatura de {self.topico} ({rc}). "
                                    "Verifique as permissões (ACL) do usuário.")
                self._ao_status("erro", f"Assinatura recusada: {self.topico}")
                self._evento("assinatura_recusada", str(rc))
            else:
                concedido = int(getattr(rc, "value", rc))
                extra = f"; broker concedeu QoS {concedido}" if concedido != self.qos else ""
                self._logar("INFO", f"Assinado {self.topico} (QoS {self.qos}{extra})")

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage) -> None:
        # Sessão persistente pode trazer mensagens de uma assinatura antiga: ignora o que não casa.
        if not mqtt.topic_matches_sub(self.topico, msg.topic):
            return
        self._ao_mensagem(msg.topic, msg.payload, msg.qos, bool(msg.retain))
