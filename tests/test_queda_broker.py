"""Erros "mqtt" do Pipeline: um erro por queda do broker (incidente), não um por tentativa de reconexão.

Cliente MQTT falso (o teste dispara os eventos como se fosse a thread do paho) e relógio injetado. Em todos os testes
confere a regra de ouro: contador de erros == registros pelo callback == linhas em logs/erros_AAAAMMDD.jsonl.
No fim, um teste de integração com o broker amqtt local: derruba o broker e sobe de novo.
"""
from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from app.config import Config, SqlServerConfig
from app.core.erros import ler_erros_do_dia
from app.core.mqtt_client import ClienteMqtt
from app.core.pipeline import Pipeline, duracao_texto
from tests.test_amostragem import ASSINATURA, ClienteFalso, Relogio, esperar, novus

INICIO = datetime(2026, 11, 3, 14, 30, 10).timestamp()
DIA = date(2026, 11, 3)
TOPICO = "NOVUS/Teste_Alumbra/events"
BROKER = "192.168.100.162"
ENDERECO = f"{BROKER}:1883"


class ClienteQueda(ClienteFalso):
    """ClienteFalso que também guarda ``ao_evento``; ``iniciar`` não conecta: o teste decide o que acontece."""

    def __init__(self, broker, porta, topico, qos, ao_mensagem, ao_status, logar, ao_evento=None, **kw) -> None:
        super().__init__(broker, porta, topico, qos, ao_mensagem, ao_status, logar, **kw)
        self.ao_evento = ao_evento
        self.parado = False

    def iniciar(self) -> None:
        self.ao_status("conectando", "Conectando (falso)")

    def parar(self) -> None:
        self.parado = True

    # atalhos que imitam o ClienteMqtt real
    def conectou(self) -> None:
        self.ao_status("conectado", "Conectado (falso)")
        self.ao_evento("conectado", "")

    def perdeu(self, motivo: str = "conexão de rede interrompida (broker fechou ou rede caiu)") -> None:
        self.ao_evento("perdida", motivo)

    def falhou(self, motivo: str = "Broker inacessível (sem resposta, porta fechada ou rede fora do ar)") -> None:
        self.ao_evento("falha_conexao", motivo)


class Coletor:
    def __init__(self) -> None:
        self.erros: list[str] = []
        self.logs: list[tuple[str, str]] = []

    def kwargs(self) -> dict:
        return dict(ao_erro=self.erros.append, ao_log=lambda n, m: self.logs.append((n, m)))

    @property
    def registros(self) -> list[dict]:
        return [json.loads(e) for e in self.erros]

    def infos(self, trecho: str) -> list[str]:
        return [m for n, m in self.logs if n == "INFO" and trecho in m]


def _config(tmp_path) -> Config:
    cfg = Config()
    cfg.sqlserver = SqlServerConfig(servidor="", banco="")
    cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
    cfg.gravacao.pasta_logs = str(tmp_path / "logs")
    cfg.gravacao.intervalo_gravacao_segundos = 0
    cfg.gravacao.lote_segundos = 0.05
    return cfg


@pytest.fixture
def montar(tmp_path):
    criados: list[Pipeline] = []

    def _montar():
        relogio = Relogio(INICIO)
        col = Coletor()
        p = Pipeline(_config(tmp_path), relogio=relogio, fabrica_cliente=ClienteQueda, sessao_persistente=False,
                     pasta=tmp_path, **col.kwargs())
        p.conectar(BROKER, 1883, ASSINATURA, 1).result(5)
        criados.append(p)
        return p, ClienteFalso.ultimo, relogio, col

    yield _montar
    for p in criados:
        p.encerrar(5)


def _bate(p: Pipeline, col: Coletor, tmp_path, esperado: int) -> None:
    """Regra de ouro: contador == registros pelo callback == linhas no arquivo do dia == esperado."""
    assert p.contadores[2] == esperado
    assert len(col.erros) == esperado
    assert len(ler_erros_do_dia(tmp_path / "logs", DIA)) == esperado


def test_duracao_texto():
    assert duracao_texto(0) == "0 s"
    assert duracao_texto(12) == "12 s"
    assert duracao_texto(192) == "3 min 12 s"
    assert duracao_texto(3600) == "1 h 0 min 0 s"
    assert duracao_texto(3725.4) == "1 h 2 min 5 s"


def test_queda_apos_conectado_gera_um_erro_mesmo_com_varias_tentativas(montar, tmp_path):
    p, cli, relogio, col = montar()
    cli.conectou()
    relogio.ir(INICIO + 5)
    cli.entregar(TOPICO, novus(1))
    relogio.ir(INICIO + 60)
    cli.perdeu("tempo de keepalive esgotado (broker não respondeu)")
    for i in range(1, 8):                     # o paho tenta de novo a cada 1–60 s
        relogio.ir(INICIO + 60 + i * 10)
        cli.falhou()
    _bate(p, col, tmp_path, 1)
    reg, = col.registros
    assert reg["categoria"] == "mqtt"
    assert reg["mensagem"] == f"Conexão com o broker {ENDERECO} perdida"
    assert "tempo de keepalive esgotado" in reg["detalhe"]
    assert "última mensagem recebida em 2026-11-03T14:30:15" in reg["detalhe"]
    assert reg["quando"] == "2026-11-03T14:31:10"
    assert reg["topico"] is None
    p.desconectar().result(5)
    _bate(p, col, tmp_path, 1)


def test_queda_sem_mensagens_recebidas(montar, tmp_path):
    p, cli, relogio, col = montar()
    cli.conectou()
    cli.perdeu()
    _bate(p, col, tmp_path, 1)
    assert "nenhuma mensagem recebida nesta conexão" in col.registros[0]["detalhe"]


def test_volta_gera_info_com_duracao_e_nova_queda_gera_outro_erro(montar, tmp_path):
    p, cli, relogio, col = montar()
    cli.conectou()
    assert col.infos("Broker voltou") == []    # a primeira conexão não é "volta"
    relogio.ir(INICIO + 100)
    cli.perdeu()
    relogio.ir(INICIO + 130)
    cli.falhou()
    relogio.ir(INICIO + 100 + 192)
    cli.conectou()
    assert col.infos("Broker voltou") == ["Broker voltou após 3 min 12 s"]
    assert not any(n == "ERRO" and "voltou" in m for n, m in col.logs)
    _bate(p, col, tmp_path, 1)

    relogio.ir(INICIO + 1000)
    cli.conectou()                             # reconexão sem incidente aberto: nada de novo
    assert len(col.infos("Broker voltou")) == 1

    relogio.ir(INICIO + 2000)
    cli.perdeu()                               # queda nova → erro novo
    cli.falhou()
    cli.falhou()
    _bate(p, col, tmp_path, 2)
    relogio.ir(INICIO + 2005)
    cli.conectou()
    assert col.infos("Broker voltou")[-1] == "Broker voltou após 5 s"
    assert [r["categoria"] for r in col.registros] == ["mqtt", "mqtt"]
    _bate(p, col, tmp_path, 2)


def test_falha_na_primeira_conexao_gera_um_erro(montar, tmp_path):
    p, cli, relogio, col = montar()
    cli.falhou()
    for i in range(5):
        relogio.ir(INICIO + i + 1)
        cli.falhou()
    _bate(p, col, tmp_path, 1)
    reg, = col.registros
    assert reg["mensagem"] == f"Não foi possível conectar ao broker {ENDERECO}"
    assert "Broker inacessível" in reg["detalhe"]
    relogio.ir(INICIO + 42)
    cli.conectou()
    assert col.infos("Broker voltou") == ["Broker voltou após 42 s"]
    _bate(p, col, tmp_path, 1)


def test_connack_recusado_gera_um_erro_com_motivo(montar, tmp_path):
    p, cli, relogio, col = montar()
    for _ in range(3):
        cli.ao_status("erro", "Conexão recusada: usuário ou senha inválidos")
        cli.falhou("Broker recusou a conexão: usuário ou senha inválidos")
    _bate(p, col, tmp_path, 1)
    assert "usuário ou senha inválidos" in col.registros[0]["detalhe"]


def test_desconectar_pelo_usuario_e_encerrar_nao_geram_erro(montar, tmp_path):
    p, cli, relogio, col = montar()
    cli.conectou()
    p.desconectar().result(5)
    assert cli.parado
    # eventos atrasados do cliente que saiu (paho ainda terminando) não contam
    cli.perdeu()
    cli.falhou()
    _bate(p, col, tmp_path, 0)

    p.conectar(BROKER, 1883, ASSINATURA, 1).result(5)
    cli2 = ClienteFalso.ultimo
    assert cli2 is not cli
    cli2.conectou()
    p.encerrar(5)                             # fechar a janela / reinício do vigia
    cli2.perdeu()
    cli2.falhou()
    _bate(p, col, tmp_path, 0)
    assert col.infos("Broker voltou") == []


def test_reconectar_pelo_usuario_descarta_incidente_aberto(montar, tmp_path):
    p, cli, relogio, col = montar()
    cli.conectou()
    cli.perdeu()
    _bate(p, col, tmp_path, 1)
    p.conectar(BROKER, 1883, ASSINATURA, 1).result(5)   # usuário clicou de novo
    cli2 = ClienteFalso.ultimo
    cli2.conectou()
    assert col.infos("Broker voltou") == []              # incidente da conexão anterior foi descartado
    cli2.falhou()                                         # (não acontece no paho, mas abriria um incidente novo)
    _bate(p, col, tmp_path, 2)


def test_suback_recusado_gera_um_erro_por_recusa(montar, tmp_path):
    p, cli, relogio, col = montar()
    cli.conectou()
    cli.ao_evento("assinatura_recusada", "Not authorized")
    _bate(p, col, tmp_path, 1)
    reg, = col.registros
    assert reg["categoria"] == "mqtt"
    assert reg["topico"] == ASSINATURA
    assert reg["mensagem"] == f"Assinatura recusada pelo broker para {ASSINATURA}."
    assert "Not authorized" in reg["detalhe"]
    cli.ao_evento("assinatura_recusada", "Not authorized")   # reassinatura após reconexão, recusada de novo
    _bate(p, col, tmp_path, 2)


# --------------------------------------------------------------------------------- ClienteMqtt real → eventos


def _rc(valor: int, falha: bool):
    return SimpleNamespace(value=valor, is_failure=falha, __str__=lambda self: str(valor))


def test_cliente_mqtt_traduz_callbacks_do_paho_em_eventos():
    eventos: list[tuple[str, str]] = []
    logs: list[tuple[str, str]] = []
    c = ClienteMqtt(BROKER, 1883, ASSINATURA, 1, ao_mensagem=lambda *a: None, ao_status=lambda e, d: None,
                    logar=lambda n, m: logs.append((n, m)), ao_evento=lambda t, m: eventos.append((t, m)))
    paho = SimpleNamespace(subscribe=lambda *a, **k: (0, 1))
    flags = SimpleNamespace(session_present=False)

    c._on_connect_fail(paho, None)
    c._on_connect(paho, None, flags, _rc(134, True), None)
    c._on_connect(paho, None, flags, _rc(0, False), None)
    c._on_subscribe(paho, None, 1, [_rc(128, True)], None)
    c._on_disconnect(paho, None, flags, _rc(141, True), None)
    c._on_disconnect(paho, None, flags, _rc(128, True), None)   # já desconectado: não é nova perda
    assert [t for t, _ in eventos] == ["falha_conexao", "falha_conexao", "conectado", "assinatura_recusada",
                                       "perdida"]
    assert "usuário ou senha inválidos" in eventos[1][1]
    assert eventos[4][1] == "tempo de keepalive esgotado (broker não respondeu)"

    eventos.clear()
    c._on_connect(paho, None, flags, _rc(0, False), None)
    c._parando = True                                            # parar() pedido pelo usuário
    c._on_disconnect(paho, None, flags, _rc(0, False), None)
    c._on_connect_fail(paho, None)
    assert eventos == [("conectado", "")]


# --------------------------------------------------------------------------------- integração com amqtt


def _porta_livre() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class BrokerLocal:
    """Broker amqtt numa thread com loop próprio; pode ser derrubado e subido de novo na mesma porta."""

    def __init__(self, porta: int) -> None:
        self.porta = porta
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._rodar, daemon=True)
        self.thread.start()
        self.broker = None

    def _rodar(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _executar(self, coro, timeout=10):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def subir(self) -> None:
        from amqtt.broker import Broker

        async def criar():
            b = Broker({
                "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{self.porta}"}},
                "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
            }, loop=self.loop)
            await b.start()
            return b
        self.broker = self._executar(criar())

    def derrubar(self) -> None:
        if self.broker is not None:
            self._executar(self.broker.shutdown())
            self.broker = None

    def fechar(self) -> None:
        try:
            self.derrubar()
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join(5)


def test_integracao_amqtt_derruba_e_sobe_broker(tmp_path):
    pytest.importorskip("amqtt.broker")
    porta = _porta_livre()
    broker = BrokerLocal(porta)
    col = Coletor()
    p = Pipeline(_config(tmp_path), sessao_persistente=False, pasta=tmp_path, **col.kwargs())
    hoje = date.today()

    def arquivo() -> int:
        return len(ler_erros_do_dia(tmp_path / "logs", hoje))

    try:
        # 1) broker ainda fora do ar: primeira conexão falha → 1 erro, mesmo com várias tentativas
        p.conectar("127.0.0.1", porta, "NOVUS/Teste_Alumbra/#", 1).result(5)
        assert esperar(lambda: p.contadores[2] == 1, 10), col.logs
        time.sleep(2.5)                                    # tempo para o paho tentar de novo (1 s, 2 s…)
        assert p.contadores[2] == 1 and len(col.erros) == 1 and arquivo() == 1
        assert col.registros[0]["mensagem"] == f"Não foi possível conectar ao broker 127.0.0.1:{porta}"

        # 2) broker sobe: conecta e registra a volta
        broker.subir()
        assert esperar(lambda: any("Assinado NOVUS/Teste_Alumbra/#" in m for _, m in col.logs), 70), col.logs
        assert len(col.infos("Broker voltou após")) == 1

        # 3) broker cai: 1 erro novo, mesmo com as tentativas de reconexão falhando
        broker.derrubar()
        assert esperar(lambda: p.contadores[2] == 2, 15), col.logs
        time.sleep(2.5)
        assert p.contadores[2] == 2 and len(col.erros) == 2 and arquivo() == 2
        assert col.registros[1]["mensagem"] == f"Conexão com o broker 127.0.0.1:{porta} perdida"

        # 4) broker volta: INFO de volta, sem erro novo
        broker.subir()
        assert esperar(lambda: len(col.infos("Broker voltou após")) == 2, 70), col.logs
        assert p.contadores[2] == 2
    finally:
        assert p.encerrar(5)
        broker.fechar()
    # desconectar pelo usuário (encerrar) não gerou erro
    assert p.contadores[2] == 2 and len(col.erros) == 2 and arquivo() == 2
