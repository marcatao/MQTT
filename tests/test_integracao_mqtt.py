"""Integração: broker amqtt local → Pipeline (paho) → parser → sink (contingência e SQL falso)."""
from __future__ import annotations

import asyncio
import json
import socket
import threading
import time

import pytest

amqtt_broker = pytest.importorskip("amqtt.broker")
import paho.mqtt.client as mqtt  # noqa: E402
from paho.mqtt.enums import CallbackAPIVersion  # noqa: E402

from app.config import Config, SqlServerConfig  # noqa: E402
from app.core.pipeline import Pipeline  # noqa: E402
from app.core.sink_sqlserver import SinkSqlServer  # noqa: E402


def _porta_livre() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def broker():
    porta = _porta_livre()
    loop = asyncio.new_event_loop()
    pronto = threading.Event()
    estado = {}

    def rodar():
        asyncio.set_event_loop(loop)
        b = amqtt_broker.Broker({
            "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{porta}"}},
            "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
        }, loop=loop)
        estado["broker"] = b
        loop.run_until_complete(b.start())
        pronto.set()
        loop.run_forever()

    t = threading.Thread(target=rodar, daemon=True)
    t.start()
    assert pronto.wait(10), "broker amqtt não subiu"
    yield porta
    fut = asyncio.run_coroutine_threadsafe(estado["broker"].shutdown(), loop)
    try:
        fut.result(10)
    finally:
        loop.call_soon_threadsafe(loop.stop)
        t.join(5)


def _esperar(cond, timeout=10.0):
    fim = time.monotonic() + timeout
    while time.monotonic() < fim:
        if cond():
            return True
        time.sleep(0.05)
    return False


def _publicar(porta, mensagens, qos=2):
    c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="teste-publicador")
    c.connect("127.0.0.1", porta)
    c.loop_start()
    try:
        for topico, payload in mensagens:
            c.publish(topico, payload, qos=qos).wait_for_publish(5)
    finally:
        c.disconnect()
        c.loop_stop()


class Eventos:
    def __init__(self):
        self.status, self.logs, self.contadores = [], [], []

    def kwargs(self):
        return dict(ao_status=lambda e, d: self.status.append((e, d)),
                    ao_log=lambda n, m: self.logs.append((n, m)),
                    ao_contadores=lambda *c: self.contadores.append(c))

    def tem_log(self, trecho):
        return any(trecho in m for _, m in self.logs)


def _config(tmp_path, sql=None):
    cfg = Config()
    cfg.sqlserver = sql or SqlServerConfig(servidor="", banco="")
    cfg.gravacao.lote_max = 100
    cfg.gravacao.lote_segundos = 0.2
    cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
    return cfg


def test_sem_sql_mensagens_vao_para_contingencia(broker, tmp_path):
    ev = Eventos()
    p = Pipeline(_config(tmp_path), sessao_persistente=False, **ev.kwargs())
    try:
        p.conectar("127.0.0.1", broker, "NOVUS/Teste_Alumbra/#", 2)
        assert _esperar(lambda: ev.tem_log("Assinado NOVUS/Teste_Alumbra/# (QoS 2)")), ev.logs
        assert ("conectado", f"Conectado a 127.0.0.1:{broker}") in ev.status

        _publicar(broker, [
            ("NOVUS/Teste_Alumbra/logger01", b'{"ts": 1730000000, "T1": 23.5, "T2": 40, "status": "ok"}'),
            ("NOVUS/Teste_Alumbra/logger02/csv", "T1;T2\n1,5;2".encode()),
            ("NOVUS/Teste_Alumbra/logger03", b"\xff\xfe\x00"),
            ("NOVUS/Teste_Alumbra/logger04", b'{"quebrado": '),
            ("OUTRO/topico", b"1"),
        ])
        assert _esperar(lambda: p.contadores == (4, 6, 1)), p.contadores
    finally:
        assert p.encerrar(5)

    assert ev.tem_log(f"Conectado a 127.0.0.1:{broker}")
    assert ev.tem_log("Mensagem de NOVUS/Teste_Alumbra/logger01: JSON, 3 campos")
    assert ev.tem_log("Mensagem de NOVUS/Teste_Alumbra/logger02/csv: tabular, 2 campos")
    assert any(n == "AVISO" and "binário" in m for n, m in ev.logs)
    assert any(n == "ERRO" and "logger04 ignorada" in m for n, m in ev.logs)
    assert sum("SQL Server não configurado" in m for _, m in ev.logs) == 1
    assert ev.status[-1][0] == "desconectado"
    assert ev.contadores[-1] == (4, 6, 1)

    arquivo, = (tmp_path / "contingencia").glob("leituras_*.jsonl")
    linhas = [json.loads(l) for l in arquivo.read_text(encoding="utf-8").splitlines()]
    por_campo = {(l["Dispositivo"], l["Campo"]): l for l in linhas}
    assert por_campo[("logger01", "T1")]["ValorNumerico"] == 23.5
    assert por_campo[("logger01", "status")]["ValorTexto"] == "ok"
    assert por_campo[("logger02", "T1")]["ValorNumerico"] == 1.5
    assert por_campo[("logger03", "payload_base64")]["ValorTexto"] == "//4A"


def test_com_sql_falso_grava_em_lote(broker, tmp_path):
    gravados = []

    class Cursor:
        fast_executemany = False

        def executemany(self, sql, linhas):
            gravados.extend(linhas)

        def close(self):
            pass

    class Conexao:
        def cursor(self):
            return Cursor()

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    sql = SqlServerConfig(servidor="FALSO", banco="MQTT")

    def fabrica(sql_cfg, config, logar):
        return SinkSqlServer(sql_cfg, config.gravacao, logar, conectar=lambda cfg: Conexao())

    ev = Eventos()
    p = Pipeline(_config(tmp_path, sql), fabrica_sink=fabrica, sessao_persistente=False, **ev.kwargs())
    try:
        p.conectar("127.0.0.1", broker, "NOVUS/Teste_Alumbra/+", 1)
        assert _esperar(lambda: ev.tem_log("Assinado NOVUS/Teste_Alumbra/+ (QoS 1)")), ev.logs
        n = 300
        inicio = time.perf_counter()
        _publicar(broker, [(f"NOVUS/Teste_Alumbra/d{i % 3}", f'{{"v": {i}}}'.encode()) for i in range(n)], qos=1)
        assert _esperar(lambda: len(gravados) == n, 15), len(gravados)
        duracao = time.perf_counter() - inicio
    finally:
        assert p.encerrar(5)
    assert sorted(r[6 - 1] for r in gravados) == [float(i) for i in range(n)]
    assert {r[2] for r in gravados} == {"d0", "d1", "d2"}
    assert ev.tem_log("registros no SQL Server")
    assert not (tmp_path / "contingencia").exists()
    print(f"\n{n} mensagens publicadas e gravadas (SQL falso) em {duracao:.2f}s")


def test_parametros_invalidos_geram_erro(tmp_path):
    ev = Eventos()
    p = Pipeline(_config(tmp_path), **ev.kwargs())
    p.conectar("127.0.0.1", 1883, "a/#/b", 2).result(5)
    p.conectar("", 1883, "a/#", 2).result(5)
    p.conectar("x", 70000, "a/#", 2).result(5)
    p.conectar("x", 1883, "a/#", 3).result(5)
    assert [e for e, _ in ev.status] == ["erro"] * 4
    assert p.encerrar(2)


def test_broker_inacessivel_avisa_e_desconecta_limpo(tmp_path):
    ev = Eventos()
    p = Pipeline(_config(tmp_path), sessao_persistente=False, **ev.kwargs())
    p.conectar("127.0.0.1", _porta_livre(), "NOVUS/#", 2)
    assert _esperar(lambda: ev.tem_log("Não foi possível conectar"), 10), ev.logs
    p.desconectar().result(5)
    assert ev.status[0][0] == "conectando" and ev.status[-1][0] == "desconectado"
    assert p.encerrar(2)
