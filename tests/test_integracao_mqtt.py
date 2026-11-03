"""Integração: broker amqtt local → Pipeline (paho) → mapeamento IOT001 → sink (contingência e SQL falso)."""
from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from decimal import Decimal

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
    cfg.gravacao.pasta_logs = str(tmp_path / "logs")
    cfg.gravacao.intervalo_gravacao_segundos = 0  # sem amostragem: grava tudo (amostragem em test_amostragem.py)
    return cfg


def test_sem_sql_mensagens_vao_para_contingencia(broker, tmp_path):
    ev = Eventos()
    p = Pipeline(_config(tmp_path), sessao_persistente=False, **ev.kwargs())
    try:
        p.conectar("127.0.0.1", broker, "NOVUS/Teste_Alumbra/#", 2)
        assert _esperar(lambda: ev.tem_log("Assinado NOVUS/Teste_Alumbra/# (QoS 2)")), ev.logs
        assert ("conectado", f"Conectado a 127.0.0.1:{broker}") in ev.status

        _publicar(broker, [
            ("NOVUS/Teste_Alumbra/logger01", b'{"pid": 1, "ts": 1730000000, "chd1": 23.5, "chd2": 40}'),
            ("NOVUS/Teste_Alumbra/logger02/csv", "pid;device_id;ts;chd1\n2;L2;1730000001;1,5\n3;L2;1730000002;2".encode()),
            ("NOVUS/Teste_Alumbra/logger03", b"\xff\xfe\x00"),
            ("NOVUS/Teste_Alumbra/logger04", b'{"quebrado": '),
            ("NOVUS/Teste_Alumbra/logger05", b'{"T1": 23.5}'),
            ("OUTRO/topico", b"1"),
        ])
        # 5 recebidas; 3 leituras completas guardadas; 3 mensagens sem pid/ts/JSON válido = 3 erros
        assert _esperar(lambda: p.contadores == (5, 3, 3)), p.contadores
    finally:
        assert p.encerrar(5)

    assert ev.tem_log(f"Conectado a 127.0.0.1:{broker}")
    assert ev.tem_log("Mensagem de NOVUS/Teste_Alumbra/logger01: JSON, 1 leitura")
    assert ev.tem_log("Mensagem de NOVUS/Teste_Alumbra/logger02/csv: tabular, 2 leituras")
    assert any(n == "ERRO" and "logger03" in m and "binário" in m for n, m in ev.logs)
    assert any(n == "ERRO" and "logger04" in m and "JSON inválido" in m for n, m in ev.logs)
    assert any(n == "ERRO" and "logger05" in m and "sem pid" in m for n, m in ev.logs)
    assert sum("SQL Server não configurado" in m for _, m in ev.logs) == 1
    assert ev.status[-1][0] == "desconectado"
    assert ev.contadores[-1] == (5, 3, 3)

    arquivo, = (tmp_path / "contingencia").glob("leituras_*.jsonl")
    linhas = [json.loads(l) for l in arquivo.read_text(encoding="utf-8").splitlines()]
    assert len(linhas) == 6
    completas = {l["pid"]: l for l in linhas if l["motivo"] is None}
    assert completas[1]["device_id"] == "logger01" and completas[1]["chd1_value"] == "23.5000"
    assert completas[2]["device_id"] == "L2" and completas[2]["ts_unix"] == 1730000001
    assert completas[3]["chd1_value"] == "2.0000"
    rejeitadas = [l for l in linhas if l["motivo"] is not None]
    assert {l["topico"] for l in rejeitadas} == {f"NOVUS/Teste_Alumbra/logger0{i}" for i in (3, 4, 5)}
    assert any(l["payload_json"] == "base64://4A" for l in rejeitadas)


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
        _publicar(broker, [(f"NOVUS/Teste_Alumbra/d{i % 3}", f'{{"pid": {i}, "ts": {1730000000 + i}, "chd1": {i}}}'.encode()) for i in range(n)], qos=1)
        assert _esperar(lambda: len(gravados) == n, 15), len(gravados)
        duracao = time.perf_counter() - inicio
    finally:
        assert p.encerrar(5)
    assert sorted(r[0] for r in gravados) == list(range(n))                       # pid
    assert sorted(r[3] for r in gravados) == [Decimal(i).quantize(Decimal("0.0001")) for i in range(n)]
    assert {r[1] for r in gravados} == {"d0", "d1", "d2"}                         # device_id do tópico
    assert all(len(r) == 8 for r in gravados)
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


def test_payload_real_novus_chega_ao_sink_com_as_8_colunas(broker, tmp_path):
    """Payload real do NOVUS (timestamp e canais dentro de "channels") vira 1 linha completa no sink."""
    from datetime import datetime, timezone

    payload = ('{"pid":51387408,"device_id":"Teste_Alumbra","channels":{"timestamp":1793386748,'
               '"chd1_value":67,"chd2_value":119,"chd3_value":0}}')
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

    def fabrica(sql_cfg, config, logar):
        return SinkSqlServer(sql_cfg, config.gravacao, logar, conectar=lambda cfg: Conexao())

    ev = Eventos()
    p = Pipeline(_config(tmp_path, SqlServerConfig(servidor="FALSO", banco="MQTT")), fabrica_sink=fabrica,
                 sessao_persistente=False, **ev.kwargs())
    antes = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        p.conectar("127.0.0.1", broker, "NOVUS/Teste_Alumbra/#", 2)
        assert _esperar(lambda: ev.tem_log("Assinado NOVUS/Teste_Alumbra/# (QoS 2)")), ev.logs
        _publicar(broker, [("NOVUS/Teste_Alumbra/events", payload.encode())])
        assert _esperar(lambda: len(gravados) == 1), (gravados, ev.logs)
        assert _esperar(lambda: p.contadores == (1, 1, 0)), p.contadores
    finally:
        assert p.encerrar(5)
    depois = datetime.now(timezone.utc).replace(tzinfo=None)

    (linha,) = gravados
    assert len(linha) == 8
    pid, device_id, ts_unix, chd1, chd2, chd3, payload_json, recebido_em = linha
    assert (pid, device_id, ts_unix) == (51387408, "Teste_Alumbra", 1793386748)
    assert (chd1, chd2, chd3) == (Decimal("67.0000"), Decimal("119.0000"), Decimal("0.0000"))
    assert payload_json == payload
    assert isinstance(recebido_em, datetime) and recebido_em.tzinfo is None
    assert antes.replace(microsecond=0) <= recebido_em <= depois                 # UTC, hora do recebimento
    assert not any(n == "ERRO" for n, _ in ev.logs), ev.logs
    assert not (tmp_path / "contingencia").exists()
