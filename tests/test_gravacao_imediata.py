"""Gravação imediata (``intervalo_gravacao_segundos = 0``, padrão desde 04/11/2026).

Cada leitura completa vai ao SQL Server assim que chega; o que já está na fila quando a thread de processamento
acorda vira um único lote (teto ``lote_max``). ``lote_segundos`` não é espera nesse modo: os testes usam 3600 s para
provar isso. Cliente MQTT, relógio e SQL Server são falsos (ver test_amostragem.py).
"""
from __future__ import annotations

import threading
import time
from datetime import datetime

import pytest

from app.config import Config, GravacaoConfig, SqlServerConfig
from app.core.mapeamento import LinhaIot
from app.core.pipeline import Pipeline, iso_local, textos_gravacao
from tests.test_amostragem import ASSINATURA, ClienteFalso, Eventos, Relogio, _fabrica_sql, _linhas, esperar, novus
from tests.test_sink import ConexaoFalsa

TOPICO = "NOVUS/Teste_Alumbra/events"
T0 = 1793386740   # 2026-10-30, início de um minuto


class Portao:
    """``ao_dispositivo`` que segura a thread de processamento na 1ª mensagem até ``abrir()``."""

    def __init__(self) -> None:
        self._liberado = threading.Event()
        self.segurando = threading.Event()

    def __call__(self, *_args) -> None:
        if not self._liberado.is_set():
            self.segurando.set()
            self._liberado.wait(5)

    def abrir(self) -> None:
        self._liberado.set()


@pytest.fixture
def montar(tmp_path):
    criados = []

    def _montar(sql: bool = True, conexao: ConexaoFalsa | None = None, lote_max: int = 500, **kw_pipeline):
        cfg = Config()   # padrão: intervalo_gravacao_segundos = 0
        cfg.sqlserver = SqlServerConfig(servidor="FALSO", banco="MQTT") if sql else SqlServerConfig(servidor="", banco="")
        cfg.gravacao.lote_max = lote_max
        cfg.gravacao.lote_segundos = 3600          # se fosse espera, nenhum teste passaria
        cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
        cfg.gravacao.pasta_logs = str(tmp_path / "logs")
        cfg.dispositivos.diagnostico_minutos = 0
        conexao = conexao or ConexaoFalsa()
        ev = Eventos()
        relogio = Relogio(T0 + 30)
        kw = dict(fabrica_sink=_fabrica_sql([conexao])) if sql else {}
        p = Pipeline(cfg, relogio=relogio, fabrica_cliente=ClienteFalso, sessao_persistente=False,
                     pasta=tmp_path, **kw, **ev.kwargs(), **kw_pipeline)
        p.LOGS_POR_SEGUNDO = 10_000
        p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
        criados.append(p)
        return p, ClienteFalso.ultimo, relogio, ev, conexao

    yield _montar
    for p in criados:
        p.encerrar(5)


def _hora(ts: int, fmt: str = "%H:%M:%S") -> str:
    return datetime.fromtimestamp(ts).strftime(fmt)


def test_padrao_e_gravacao_imediata():
    assert Config().gravacao.intervalo_gravacao_segundos == 0
    assert GravacaoConfig().intervalo_gravacao_segundos == 0


def test_uma_leitura_e_gravada_sem_esperar_lote_segundos(montar):
    p, cli, relogio, ev, conexao = montar()
    t = time.perf_counter()
    cli.entregar(TOPICO, novus(1, ts=T0 + 5))
    assert esperar(lambda: len(_linhas(conexao)) == 1, timeout=2.0)   # lote_segundos = 3600
    latencia = time.perf_counter() - t
    assert latencia < 1.0
    assert esperar(lambda: p.contadores == (1, 1, 0))
    assert len(conexao.gravado) == 1
    assert esperar(lambda: ev.com("Gravada leitura de Teste_Alumbra"))
    assert ev.com("Gravada leitura de Teste_Alumbra")[0] == \
        f"Gravada leitura de Teste_Alumbra ({_hora(T0 + 5)}) no SQL Server"
    assert not ev.com("Gravados ")                  # o log por lote do sink fica de fora
    assert ev.gravacoes_ok == [iso_local(T0 + 30)]  # ultima_gravacao_ok a cada commit
    assert ev.proximas == []                        # sem ciclo: proxima_gravacao não é emitido
    print(f"\nlatência mensagem → commit (SQL falso): {latencia * 1000:.1f} ms")


def test_cada_leitura_isolada_e_um_commit_e_um_log(montar):
    p, cli, relogio, ev, conexao = montar()
    for i in range(3):
        cli.entregar(TOPICO, novus(i, ts=T0 + 10 * i))
        assert esperar(lambda: len(_linhas(conexao)) == i + 1)
    assert len(conexao.gravado) == 3
    assert esperar(lambda: len(ev.com("Gravada leitura de")) == 3)
    assert esperar(lambda: len(ev.gravacoes_ok) == 3)


def test_rajada_de_50_vira_um_lote_e_um_resumo(montar):
    portao = Portao()
    p, cli, relogio, ev, conexao = montar(ao_dispositivo=portao)
    cli.entregar(TOPICO, novus(0, ts=T0))
    assert portao.segurando.wait(2)                 # a thread está presa na 1ª; as outras 49 ficam na fila
    for i in range(1, 50):                          # reenvio da memória circular: timestamps de 10 em 10 s
        cli.entregar(TOPICO, novus(i, ts=T0 + 10 * i))
    portao.abrir()
    assert esperar(lambda: len(_linhas(conexao)) == 50)
    assert len(conexao.gravado) == 1                # um único executemany/commit
    assert [r[0] for r in _linhas(conexao)] == list(range(50))
    assert esperar(lambda: p.contadores == (50, 50, 0))
    esperado = (f"Gravadas 50 leituras de Teste_Alumbra ({_hora(T0, '%H:%M')}–{_hora(T0 + 490, '%H:%M')}) "
                "no SQL Server")
    assert esperar(lambda: ev.com("Gravadas 50 leituras"))
    assert ev.com("Gravadas 50 leituras") == [esperado]
    assert not ev.com("Gravada leitura de")        # nada de 50 logs individuais
    assert len(ev.gravacoes_ok) == 1


def test_rajada_respeita_lote_max(montar):
    portao = Portao()
    p, cli, relogio, ev, conexao = montar(lote_max=20, ao_dispositivo=portao)
    cli.entregar(TOPICO, novus(0, ts=T0))
    assert portao.segurando.wait(2)
    for i in range(1, 50):
        cli.entregar(TOPICO, novus(i, ts=T0 + 10 * i))
    portao.abrir()
    assert esperar(lambda: len(_linhas(conexao)) == 50)
    assert [len(linhas) for _sql, linhas in conexao.gravado] == [20, 20, 10]


def test_duplicata_ignorada_no_modo_imediato(montar):
    p, cli, relogio, ev, conexao = montar()
    cli.entregar(TOPICO, novus(1, ts=T0 + 5))
    assert esperar(lambda: len(_linhas(conexao)) == 1)
    cli.entregar(TOPICO, novus(1, ts=T0 + 5), retida=True)   # mesma (device_id, ts_unix): retida/reenvio QoS 1
    cli.entregar(TOPICO, novus(2, ts=T0 + 5))                # outro pid, mesmo ts: também duplicata
    assert esperar(lambda: len(ev.com("duplicada ignorada")) == 2)
    time.sleep(0.2)
    assert len(_linhas(conexao)) == 1
    assert p.contadores == (3, 1, 0)                          # duplicata não é erro nem gravação
    assert len(ev.com("Gravada leitura de")) == 1


def test_sem_sql_log_diz_contingencia(montar, tmp_path):
    p, cli, relogio, ev, _ = montar(sql=False)
    cli.entregar(TOPICO, novus(1, ts=T0 + 5))
    assert esperar(lambda: ev.com("Guardada leitura de"))
    assert ev.com("Guardada leitura de")[0] == \
        f"Guardada leitura de Teste_Alumbra ({_hora(T0 + 5)}) na contingência"
    assert list((tmp_path / "contingencia").glob("*.jsonl"))
    assert ev.gravacoes_ok == []                               # contingência não é gravação ok


def test_falha_do_banco_log_diz_contingencia(montar):
    p, cli, relogio, ev, conexao = montar(conexao=ConexaoFalsa(falhas=10, sqlstate="42S02"))
    cli.entregar(TOPICO, novus(1, ts=T0 + 5))
    assert esperar(lambda: ev.com("Guardada leitura de"))
    assert ev.com("Guardada leitura de")[0].endswith("na contingência")
    assert ev.gravacoes_ok == [] and conexao.gravado == []
    assert esperar(lambda: p.contadores[2] == 1)               # 1 erro "sql"


def test_sem_busy_loop_ocioso(montar):
    montar()
    time.sleep(0.1)
    cpu = time.process_time()
    time.sleep(0.6)
    assert time.process_time() - cpu < 0.3                     # ociosa: a thread dorme em fila.get(timeout)


def _linha(dev: str, ts: int) -> LinhaIot:
    return LinhaIot(1, dev, ts, None, None, None, "{}", datetime(2026, 11, 4, 12, 0, 0), TOPICO)


def test_textos_gravacao():
    um = [_linha("A", T0 + 5)]
    assert textos_gravacao(um, 1, 1) == [f"Gravada leitura de A ({_hora(T0 + 5)}) no SQL Server"]
    assert textos_gravacao(um, 0, 0) == [f"Não gravada leitura de A ({_hora(T0 + 5)}) (ver erro acima)"]
    varios = [_linha("A", T0 + 1), _linha("B", T0 + 2), _linha("A", T0 + 3)]
    assert textos_gravacao(varios, 3, 2) == [
        f"Gravadas 2 leituras de A ({_hora(T0 + 1)}–{_hora(T0 + 3)}) (lote: 2 no SQL Server e 1 na contingência)",
        f"Gravada leitura de B ({_hora(T0 + 2)}) (lote: 2 no SQL Server e 1 na contingência)",
    ]
