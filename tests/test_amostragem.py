"""Amostragem de gravação (1 leitura por device_id por minuto de ts_unix, gravada no ciclo alinhado ao relógio)
e os avisos de atividade.

Sem broker e sem esperar 60 s: o cliente MQTT é falso (as mensagens são entregues direto em ``ao_mensagem``) e o
relógio do pipeline é injetado; o teste avança o relógio e espera a thread de processamento reagir.
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from functools import partial

import pytest

from app.config import Config, SqlServerConfig
from app.core.mapeamento import LinhaIot
from app.core.pipeline import Pipeline, _Amostragem, iso_local, proximo_ciclo
from app.core.sink_sqlserver import SinkSqlServer
from tests.test_sink import ConexaoFalsa

ASSINATURA = "NOVUS/Teste_Alumbra/#"


class Relogio:
    def __init__(self, t: float) -> None:
        self.t = t
        self._trava = threading.Lock()

    def __call__(self) -> float:
        with self._trava:
            return self.t

    def ir(self, t: float) -> None:
        with self._trava:
            self.t = t


class ClienteFalso:
    """Substitui o ClienteMqtt: o teste chama ``entregar`` como se fosse a thread do paho."""

    ultimo: "ClienteFalso | None" = None

    def __init__(self, broker, porta, topico, qos, ao_mensagem, ao_status, logar, **_kw) -> None:
        self.ao_mensagem = ao_mensagem
        self.ao_status = ao_status
        self.topico = topico
        self.publicados: list[tuple[str, str, int]] = []
        self.assinaturas_extras: list[str] = []
        self.falhar_publicacao: str | None = None
        ClienteFalso.ultimo = self

    def iniciar(self) -> None:
        self.ao_status("conectado", "Conectado (falso)")

    def publicar(self, topico: str, payload: str, qos: int = 1) -> tuple[bool, str]:
        if self.falhar_publicacao:
            return False, self.falhar_publicacao
        self.publicados.append((topico, payload, qos))
        return True, ""

    def assinar_extra(self, filtro: str, qos: int = 1) -> None:
        if filtro not in self.assinaturas_extras:
            self.assinaturas_extras.append(filtro)

    def parar(self) -> None:
        pass

    def entregar(self, topico: str, payload: dict | bytes, retida: bool = False) -> None:
        if isinstance(payload, dict):
            payload = json.dumps(payload).encode()
        self.ao_mensagem(topico, payload, 1, retida)


def novus(pid: int, device: str = "Teste_Alumbra", chd1: float = 1.0, ts: int | None = None) -> dict:
    """Leitura NOVUS; sem ``ts``, ``timestamp`` = 1793386748 + pid (2026-10-30, distinto por pid)."""
    return {"pid": pid, "device_id": device,
            "channels": {"timestamp": 1793386748 + pid if ts is None else int(ts), "chd1_value": chd1,
                         "chd2_value": 2, "chd3_value": 0}}


def esperar(cond, timeout: float = 5.0) -> bool:
    fim = time.monotonic() + timeout
    while time.monotonic() < fim:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


class Eventos:
    def __init__(self) -> None:
        self.logs: list[tuple[str, str]] = []
        self.proximas: list[float] = []
        self.gravacoes_ok: list[str] = []
        self.leituras: list[str] = []
        self.status: list[tuple[str, str]] = []

    def kwargs(self) -> dict:
        return dict(ao_log=lambda n, m: self.logs.append((n, m)),
                    ao_status=lambda e, d: self.status.append((e, d)),
                    ao_proxima_gravacao=self.proximas.append,
                    ao_gravacao_ok=self.gravacoes_ok.append,
                    ao_leitura=self.leituras.append)

    def com(self, trecho: str) -> list[str]:
        return [m for _, m in self.logs if trecho in m]


@pytest.fixture
def inicio() -> float:
    """Meio segundo depois de uma virada de minuto local (como 14:31:00.5)."""
    return proximo_ciclo(time.time(), 60) + 0.5


def _config(tmp_path, sql: SqlServerConfig | None = None, intervalo: int = 60) -> Config:
    cfg = Config()
    cfg.sqlserver = sql or SqlServerConfig(servidor="", banco="")
    cfg.gravacao.lote_max = 500
    cfg.gravacao.lote_segundos = 0.05
    cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
    cfg.gravacao.pasta_logs = str(tmp_path / "logs")
    cfg.gravacao.intervalo_gravacao_segundos = intervalo
    cfg.dispositivos.diagnostico_minutos = 0   # diagnóstico automático testado em test_digirail.py
    return cfg


def _fabrica_sql(conexoes: list):
    def fabrica(sql_cfg, config, logar):
        def conectar(_cfg):
            return conexoes.pop(0) if len(conexoes) > 1 else conexoes[0]
        return SinkSqlServer(sql_cfg, config.gravacao, logar, conectar=conectar, dormir=lambda s: None)
    return fabrica


def _linhas(conexao: ConexaoFalsa) -> list[tuple]:
    return [linha for _sql, linhas in conexao.gravado for linha in linhas]


@pytest.fixture
def montar(tmp_path, inicio):
    """Pipeline com cliente falso, relógio falso e SQL Server falso (ou só contingência)."""
    criados = []

    def _montar(sql: bool = True, intervalo: int = 60, conexao: ConexaoFalsa | None = None):
        relogio = Relogio(inicio)
        ev = Eventos()
        conexao = conexao or ConexaoFalsa()
        kw = dict(fabrica_sink=_fabrica_sql([conexao])) if sql else {}
        cfg = _config(tmp_path, SqlServerConfig(servidor="FALSO", banco="MQTT") if sql else None, intervalo)
        p = Pipeline(cfg, relogio=relogio, fabrica_cliente=ClienteFalso, sessao_persistente=False,
                     pasta=tmp_path, **kw, **ev.kwargs())
        p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
        criados.append(p)
        return p, ClienteFalso.ultimo, relogio, ev, conexao

    yield _montar
    for p in criados:
        p.encerrar(5)


# ------------------------------------------------------------------------------------------ funções puras


def test_proximo_ciclo_alinhado_ao_minuto_local():
    agora = time.time()
    p = proximo_ciclo(agora, 60)
    local = datetime.fromtimestamp(p)
    assert local.second == 0 and local.microsecond == 0
    assert 0 < p - agora <= 60
    assert proximo_ciclo(p, 60) == p + 60          # exatamente na virada: o próximo é o seguinte
    assert proximo_ciclo(p - 0.001, 60) == p


def test_proximo_ciclo_de_hora_alinha_na_hora_local():
    p = proximo_ciclo(time.time(), 3600)
    local = datetime.fromtimestamp(p)
    assert (local.minute, local.second) == (0, 0)


def test_iso_local():
    t = datetime(2026, 11, 3, 14, 31, 5).timestamp()
    assert iso_local(t) == "2026-11-03T14:31:05"


def _l(pid: int, ts: int, dev: str = "D") -> LinhaIot:
    return LinhaIot(pid, dev, ts, None, None, None, "{}", datetime(2026, 11, 3, 17, 31, 0), "t")


def test_amostragem_maior_ts_do_minuto_vence_e_mesmo_ts_e_duplicata():
    a = _Amostragem(60)
    m = 1793386740                                    # início de um minuto (múltiplo de 60)
    assert a.adicionar([_l(1, m + 10), _l(2, m + 50), _l(3, m + 30)], m + 100) == 0
    assert a.adicionar([_l(9, m + 50)], m + 100) == 1  # mesmo (device, ts): duplicata, não substitui
    normais, recuperados, dup = a.retirar(m + 60, m + 61)
    assert [b.linha.pid for b in normais] == [2] and recuperados == []
    assert normais[0].recebidas == 3 and dup == 1
    assert a.retirar(m + 120, m + 121) == ([], [], 0)
    # minuto já gravado: qualquer leitura dele (ou o mesmo ts) é duplicata
    assert a.adicionar([_l(4, m + 59), _l(5, m + 50)], m + 130) == 2
    assert a.pendentes == 0


# ------------------------------------------------------------------------------------------ pipeline


def test_seis_leituras_do_mesmo_device_gravam_so_a_mais_recente(montar, inicio):
    p, cli, relogio, ev, conexao = montar()
    for i in range(6):                                   # uma a cada 10 s, como o NOVUS
        relogio.ir(inicio + 5 + 10 * i)
        cli.entregar("NOVUS/Teste_Alumbra/events", novus(100 + i, chd1=i, ts=inicio + 4 + 10 * i))
    assert esperar(lambda: len(ev.com("Mensagem de NOVUS/Teste_Alumbra/events")) == 6)
    assert _linhas(conexao) == []                        # nada antes da virada (nem por lote_segundos)

    virada = inicio - 0.5 + 60
    relogio.ir(virada)
    assert esperar(lambda: ev.com("Gravação ")), ev.logs
    linhas = _linhas(conexao)
    assert [l[0] for l in linhas] == [105]               # pid da mais recente
    rotulo = datetime.fromtimestamp(virada).strftime("%H:%M")
    assert ev.com("Gravação ") == [f"Gravação {rotulo}: 1 leitura gravada no SQL Server "
                                    f"(6 recebidas, 5 descartadas pela amostragem)"]
    assert not ev.com("Gravados ")                        # o log por lote do sink fica de fora: 1 por ciclo
    assert esperar(lambda: p.contadores == (6, 1, 0)), p.contadores   # descartadas não são erro nem gravadas
    assert ev.proximas == [virada, virada + 60]


def test_dois_devices_gravam_uma_linha_cada(montar, inicio):
    p, cli, relogio, ev, conexao = montar()
    for i, dev in enumerate(["A", "B", "A", "B", "A"]):
        relogio.ir(inicio + 1 + i)
        cli.entregar(f"NOVUS/Teste_Alumbra/{dev}", novus(i, device=dev, ts=inicio + i))
    assert esperar(lambda: len(ev.com("Mensagem de")) == 5)
    relogio.ir(inicio + 60)
    assert esperar(lambda: ev.com("Gravação "))
    assert sorted((l[1], l[0]) for l in _linhas(conexao)) == [("A", 4), ("B", 3)]
    assert "2 leituras gravadas no SQL Server (5 recebidas, 3 descartadas pela amostragem)" in ev.com("Gravação ")[0]


def test_janela_sem_leitura_nao_grava_nada(montar, inicio):
    p, cli, relogio, ev, conexao = montar()
    relogio.ir(inicio + 60)
    assert esperar(lambda: len(ev.proximas) == 2)
    assert ev.com("nenhuma leitura completa recebida")
    assert conexao.gravado == [] and ev.gravacoes_ok == []
    assert p.contadores == (0, 0, 0)


def test_minuto_corrente_espera_o_proximo_ciclo(montar, inicio):
    """Leitura do minuto que acabou de começar (ts depois da virada) não vai no ciclo da virada."""
    p, cli, relogio, ev, conexao = montar()
    relogio.ir(inicio + 10)
    cli.entregar("NOVUS/Teste_Alumbra/events", novus(1, ts=inicio + 9))
    relogio.ir(inicio + 59.5 + 1)                         # já passou da virada
    cli.entregar("NOVUS/Teste_Alumbra/events", novus(2, ts=inicio + 59.5 + 0.6))   # minuto novo
    assert esperar(lambda: ev.com("Gravação "))
    assert [l[0] for l in _linhas(conexao)] == [1]
    relogio.ir(inicio + 119.5)
    assert esperar(lambda: len(ev.com("Gravação ")) == 2)
    assert [l[0] for l in _linhas(conexao)] == [1, 2]


def test_leitura_atrasada_do_minuto_anterior_entra_se_o_ciclo_ainda_nao_rodou(montar, inicio):
    """Recebida logo depois da virada, mas com ts do minuto que fechou: ainda conta para ele (é a mais recente)."""
    p, cli, relogio, ev, conexao = montar()
    relogio.ir(inicio + 10)
    cli.entregar("NOVUS/Teste_Alumbra/events", novus(1, ts=inicio + 9))
    assert esperar(lambda: ev.com("Mensagem de"))
    p._fila.put(("NOVUS/Teste_Alumbra/events", json.dumps(novus(2, ts=inicio + 58)).encode(),
                 datetime.now(timezone.utc), False, inicio + 59.5 + 0.2))
    relogio.ir(inicio + 59.5 + 0.2)
    assert esperar(lambda: ev.com("Gravação "))
    assert [l[0] for l in _linhas(conexao)] == [2]


def test_incompleta_vai_na_hora_para_a_contingencia(montar, inicio, tmp_path):
    p, cli, relogio, ev, conexao = montar()
    relogio.ir(inicio + 3)
    cli.entregar("NOVUS/Teste_Alumbra/x", {"device_id": "X", "chd1_value": 1})   # sem pid nem timestamp
    assert esperar(lambda: list((tmp_path / "contingencia").glob("*.jsonl")))
    assert esperar(lambda: p.contadores == (1, 0, 1))
    assert conexao.gravado == []


def test_encerrar_grava_a_pendente(montar, inicio):
    p, cli, relogio, ev, conexao = montar()
    for i in range(3):
        relogio.ir(inicio + 1 + i)
        cli.entregar("NOVUS/Teste_Alumbra/events", novus(i, ts=inicio + i))
    assert p.encerrar(5)
    assert [l[0] for l in _linhas(conexao)] == [2]
    (log,) = ev.com("Gravação ")
    assert "(ao desconectar): 1 leitura gravada no SQL Server (3 recebidas, 2 descartadas" in log
    assert p.contadores == (3, 1, 0)
    assert ev.proximas == [inicio - 0.5 + 60]            # não anuncia próximo ciclo ao parar
    assert ev.gravacoes_ok == [iso_local(inicio + 3)]


def test_desconectar_grava_a_pendente_na_contingencia_sem_sql(montar, inicio, tmp_path):
    p, cli, relogio, ev, _ = montar(sql=False)
    relogio.ir(inicio + 2)
    cli.entregar("NOVUS/Teste_Alumbra/events", novus(7, ts=inicio + 1))
    cli.entregar("NOVUS/Teste_Alumbra/events", novus(8, ts=inicio + 2))
    p.desconectar().result(5)
    (arq,) = (tmp_path / "contingencia").glob("*.jsonl")
    assert [json.loads(l)["pid"] for l in arq.read_text().splitlines()] == [8]
    assert "1 leitura guardada na contingência (2 recebidas, 1 descartada pela amostragem)" in ev.com("Gravação ")[0]
    assert ev.gravacoes_ok == []                          # contingência não é gravação ok


def test_intervalo_zero_mantem_comportamento_antigo(montar, inicio):
    p, cli, relogio, ev, conexao = montar(intervalo=0)
    for i in range(6):
        relogio.ir(inicio + i)
        cli.entregar("NOVUS/Teste_Alumbra/events", novus(i))
    assert esperar(lambda: len(_linhas(conexao)) == 6)    # sem esperar ciclo: lote_segundos
    assert esperar(lambda: p.contadores == (6, 6, 0))
    assert ev.com("Gravados ")                            # log por lote, como antes
    assert not ev.com("Gravação ") and not ev.com("Amostragem")
    relogio.ir(inicio + 120)
    time.sleep(0.3)
    assert ev.proximas == []                              # sem amostragem: não emite próxima gravação
    assert ev.gravacoes_ok                                # mas avisa as gravações com sucesso


def test_intervalo_injetado_sobrepoe_a_config(tmp_path, inicio):
    relogio = Relogio(inicio)
    ev = Eventos()
    p = Pipeline(_config(tmp_path, intervalo=0), relogio=relogio, intervalo_gravacao=5,
                 fabrica_cliente=ClienteFalso, pasta=tmp_path, **ev.kwargs())
    try:
        p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
        assert ev.proximas == [proximo_ciclo(inicio, 5)]
    finally:
        p.encerrar(5)


def test_ultima_leitura_no_maximo_a_cada_5_s(montar, inicio):
    p, cli, relogio, ev, _ = montar()
    for dt in (1, 2, 3, 5.9, 6, 6.5, 12):
        relogio.ir(inicio + dt)
        cli.entregar("NOVUS/Teste_Alumbra/events", novus(int(dt * 10)))
    assert ev.leituras == [iso_local(inicio + 1), iso_local(inicio + 6), iso_local(inicio + 12)]


def test_gravacao_ok_so_com_sucesso_no_sql(montar, inicio):
    # tabela inexistente (42S02): o lote vai para a contingência; não é sucesso
    p, cli, relogio, ev, conexao = montar(conexao=ConexaoFalsa(falhas=10, sqlstate="42S02"))
    relogio.ir(inicio + 1)
    cli.entregar("NOVUS/Teste_Alumbra/events", novus(1))
    relogio.ir(inicio + 59.5)
    assert esperar(lambda: ev.com("Gravação "))
    assert "1 leitura guardada na contingência" in ev.com("Gravação ")[0]
    assert ev.gravacoes_ok == [] and conexao.gravado == []


def test_gravacao_ok_emitida_no_commit(montar, inicio):
    p, cli, relogio, ev, conexao = montar()
    relogio.ir(inicio + 1)
    cli.entregar("NOVUS/Teste_Alumbra/events", novus(1))
    virada = inicio + 59.5
    relogio.ir(virada)
    assert esperar(lambda: ev.gravacoes_ok)
    assert ev.gravacoes_ok == [iso_local(virada)]


# ------------------------------------------------------------------------------------------ bridge (Qt)


def test_bridge_emite_os_tres_sinais(tmp_path, inicio, monkeypatch):
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    QtWidgets = pytest.importorskip("PySide6.QtWidgets")
    from app import bridge as modulo_bridge

    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
    relogio = Relogio(inicio)
    conexao = ConexaoFalsa()
    monkeypatch.setattr(modulo_bridge, "Pipeline",
                        partial(Pipeline, relogio=relogio, fabrica_cliente=ClienteFalso,
                                fabrica_sink=_fabrica_sql([conexao]), pasta=tmp_path))
    cfg = _config(tmp_path, SqlServerConfig(servidor="FALSO", banco="MQTT"))
    b = modulo_bridge.PipelineBridge(cfg)
    proximas, oks, leituras = [], [], []
    b.proxima_gravacao.connect(proximas.append)
    b.ultima_gravacao_ok.connect(oks.append)
    b.ultima_leitura.connect(leituras.append)

    def esperar_qt(cond, timeout=5.0):
        fim = time.monotonic() + timeout
        while time.monotonic() < fim:
            app.processEvents()
            if cond():
                return True
            time.sleep(0.01)
        return False

    try:
        b.conectar("127.0.0.1", 1883, ASSINATURA, 1)
        assert esperar_qt(lambda: proximas)
        assert proximas == [inicio - 0.5 + 60]
        relogio.ir(inicio + 2)
        ClienteFalso.ultimo.entregar("NOVUS/Teste_Alumbra/events", novus(1))
        assert esperar_qt(lambda: leituras)
        assert leituras == [iso_local(inicio + 2)]
        relogio.ir(inicio + 59.5)
        assert esperar_qt(lambda: oks and len(proximas) == 2)
        assert oks == [iso_local(inicio + 59.5)] and proximas[1] == inicio - 0.5 + 120
        assert [l[0] for l in _linhas(conexao)] == [1]
    finally:
        b.encerrar(5)
    # o recebido_em gravado vem do relógio do pipeline, em UTC sem fuso (como antes)
    assert _linhas(conexao)[0][7] == datetime.fromtimestamp(inicio + 2, timezone.utc).replace(tzinfo=None)
