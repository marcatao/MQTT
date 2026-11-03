"""Sinais ``dispositivo_visto`` e ``erro_registrado`` (callbacks ``ao_dispositivo``/``ao_erro`` do Pipeline) e o
arquivo ``<pasta_logs>/erros_AAAAMMDD.jsonl`` lido por ``ler_erros_do_dia``.

Regra de ouro conferida em todos os testes de erro: contador de erros == número de registros (callback e arquivo).
Cliente MQTT falso e relógio injetado: nenhum teste espera intervalos de verdade.
"""
from __future__ import annotations

import json
import os
import queue
import threading
from datetime import date, datetime

import pytest

from app.config import Config, SqlServerConfig
from app.core import erros
from app.core.erros import CHAVES, ler_erros_do_dia
from app.core.pipeline import Pipeline
from app.core.sink_sqlserver import SinkSqlServer
from tests.test_amostragem import ASSINATURA, ClienteFalso, Relogio, esperar, novus
from tests.test_sink import ConexaoFalsa, ErroOdbc

INICIO = datetime(2026, 11, 3, 14, 30, 10).timestamp()
DIA = date(2026, 11, 3)
TOPICO = "NOVUS/Teste_Alumbra/events"


class Coletor:
    def __init__(self) -> None:
        self.trava = threading.Lock()
        self.dispositivos: list[tuple[str, str, str]] = []
        self.erros: list[str] = []
        self.logs: list[tuple[str, str]] = []

    def kwargs(self) -> dict:
        return dict(ao_dispositivo=lambda d, t, q: self.dispositivos.append((d, t, q)),
                    ao_erro=self.erros.append,
                    ao_log=lambda n, m: self.logs.append((n, m)))

    @property
    def registros(self) -> list[dict]:
        return [json.loads(e) for e in self.erros]


def _config(tmp_path, sql: SqlServerConfig | None = None) -> Config:
    cfg = Config()
    cfg.sqlserver = sql or SqlServerConfig(servidor="", banco="")
    cfg.gravacao.lote_max = 500
    cfg.gravacao.lote_segundos = 0.05
    cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
    cfg.gravacao.pasta_logs = str(tmp_path / "logs")
    cfg.gravacao.intervalo_gravacao_segundos = 0  # grava tudo: os erros do banco aparecem a cada lote
    return cfg


@pytest.fixture
def montar(tmp_path):
    criados: list[Pipeline] = []

    def _montar(sql: SqlServerConfig | None = None, conectar_sql=None, cfg: Config | None = None):
        relogio = Relogio(INICIO)
        col = Coletor()
        kw = {}
        if conectar_sql is not None:
            def fabrica(sql_cfg, config, logar):
                return SinkSqlServer(sql_cfg, config.gravacao, logar, conectar=conectar_sql,
                                     dormir=lambda s: None)
            kw["fabrica_sink"] = fabrica
        p = Pipeline(cfg or _config(tmp_path, sql), relogio=relogio, fabrica_cliente=ClienteFalso,
                     sessao_persistente=False, pasta=tmp_path, **kw, **col.kwargs())
        p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
        criados.append(p)
        return p, ClienteFalso.ultimo, relogio, col

    yield _montar
    for p in criados:
        p.encerrar(5)


def _bate(p: Pipeline, col: Coletor, pasta_logs) -> None:
    """Regra de ouro: contador de erros == registros pelo callback == linhas no arquivo do dia."""
    n = p.contadores[2]
    assert len(col.erros) == n
    assert len(ler_erros_do_dia(pasta_logs, DIA)) == n


# --------------------------------------------------------------------------------- dispositivo_visto


def test_dispositivo_visto_limita_por_device(montar):
    p, cli, relogio, col = montar()
    cli.entregar(TOPICO, novus(1, "A"))                       # t=0  → A sai (primeira)
    relogio.ir(INICIO + 1)
    cli.entregar(TOPICO, novus(2, "A"))                       # t=1  → suprimida
    cli.entregar("NOVUS/Teste_Alumbra/b", novus(3, "B"))      # t=1  → B sai (primeira de B)
    relogio.ir(INICIO + 4.9)
    cli.entregar(TOPICO, novus(4, "A"))                       # t=4.9 → suprimida
    cli.entregar("NOVUS/Teste_Alumbra/b", novus(5, "B"))      # t=4.9 → suprimida
    relogio.ir(INICIO + 5)
    cli.entregar(TOPICO, novus(6, "A"))                       # t=5  → A sai de novo
    relogio.ir(INICIO + 6)
    cli.entregar("NOVUS/Teste_Alumbra/b", novus(7, "B"))      # t=6  → B sai de novo (5 s depois da sua 1ª)
    assert esperar(lambda: len(col.dispositivos) >= 4)
    p.desconectar().result(5)
    iso = lambda t: datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")  # noqa: E731
    assert col.dispositivos == [
        ("A", TOPICO, iso(INICIO)),
        ("B", "NOVUS/Teste_Alumbra/b", iso(INICIO + 1)),
        ("A", TOPICO, iso(INICIO + 5)),
        ("B", "NOVUS/Teste_Alumbra/b", iso(INICIO + 6)),
    ]


def test_dispositivo_visto_inclui_incompleta_e_ignora_sem_device(montar):
    p, cli, relogio, col = montar()
    cli.entregar(TOPICO, {"pid": 1, "device_id": "C"})        # sem ts_unix: incompleta, mas tem device
    cli.entregar(TOPICO, b"{quebrado")                         # sem device conhecido
    cli.entregar(TOPICO, b"\x00\x01\xff")                      # binário
    p.desconectar().result(5)
    assert [d for d, _, _ in col.dispositivos] == ["C"]


def test_dispositivo_visto_device_do_topico(montar):
    p, cli, relogio, col = montar()
    cli.entregar("NOVUS/Teste_Alumbra/N7", {"pid": 1, "ts": 1793386748})  # device deduzido do tópico
    p.desconectar().result(5)
    assert col.dispositivos[0][:2] == ("N7", "NOVUS/Teste_Alumbra/N7")


# --------------------------------------------------------------------------------- erros: payload


@pytest.mark.parametrize("payload, detalhe", [
    (json.dumps({"pid": 1, "device_id": "A", "channels": {"chd1_value": 1}}).encode(), "sem ts_unix"),
    (b"{quebrado", "JSON inválido"),
    (b"\x00\x01\xff\xfe", "payload binário"),
    (b"   ", "payload vazio"),
])
def test_erro_payload(montar, tmp_path, payload, detalhe):
    p, cli, relogio, col = montar()
    cli.entregar(TOPICO, payload)
    p.desconectar().result(5)
    assert p.contadores[2] == 1
    _bate(p, col, tmp_path / "logs")
    reg = col.registros[0]
    assert reg["categoria"] == "payload"
    assert reg["topico"] == TOPICO
    assert detalhe in reg["detalhe"]
    assert reg["quando"] == datetime.fromtimestamp(INICIO).strftime("%Y-%m-%dT%H:%M:%S")


def test_erro_payload_lista_com_duas_incompletas_gera_dois_registros(montar, tmp_path):
    p, cli, relogio, col = montar()
    cli.entregar(TOPICO, json.dumps([novus(1), {"pid": 2, "device_id": "A"}, {"device_id": "A", "ts": 1}]).encode())
    p.desconectar().result(5)
    assert p.contadores[2] == 2
    _bate(p, col, tmp_path / "logs")
    assert {r["detalhe"].split(";")[0] for r in col.registros} == {"sem ts_unix", "sem pid"}


def test_json_tem_as_chaves_certas_e_payload_truncado(montar, tmp_path):
    p, cli, relogio, col = montar()
    grande = json.dumps({"pid": 1, "device_id": "A", "lixo": "x" * 5000})  # sem ts_unix
    cli.entregar(TOPICO, grande.encode())
    p.desconectar().result(5)
    reg = col.registros[0]
    assert list(reg) == list(CHAVES)
    assert len(reg["payload"]) == 2000 and reg["payload"] == grande[:2000]
    assert isinstance(reg["mensagem"], str) and reg["mensagem"]


# --------------------------------------------------------------------------------- erros: sql


def test_erro_sql_lote_para_contingencia_sem_senha(montar, tmp_path):
    sql = SqlServerConfig(servidor="FALSO", banco="MQTT", senha="segredo-teste")

    def conectar(_cfg):
        raise ErroOdbc("08001", "[08001] Falha de conexão (DRIVER={x};SERVER=FALSO;UID=mqtt;PWD=segredo-teste;)")

    p, cli, relogio, col = montar(sql, conectar_sql=conectar)
    cli.entregar(TOPICO, novus(1))
    cli.entregar(TOPICO, novus(2))
    assert esperar(lambda: p.contadores[2] >= 1)
    p.desconectar().result(5)
    n = p.contadores[2]
    assert n >= 1
    _bate(p, col, tmp_path / "logs")
    for reg in col.registros:
        assert reg["categoria"] == "sql"
        assert "contingência" in reg["mensagem"] and "registro" in reg["mensagem"]
        assert reg["detalhe"]
    texto = "".join(col.erros) + (tmp_path / "logs" / "erros_20261103.jsonl").read_text(encoding="utf-8")
    assert "segredo-teste" not in texto


def test_erro_sql_bisseccao_isola_linha_ruim(montar, tmp_path):
    conexao = ConexaoFalsa(campos_ruins={"RUIM"})
    p, cli, relogio, col = montar(SqlServerConfig(servidor="FALSO", banco="MQTT"),
                                  conectar_sql=lambda _cfg: conexao)
    payload_ruim = json.dumps(novus(2, "RUIM"))
    cli.entregar(TOPICO, novus(1, "A"))
    cli.entregar("NOVUS/Teste_Alumbra/r", payload_ruim.encode())
    cli.entregar(TOPICO, novus(3, "B"))
    p.desconectar().result(5)
    assert p.contadores[2] == 1
    _bate(p, col, tmp_path / "logs")
    reg = col.registros[0]
    assert reg["categoria"] == "sql"
    assert reg["topico"] == "NOVUS/Teste_Alumbra/r"
    assert reg["payload"] == payload_ruim
    assert "RUIM" in reg["mensagem"] and reg["detalhe"]


# --------------------------------------------------------------------------------- erros: contingência


def test_erro_contingencia_sem_disco(montar, tmp_path):
    bloqueio = tmp_path / "nao_e_pasta"
    bloqueio.write_text("arquivo no lugar da pasta")
    cfg = _config(tmp_path)
    cfg.gravacao.pasta_contingencia = str(bloqueio / "contingencia")
    p, cli, relogio, col = montar(cfg=cfg)
    cli.entregar(TOPICO, novus(1))
    assert esperar(lambda: p.contadores[2] >= 1)
    p.desconectar().result(5)
    assert p.contadores[2] >= 1
    _bate(p, col, tmp_path / "logs")
    assert {r["categoria"] for r in col.registros} == {"contingencia"}
    assert all(r["detalhe"] for r in col.registros)


# --------------------------------------------------------------------------------- erros: fila


class _FilaCheia:
    def put(self, item, timeout=None):
        raise queue.Full


def test_erro_fila_cheia(tmp_path):
    col = Coletor()
    p = Pipeline(_config(tmp_path), relogio=Relogio(INICIO), pasta=tmp_path, **col.kwargs())
    p._fila = _FilaCheia()  # sem esperar os 5 s de contrapressão
    p._ao_mensagem(TOPICO, json.dumps(novus(1)).encode(), 1, False)
    p._ao_mensagem(TOPICO, b"x" * 3000, 1, False)
    p._fila = None
    p.encerrar(2)
    assert p.contadores == (2, 0, 2)
    _bate(p, col, tmp_path / "logs")
    assert [r["categoria"] for r in col.registros] == ["fila", "fila"]
    assert col.registros[0]["topico"] == TOPICO
    assert len(col.registros[1]["payload"]) == 2000


# mqtt: queda/recusa do broker contam um erro por incidente; testes em tests/test_queda_broker.py


# --------------------------------------------------------------------------------- arquivo do dia


def test_arquivo_do_dia_lido_com_linha_corrompida(montar, tmp_path):
    p, cli, relogio, col = montar()
    cli.entregar(TOPICO, b"{quebrado")
    cli.entregar(TOPICO, {"pid": 1, "device_id": "A"})
    p.desconectar().result(5)
    arquivo = tmp_path / "logs" / "erros_20261103.jsonl"
    assert arquivo.exists()
    with arquivo.open("a", encoding="utf-8") as f:
        f.write('{"quando": "2026-11-03T14:3\n')   # linha cortada (queda de energia)
        f.write("[1, 2]\n\n")                       # JSON que não é objeto + linha vazia
        f.write('{"categoria": "sql"}\n')           # objeto parcial: chaves faltantes viram None
    lidos = ler_erros_do_dia(tmp_path / "logs", DIA)
    assert lidos[:2] == col.registros
    assert lidos[2] == {"quando": None, "categoria": "sql", "mensagem": None, "topico": None,
                        "detalhe": None, "payload": None}
    assert len(lidos) == 3
    assert ler_erros_do_dia(tmp_path / "logs", datetime(2026, 11, 3, 23, 59)) == lidos
    assert ler_erros_do_dia(tmp_path / "logs", date(2026, 11, 4)) == []
    assert ler_erros_do_dia(tmp_path / "nao_existe") == []


def test_pasta_logs_relativa_a_pasta_base(tmp_path, monkeypatch):
    assert erros.resolver_pasta("logs", tmp_path) == tmp_path / "logs"
    assert erros.resolver_pasta(tmp_path / "x") == tmp_path / "x"
    monkeypatch.setattr(erros, "pasta_base", lambda: tmp_path)
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "erros_20261103.jsonl").write_text(
        json.dumps({"quando": "2026-11-03T10:00:00", "categoria": "fila", "mensagem": "m", "topico": None,
                    "detalhe": None, "payload": None}, ensure_ascii=False) + "\n", encoding="utf-8")
    assert ler_erros_do_dia("logs", DIA)[0]["categoria"] == "fila"


def test_falha_ao_gravar_arquivo_avisa_uma_vez_e_segue(montar, tmp_path):
    bloqueio = tmp_path / "logs_e_arquivo"
    bloqueio.write_text("x")
    cfg = _config(tmp_path)
    cfg.gravacao.pasta_logs = str(bloqueio)
    p, cli, relogio, col = montar(cfg=cfg)
    for i in range(3):
        cli.entregar(TOPICO, {"pid": i, "device_id": "A"})   # 3 erros de payload
    cli.entregar(TOPICO, novus(10))                           # e uma leitura boa, que segue normalmente
    p.desconectar().result(5)
    assert p.contadores[2] == 3 and len(col.erros) == 3       # o callback continua recebendo todos
    avisos = [m for n, m in col.logs if n == "AVISO" and "registro de erros" in m]
    assert len(avisos) == 1
    assert p.contadores[1] == 1                               # a leitura boa foi para a contingência


# --------------------------------------------------------------------------------- bridge real


def test_bridge_real_emite_dispositivo_e_erro(tmp_path):
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    QtWidgets = pytest.importorskip("PySide6.QtWidgets")
    from app.bridge import PipelineBridge

    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])
    b = PipelineBridge(_config(tmp_path))
    b._pipeline._fabrica_cliente = ClienteFalso
    b._pipeline._relogio = Relogio(INICIO)
    b._pipeline._pasta = tmp_path
    vistos, erros_json, contadores = [], [], []
    b.dispositivo_visto.connect(lambda d, t, q: vistos.append((d, t, q)))
    b.erro_registrado.connect(erros_json.append)
    b.contadores.connect(lambda r, g, e: contadores.append((r, g, e)))
    ClienteFalso.ultimo = None
    b.conectar("127.0.0.1", 1883, ASSINATURA, 1)
    assert esperar(lambda: ClienteFalso.ultimo is not None)
    cli = ClienteFalso.ultimo
    cli.entregar(TOPICO, novus(1))
    cli.entregar(TOPICO, b"{quebrado")

    def chegou():
        app.processEvents()
        return vistos and erros_json and contadores and contadores[-1][2] == 1
    assert esperar(chegou)
    b.encerrar(5)
    app.processEvents()
    assert vistos == [("Teste_Alumbra", TOPICO, datetime.fromtimestamp(INICIO).strftime("%Y-%m-%dT%H:%M:%S"))]
    assert len(erros_json) == 1
    reg = json.loads(erros_json[0])
    assert reg["categoria"] == "payload" and reg["payload"] == "{quebrado"
    assert ler_erros_do_dia(tmp_path / "logs", DIA) == [reg]
