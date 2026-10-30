"""Testes do sink do SQL Server (com conexão/cursor falsos) e da contingência."""
from __future__ import annotations

import json
from datetime import datetime

import pytest

from app.config import GravacaoConfig, SqlServerConfig
from app.core import sink_sqlserver
from app.core.parser import Registro
from app.core.sink_sqlserver import SinkContingencia, SinkSqlServer, criar_sink, montar_insert


class ErroOdbc(Exception):
    """Imita pyodbc.Error: args == (sqlstate, mensagem)."""


class CursorFalso:
    def __init__(self, conexao):
        self.conexao = conexao
        self.fast_executemany = False

    def executemany(self, sql, linhas):
        linhas = list(linhas)
        if self.conexao.falhas_restantes > 0:
            self.conexao.falhas_restantes -= 1
            raise ErroOdbc(self.conexao.sqlstate, f"[{self.conexao.sqlstate}] falha simulada")
        ruins = [l for l in linhas if l[4] in self.conexao.campos_ruins]
        if ruins:
            raise ErroOdbc("22003", "[22003] Numeric value out of range")
        assert self.fast_executemany is True
        self.conexao.pendente.append((sql, linhas))

    def close(self):
        pass


class ConexaoFalsa:
    def __init__(self, falhas=0, sqlstate="08S01", campos_ruins=()):
        self.falhas_restantes = falhas
        self.sqlstate = sqlstate
        self.campos_ruins = set(campos_ruins)
        self.pendente = []
        self.gravado = []
        self.fechada = False
        self.rollbacks = 0

    def cursor(self):
        return CursorFalso(self)

    def commit(self):
        self.gravado.extend(self.pendente)
        self.pendente = []

    def rollback(self):
        self.rollbacks += 1
        self.pendente = []

    def close(self):
        self.fechada = True


def registros(n, campo="t"):
    agora = datetime(2026, 10, 30, 12, 0, 0)
    return [Registro(agora, "NOVUS/Teste_Alumbra/x", "x", None, f"{campo}{i}", float(i), None) for i in range(n)]


@pytest.fixture
def sql():
    return SqlServerConfig(servidor="TESTE", banco="MQTT", usuario="mqtt", senha="segredo")


@pytest.fixture
def grav(tmp_path):
    return GravacaoConfig(lote_max=3, lote_segundos=60, pasta_contingencia=str(tmp_path / "cont"))


def novo_sink(sql, grav, conexoes, logs=None, **kw):
    logs = logs if logs is not None else []

    def conectar(cfg):
        c = conexoes.pop(0)
        conectar.abertas.append(c)
        return c
    conectar.abertas = []
    sink = SinkSqlServer(sql, grav, lambda n, m: logs.append((n, m)), conectar=conectar,
                         dormir=lambda s: None, **kw)
    return sink, conectar.abertas


def test_montar_insert_e_validacao_da_tabela():
    assert montar_insert("dbo.LeiturasMqtt") == (
        "INSERT INTO [dbo].[LeiturasMqtt] ([RecebidoEm], [Topico], [Dispositivo], [DataHoraLeitura], [Campo], "
        "[ValorNumerico], [ValorTexto]) VALUES (?, ?, ?, ?, ?, ?, ?)")
    assert montar_insert("[dbo].[Leituras_2]").startswith("INSERT INTO [dbo].[Leituras_2]")
    for ruim in ["dbo.Leituras; DROP TABLE x", "dbo.[a]]b]", "a b", "x.y.z.w", ""]:
        with pytest.raises(ValueError):
            montar_insert(ruim)


def test_lote_por_quantidade(sql, grav):
    sink, abertas = novo_sink(sql, grav, [ConexaoFalsa()])
    sink.adicionar(registros(2))
    assert not sink.deve_descarregar()
    sink.adicionar(registros(1))
    assert sink.deve_descarregar()
    assert sink.descarregar() == 3
    (sql_insert, linhas), = abertas[0].gravado
    assert sql_insert.startswith("INSERT INTO [dbo].[LeiturasMqtt]")
    assert linhas[0] == (datetime(2026, 10, 30, 12), "NOVUS/Teste_Alumbra/x", "x", None, "t0", 0.0, None)
    assert sink.pendentes() == 0


def test_lote_por_tempo(sql, grav, monkeypatch):
    grav.lote_segundos = 0.5
    sink, _ = novo_sink(sql, grav, [ConexaoFalsa()])
    relogio = [100.0]
    monkeypatch.setattr(sink_sqlserver.time, "monotonic", lambda: relogio[0])
    sink.adicionar(registros(1))
    assert not sink.deve_descarregar()
    relogio[0] += 0.6
    assert sink.deve_descarregar()


def test_retry_com_backoff_e_reconexao(sql, grav):
    logs = []
    esperas = []
    falha, boa = ConexaoFalsa(falhas=1), ConexaoFalsa()
    sink, abertas = novo_sink(sql, grav, [falha, boa], logs)
    sink._dormir = esperas.append
    sink.adicionar(registros(3))
    assert sink.descarregar() == 3
    assert falha.fechada and falha.rollbacks == 1
    assert len(boa.gravado) == 1
    assert esperas == []  # 1ª queda 08S01: reconecta e tenta na hora
    assert any(n == "AVISO" and "reconectando" in m for n, m in logs)
    assert ("INFO", "Gravados 3 registros no SQL Server") in logs
    assert sink.falhas() == 0


def test_falha_persistente_vai_para_contingencia(sql, grav, tmp_path):
    logs = []
    sink, _ = novo_sink(sql, grav, [ConexaoFalsa(falhas=99) for _ in range(4)], logs)
    esperas = []
    sink._dormir = esperas.append
    sink.adicionar(registros(3))
    assert sink.descarregar() == 3
    assert esperas == [0.5, 1.0]  # 08S01: 1 repetição imediata + 3 tentativas com backoff
    assert sink.falhas() == 1
    arquivos = list((tmp_path / "cont").glob("leituras_*.jsonl"))
    assert len(arquivos) == 1
    linhas = [json.loads(l) for l in arquivos[0].read_text(encoding="utf-8").splitlines()]
    assert [l["Campo"] for l in linhas] == ["t0", "t1", "t2"]
    assert linhas[0]["RecebidoEm"] == "2026-10-30T12:00:00.000"
    assert any(n == "ERRO" and "contingência" in m for n, m in logs)
    assert not any("segredo" in m for _, m in logs)

    # durante a pausa, vai direto para a contingência sem tentar o banco
    sink.adicionar(registros(2, "u"))
    assert sink.descarregar() == 2
    assert len(arquivos[0].read_text(encoding="utf-8").splitlines()) == 5


def test_erro_traduzido_por_sql_conexao(sql, grav, monkeypatch):
    monkeypatch.setattr(sink_sqlserver.sql_conexao, "traduzir_erro", lambda exc: "MENSAGEM TRADUZIDA\nDetalhe")
    logs = []
    sink, _ = novo_sink(sql, grav, [ConexaoFalsa(falhas=99)], logs, tentativas=1)
    sink.adicionar(registros(1))
    sink.descarregar()
    assert any("MENSAGEM TRADUZIDA" in m for _, m in logs)


def test_conectar_padrao_usa_sql_conexao(sql, grav, monkeypatch):
    conexao = ConexaoFalsa()
    chamadas = []
    monkeypatch.setattr(sink_sqlserver.sql_conexao, "conectar", lambda cfg: chamadas.append(cfg) or conexao)
    sink = SinkSqlServer(sql, grav, conectar=None)
    sink.adicionar(registros(1))
    assert sink.fechar() == 1
    assert chamadas == [sql] and conexao.fechada and len(conexao.gravado) == 1


def test_definir_sql_aplica_no_proximo_lote(sql, grav):
    logs = []
    sink, abertas = novo_sink(sql, grav, [ConexaoFalsa(), ConexaoFalsa()], logs)
    sink.adicionar(registros(1))
    sink.descarregar()
    sink.definir_sql(SqlServerConfig(servidor="OUTRO", banco="B", tabela="dbo.Outra"))
    sink.adicionar(registros(1))
    sink.descarregar()
    assert abertas[0].fechada
    assert abertas[1].gravado[0][0].startswith("INSERT INTO [dbo].[Outra]")


def test_sem_sql_usa_contingencia(grav, tmp_path):
    sink = criar_sink(SqlServerConfig(servidor="", banco=""), grav)
    assert isinstance(sink, SinkContingencia)
    sink.adicionar(registros(2))
    assert sink.fechar() == 2
    assert len(next((tmp_path / "cont").glob("*.jsonl")).read_text().splitlines()) == 2



def test_timeout_repete_com_backoff_desde_a_primeira(sql, grav):
    logs, esperas = [], []
    sink, abertas = novo_sink(sql, grav, [ConexaoFalsa(falhas=1, sqlstate="HYT00"), ConexaoFalsa()], logs)
    sink._dormir = esperas.append
    sink.adicionar(registros(2))
    assert sink.descarregar() == 2
    assert esperas == [0.5] and len(abertas[1].gravado) == 1
    assert any("tentativa 1/3" in m for _, m in logs)


def test_driver_ausente_e_erro_de_conexao(sql, grav, tmp_path):
    from app.core.sink_sqlserver import classificar_erro
    from app.core.sql_conexao import DriverOdbcAusente
    assert classificar_erro(DriverOdbcAusente("x")) == ("conexao", "")
    assert classificar_erro(ErroOdbc("08001", "x")) == ("conexao", "08001")
    assert classificar_erro(ErroOdbc("HYT01", "x")) == ("conexao", "HYT01")
    assert classificar_erro(ErroOdbc("42S02", "x")) == ("esquema", "42S02")
    assert classificar_erro(ErroOdbc("23000", "x")) == ("dados", "23000")
    assert classificar_erro(ValueError("x")) == ("outro", "")


def test_erro_de_esquema_nao_repete_e_pausa(sql, grav, tmp_path):
    logs, esperas = [], []
    conexao = ConexaoFalsa(falhas=99, sqlstate="42S02")
    sink, abertas = novo_sink(sql, grav, [conexao], logs)
    sink._dormir = esperas.append
    sink.adicionar(registros(3))
    assert sink.descarregar() == 3
    assert esperas == [] and len(abertas) == 1 and conexao.falhas_restantes == 98
    assert sink.falhas() == 1
    assert any(n == "ERRO" and "tabela/permissão" in m for n, m in logs)
    # banco em pausa: o próximo lote vai direto para a contingência
    sink.adicionar(registros(1, "u"))
    sink.descarregar()
    assert len(abertas) == 1
    assert len(next((tmp_path / "cont").glob("*.jsonl")).read_text().splitlines()) == 4


def test_erro_de_dados_isola_so_as_linhas_ruins(sql, grav, tmp_path):
    grav.lote_max = 100
    logs = []
    conexao = ConexaoFalsa(campos_ruins={"t3", "t6"})
    sink, abertas = novo_sink(sql, grav, [conexao], logs)
    sink.adicionar(registros(8))
    assert sink.descarregar() == 8
    gravados = sorted(l[4] for _, linhas in conexao.gravado for l in linhas)
    assert gravados == ["t0", "t1", "t2", "t4", "t5", "t7"]
    cont = [json.loads(l)["Campo"] for l in next((tmp_path / "cont").glob("*.jsonl")).read_text().splitlines()]
    assert sorted(cont) == ["t3", "t6"]
    assert sink.falhas() == 2
    assert sink._indisponivel_ate == 0.0  # erro de dados não pausa o banco
    assert sum(n == "ERRO" and "Registro recusado" in m for n, m in logs) == 2
    # próximo lote vai ao banco normalmente
    sink.adicionar(registros(1, "ok"))
    sink.descarregar()
    assert conexao.gravado[-1][1][0][4] == "ok0"


def test_fechar_faz_uma_unica_tentativa(sql, grav, tmp_path):
    logs, esperas = [], []
    conexoes = [ConexaoFalsa(falhas=99, sqlstate="08001") for _ in range(5)]
    sink, abertas = novo_sink(sql, grav, list(conexoes), logs)
    sink._dormir = esperas.append
    sink.adicionar(registros(2))
    assert sink.fechar() == 2
    assert len(abertas) == 1 and esperas == []
    assert len(next((tmp_path / "cont").glob("*.jsonl")).read_text().splitlines()) == 2


def test_fechar_chama_fechar_da_contingencia(sql, grav):
    sink, _ = novo_sink(sql, grav, [ConexaoFalsa()])
    chamado = []
    sink._fallback.fechar = lambda: chamado.append(1) or 0
    sink._inserir = lambda itens: (_ for _ in ()).throw(RuntimeError("inesperado"))
    sink.adicionar(registros(1))
    sink._dormir = lambda s: None
    sink.fechar()
    assert chamado == [1]
