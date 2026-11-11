"""Testes do sink do SQL Server (com conexão/cursor falsos) e da contingência."""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

import pytest

from app.config import GravacaoConfig, SqlServerConfig
from app.core import sink_sqlserver
from app.core.mapeamento import LinhaIot
from app.core.sink_sqlserver import SinkContingencia, SinkSqlServer, criar_sink, montar_insert


class ErroOdbc(Exception):
    """Imita pyodbc.Error: args == (sqlstate, mensagem)."""


COLUNAS_TABELA = ("id", "pid", "device_id", "ts_unix", "data_hora_utc", "chd1_value", "chd2_value", "chd3_value",
                  "chd4_value", "chd5_value", "chd6_value", "payload_json", "recebido_em")
COLUNAS_ANTIGAS = tuple(c for c in COLUNAS_TABELA if c not in ("chd4_value", "chd5_value", "chd6_value"))


class CursorFalso:
    def __init__(self, conexao):
        self.conexao = conexao
        self.fast_executemany = False
        self._linhas = []

    def execute(self, sql, *params):
        self.conexao.consultas.append((sql, params))
        assert "INFORMATION_SCHEMA.COLUMNS" in sql and params == ("dbo", self.conexao.tabela)
        self._linhas = [(c,) for c in self.conexao.colunas]
        return self

    def fetchall(self):
        return self._linhas

    def executemany(self, sql, linhas):
        linhas = list(linhas)
        if self.conexao.falhas_restantes > 0:
            self.conexao.falhas_restantes -= 1
            raise ErroOdbc(self.conexao.sqlstate, f"[{self.conexao.sqlstate}] falha simulada")
        ruins = [l for l in linhas if l[1] in self.conexao.campos_ruins]
        if ruins:
            raise ErroOdbc("22003", "[22003] Numeric value out of range")
        assert self.fast_executemany is True
        self.conexao.pendente.append((sql, linhas))

    def close(self):
        pass


class ConexaoFalsa:
    def __init__(self, falhas=0, sqlstate="08S01", campos_ruins=(), colunas=COLUNAS_TABELA, tabela="IOT001"):
        self.colunas = colunas
        self.tabela = tabela
        self.consultas = []
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


RECEBIDO = datetime(2026, 10, 30, 12, 0, 0)


def registros(n, campo="t"):
    """n linhas completas da IOT001; device_id = f"{campo}{i}" (usado para marcar linhas ruins)."""
    return [LinhaIot(i, f"{campo}{i}", 1730000000 + i, Decimal(i).quantize(Decimal("0.0001")), None, None,
                     f'{{"pid": {i}}}', RECEBIDO, "NOVUS/Teste_Alumbra/x") for i in range(n)]


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
    assert montar_insert("dbo.IOT001") == (
        "INSERT INTO [dbo].[IOT001] ([pid], [device_id], [ts_unix], [chd1_value], [chd2_value], [chd3_value], "
        "[chd4_value], [chd5_value], [chd6_value], [payload_json], [recebido_em]) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    assert montar_insert("dbo.IOT001", sink_sqlserver.colunas_insert(set(COLUNAS_ANTIGAS))) == (
        "INSERT INTO [dbo].[IOT001] ([pid], [device_id], [ts_unix], [chd1_value], [chd2_value], [chd3_value], "
        "[payload_json], [recebido_em]) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
    with pytest.raises(ValueError):
        montar_insert("dbo.IOT001", ("pid", "x]; DROP TABLE y; --"))
    assert "[id]" not in montar_insert("dbo.IOT001") and "data_hora_utc" not in montar_insert("dbo.IOT001")
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
    assert sql_insert.startswith("INSERT INTO [dbo].[IOT001]")
    assert linhas[0] == (0, "t0", 1730000000, Decimal("0.0000"), None, None, None, None, None, '{"pid": 0}',
                         RECEBIDO)
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
    assert [l["device_id"] for l in linhas] == ["t0", "t1", "t2"]
    assert linhas[0]["recebido_em"] == "2026-10-30T12:00:00.000Z"
    assert linhas[0]["chd1_value"] == "0.0000" and linhas[0]["motivo"] is None
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
    gravados = sorted(l[1] for _, linhas in conexao.gravado for l in linhas)
    assert gravados == ["t0", "t1", "t2", "t4", "t5", "t7"]
    cont = [json.loads(l)["device_id"] for l in next((tmp_path / "cont").glob("*.jsonl")).read_text().splitlines()]
    assert sorted(cont) == ["t3", "t6"]
    assert sink.falhas() == 2
    assert sink._indisponivel_ate == 0.0  # erro de dados não pausa o banco
    assert sum(n == "ERRO" and "Registro recusado" in m for n, m in logs) == 2
    # próximo lote vai ao banco normalmente
    sink.adicionar(registros(1, "ok"))
    sink.descarregar()
    assert conexao.gravado[-1][1][0][1] == "ok0"


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


# ------------------------------------------------------------------------------------------ IOT001


def _incompleta(motivo="sem pid"):
    return LinhaIot(None, "N1", None, None, None, None, "x", RECEBIDO, "NOVUS/Teste_Alumbra/N1", motivo)


def test_linhas_incompletas_vao_para_contingencia_e_nao_ao_banco(sql, grav, tmp_path):
    conexao = ConexaoFalsa()
    sink, _ = novo_sink(sql, grav, [conexao])
    sink.adicionar(registros(2) + [_incompleta()])
    assert sink.descarregar() == 2          # só as completas contam como gravadas
    assert [l[1] for _, ls in conexao.gravado for l in ls] == ["t0", "t1"]
    cont = [json.loads(l) for l in next((tmp_path / "cont").glob("*.jsonl")).read_text().splitlines()]
    assert len(cont) == 1 and cont[0]["motivo"] == "sem pid" and cont[0]["payload_json"] == "x"
    assert sink.falhas() == 0               # o pipeline já contou a rejeição


def test_lote_so_com_incompletas_nao_abre_conexao(sql, grav, tmp_path):
    sink, abertas = novo_sink(sql, grav, [ConexaoFalsa()])
    sink.adicionar([_incompleta(), _incompleta("sem ts_unix")])
    assert sink.descarregar() == 0
    assert abertas == []
    assert len(next((tmp_path / "cont").glob("*.jsonl")).read_text().splitlines()) == 2


def test_contingencia_sem_sql_conta_so_completas(grav, tmp_path):
    sink = criar_sink(SqlServerConfig(servidor="", banco=""), grav)
    sink.adicionar(registros(1) + [_incompleta()])
    assert sink.fechar() == 1
    assert len(next((tmp_path / "cont").glob("*.jsonl")).read_text().splitlines()) == 2


class PyodbcFalso:
    SQL_BIGINT, SQL_VARCHAR, SQL_WVARCHAR, SQL_DECIMAL, SQL_TYPE_TIMESTAMP = -5, 12, -9, 3, 93


def test_setinputsizes_com_tipos_da_iot001(sql, grav, monkeypatch):
    monkeypatch.setitem(sink_sqlserver.sys.modules, "pyodbc", PyodbcFalso)
    p = PyodbcFalso
    assert sink_sqlserver._tipos_parametros() == [
        (p.SQL_BIGINT, 0, 0), (p.SQL_VARCHAR, 100, 0), (p.SQL_BIGINT, 0, 0),
        (p.SQL_DECIMAL, 18, 4), (p.SQL_DECIMAL, 18, 4), (p.SQL_DECIMAL, 18, 4),
        (p.SQL_DECIMAL, 18, 4), (p.SQL_DECIMAL, 18, 4), (p.SQL_DECIMAL, 18, 4),
        (p.SQL_WVARCHAR, 0, 0), (p.SQL_TYPE_TIMESTAMP, 23, 3),
    ]
    antigas = sink_sqlserver.colunas_insert(set(COLUNAS_ANTIGAS))
    assert sink_sqlserver._tipos_parametros(antigas) == [
        (p.SQL_BIGINT, 0, 0), (p.SQL_VARCHAR, 100, 0), (p.SQL_BIGINT, 0, 0),
        (p.SQL_DECIMAL, 18, 4), (p.SQL_DECIMAL, 18, 4), (p.SQL_DECIMAL, 18, 4),
        (p.SQL_WVARCHAR, 0, 0), (p.SQL_TYPE_TIMESTAMP, 23, 3),
    ]
    tamanhos = []

    class CursorComTipos(CursorFalso):
        def setinputsizes(self, tipos):
            tamanhos.append(tipos)

    conexao = ConexaoFalsa()
    conexao.cursor = lambda: CursorComTipos(conexao)
    sink, _ = novo_sink(sql, grav, [conexao])
    sink.adicionar(registros(1))
    sink.descarregar()
    assert len(tamanhos) == 1 and len(tamanhos[0]) == len(LinhaIot.COLUNAS)


# ------------------------------------------------------------------- colunas detectadas (canais 4–6, 11/11/2026)


def _com_canais_novos(n, campo="c"):
    linhas = registros(n, campo)
    for l in linhas:
        l.chd4_value, l.chd6_value = Decimal("4.0000"), Decimal("6.0000")
    return linhas


def test_insert_com_chd4_a_chd6_quando_a_tabela_tem(sql, grav):
    logs = []
    conexao = ConexaoFalsa()
    sink, _ = novo_sink(sql, grav, [conexao], logs)
    sink.adicionar(_com_canais_novos(1))
    assert sink.descarregar() == 1
    (sql_insert, linhas), = conexao.gravado
    assert "[chd4_value], [chd5_value], [chd6_value]" in sql_insert and sql_insert.count("?") == 11
    assert linhas[0][6:9] == (Decimal("4.0000"), None, Decimal("6.0000"))
    assert not any("canais_4_a_6" in m for _, m in logs)


def test_insert_sem_chd4_a_chd6_antes_do_script_e_aviso_unico(sql, grav):
    logs = []
    conexao = ConexaoFalsa(colunas=COLUNAS_ANTIGAS)
    sink, _ = novo_sink(sql, grav, [conexao], logs)
    sink.adicionar(registros(1))            # sem valor nos canais 4–6: nada a avisar
    sink.descarregar()
    assert not any("canais_4_a_6" in m for _, m in logs)
    for _ in range(3):
        sink.adicionar(_com_canais_novos(2))
        assert sink.descarregar() == 2
    for sql_insert, linhas in conexao.gravado:
        assert "chd4_value" not in sql_insert and sql_insert.count("?") == 8
        assert all(len(l) == 8 for l in linhas)
    avisos = [m for n, m in logs if n == "AVISO" and "canais_4_a_6" in m]
    assert avisos == ["A tabela dbo.IOT001 ainda não tem chd4_value..chd6_value: rode "
                      "sql/canais_4_a_6_iot001.sql. Os valores continuam no payload_json."]
    # a detecção é feita uma vez por conexão (cache), não a cada lote
    assert len(conexao.consultas) == 1


def test_colunas_lidas_de_novo_ao_reconectar(sql, grav):
    """Queda de conexão: a conexão nova lê as colunas de novo (ex.: o script foi rodado nesse meio tempo)."""
    antiga = ConexaoFalsa(colunas=COLUNAS_ANTIGAS)
    nova = ConexaoFalsa()
    sink, abertas = novo_sink(sql, grav, [antiga, nova])
    sink.adicionar(_com_canais_novos(1))
    sink.descarregar()
    assert "chd4_value" not in antiga.gravado[0][0]
    antiga.falhas_restantes = 1             # 08S01: cai, reconecta na hora
    sink.adicionar(_com_canais_novos(1, "d"))
    assert sink.descarregar() == 1
    assert antiga.fechada and len(abertas) == 2
    assert "chd4_value" in nova.gravado[0][0] and len(nova.consultas) == 1


def test_falha_ao_ler_colunas_usa_chd1_a_chd3(sql, grav):
    logs = []
    conexao = ConexaoFalsa()

    class CursorSemMetadados(CursorFalso):
        def execute(self, sql, *params):
            raise ErroOdbc("42000", "[42000] permissão negada (simulada)")

    conexao.cursor = lambda: CursorSemMetadados(conexao)
    sink, _ = novo_sink(sql, grav, [conexao], logs)
    sink.adicionar(registros(1))
    assert sink.descarregar() == 1
    assert "chd4_value" not in conexao.gravado[0][0]
    assert any(n == "AVISO" and "Não foi possível ler as colunas" in m for n, m in logs)


def test_tabela_invisivel_insere_colunas_base_e_erro_do_insert_decide(sql, grav):
    assert sink_sqlserver.colunas_insert(set()) == (
        "pid", "device_id", "ts_unix", "chd1_value", "chd2_value", "chd3_value", "payload_json", "recebido_em")


def test_setinputsizes_acompanha_colunas_detectadas(sql, grav, monkeypatch):
    monkeypatch.setitem(sink_sqlserver.sys.modules, "pyodbc", PyodbcFalso)
    tamanhos = []

    class CursorComTipos(CursorFalso):
        def setinputsizes(self, tipos):
            tamanhos.append(tipos)

    conexao = ConexaoFalsa(colunas=COLUNAS_ANTIGAS)
    conexao.cursor = lambda: CursorComTipos(conexao)
    sink, _ = novo_sink(sql, grav, [conexao])
    sink.adicionar(registros(1))
    sink.descarregar()
    assert len(tamanhos) == 1 and len(tamanhos[0]) == 8

