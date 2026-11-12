"""Canais 4–6 (script), diagnóstico de 3 min, dispositivo_visto só com leituras e cadastro no portal OEE
(``app.core.oee_dispositivos``), com conexão/cursor falsos e relógio injetado. Nenhum banco real é usado.
"""
from __future__ import annotations

import re
import tomllib
from datetime import datetime
from pathlib import Path

import pytest

from app.config import Config, OeeConfig, SqlServerConfig, carregar
from app.core import oee_dispositivos as oee
from app.core.oee_dispositivos import CadastroOee, ip_do_diag, observacao_diag, utc_sql
from app.core.pipeline import Pipeline
from app.core.sink_sqlserver import SinkSqlServer
from tests.test_amostragem import ClienteFalso, Relogio, esperar, novus
from tests.test_digirail import DIAG, EVENTO_MANUAL, Coletor, resposta_diag
from tests.test_sink import ConexaoFalsa as ConexaoIot

RAIZ = Path(__file__).resolve().parent.parent
DEV = "Teste_Alumbra"
T0 = 1794407460.123456          # 2026-11-11 14:31:00.123456 UTC
SENHA_FALSA = "nao-e-senha-real"  # só para o cadastro se considerar configurado (conexão é falsa)
COLUNAS_OEE = ("id", "codigo", "nome", "tipo", "protocolo", "endereco", "porta", "topico", "maquina_id",
               "ultimo_sinal_em", "observacao", "ativo", "criado_em", "criado_por_id", "atualizado_em",
               "atualizado_por_id", "unidade")
PROIBIDAS_NO_UPDATE = ("maquina_id", "nome", "tipo", "protocolo", "porta", "ativo", "unidade", "criado_em",
                       "criado_por_id", "atualizado_por_id")


class ErroOdbc(Exception):
    """Imita pyodbc.Error: args == (sqlstate, mensagem)."""


# ============================================================================================ scripts e config


def _codigo_sql(caminho: Path) -> str:
    """Texto do script sem comentários (/* */ e --)."""
    texto = caminho.read_text(encoding="utf-8")
    texto = re.sub(r"/\*.*?\*/", "", texto, flags=re.S)
    return re.sub(r"--[^\n]*", "", texto)


def test_script_canais_4_a_6_so_acrescenta_colunas():
    caminho = RAIZ / "sql" / "canais_4_a_6_iot001.sql"
    texto = caminho.read_text(encoding="utf-8").upper()
    for proibida in ("DROP", "DELETE", "UPDATE", "TRUNCATE", "MERGE", "ALTER COLUMN"):
        assert proibida not in texto, proibida
    codigo = _codigo_sql(caminho)
    assert "USE [MQTT]" in codigo
    for n in (4, 5, 6):
        assert f"COL_LENGTH(N'dbo.IOT001', N'chd{n}_value') IS NULL" in codigo
        assert f"ALTER TABLE [dbo].[IOT001] ADD [chd{n}_value] decimal(18, 4) NULL;" in codigo
    assert codigo.rstrip().endswith("GO") and "INFORMATION_SCHEMA.COLUMNS" in codigo   # conferência no final


def test_script_permissoes_oee():
    caminho = RAIZ / "sql" / "permissoes_oee.sql"
    texto = caminho.read_text(encoding="utf-8")
    codigo = _codigo_sql(caminho).upper()
    # DELETE só como permissão em apontamentos_estado (apontamentos reais); o script nunca apaga nada
    grant_apont = "GRANT SELECT, INSERT, UPDATE, DELETE ON OBJECT::DBO.APONTAMENTOS_ESTADO TO [MQTT]"
    assert grant_apont in codigo
    sem_grant = codigo.replace(grant_apont, "").replace("SELECT, INSERT, UPDATE, DELETE EM DBO.APONTAMENTOS_ESTADO", "")
    for proibida in ("DROP", "DELETE", "TRUNCATE", "ALTER TABLE", "CREATE TABLE"):
        assert proibida not in sem_grant, proibida
    for grant in ("GRANT SELECT, UPDATE ON OBJECT::DBO.MAQUINAS TO [MQTT]",
                  "GRANT SELECT ON OBJECT::DBO.LEGENDAS TO [MQTT]",
                  "GRANT SELECT, UPDATE ON OBJECT::DBO.ORDENS_PRODUCAO TO [MQTT]"):
        assert grant in codigo, grant
    assert "USE [OEE]" in codigo
    assert "CREATE USER [MQTT] FOR LOGIN [MQTT]" in codigo and "DATABASE_PRINCIPAL_ID(N'MQTT') IS NULL" in codigo
    assert "GRANT SELECT, INSERT, UPDATE ON OBJECT::DBO.DISPOSITIVOS_IOT TO [MQTT]" in codigo
    assert "OEE_Alumbra" in texto      # comentário: o banco pode ter outro nome


def test_diagnostico_padrao_3_minutos_e_config_oee():
    cfg = Config()
    assert cfg.dispositivos.diagnostico_minutos == 3
    assert cfg.oee == OeeConfig(habilitado=True, banco="OEE", tabela="dbo.dispositivos_iot", unidade="manutencao",
                                tipo="contador")
    exemplo = tomllib.loads((RAIZ / "config.example.toml").read_text(encoding="utf-8"))
    assert exemplo["dispositivos"]["diagnostico_minutos"] == 3
    assert exemplo["oee"] == {"habilitado": True, "banco": "OEE", "tabela": "dbo.dispositivos_iot",
                              "unidade": "manutencao", "tipo": "contador"}
    for n in (4, 5, 6):
        assert exemplo["mapeamento"][f"chd{n}"] == [f"chd{n}_value", f"chd{n}.value", f"chd{n}"]


def test_carregar_le_secao_oee(tmp_path):
    arq = tmp_path / "config.toml"
    arq.write_text('[oee]\nbanco = "OEE_Alumbra"\nhabilitado = false\n', encoding="utf-8")
    cfg = carregar(arq)
    assert cfg.oee.banco == "OEE_Alumbra" and cfg.oee.habilitado is False and cfg.oee.unidade == "manutencao"


# ============================================================================================ funções puras


@pytest.mark.parametrize("valor,esperado", [
    ([172, 16, 30, 140], "172.16.30.140"),
    (["192", "168", "0", "23"], "192.168.0.23"),
    ("10.0.0.5", "10.0.0.5"),
    ([0, 0, 0, 0], None),
    ([1, 2, 3], None),
    ([1, 2, 3, 256], None),
    ([1, 2, True, 4], None),
    (None, None),
])
def test_ip_do_diag(valor, esperado):
    assert ip_do_diag({"ipv4": valor}) == esperado


def test_observacao_do_diag():
    diag = {"sn": "21004417", "fw_v": "1.23", "avg_rssi": "-55", "mqtt_queue": 0, "ipv4": [172, 16, 30, 140]}
    assert observacao_diag(diag, datetime(2026, 11, 11, 14, 31)) == (
        "Diag 11/11/2026 14:31 · Nº série 21004417 · Firmware 1.23 · Wi-Fi -55 dBm · Fila 0 · IP 172.16.30.140 "
        "(atualizado pelo Integrador MQTT)")
    assert observacao_diag({}, datetime(2026, 11, 11, 14, 31)) == \
        "Diag 11/11/2026 14:31 (atualizado pelo Integrador MQTT)"


def test_observacao_truncada_em_500():
    obs = observacao_diag({"sn": "9" * 800, "fw_v": "1.23"}, datetime(2026, 11, 11, 14, 31))
    assert len(obs) == 500 and obs.startswith("Diag 11/11/2026 14:31 · Nº série 999")


def test_datetime_sem_fracao_abaixo_do_milissegundo():
    assert utc_sql(T0) == datetime(2026, 11, 11, 14, 31, 0, 123000)
    assert utc_sql(T0).tzinfo is None


# ============================================================================================ conexão falsa


class CursorOee:
    def __init__(self, con: "ConexaoOee") -> None:
        self.con = con
        self.rowcount = -1
        self._um = None
        self._todas: list = []

    def execute(self, sql, *params):
        con = self.con
        con.consultas.append((sql, params))
        if sql.startswith(("UPDATE", "INSERT")) and con.falha is not None:
            raise con.falha
        if sql == "SELECT DB_ID(?)":
            assert params == ("OEE",)
            self._um = (7 if con.banco else None,)
        elif sql.startswith("SELECT OBJECT_ID("):
            assert params == ("[OEE].[dbo].[dispositivos_iot]",)
            self._um = (99 if con.tabela else None,)
        elif "sp_executesql" in sql:
            assert sql.startswith("EXEC [OEE].sys.sp_executesql") and params == ("[dbo].[dispositivos_iot]",)
            self._um = (con.insert, con.update)
        elif "INFORMATION_SCHEMA.COLUMNS" in sql:
            assert "[OEE].INFORMATION_SCHEMA.COLUMNS" in sql and params == ("dbo", "dispositivos_iot")
            self._todas = [(c,) for c in con.colunas]
        elif sql.startswith("UPDATE"):
            campos = re.findall(r"\[(\w+)\] = \?", sql.split(" WHERE ", 1)[0])
            assert params[-1] is not None and len(params) == len(campos) + 1
            reg = con.registros.get(params[-1])
            self.rowcount = 0 if reg is None else 1
            if reg is not None:
                reg.update(zip(campos, params[:-1]))
        elif sql.startswith("INSERT"):
            campos = re.findall(r"\[(\w+)\]", sql.split("(", 1)[1].split(")", 1)[0])
            assert len(campos) == len(params)
            dados = dict(zip(campos, params))
            con.registros[dados["codigo"]] = dados
            self.rowcount = 1
        else:  # pragma: no cover
            raise AssertionError(f"SQL inesperado: {sql}")
        return self

    def fetchone(self):
        return self._um

    def fetchall(self):
        return self._todas

    def close(self):
        pass


class ConexaoOee:
    def __init__(self, banco=True, tabela=True, insert=1, update=1, colunas=COLUNAS_OEE, registros=None):
        self.banco, self.tabela, self.insert, self.update = banco, tabela, insert, update
        self.colunas = colunas
        self.registros: dict[str, dict] = registros if registros is not None else {}
        self.consultas: list[tuple[str, tuple]] = []
        self.falha: Exception | None = None
        self.commits = self.rollbacks = 0
        self.fechada = False

    def cursor(self):
        return CursorOee(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.fechada = True

    def dml(self) -> list[tuple[str, tuple]]:
        return [(s, p) for s, p in self.consultas if s.startswith(("UPDATE", "INSERT"))]


def _sql_cfg() -> SqlServerConfig:
    return SqlServerConfig(servidor="FALSO", banco="MQTT", usuario="mqtt", senha=SENHA_FALSA)


def novo_cadastro(con: ConexaoOee | list, relogio=None, oee_cfg: OeeConfig | None = None, logs=None):
    logs = logs if logs is not None else []
    conexoes = con if isinstance(con, list) else [con]
    abertas = []

    def conectar(cfg):
        assert cfg.senha == SENHA_FALSA
        c = conexoes.pop(0) if len(conexoes) > 1 else conexoes[0]
        abertas.append(c)
        return c

    cad = CadastroOee(_sql_cfg(), oee_cfg or OeeConfig(), lambda n, m: logs.append((n, m)), conectar=conectar,
                      relogio=relogio or Relogio(T0), iniciar_thread=False)
    cad.abertas = abertas
    return cad, logs


EXISTENTE = {"codigo": DEV, "nome": "Contador linha 3", "tipo": "sensor", "protocolo": "mqtt", "endereco": "1.1.1.1",
             "porta": 1883, "topico": "antigo", "maquina_id": 42, "ultimo_sinal_em": None, "observacao": "manual",
             "ativo": 0, "unidade": "tecnologia", "criado_em": datetime(2026, 11, 6), "criado_por_id": 5,
             "atualizado_em": None, "atualizado_por_id": 5}


# ============================================================================================ detecção


@pytest.mark.parametrize("kw,motivo", [
    (dict(banco=False), "banco"),
    (dict(tabela=False), "tabela"),
    (dict(insert=0), "permissão"),
    (dict(update=0), "permissão"),
    (dict(colunas=("id", "nome")), "colunas"),
])
def test_deteccao_negativa_desliga_em_silencio(kw, motivo):
    con = ConexaoOee(**kw)
    cad, logs = novo_cadastro(con)
    cad.diagnostico(DEV, DIAG, "NOVUS/Teste_Alumbra/events")
    cad.sinal(DEV)
    cad.executar_pendentes()
    assert logs == [("INFO", "Portal OEE não encontrado neste SQL Server (banco OEE.dbo.dispositivos_iot): "
                             "cadastro de dispositivos desligado.")]
    assert con.dml() == [] and con.fechada
    n = len(con.consultas)
    cad.diagnostico(DEV, DIAG, None)            # desligado: nem consulta o banco
    cad.executar_pendentes()
    assert len(con.consultas) == n and len(logs) == 1
    cad.nova_sessao()                           # próxima conexão MQTT: tenta de novo, sem repetir o INFO
    cad.diagnostico(DEV, DIAG, None)
    cad.executar_pendentes()
    assert len(con.consultas) > n and len(logs) == 1


def test_sem_senha_ou_desabilitado_nao_conecta():
    for sql, cfg in ((SqlServerConfig(servidor="X", banco="MQTT", senha=""), OeeConfig()),
                     (_sql_cfg(), OeeConfig(habilitado=False)),
                     (SqlServerConfig(servidor="", banco=""), OeeConfig())):
        cad = CadastroOee(sql, cfg, conectar=lambda c: pytest.fail("não deveria conectar"), iniciar_thread=False)
        cad.diagnostico(DEV, DIAG, None)
        cad.sinal(DEV)
        cad.executar_pendentes()


def test_banco_invalido_na_config_desliga():
    con = ConexaoOee()
    cad, logs = novo_cadastro(con, oee_cfg=OeeConfig(banco="OEE];DROP"))
    cad.diagnostico(DEV, DIAG, None)
    cad.executar_pendentes()
    assert con.consultas == [] and len(logs) == 1 and "desligado" in logs[0][1]


# ============================================================================================ upsert


def test_insert_de_device_novo_com_os_campos_exatos():
    con = ConexaoOee()
    cad, logs = novo_cadastro(con)
    diag = {"sn": "21004417", "fw_v": "1.23", "avg_rssi": "-55", "mqtt_queue": 0, "ipv4": [172, 16, 30, 140]}
    cad.diagnostico(DEV, diag, "NOVUS/Teste_Alumbra/events")
    cad.executar_pendentes()
    (upd, _), (ins, params) = con.dml()
    assert upd.startswith("UPDATE [OEE].[dbo].[dispositivos_iot] WITH (UPDLOCK, SERIALIZABLE) SET ")
    assert ins == ("INSERT INTO [OEE].[dbo].[dispositivos_iot] ([codigo], [nome], [tipo], [protocolo], [endereco], "
                   "[topico], [ultimo_sinal_em], [observacao], [ativo], [unidade], [criado_em], [atualizado_em]) "
                   "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    agora = datetime(2026, 11, 11, 14, 31, 0, 123000)
    obs = observacao_diag(diag, datetime.fromtimestamp(T0))
    assert params == (DEV, DEV, "contador", "mqtt", "172.16.30.140", "NOVUS/Teste_Alumbra/events", agora, obs, 1,
                      "manutencao", agora, agora)
    assert "maquina_id" not in ins and "_por_id" not in ins and "porta" not in ins and "MERGE" not in ins
    assert con.commits >= 1 and con.rollbacks == 0
    assert logs == [("INFO", "Dispositivo Teste_Alumbra cadastrado no portal OEE")]
    # o 2º diagnóstico já atualiza; o INFO de cadastro não se repete e o de "atualizado" não aparece na mesma sessão
    cad.diagnostico(DEV, diag, "NOVUS/Teste_Alumbra/events")
    cad.executar_pendentes()
    assert len(con.dml()) == 3 and con.dml()[-1][0].startswith("UPDATE")
    assert logs == [("INFO", "Dispositivo Teste_Alumbra cadastrado no portal OEE")]


def test_codigo_ate_40_caracteres():
    con = ConexaoOee()
    cad, _ = novo_cadastro(con)
    longo = "D" * 60
    cad.diagnostico(longo, {}, None)
    cad.executar_pendentes()
    assert list(con.registros) == ["D" * 40] and con.registros["D" * 40]["nome"] == longo


def test_update_de_existente_nao_toca_campos_do_portal():
    con = ConexaoOee(registros={DEV: dict(EXISTENTE)})
    cad, logs = novo_cadastro(con)
    cad.diagnostico(DEV, DIAG, "NOVUS/Teste_Alumbra/events")
    cad.executar_pendentes()
    ((sql, params),) = con.dml()                # só o UPDATE: nenhuma inserção
    assert sql == ("UPDATE [OEE].[dbo].[dispositivos_iot] WITH (UPDLOCK, SERIALIZABLE) SET [endereco] = ?, "
                   "[topico] = ?, [ultimo_sinal_em] = ?, [observacao] = ?, [atualizado_em] = ? WHERE [codigo] = ?")
    for coluna in PROIBIDAS_NO_UPDATE:
        assert f"[{coluna}]" not in sql
    agora = utc_sql(T0)
    assert params == ("192.168.0.23", "NOVUS/Teste_Alumbra/events", agora,
                      observacao_diag(DIAG, datetime.fromtimestamp(T0)), agora, DEV)
    reg = con.registros[DEV]
    for coluna in PROIBIDAS_NO_UPDATE:
        assert reg[coluna] == EXISTENTE[coluna], coluna
    assert reg["endereco"] == "192.168.0.23" and reg["ultimo_sinal_em"] == agora
    assert logs == [("INFO", "Dispositivo Teste_Alumbra atualizado no portal OEE")]
    cad.diagnostico(DEV, DIAG, "NOVUS/Teste_Alumbra/events")   # 3 min depois: sem INFO repetido
    cad.executar_pendentes()
    assert len(logs) == 1
    cad.nova_sessao()                                           # nova conexão: 1 INFO de novo
    cad.diagnostico(DEV, DIAG, "NOVUS/Teste_Alumbra/events")
    cad.executar_pendentes()
    assert [m for _, m in logs].count("Dispositivo Teste_Alumbra atualizado no portal OEE") == 2


def test_update_sem_ip_nao_mexe_no_endereco():
    con = ConexaoOee(registros={DEV: dict(EXISTENTE)})
    cad, _ = novo_cadastro(con)
    cad.diagnostico(DEV, {"sn": "1"}, "NOVUS/Teste_Alumbra/events")
    cad.executar_pendentes()
    ((sql, _),) = con.dml()
    assert "[endereco]" not in sql and con.registros[DEV]["endereco"] == "1.1.1.1"


def test_sem_coluna_unidade_nao_grava_unidade():
    con = ConexaoOee(colunas=tuple(c for c in COLUNAS_OEE if c != "unidade"))
    cad, _ = novo_cadastro(con)
    cad.diagnostico(DEV, DIAG, None)
    cad.executar_pendentes()
    ins = con.dml()[-1][0]
    assert ins.startswith("INSERT") and "unidade" not in ins and "unidade" not in con.registros[DEV]


# ============================================================================================ ultimo_sinal_em


def test_sinal_das_leituras_no_maximo_1_por_minuto():
    con = ConexaoOee(registros={DEV: dict(EXISTENTE)})
    relogio = Relogio(T0)
    cad, _ = novo_cadastro(con, relogio=relogio)
    for t in (0, 10, 59.9, 60, 61, 125):
        relogio.ir(T0 + t)
        cad.sinal(DEV)
        cad.sinal("Outro")                      # cada device tem o seu limite
    cad.executar_pendentes()
    updates = [(s, p) for s, p in con.dml() if p[-1] == DEV]
    assert len(updates) == 3                    # T0, T0+60, T0+125
    assert all(s == "UPDATE [OEE].[dbo].[dispositivos_iot] SET [ultimo_sinal_em] = ?, [atualizado_em] = ? "
                    "WHERE [codigo] = ?" for s, _ in updates)
    assert updates[1][1] == (utc_sql(T0 + 60), utc_sql(T0 + 60), DEV)
    assert len([p for _, p in con.dml() if p[-1] == "Outro"]) == 3   # sem registro: UPDATE sem efeito, sem INSERT
    assert "Outro" not in con.registros and con.registros[DEV]["maquina_id"] == 42


# ============================================================================================ falhas


def test_falha_vira_aviso_limitado_e_nao_levanta():
    con = ConexaoOee()
    con.falha = ErroOdbc("42000", "[42000] The UPDATE permission was denied (simulado)")
    relogio = Relogio(T0)
    cad, logs = novo_cadastro(con, relogio=relogio)
    for t in (0, 180, 360):                     # diag a cada 3 min
        relogio.ir(T0 + t)
        cad.diagnostico(DEV, DIAG, None)
        cad.executar_pendentes()
    avisos = [m for n, m in logs if n == "AVISO"]
    assert len(avisos) == 1 and "Teste_Alumbra" in avisos[0] and "portal OEE" in avisos[0]
    assert SENHA_FALSA not in avisos[0]
    relogio.ir(T0 + 601)                        # passou 10 min: avisa de novo
    cad.diagnostico(DEV, DIAG, None)
    cad.executar_pendentes()
    assert len([m for n, m in logs if n == "AVISO"]) == 2
    assert con.rollbacks >= 1


def test_queda_de_conexao_reconecta_e_detecta_de_novo():
    caiu, nova = ConexaoOee(), ConexaoOee()
    caiu.falha = ErroOdbc("08S01", "[08S01] Communication link failure")
    cad, logs = novo_cadastro([caiu, nova])
    cad.diagnostico(DEV, DIAG, None)
    cad.executar_pendentes()
    assert caiu.fechada and any(n == "AVISO" for n, _ in logs)
    cad.diagnostico(DEV, DIAG, None)
    cad.executar_pendentes()
    assert any(s == "SELECT DB_ID(?)" for s, _ in nova.consultas) and DEV in nova.registros


def test_falha_ao_conectar_nao_levanta():
    def conectar(cfg):
        raise ErroOdbc("08001", "[08001] servidor inacessível (simulado)")

    logs = []
    cad = CadastroOee(_sql_cfg(), OeeConfig(), lambda n, m: logs.append((n, m)), conectar=conectar,
                      relogio=Relogio(T0), iniciar_thread=False)
    cad.diagnostico(DEV, DIAG, None)
    cad.executar_pendentes()
    assert [n for n, _ in logs] == ["AVISO"]


# ============================================================================================ pipeline


@pytest.fixture
def montar_pipeline(tmp_path):
    criados = []

    def _montar(con_oee: ConexaoOee):
        cfg = Config()
        cfg.sqlserver = _sql_cfg()
        cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
        cfg.gravacao.pasta_logs = str(tmp_path / "logs")
        cfg.gravacao.intervalo_gravacao_segundos = 0
        cfg.dispositivos.diagnostico_minutos = 0
        conexao = ConexaoIot()
        relogio = Relogio(T0)
        col = Coletor()
        cad = CadastroOee(cfg.sqlserver, cfg.oee, col.kwargs()["ao_log"], conectar=lambda c: con_oee,
                          relogio=relogio)

        def fabrica(sql_cfg, config, logar):
            return SinkSqlServer(sql_cfg, config.gravacao, logar, conectar=lambda _c: conexao, dormir=lambda s: None)

        p = Pipeline(cfg, relogio=relogio, fabrica_cliente=ClienteFalso, sessao_persistente=False, pasta=tmp_path,
                     fabrica_sink=fabrica, cadastro_oee=cad, **col.kwargs())
        p.conectar("127.0.0.1", 1883, f"NOVUS/{DEV}/#", 1).result(5)
        criados.append(p)
        return p, ClienteFalso.ultimo, relogio, col, conexao

    yield _montar
    for p in criados:
        p.encerrar(5)


def test_pipeline_diag_cadastra_e_leitura_renova_sinal(montar_pipeline):
    con = ConexaoOee()
    p, cli, relogio, col, conexao = montar_pipeline(con)
    cli.entregar(f"NOVUS/{DEV}/ack/command", resposta_diag(1589326517))
    assert esperar(lambda: DEV in con.registros)
    assert con.registros[DEV]["topico"] == f"NOVUS/{DEV}/events"      # tópico de dados, não o do ack
    assert con.registros[DEV]["endereco"] == "192.168.0.23"
    relogio.ir(T0 + 61)
    cli.entregar(f"NOVUS/{DEV}/events", novus(1, ts=int(T0)))
    assert esperar(lambda: conexao.gravado)
    assert esperar(lambda: any(s.startswith("UPDATE [OEE].[dbo].[dispositivos_iot] SET [ultimo_sinal_em]")
                               for s, _ in con.dml()))
    assert col.com("Dispositivo Teste_Alumbra cadastrado no portal OEE", "INFO")


def test_pipeline_falha_no_oee_nao_derruba_iot001_nem_conta_erro(montar_pipeline):
    con = ConexaoOee()
    con.falha = ErroOdbc("42000", "[42000] permissão negada (simulado)")
    p, cli, relogio, col, conexao = montar_pipeline(con)
    cli.entregar(f"NOVUS/{DEV}/ack/command", resposta_diag(1589326517))
    cli.entregar(f"NOVUS/{DEV}/events", novus(1, ts=int(T0)))
    assert esperar(lambda: conexao.gravado)
    assert esperar(lambda: col.com("portal OEE", "AVISO"))
    p.desconectar().result(5)
    assert p.contadores == (2, 1, 0) and col.erros == []


def test_pipeline_dispositivo_visto_so_com_leitura(montar_pipeline):
    con = ConexaoOee(banco=False)
    p, cli, relogio, col, conexao = montar_pipeline(con)
    cli.entregar(f"NOVUS/{DEV}/events", EVENTO_MANUAL)
    cli.entregar(f"NOVUS/{DEV}/ack/command", resposta_diag(1589326517))
    assert esperar(lambda: col.diags and col.com("Evento chd1"))
    assert col.vistos == []                     # nem evento nem diagnóstico mexem no "Último envio"
    cli.entregar(f"NOVUS/{DEV}/events", novus(1, ts=int(T0)))
    assert esperar(lambda: col.vistos)
    assert [v[:2] for v in col.vistos] == [(DEV, f"NOVUS/{DEV}/events")]
    p.desconectar().result(5)
    assert col.erros == []
    assert oee.INTERVALO_SINAL == 60.0
