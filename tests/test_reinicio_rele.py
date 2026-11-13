"""Reiniciar pelo relé (saída digital outN do DigiRail), diagnóstico de 1 min sem duplicar e lista do cadastro OEE.

Cliente MQTT falso (grava as publicações), relógio injetado e cadastro OEE com conexão falsa: sem esperas reais além
do laço de 0,1 s da thread de processamento.
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone

import pytest

from app.config import Config, ReinicioConfig, SqlServerConfig
from app.core import digirail
from app.core.oee_dispositivos import CadastroOee, item_lista, montar_select_lista
from app.core.pipeline import Pipeline, tempo_texto
from app.core.sink_sqlserver import SinkSqlServer
from tests.test_amostragem import ClienteFalso, Relogio, esperar, novus
from tests.test_digirail import (DEV, T_ACK_CMD, T_CMD, T_DADOS, Coletor, inicio,  # noqa: F401
                                 resposta_diag)
from tests.test_sink import ConexaoFalsa

ASSINATURA = f"NOVUS/{DEV}/#"
CMD_REAL = "NOÏUS/Teste_Alumbra/command"
ACK_REAL = "NOÏUS/Teste_Alumbra/ack/command"


def ack_output(ts: int, erro: int = 0, saida: int = 1) -> dict:
    return {"pid": 51584019, "device_id": DEV, "timestamp": ts,
            "reported": {"output": {"error": erro, f"out{saida}": 1}}}


def pedido(ts: int, saida: int, valor: int) -> str:
    return json.dumps({"timestamp": ts, "desired": {"output": {f"out{saida}": valor}}}, separators=(",", ":"))


class ColetorR(Coletor):
    def __init__(self) -> None:
        super().__init__()
        self.etapas: list[tuple[str, str]] = []
        self.oee: list[list] = []

    def kwargs(self) -> dict:
        return {**super().kwargs(), "ao_reinicio_etapa": lambda d, e: self.etapas.append((d, e)),
                "ao_dispositivos_oee": lambda j: self.oee.append(json.loads(j))}

    def reinicio(self) -> list[tuple]:
        return [r for r in self.respostas if r[1] == "reiniciar"]


@pytest.fixture
def montar(tmp_path, inicio):
    criados: list[Pipeline] = []

    def _montar(saida: int = 1, diag_min: float = 0, pulso: float = 5, timeout: float = 180,
                cadastro: CadastroOee | None = None, sql: SqlServerConfig | None = None):
        cfg = Config()
        cfg.sqlserver = sql or SqlServerConfig(servidor="FALSO", banco="MQTT")
        cfg.gravacao.pasta_contingencia = str(tmp_path / "contingencia")
        cfg.gravacao.pasta_logs = str(tmp_path / "logs")
        cfg.gravacao.intervalo_gravacao_segundos = 0
        cfg.dispositivos.diagnostico_minutos = diag_min
        cfg.reinicio = ReinicioConfig(saida=saida, pulso_segundos=pulso, timeout_volta_segundos=timeout)
        conexao = ConexaoFalsa()
        relogio = Relogio(inicio)
        col = ColetorR()

        def fabrica(sql_cfg, config, logar):
            return SinkSqlServer(sql_cfg, config.gravacao, logar, conectar=lambda _c: conexao, dormir=lambda s: None)

        p = Pipeline(cfg, relogio=relogio, fabrica_cliente=ClienteFalso, sessao_persistente=False, pasta=tmp_path,
                     fabrica_sink=fabrica, cadastro_oee=cadastro, **col.kwargs())
        p.conectar("127.0.0.1", 1883, ASSINATURA, 1).result(5)
        criados.append(p)
        return p, ClienteFalso.ultimo, relogio, col

    yield _montar
    for p in criados:
        p.encerrar(5)


def _saidas(cli) -> list[tuple[str, dict]]:
    """Publicações de output: (tópico, {"outN": v}) na ordem."""
    return [(t, json.loads(pl)["desired"]["output"]) for t, pl, _q in cli.publicados
            if "output" in json.loads(pl).get("desired", {})]


def _categorias(col) -> list[str]:
    return [json.loads(e)["categoria"] for e in col.erros]


# =========================================================================================== funções puras


def test_funcoes_puras_output():
    assert digirail.desejado_output(1, 1) == {"out1": 1} and digirail.desejado_output(2, 0) == {"out2": 0}
    assert digirail.payload_comando(7, "output", {"out1": 1}) == '{"timestamp":7,"desired":{"output":{"out1":1}}}'
    assert digirail.validar_saida("2") == 2
    for ruim in (0, 3, "x", None, True):
        with pytest.raises(ValueError):
            digirail.validar_saida(ruim)
    assert digirail.mensagem_output(0, 1) == (True, "Saída 1 acionada.")
    assert digirail.mensagem_output(1, 1) == (False, "O aparelho recusou o comando da saída 1 (erro 1: parâmetro "
                                                     "fora da faixa).")
    assert digirail.mensagem_output(2, 2)[1].endswith("(erro 2: parâmetro desconhecido).")
    assert tempo_texto(180) == "3 min" and tempo_texto(42) == "42 s" and tempo_texto(90) == "1 min 30 s"
    assert tempo_texto(3600) == "1 h" and tempo_texto(3720) == "1 h 2 min"


def test_config_reinicio_e_diag_1_min():
    cfg = Config()
    assert cfg.reinicio == ReinicioConfig(saida=1, pulso_segundos=5, timeout_volta_segundos=180)
    assert cfg.dispositivos.diagnostico_minutos == 1


# =========================================================================================== reinício


def test_reinicio_com_sucesso(montar, inicio):
    p, cli, relogio, col = montar()
    t0 = inicio + 1
    relogio.ir(t0)
    p.reiniciar_dispositivo(DEV).result(5)
    assert cli.publicados == [(T_CMD, pedido(int(t0), 1, 1), 1)]
    assert col.etapas == [(DEV, "Relé acionado (saída 1)")]
    relogio.ir(t0 + 0.5)
    cli.entregar(T_ACK_CMD, ack_output(int(t0)))          # ack antes de 1 s: normal, ainda não caiu
    assert esperar(lambda: p.contadores[0] == 1)
    relogio.ir(t0 + 5)
    assert esperar(lambda: len(col.etapas) == 2)
    assert col.etapas[1] == (DEV, "Aguardando o aparelho voltar…")
    assert col.reinicio() == []
    relogio.ir(t0 + 42)
    cli.entregar(T_DADOS, novus(51584019, ts=int(t0 + 42)))   # voltou
    assert esperar(lambda: col.reinicio())
    assert col.reinicio() == [(DEV, "reiniciar", True, "Reiniciado: voltou em 42 s")]
    assert _saidas(cli) == [(T_CMD, {"out1": 1}), (T_CMD, {"out1": 0})]
    assert col.com("Reiniciado: voltou em 42 s", "INFO")
    assert p.contadores[2] == 0 and col.erros == []
    # terminou: um novo reinício é aceito
    p.reiniciar_dispositivo(DEV).result(5)
    assert len(_saidas(cli)) == 3


def test_aparelho_continua_online_falha_do_rele(montar, inicio):
    p, cli, relogio, col = montar()
    t0 = inicio + 1
    relogio.ir(t0)
    p.reiniciar_dispositivo(DEV).result(5)
    relogio.ir(t0 + 2)
    cli.entregar(T_DADOS, novus(51584019, ts=int(t0 + 2)))    # continua mandando dados
    assert esperar(lambda: p.contadores[0] == 1)
    time.sleep(0.15)
    assert col.reinicio() == []                               # decide só no fim do pulso
    relogio.ir(t0 + 5)
    assert esperar(lambda: col.reinicio())
    assert col.reinicio() == [(DEV, "reiniciar", False,
                               "A saída 1 ligou mas o aparelho não reiniciou: confira a ligação do relé")]
    assert _saidas(cli) == [(T_CMD, {"out1": 1}), (T_CMD, {"out1": 0})]
    assert col.etapas == [(DEV, "Relé acionado (saída 1)")]   # nunca "Aguardando"
    assert p.contadores[2] == 0


def test_ack_do_output_depois_de_1s_conta_como_online(montar, inicio):
    p, cli, relogio, col = montar()
    t0 = inicio + 1
    relogio.ir(t0)
    p.reiniciar_dispositivo(DEV).result(5)
    relogio.ir(t0 + 1.5)
    cli.entregar(T_ACK_CMD, ack_output(int(t0)))
    assert esperar(lambda: p.contadores[0] == 1)
    relogio.ir(t0 + 5)
    assert esperar(lambda: col.reinicio())
    assert col.reinicio()[0][2] is False and "não reiniciou" in col.reinicio()[0][3]


def test_ack_com_erro_2_falha_imediata(montar, inicio):
    p, cli, relogio, col = montar()
    t0 = inicio + 1
    relogio.ir(t0)
    p.reiniciar_dispositivo(DEV).result(5)
    cli.entregar(T_ACK_CMD, ack_output(int(t0), erro=2))
    assert esperar(lambda: col.reinicio())
    assert col.reinicio() == [(DEV, "reiniciar", False,
                               "O aparelho recusou o comando da saída 1 (erro 2: parâmetro desconhecido).")]
    relogio.ir(t0 + 10)
    time.sleep(0.2)
    assert col.etapas == [(DEV, "Relé acionado (saída 1)")]   # acabou: nada de "Aguardando"
    assert len(col.reinicio()) == 1 and p.contadores[2] == 0


def test_timeout_falha_com_erro_e_volta_tardia_desliga_a_saida(montar, inicio):
    p, cli, relogio, col = montar()
    t0 = inicio + 1
    relogio.ir(t0)
    p.reiniciar_dispositivo(DEV).result(5)
    relogio.ir(t0 + 5)
    assert esperar(lambda: len(col.etapas) == 2)
    relogio.ir(t0 + 179)
    time.sleep(0.15)
    assert col.reinicio() == []
    relogio.ir(t0 + 180)
    assert esperar(lambda: col.reinicio())
    assert col.reinicio() == [(DEV, "reiniciar", False, "Não voltou em 3 min")]
    assert esperar(lambda: p.contadores[2] == 1) and _categorias(col) == ["comando"]
    registro = json.loads(col.erros[0])
    assert DEV in registro["mensagem"] and registro["topico"] == T_CMD
    assert _saidas(cli) == [(T_CMD, {"out1": 1})]
    # volta depois do prazo: desliga a saída, sem nova resposta nem erro
    relogio.ir(t0 + 300)
    cli.entregar(T_DADOS, novus(51584019, ts=int(t0 + 300)))
    assert esperar(lambda: len(_saidas(cli)) == 2)
    assert _saidas(cli)[1] == (T_CMD, {"out1": 0})
    assert col.com("fora do prazo", "INFO")
    assert len(col.reinicio()) == 1 and p.contadores[2] == 1
    # a segunda mensagem não publica de novo
    cli.entregar(T_DADOS, novus(51584019, ts=int(t0 + 310)))
    assert esperar(lambda: p.contadores[0] == 2)
    time.sleep(0.15)
    assert len(_saidas(cli)) == 2


def test_timeout_configurado_muda_o_texto(montar, inicio):
    p, cli, relogio, col = montar(timeout=90)
    relogio.ir(inicio + 1)
    p.reiniciar_dispositivo(DEV).result(5)
    relogio.ir(inicio + 6)
    assert esperar(lambda: len(col.etapas) == 2)
    relogio.ir(inicio + 91)
    assert esperar(lambda: col.reinicio())
    assert col.reinicio()[0][3] == "Não voltou em 1 min 30 s"


def test_reinicio_duplicado_e_rejeitado(montar, inicio):
    p, cli, relogio, col = montar()
    relogio.ir(inicio + 1)
    p.reiniciar_dispositivo(DEV).result(5)
    p.reiniciar_dispositivo(DEV).result(5)
    assert col.reinicio() == [(DEV, "reiniciar", False, "Já há um reinício em andamento")]
    assert len(_saidas(cli)) == 1
    relogio.ir(inicio + 6)                                    # aguardando: ainda em andamento
    assert esperar(lambda: len(col.etapas) == 2)
    p.reiniciar_dispositivo(DEV).result(5)
    assert len(col.reinicio()) == 2 and len(_saidas(cli)) == 1


def test_desconectar_cancela_sem_erro(montar, inicio):
    p, cli, relogio, col = montar()
    relogio.ir(inicio + 1)
    p.reiniciar_dispositivo(DEV).result(5)
    relogio.ir(inicio + 6)
    assert esperar(lambda: len(col.etapas) == 2)
    p.desconectar().result(5)
    assert col.reinicio() == [(DEV, "reiniciar", False, "Reinício cancelado: conexão com o broker encerrada.")]
    assert _saidas(cli) == [(T_CMD, {"out1": 1}), (T_CMD, {"out1": 0})]   # segurança
    assert p.contadores[2] == 0 and col.erros == []
    relogio.ir(inicio + 400)
    time.sleep(0.15)
    assert len(col.reinicio()) == 1


def test_sem_conexao_responde_falha_sem_erro(montar, inicio):
    p, cli, relogio, col = montar()
    p.desconectar().result(5)
    p.reiniciar_dispositivo(DEV).result(5)
    assert col.reinicio() == [(DEV, "reiniciar", False, "Não foi possível acionar a saída 1: não conectado ao "
                                                        "broker.")]
    assert p.contadores[2] == 0


def test_topico_noius_salvo_e_usado(montar, inicio):
    p, cli, relogio, col = montar()
    p.salvar_topicos_dispositivo(DEV, {"events": T_DADOS, "config": f"NOVUS/{DEV}/config",
                                       "ack_config": f"NOVUS/{DEV}/ack/config", "command": CMD_REAL,
                                       "ack_command": ACK_REAL}).result(5)
    relogio.ir(inicio + 1)
    p.reiniciar_dispositivo(DEV).result(5)
    assert _saidas(cli) == [(CMD_REAL, {"out1": 1})]
    assert ACK_REAL in cli.assinaturas_extras
    cli.entregar(ACK_REAL, {"timestamp": int(inicio + 1), "reported": {"output": {"error": 1}}})  # sem device_id
    assert esperar(lambda: col.reinicio())
    assert col.reinicio() == [(DEV, "reiniciar", False,
                               "O aparelho recusou o comando da saída 1 (erro 1: parâmetro fora da faixa).")]


def test_saida_2_usa_out2(montar, inicio):
    p, cli, relogio, col = montar(saida=2)
    t0 = inicio + 1
    relogio.ir(t0)
    p.reiniciar_dispositivo(DEV).result(5)
    assert cli.publicados[0] == (T_CMD, pedido(int(t0), 2, 1), 1)
    assert col.etapas == [(DEV, "Relé acionado (saída 2)")]
    relogio.ir(t0 + 5)
    assert esperar(lambda: len(col.etapas) == 2)
    relogio.ir(t0 + 30)
    cli.entregar(T_ACK_CMD, resposta_diag(1))                # diag periódico do próprio aparelho = voltou
    assert esperar(lambda: col.reinicio())
    assert col.reinicio() == [(DEV, "reiniciar", True, "Reiniciado: voltou em 30 s")]
    assert _saidas(cli) == [(T_CMD, {"out2": 1}), (T_CMD, {"out2": 0})]


def test_saida_invalida_na_config(montar, inicio):
    p, cli, relogio, col = montar(saida=3)
    p.reiniciar_dispositivo(DEV).result(5)
    assert cli.publicados == [] and col.reinicio()[0][2] is False and "[reinicio] saida" in col.reinicio()[0][3]


def test_sem_diag_automatico_durante_o_reinicio(montar, inicio):
    p, cli, relogio, col = montar(diag_min=1)
    relogio.ir(inicio + 1)
    p.reiniciar_dispositivo(DEV).result(5)
    relogio.ir(inicio + 6)
    assert esperar(lambda: len(col.etapas) == 2)
    relogio.ir(inicio + 50)
    cli.entregar(T_DADOS, novus(51584019, ts=int(inicio + 50)))   # volta: 1ª vez nesta conexão
    assert esperar(lambda: col.reinicio())
    # o diag da 1ª vez sai depois que o reinício terminou (a volta é tratada antes)
    assert esperar(lambda: any("diag" in pl for _t, pl, _q in cli.publicados))


# =========================================================================================== diagnóstico 1 min


def _diags(cli) -> list[int]:
    return [json.loads(pl)["timestamp"] for _t, pl, _q in cli.publicados if '"diag"' in pl]


def test_diag_a_cada_1_min_sem_duplicar_com_os_apontamentos(montar, inicio):
    p, cli, relogio, col = montar(diag_min=1)
    t0 = inicio + 1
    relogio.ir(t0)
    cli.entregar(T_DADOS, novus(51584019, ts=int(t0)))
    assert esperar(lambda: len(_diags(cli)) == 1)              # 1ª vez
    cli.entregar(T_ACK_CMD, resposta_diag(_diags(cli)[0]))
    assert esperar(lambda: col.diags)
    relogio.ir(t0 + 30)
    time.sleep(0.15)
    assert len(_diags(cli)) == 1
    relogio.ir(t0 + 60)
    assert esperar(lambda: len(_diags(cli)) == 2)              # periódico de 1 min
    # apontamentos pedem 5 s depois: espera a resposta do automático em vez de publicar outro
    relogio.ir(t0 + 65)
    resultado: dict = {}
    th = threading.Thread(target=lambda: resultado.update(p.diagnosticar_dispositivos([DEV], 5)))
    th.start()
    time.sleep(0.2)
    assert len(_diags(cli)) == 2
    cli.entregar(T_ACK_CMD, resposta_diag(_diags(cli)[1]))
    th.join(5)
    assert resultado == {DEV: True}
    # apontamentos pedem em t0+105 (publica); o periódico de t0+120 é dispensado (diag há só 15 s)
    relogio.ir(t0 + 105)
    th = threading.Thread(target=lambda: resultado.update(p.diagnosticar_dispositivos([DEV], 5)))
    th.start()
    assert esperar(lambda: len(_diags(cli)) == 3)
    cli.entregar(T_ACK_CMD, resposta_diag(_diags(cli)[2]))
    th.join(5)
    relogio.ir(t0 + 119)
    time.sleep(0.15)
    relogio.ir(t0 + 120)
    time.sleep(0.2)
    assert len(_diags(cli)) == 3
    relogio.ir(t0 + 180)                                       # próximo periódico normal
    assert esperar(lambda: len(_diags(cli)) == 4)
    assert p.contadores[2] == 0


# =========================================================================================== lista do cadastro OEE

SQL = SqlServerConfig(servidor="FALSO", banco="MQTT", usuario="mqtt", senha="senha-de-teste")
SINAL = datetime(2026, 11, 13, 15, 0, 7)   # UTC ingênuo, como na coluna datetime


def _local(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).astimezone().strftime("%Y-%m-%dT%H:%M:%S")


class CursorOee:
    def __init__(self, con: "ConexaoOee") -> None:
        self.con = con
        self._res: list = []

    def execute(self, sql: str, *params):
        self.con.consultas.append((sql, params))
        if sql == "SELECT DB_ID(?)":
            self._res = [(None if not self.con.banco else 7,)]
        elif sql.startswith("SELECT OBJECT_ID"):
            nome = params[0]
            existe = ("dispositivos_iot" in nome) or ("maquinas" in nome and self.con.maquinas)
            self._res = [(123 if existe else None,)]
        elif "INFORMATION_SCHEMA.COLUMNS" in sql:
            self._res = [(c,) for c in (self.con.cols_disp if params[1] == "dispositivos_iot"
                                        else self.con.cols_maq)]
        elif "HAS_PERMS_BY_NAME" in sql:
            self._res = [(1, 1)]
        elif sql.startswith("SELECT d."):
            self.con.selects.append(sql)
            self._res = list(self.con.linhas)
        else:
            raise AssertionError(sql)
        return self

    def fetchone(self):
        return self._res[0] if self._res else None

    def fetchall(self):
        return self._res

    def close(self):
        pass


class ConexaoOee:
    def __init__(self, maquinas: bool = True, banco: bool = True) -> None:
        self.maquinas = maquinas
        self.banco = banco
        self.cols_disp = ["id", "codigo", "nome", "tipo", "protocolo", "endereco", "porta", "topico", "maquina_id",
                          "ultimo_sinal_em", "observacao", "ativo", "unidade"]
        self.cols_maq = ["id", "codigo", "nome", "ativo"]
        self.consultas: list = []
        self.selects: list[str] = []
        m = (lambda c, n: (c, n)) if maquinas else (lambda c, n: (None, None))
        self.linhas = [
            ("Teste_Alumbra", "Teste_Alumbra", "172.16.30.140", "NOVUS/Teste_Alumbra/events", SINAL, "mqtt",
             "manutencao", *m("PRS-01", "Prensa 1")),
            ("Sem_Maquina", "Sem máquina", None, None, None, "mqtt", "tecnologia", None, None),
        ]

    def cursor(self):
        return CursorOee(self)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def _listar(con) -> tuple[list, list]:
    logs: list = []
    cad = CadastroOee(SQL, None, lambda n, m: logs.append((n, m)), conectar=lambda c: con, iniciar_thread=False)
    recebidas: list = []
    cad.listar(recebidas.append)
    cad.executar_pendentes()
    return recebidas, logs


def test_lista_oee_com_maquinas():
    con = ConexaoOee()
    recebidas, logs = _listar(con)
    assert recebidas == [[
        {"codigo": "Teste_Alumbra", "nome": "Teste_Alumbra", "endereco": "172.16.30.140",
         "topico": "NOVUS/Teste_Alumbra/events", "ultimo_sinal_em": _local(SINAL), "protocolo": "mqtt",
         "maquina": "PRS-01 – Prensa 1", "unidade": "manutencao"},
        {"codigo": "Sem_Maquina", "nome": "Sem máquina", "endereco": None, "topico": None, "ultimo_sinal_em": None,
         "protocolo": "mqtt", "maquina": None, "unidade": "tecnologia"}]]
    (sql,) = con.selects
    assert ("LEFT JOIN [OEE].[dbo].[maquinas] AS m ON m.[id] = d.[maquina_id]" in sql
            and "WHERE d.[ativo] = 1" in sql and sql.startswith("SELECT d.[codigo], d.[nome]"))
    assert "m.[codigo] AS [maquina_codigo], m.[nome] AS [maquina_nome]" in sql
    assert not any(s.split()[0] in ("INSERT", "UPDATE", "DELETE") for s, _ in con.consultas)   # só SELECT
    assert logs == []


def test_lista_oee_sem_tabela_maquinas():
    con = ConexaoOee(maquinas=False)
    recebidas, _ = _listar(con)
    (sql,) = con.selects
    assert "JOIN" not in sql and "NULL AS [maquina_codigo], NULL AS [maquina_nome]" in sql
    assert [i["maquina"] for i in recebidas[0]] == [None, None]


def test_lista_oee_sem_coluna_unidade_nem_ativo():
    con = ConexaoOee()
    con.cols_disp = [c for c in con.cols_disp if c not in ("unidade", "ativo")]
    _listar(con)
    (sql,) = con.selects
    assert "NULL AS [unidade]" in sql and "WHERE" not in sql


def test_sem_oee_nao_chama_nem_loga():
    con = ConexaoOee(banco=False)
    recebidas, logs = _listar(con)
    assert recebidas == [] and logs == [] and con.selects == []


def test_sem_senha_nao_lista():
    cad = CadastroOee(SqlServerConfig(servidor="FALSO", banco="MQTT"), None, conectar=lambda c: ConexaoOee(),
                      iniciar_thread=False)
    recebidas: list = []
    cad.listar(recebidas.append)
    cad.executar_pendentes()
    assert recebidas == []


def test_item_lista_maquina_parcial_e_texto_iso():
    base = ("X", "X", None, None, "2026-11-13T15:00:07", None, None)
    assert item_lista(base + ("PRS-01", None))["maquina"] == "PRS-01"
    assert item_lista(base + (None, "Prensa"))["maquina"] == "Prensa"
    assert item_lista(base + (None, None))["ultimo_sinal_em"] == _local(SINAL)
    assert montar_select_lista({"alvo": "[OEE].[dbo].[dispositivos_iot]", "colunas": {"codigo"},
                                "maquinas": None, "maq_colunas": set()}).startswith("SELECT d.[codigo], NULL AS")


def test_pipeline_emite_json_ao_conectar_e_a_cada_60_s(montar, inicio):
    con = ConexaoOee()
    relogio_cad = Relogio(inicio)
    cad = CadastroOee(SQL, None, conectar=lambda c: con, relogio=relogio_cad)
    p, cli, relogio, col = montar(cadastro=cad, sql=SQL)
    assert esperar(lambda: len(col.oee) == 1)                  # logo ao conectar
    assert col.oee[0][0]["maquina"] == "PRS-01 – Prensa 1"
    assert col.oee[0][0]["ultimo_sinal_em"] == _local(SINAL)
    assert set(col.oee[0][0]) == {"codigo", "nome", "endereco", "topico", "ultimo_sinal_em", "protocolo",
                                  "maquina", "unidade"}
    relogio.ir(inicio + 30)
    time.sleep(0.2)
    assert len(col.oee) == 1
    relogio.ir(inicio + 60)
    assert esperar(lambda: len(col.oee) == 2)
    assert col.erros == [] and not col.com("", "ERRO")


def test_pipeline_sem_oee_nao_emite(montar, inicio):
    con = ConexaoOee(banco=False)
    cad = CadastroOee(SQL, None, conectar=lambda c: con)
    p, cli, relogio, col = montar(cadastro=cad, sql=SQL)
    assert esperar(lambda: any(s == "SELECT DB_ID(?)" for s, _ in con.consultas))
    relogio.ir(inicio + 61)
    time.sleep(0.3)
    assert col.oee == [] and col.erros == []
    assert not [m for n, m in col.logs if "OEE" in m and n in ("AVISO", "ERRO")]


def test_bridge_tem_reinicio_e_sinais():
    from app.bridge import PipelineBridge
    assert hasattr(PipelineBridge, "reinicio_etapa") and hasattr(PipelineBridge, "dispositivos_oee")
    assert callable(getattr(PipelineBridge, "reiniciar_dispositivo"))
