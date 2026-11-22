"""Apontamentos reais pelo IoT (app/core/apontamentos_oee.py): funções puras e a classe com um banco falso.

Nada aqui acessa um SQL Server de verdade: ``BancoFalso`` imita, em memória, só as consultas que o módulo faz.
"""
from __future__ import annotations

import copy
import re
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app.config import ApontamentosConfig, Config, OeeConfig, SqlServerConfig, carregar
from app.core import apontamentos_oee as ap
from app.core.apontamentos_oee import (FORA_TURNO, MACRO, MICRO, PRODUZINDO, SEM_IOT, ApontamentosOee, LegendasIot,
                                       Trecho, dividir_por_dia, marcar_sem_resposta, planejar, producoes,
                                       segmentos_continuos, situacao_pela_legenda, status_op)
from app.core.pipeline import Pipeline
from tests.test_amostragem import esperar
from tests.test_sink import ErroOdbc

FUSO = ZoneInfo("America/Sao_Paulo")
SENHA_FALSA = "nao-e-senha-real"   # só para a etapa se considerar configurada (a conexão é falsa)
LEG = LegendasIot(produzindo=2, micro=99, macro=98)
SETUP = 500                        # legenda trocada pelo operador
MICRO_S, MACRO_S = 60, 300


def T(h: int, m: int = 0, s: int = 0, dia: int = 12) -> int:
    """Epoch de 2026-11-<dia> h:m:s em Brasília."""
    return int(datetime(2026, 11, dia, h, m, s, tzinfo=FUSO).timestamp())


HOJE = T(0)
ONTEM = T(0, dia=11)


def seg(prods, ini, agora, micro=MICRO_S, macro=MACRO_S):
    return segmentos_continuos(prods, ini, agora, micro, macro)


def apont(id_, legenda, inicio, fim, iot=True, op=None):
    return {"id": id_, "legenda_id": legenda, "inicio": inicio, "fim": fim,
            "observacao": ap.OBSERVACAO if iot else "Apontado pelo operador", "ordem_producao_id": op}


# ============================================================================================ produções


def test_producao_e_subida_do_contador_e_reset_vira_nova_base():
    leituras = [(0, 5), (10, 5), (20, 6), (30, 6), (40, 8), (50, 0), (60, 1), (70, None), (80, 1), (90, 3)]
    assert producoes(leituras) == [20, 40, 60, 90]


def test_producao_ignora_ordem_e_primeira_leitura_e_so_base():
    assert producoes([(30, 7), (10, 5), (20, 5)]) == [30]
    assert producoes([(10, 5)]) == []
    assert producoes([]) == []


def test_dois_canais_vale_a_producao_de_qualquer_um():
    a = producoes([(0, 1), (10, 2), (20, 2), (30, 2)])
    b = producoes([(0, 9), (10, 9), (20, 9), (30, 10)])
    assert ap.juntar_producoes(a, b) == [10, 30]


# ============================================================================================ segmentos


def test_gap_menor_que_micro_e_produzindo_em_um_so_trecho():
    ini = T(8)
    prods = [ini - 10] + [ini + 30 * k for k in range(1, 10)]
    assert seg(prods, ini, ini + 280) == [(ini, ini + 280, PRODUZINDO)]


def test_gap_entre_micro_e_macro_e_microparada_que_comeca_na_ultima_producao():
    ini = T(8)
    prods = [ini - 10, ini + 20, ini + 120, ini + 150]
    assert seg(prods, ini, ini + 170) == [
        (ini, ini + 20, PRODUZINDO), (ini + 20, ini + 120, MICRO), (ini + 120, ini + 170, PRODUZINDO)]


def test_gap_maior_ou_igual_a_macro_e_macroparada():
    ini = T(8)
    prods = [ini - 10, ini + 20, ini + 320, ini + 330]
    assert seg(prods, ini, ini + 340) == [
        (ini, ini + 20, PRODUZINDO), (ini + 20, ini + 320, MACRO), (ini + 320, ini + 340, PRODUZINDO)]


def test_parada_em_aberto_passa_de_produzindo_para_99_e_depois_98_com_o_mesmo_inicio():
    ini = T(8)
    p = ini + 100
    prods = [ini - 5, ini + 50, p]
    assert seg(prods, ini, p + 30)[-1] == (ini, p + 30, PRODUZINDO)
    assert seg(prods, ini, p + 100)[-1] == (p, p + 100, MICRO)
    assert seg(prods, ini, p + 400)[-1] == (p, p + 400, MACRO)


def test_sem_producao_antes_da_janela_e_macroparada_ate_a_primeira():
    ini = T(8)
    assert seg([], ini, ini + 1000) == [(ini, ini + 1000, MACRO)]
    assert seg([ini + 50, ini + 70], ini, ini + 80) == [(ini, ini + 50, MACRO), (ini + 50, ini + 80, PRODUZINDO)]


def test_producao_no_futuro_relogio_adiantado_e_ignorada():
    ini = T(8)
    assert seg([ini - 5, ini + 9999], ini, ini + 30) == [(ini, ini + 30, PRODUZINDO)]


# ============================================================================================ 97 só pelo diagnóstico
# Regra revista em 13/11/2026: buraco sem linhas na IOT001 é máquina parada (o contador não aumentou), não 97.


def test_funcoes_de_buraco_sem_leitura_sairam_do_modulo():
    assert not hasattr(ap, "buracos_sem_leitura") and not hasattr(ap, "_sem_leitura_em")


def test_buraco_de_30_min_sem_linhas_no_meio_da_producao_e_98_sem_97():
    ini = T(8)
    a, b = ini + 600, ini + 600 + 1800                # sem nenhuma linha na IOT001 por 30 min
    prods = list(range(ini - 10, a + 1, 30)) + list(range(b, ini + 3000, 30))
    agora = ini + 3000
    ultima_antes = max(p for p in prods if p <= a)
    segs = seg(prods, ini, agora)
    assert segs == [(ini, ultima_antes, PRODUZINDO), (ultima_antes, b, MACRO), (b, agora, PRODUZINDO)]
    assert SEM_IOT not in {e for *_, e in segs}


def test_buraco_curto_entre_micro_e_macro_e_99_e_menor_que_micro_e_2():
    ini = T(8)
    p = ini + 100
    assert seg([ini - 10, ini + 30, ini + 60, p, p + 120, p + 150], ini, p + 160) == [
        (ini, p, PRODUZINDO), (p, p + 120, MICRO), (p + 120, p + 160, PRODUZINDO)]       # 2 min sem linhas
    assert seg([ini - 10, ini + 30, ini + 60, p, p + 40, p + 70], ini, p + 80) == [(ini, p + 80, PRODUZINDO)]


def test_buraco_no_fim_aparelho_ainda_fora_continua_parada_e_nao_97():
    ini = T(8)
    p = ini + 100
    assert seg([ini - 5, p], ini, p + 3600)[-1] == (p, p + 3600, MACRO)
    assert seg([], ini, T(9)) == [(ini, T(9), MACRO)]          # nenhuma leitura na janela: parada (98), não 97


def test_buraco_fora_do_turno_e_96_e_dentro_do_turno_e_98():
    from app.core.apontamentos_oee import intervalos_turno, montar_jornada, recortar_fora_turno
    j = montar_jornada(None, "12345", "05:00", "14:48")       # 12/11/2026 é quinta-feira
    turnos = intervalos_turno(j, HOJE, T(16), FUSO)
    # dentro do turno: produz até 9:00, 30 min sem linhas, volta a produzir 9:30
    prods = [HOJE - 10, *range(T(8), T(9) + 1, 30), *range(T(9, 30), T(10) + 1, 30)]
    segs = recortar_fora_turno(seg(prods, HOJE, T(10)), turnos)
    assert (T(9), T(9, 30), MACRO) in segs and SEM_IOT not in {e for *_, e in segs}
    # fora do turno: produz até 15:00, 30 min sem linhas, volta 15:30
    prods = [T(14, 50) - 10, *range(T(14, 50), T(15) + 1, 30), *range(T(15, 30), T(16) + 1, 30)]
    segs = recortar_fora_turno(seg(prods, T(14, 50), T(16)), turnos)
    assert (T(15), T(15, 30), FORA_TURNO) in segs and SEM_IOT not in {e for *_, e in segs}


def test_97_atravessando_a_meia_noite_e_legenda_do_operador_continua():
    a = T(23, dia=11)
    prods = [a - 30, a]
    segs = marcar_sem_resposta(segmentos_continuos(prods, ONTEM, T(2), MICRO_S, MACRO_S), prods, ONTEM, a)
    trechos = dividir_por_dia(segs, FUSO)
    assert Trecho(a, HOJE - 1, SEM_IOT) in trechos and Trecho(HOJE, None, SEM_IOT) in trechos
    legendas = LegendasIot(2, 99, 98, 97)
    assert legendas.todas == {2, 99, 98, 97}
    existentes = [apont(1, SETUP, a, None)]          # o operador trocou o 97 por um motivo
    plano = planejar([t for t in trechos if t.inicio >= a], existentes, ONTEM, T(2), legendas, None, FUSO)
    assert [(x["id"], x["legenda_id"]) for x in plano.atualizar] == [(1, SETUP)]
    assert plano.inserir[0]["inicio"] == HOJE and plano.inserir[0]["legenda_id"] == SETUP


def test_legendas_sem_97_nao_geram_sem_iot():
    legendas = LegendasIot(2, 99, 98)
    assert legendas.todas == {2, 99, 98}
    with pytest.raises(ValueError):
        legendas.de(SEM_IOT)


# ============================================================================================ divisão por dia


def test_virada_da_meia_noite_local_fecha_23_59_59_e_continua_as_00_00_00():
    a = T(23, 50, dia=11)
    agora = T(0, 20)
    trechos = dividir_por_dia([(a, agora, MACRO)], FUSO)
    assert trechos == [Trecho(a, T(23, 59, 59, dia=11), MACRO), Trecho(HOJE, None, MACRO)]
    # o dia é o de Brasília, não o UTC: 21:00 local (00:00 UTC) não quebra
    b = T(20, 0)
    assert dividir_por_dia([(b, T(22, 0), PRODUZINDO)], FUSO) == [Trecho(b, None, PRODUZINDO)]


def test_encadeados_fim_e_proximo_inicio_menos_1s_e_ultimo_em_aberto():
    ini = T(8)
    segs = [(ini, ini + 20, PRODUZINDO), (ini + 20, ini + 120, MICRO), (ini + 120, ini + 170, PRODUZINDO)]
    assert dividir_por_dia(segs, FUSO) == [
        Trecho(ini, ini + 19, PRODUZINDO), Trecho(ini + 20, ini + 119, MICRO), Trecho(ini + 120, None, PRODUZINDO)]


def test_trecho_menor_que_2s_e_absorvido_pelo_anterior_e_mescla_os_iguais():
    ini = T(8)
    segs = [(ini, ini + 100, PRODUZINDO), (ini + 100, ini + 101, MICRO), (ini + 101, ini + 200, PRODUZINDO)]
    assert dividir_por_dia(segs, FUSO) == [Trecho(ini, None, PRODUZINDO)]


def test_trecho_curto_no_inicio_vai_para_o_seguinte_e_respeita_o_dia():
    # 1 s às 00:00:00 (sem anterior no mesmo dia): absorvido pelo seguinte
    segs = [(HOJE - 100, HOJE, PRODUZINDO), (HOJE, HOJE + 1, MACRO), (HOJE + 1, HOJE + 50, PRODUZINDO)]
    assert dividir_por_dia(segs, FUSO) == [Trecho(HOJE - 100, HOJE - 1, PRODUZINDO), Trecho(HOJE, None, PRODUZINDO)]
    # 1 s às 23:59:59: absorvido pelo anterior (mesmo dia), nunca atravessa a meia-noite
    segs = [(HOJE - 100, HOJE - 1, PRODUZINDO), (HOJE - 1, HOJE, MICRO), (HOJE, HOJE + 50, MACRO)]
    assert dividir_por_dia(segs, FUSO) == [Trecho(HOJE - 100, HOJE - 1, PRODUZINDO), Trecho(HOJE, None, MACRO)]
    for t in dividir_por_dia(segs, FUSO):
        assert t.fim is None or t.fim > t.inicio


def test_primeira_producao_1s_depois_do_inicio_da_janela_nao_gera_trecho_invalido():
    ini = T(8)
    trechos = dividir_por_dia(seg([ini + 1, ini + 20, ini + 40], ini, ini + 50), FUSO)
    assert trechos == [Trecho(ini, None, PRODUZINDO)]


# ============================================================================================ plano


def test_plano_sem_existentes_insere_com_a_op_da_maquina():
    d = [Trecho(T(8), T(8, 10) - 1, PRODUZINDO), Trecho(T(8, 10), None, MACRO)]
    plano = planejar(d, [], HOJE, T(9), LEG, 77, FUSO)
    assert plano.inserir == [
        {"legenda_id": 2, "inicio": T(8), "fim": T(8, 10) - 1, "ordem_producao_id": 77},
        {"legenda_id": 98, "inicio": T(8, 10), "fim": None, "ordem_producao_id": 77}]
    assert not plano.atualizar and not plano.excluir
    assert plano.ops_afetadas() == {77}


def test_plano_reaproveita_pelo_mesmo_inicio_e_nao_mexe_no_que_nao_mudou():
    p = T(8, 10)
    existentes = [apont(1, 2, HOJE, None, op=5)]
    d = [Trecho(HOJE, p - 1, PRODUZINDO), Trecho(p, None, MICRO)]
    plano = planejar(d, existentes, HOJE, T(8, 12), LEG, 9, FUSO)
    assert [(x["id"], x["fim"], x["legenda_id"], x["ordem_producao_id"]) for x in plano.atualizar] == [
        (1, p - 1, 2, 5)]                                   # OP do reaproveitado é mantida
    assert plano.inserir == [{"legenda_id": 99, "inicio": p, "fim": None, "ordem_producao_id": 9}]
    # mesma situação de novo: nada a fazer
    existentes = [apont(1, 2, HOJE, p - 1), apont(2, 99, p, None)]
    assert planejar(d, existentes, HOJE, T(8, 12), LEG, 9, FUSO).vazio


def test_plano_parada_em_aberto_passa_de_99_para_98_no_mesmo_apontamento():
    p = T(8, 10)
    existentes = [apont(1, 2, HOJE, p - 1), apont(2, 99, p, None)]
    d = [Trecho(HOJE, p - 1, PRODUZINDO), Trecho(p, None, MACRO)]
    plano = planejar(d, existentes, HOJE, T(9), LEG, None, FUSO)
    assert [(x["id"], x["legenda_id"], x["fim"]) for x in plano.atualizar] == [(2, 98, None)]
    assert not plano.inserir and not plano.excluir


def test_plano_mantem_a_legenda_do_operador_enquanto_continua_parada():
    p = T(8, 10)
    existentes = [apont(1, 2, HOJE, p - 1), apont(2, SETUP, p, None)]
    d = [Trecho(HOJE, p - 1, PRODUZINDO), Trecho(p, None, MACRO)]
    assert planejar(d, existentes, HOJE, T(9), LEG, None, FUSO).vazio
    # voltou a produzir: a parada fecha com a legenda do operador e entra um novo "2"
    t = T(9)
    d = [Trecho(HOJE, p - 1, PRODUZINDO), Trecho(p, t - 1, MACRO), Trecho(t, None, PRODUZINDO)]
    plano = planejar(d, existentes, HOJE, T(9, 1), LEG, None, FUSO)
    assert [(x["id"], x["legenda_id"], x["fim"]) for x in plano.atualizar] == [(2, SETUP, t - 1)]
    assert [x["legenda_id"] for x in plano.inserir] == [2]


def test_plano_trecho_do_operador_que_virou_producao_volta_para_2():
    existentes = [apont(1, SETUP, HOJE, None)]
    plano = planejar([Trecho(HOJE, None, PRODUZINDO)], existentes, HOJE, T(9), LEG, None, FUSO)
    assert [(x["id"], x["legenda_id"]) for x in plano.atualizar] == [(1, 2)]


def test_plano_legenda_do_operador_continua_depois_da_meia_noite():
    a = T(22, dia=11)
    existentes = [apont(1, SETUP, a, None)]
    d = [Trecho(a, HOJE - 1, MACRO), Trecho(HOJE, None, MACRO)]
    plano = planejar(d, existentes, ONTEM, T(1), LEG, 4, FUSO)
    assert [(x["id"], x["legenda_id"], x["fim"]) for x in plano.atualizar] == [(1, SETUP, HOJE - 1)]
    assert plano.inserir == [{"legenda_id": SETUP, "inicio": HOJE, "fim": None, "ordem_producao_id": 4}]


def test_plano_manual_coberto_e_excluido_e_manual_cruzando_o_inicio_e_aparado():
    manual_dentro = apont(10, SETUP, T(8), T(8, 30), iot=False, op=3)
    manual_antes = apont(11, SETUP, T(23, dia=10), None, iot=False)      # em aberto desde anteontem
    d = [Trecho(ONTEM, None, MACRO)]
    plano = planejar(d, [manual_dentro, manual_antes], ONTEM, T(9), LEG, None, FUSO)
    assert [(x["id"], x["manual"]) for x in plano.excluir] == [(10, True)]
    assert [(x["id"], x["fim"], x["manual"]) for x in plano.atualizar] == [(11, ONTEM - 1, True)]
    assert plano.ops_afetadas() == {3}
    assert len(plano.inserir) == 1


def test_plano_iot_em_aberto_de_dias_atras_e_fechado_no_fim_do_dia_dele():
    velho = apont(1, 98, T(10, dia=9), None)
    plano = planejar([Trecho(HOJE, None, MACRO)], [velho], HOJE, T(9), LEG, None, FUSO)
    assert [(x["id"], x["fim"], x["manual"]) for x in plano.atualizar] == [(1, T(23, 59, 59, dia=9), False)]


def test_plano_sobras_do_integrador_sao_reaproveitadas_e_o_excedente_excluido():
    # a reclassificação juntou três apontamentos num só
    existentes = [apont(1, 2, HOJE, T(8) - 1), apont(2, 99, T(8), T(8, 5) - 1), apont(3, 2, T(8, 5), None)]
    plano = planejar([Trecho(HOJE, None, PRODUZINDO)], existentes, HOJE, T(9), LEG, None, FUSO)
    assert [(x["id"], x["fim"]) for x in plano.atualizar] == [(1, None)]
    assert sorted(x["id"] for x in plano.excluir) == [2, 3]
    # e o contrário: um novo trecho reaproveita a sobra (muda o início) em vez de excluir + inserir
    existentes = [apont(1, 2, HOJE, None), apont(7, 98, T(3), T(4))]
    d = [Trecho(HOJE, T(8) - 1, PRODUZINDO), Trecho(T(8), None, MICRO)]
    plano = planejar(d, existentes, HOJE, T(9), LEG, None, FUSO)
    assert [(x["id"], x["inicio"], x["fim"], x["legenda_id"]) for x in plano.atualizar] == [
        (1, HOJE, T(8) - 1, 2), (7, T(8), None, 99)]
    assert not plano.inserir and not plano.excluir


# ============================================================================================ máquina e OP


def test_situacao_pela_legenda():
    assert situacao_pela_legenda("produzindo") == "produzindo"
    for tipo in ("parada", "planejada", "desligada", ""):
        assert situacao_pela_legenda(tipo) == "parada"
    assert situacao_pela_legenda(None) is None


@pytest.mark.parametrize("args, esperado", [
    (("20261110", "", 0, "produzindo", True), "fechada"),
    (("", "U", 0, "produzindo", True), "parada"),
    (("", "", 0, "parada", True), "parada"),
    (("", "", 0, "", True), "parada"),               # em aberto sem legenda
    (("", "", 0, "produzindo", True), "andamento"),
    (("", "", 5, None, False), "andamento"),
    (("", "", 0, None, True), "andamento"),
    ((" ", " ", 0, None, False), "aberta"),
])
def test_status_op_regra_do_portal(args, esperado):
    assert status_op(*args) == esperado


# ============================================================================================ banco falso


class Cursor:
    def __init__(self, banco: "BancoFalso") -> None:
        self.b = banco
        self._linhas: list = []
        self.rowcount = -1

    def execute(self, sql: str, *params):
        self.b.comandos.append((sql, params))
        for pedaco, exc in self.b.falhas:
            if pedaco in sql and (exc.args[0] != "fk" or params[0] in self.b.fk_ids):
                raise ErroOdbc("23000", "conflito com FOREIGN KEY") if exc.args[0] == "fk" else exc
        self._linhas = self.b.responder(sql, params)
        return self

    def fetchone(self):
        return self._linhas[0] if self._linhas else None

    def fetchall(self):
        return list(self._linhas)

    def close(self):
        pass


# coluna do SQL → chave do registro falso
CAMPOS = {"inicio": "inicio", "fim": "fim", "legenda_id": "legenda", "ciclo_medio": "ciclo", "ciclos": "ciclos",
          "iot_dispositivo_id": "dev", "iot_canal": "canal", "atualizado_em": "em", "atualizado_por_id": "por"}


def _dt(t):
    return None if t is None else datetime.fromtimestamp(t, timezone.utc).replace(tzinfo=None)


def _ep(v):
    return ap.epoch_de(v)


class BancoFalso:
    """SQL Server de mentira: guarda as tabelas em memória e responde às consultas do módulo pelo texto."""

    def __init__(self, com_97: bool = True, com_96: bool = True, jornada: bool = True, com_ciclo: bool = True,
                 com_origem: bool = True, com_auditoria: bool = True, com_ciclos: bool = True) -> None:
        self.com_ciclo = com_ciclo    # coluna apontamentos_estado.ciclo_medio
        self.com_ciclos = com_ciclos  # coluna apontamentos_estado.ciclos (peças)
        self.com_origem = com_origem  # colunas apontamentos_estado.iot_dispositivo_id / iot_canal
        self.com_auditoria = com_auditoria   # coluna apontamentos_estado.atualizado_por_id
        legendas = {2: ("2", "Produção Normal", "produzindo", True),
                    99: ("99", "Microparada", "parada", True),
                    98: ("98", "Macro parada", "parada", True),
                    SETUP: ("10", "Setup", "planejada", True)}
        if com_97:
            legendas[97] = ("97", "Sem apontamento IOT", "parada", True)
        if com_96:
            legendas[96] = ("96", "Fora de turno", "planejada", True)
        self.jornada = jornada        # colunas de jornada em maquinas + tabela calendario_dias
        self.dados = {
            "legendas": legendas,
            "dispositivos": {1: ("DEV1", True), 2: ("DEV2", True)},
            "maquinas": {},
            "apont": {},
            "ops": {},
            "calendario": [],   # (calendario_id, date, tipo)
            "iot": [],          # (device, ts, {canal: valor})
        }
        self.proximo_id = 1000
        self.comandos: list = []
        self.falhas: list = []      # (trecho do SQL, exceção) — ErroOdbc("fk") = só para ids em fk_ids
        self.fk_ids: set[int] = set()
        self.db_existe = True
        self.colunas_iot = {"id", "device_id", "ts_unix", *ap.CANAIS}
        self.commits = 0
        self.rollbacks = 0
        self.fechada = False
        self._salvo = copy.deepcopy(self.dados)

    # -- montagem dos dados --
    def maquina(self, id_, codigo="INJ01", nome="Injetora 01", micro=MICRO_S, macro=MACRO_S, canais=((1, "chd1_value"),),
                op=None, situacao=None, calendario_id=None, horarios_semana=None, dias_semana=None,
                jornada_inicio=None, jornada_fim=None):
        slots = list(canais) + [(None, None)] * (3 - len(canais))
        self.dados["maquinas"][id_] = {
            "codigo": codigo, "nome": nome, "micro": micro, "macro": macro, "situacao": situacao, "desde": None,
            "legenda": None, "op": op, "slots": [(d, c, "producao" if d else None) for d, c in slots],
            "jornada": (calendario_id, horarios_semana, dias_semana, jornada_inicio, jornada_fim)}
        self._salvo = copy.deepcopy(self.dados)

    def apontamento(self, maquina, legenda, inicio, fim, iot=True, op=None, por=None, em=None):
        self.proximo_id += 1
        self.dados["apont"][self.proximo_id] = {"maquina": maquina, "legenda": legenda, "inicio": _dt(inicio),
                                                "fim": _dt(fim), "obs": ap.OBSERVACAO if iot else "manual",
                                                "op": op, "por": por, "em": em}
        self._salvo = copy.deepcopy(self.dados)
        return self.proximo_id

    def leituras(self, device, canal, pares):
        for ts, v in pares:
            self.dados["iot"].append((device, ts, {canal: v}))
        self._salvo = copy.deepcopy(self.dados)

    def ciclos(self, maquina):
        """[(inicio, ciclo_medio)] dos apontamentos da máquina, em ordem."""
        return sorted((_ep(a["inicio"]), a.get("ciclo")) for a in self.dados["apont"].values()
                      if a["maquina"] == maquina)

    def lista(self, maquina):
        """[(legenda, inicio, fim, iot)] da máquina, em ordem."""
        return sorted(((a["legenda"], _ep(a["inicio"]), _ep(a["fim"]), a["obs"].startswith("[IoT]"))
                       for a in self.dados["apont"].values() if a["maquina"] == maquina), key=lambda x: x[1])

    # -- conexão --
    def cursor(self):
        return Cursor(self)

    def commit(self):
        self.commits += 1
        self._salvo = copy.deepcopy(self.dados)

    def rollback(self):
        self.rollbacks += 1
        self.dados = copy.deepcopy(self._salvo)

    def close(self):
        self.fechada = True

    # -- respostas --
    def responder(self, sql: str, p: tuple):
        d = self.dados
        if sql.startswith("SELECT DB_ID("):
            return [(5 if self.db_existe else None,)]
        if sql.startswith("SELECT OBJECT_ID("):
            return [(123,)]
        if "HAS_PERMS_BY_NAME(N''dbo." in sql:
            return [tuple(1 for _ in ap.PERMISSOES_OEE)]
        if "HAS_PERMS_BY_NAME(@obj" in sql:
            return [(1,)]
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            tabela = p[1]
            if tabela == "IOT001":
                return [(c,) for c in self.colunas_iot]
            if tabela == "calendario_dias":
                return [(c,) for c in ("id", "calendario_id", "data", "tipo", "descricao")]
            extras = {"maquinas": {"atualizado_em", *(ap.COLUNAS_JORNADA if self.jornada else ())},
                      "apontamentos_estado": ({"ciclo_medio"} if self.com_ciclo else set())
                      | ({"iot_dispositivo_id", "iot_canal"} if self.com_origem else set())
                      | ({"atualizado_por_id"} if self.com_auditoria else set())
                      | ({"ciclos"} if self.com_ciclos else set()),
                      "ordens_producao": {"status_em"}}.get(tabela, set())
            return [(c,) for c in ap.TABELAS_OEE[tabela] | extras]
        if "FROM [OEE].[dbo].[legendas]" in sql and sql.startswith("SELECT [id], [codigo]"):
            return [(i, c, desc, tipo, at) for i, (c, desc, tipo, at) in d["legendas"].items()]
        if sql.startswith("SELECT m.[id]"):
            linhas = []
            for i, m in d["maquinas"].items():
                linha = [i, m["codigo"], m["nome"], m["micro"], m["macro"], m["situacao"], _dt(m["desde"]),
                         m["legenda"], m["op"]]
                for dev, canal, tipo in m["slots"]:
                    disp = d["dispositivos"].get(dev)
                    linha += [canal, tipo, disp[0] if disp else None, disp[1] if disp else None]
                linha += list(m["jornada"]) if self.jornada else [None] * 5
                linha += [dev for dev, _c, _t in m["slots"]]          # iot1..3_dispositivo_id
                linhas.append(tuple(linha))
            return linhas
        if sql.startswith("SELECT [calendario_id], [data], [tipo]"):
            *ids, de, ate = p
            return [(c, dt, tp) for c, dt, tp in d["calendario"] if c in ids and de <= dt <= ate]
        if sql.startswith("SELECT [inicio], [fim] FROM"):          # candidatos da correção retroativa (97)
            maq, leg, marca, de, ate = p
            return sorted((a["inicio"], a["fim"]) for a in d["apont"].values()
                          if a["maquina"] == maq and a["legenda"] == leg and a["obs"].startswith(marca)
                          and de <= a["inicio"] < ate)
        if sql.startswith("SELECT TOP 1 1 FROM [MQTT]"):            # leitura dentro do 97?
            dev, de, ate = p
            return [(1,)] if any(dv == dev and de < ts <= ate for dv, ts, _ in d["iot"]) else []
        if "LEFT([observacao], 5)" in sql:
            maq, a, b, marca = p
            return [(1,)] if any(
                x["maquina"] == maq and a <= x["inicio"] < b and x["obs"].startswith(marca)
                for x in d["apont"].values()) else []
        if sql.startswith("SELECT [ts_unix],"):
            cols = [c.strip("[] ") for c in sql.split("SELECT [ts_unix],")[1].split(" FROM ")[0].split(",")]
            dev, desde, ate = p
            linhas = sorted((ts, *(v.get(c) for c in cols)) for (dv, ts, v) in d["iot"]
                            if dv == dev and desde <= ts <= ate)
            return linhas
        if sql.startswith("SELECT MAX([ts_unix])"):
            tss = [ts for dv, ts, _ in d["iot"] if dv == p[0]]
            return [(max(tss) if tss else None,)]
        if sql.startswith("SELECT [id], [legenda_id], [inicio]"):
            maq, agora, ini = p
            extras = [c for c in re.findall(r"\[(\w+)\]", sql.split("[ordem_producao_id]", 1)[1].split(" FROM ")[0])]
            return sorted(((i, a["legenda"], a["inicio"], a["fim"], a["obs"], a["op"],
                            *[a.get(CAMPOS[c]) for c in extras]) for i, a in d["apont"].items()
                           if a["maquina"] == maq and a["inicio"] <= agora and (a["fim"] is None or a["fim"] >= ini)),
                          key=lambda r: (r[2], r[0]))
        if sql.startswith("DELETE FROM"):
            d["apont"].pop(p[0])
            return []
        if sql.startswith("UPDATE [OEE].[dbo].[apontamentos_estado]"):
            a = d["apont"][p[-1]]
            sets = sql.split(" SET ", 1)[1].rsplit(" WHERE ", 1)[0]
            pares = re.findall(r"\[(\w+)\] = (CASE WHEN \[atualizado_por_id\] IS NULL THEN \? ELSE \[\w+\] END|\?)",
                               sets)
            assert "atualizado_por_id] =" not in sets.replace("WHEN [atualizado_por_id] IS NULL", "")
            for (coluna, expr), valor in zip(pares, p[:-1]):
                if expr.startswith("CASE") and a.get("por") is not None:
                    continue                                   # editado por usuário: preserva atualizado_em
                a[CAMPOS[coluna]] = valor
            return []
        if sql.startswith("INSERT INTO [OEE].[dbo].[apontamentos_estado]"):
            self.proximo_id += 1
            cols = [c.strip(" []") for c in sql.split("(", 1)[1].split(")", 1)[0].split(",")]
            v = dict(zip(cols, p))
            d["apont"][self.proximo_id] = {"maquina": v["maquina_id"], "legenda": v["legenda_id"],
                                           "op": v["ordem_producao_id"], "inicio": v["inicio"], "fim": v["fim"],
                                           "obs": v["observacao"], "ciclo": v.get("ciclo_medio"),
                                           "ciclos": v.get("ciclos"),
                                           "dev": v.get("iot_dispositivo_id"), "canal": v.get("iot_canal"),
                                           "por": None, "em": v.get("atualizado_em")}
            return [(self.proximo_id,)] if "OUTPUT INSERTED.[id]" in sql else []
        if sql.startswith("SELECT TOP (") and "ORDER BY [ts_unix] DESC" in sql:   # leituras antes (ciclo médio)
            n = int(sql.split("TOP (", 1)[1].split(")", 1)[0])
            canal = sql.split("[ts_unix], [", 1)[1].split("]", 1)[0]
            dev, ate = p
            linhas = sorted(((ts, v.get(canal)) for dv, ts, v in d["iot"] if dv == dev and ts < ate), reverse=True)
            return linhas[:n]
        if sql.startswith("SELECT TOP 1 a.[inicio]"):
            cand = sorted(((a["inicio"], i, a) for i, a in d["apont"].items() if a["maquina"] == p[0]),
                          key=lambda x: (x[0], x[1]))
            if not cand:
                return []
            _, _, a = cand[-1]
            leg = d["legendas"].get(a["legenda"])
            return [(a["inicio"], a["legenda"], a["op"], leg[2] if leg else None,
                     *([a.get("ciclo")] if "[ciclo_medio]" in sql else []))]
        if sql.startswith("UPDATE [OEE].[dbo].[maquinas]"):
            m = d["maquinas"][p[-1]]
            m["situacao"], m["desde"], m["legenda"], m["op"] = p[0], _ep(p[1]), p[2], p[3]
            return []
        if sql.startswith("SELECT [c2_datrf]"):
            op = d["ops"].get(p[0])
            return [(op["datrf"], op["c2_status"], op["quje"], op["status"])] if op else []
        if sql.startswith("SELECT TOP 1 l.[tipo]"):
            cand = sorted(((a["inicio"], i, a) for i, a in d["apont"].items() if a["op"] == p[0] and a["fim"] is None),
                          key=lambda x: (x[0], x[1]))
            if not cand:
                return []
            leg = d["legendas"].get(cand[-1][2]["legenda"])
            return [(leg[2] if leg else None,)]
        if sql.startswith("SELECT TOP 1 1 FROM") and "[ordem_producao_id] = ?" in sql:
            return [(1,)] if any(a["op"] == p[0] for a in d["apont"].values()) else []
        if sql.startswith("UPDATE [OEE].[dbo].[ordens_producao]"):
            d["ops"][p[-1]]["status"] = p[0]
            return []
        if sql.startswith("SELECT COUNT(*)"):
            return [(sum(1 for a in d["apont"].values() if a["maquina"] == p[0] and a["inicio"] >= p[1]),)]
        raise AssertionError(f"consulta inesperada: {sql}")


class Relogio:
    def __init__(self, t: float) -> None:
        self.t = float(t)

    def __call__(self) -> float:
        return self.t


def novo(banco: BancoFalso, agora: int, cfg: ApontamentosConfig | None = None, sql: SqlServerConfig | None = None):
    logs: list[tuple[str, str]] = []
    erros: list[tuple[str, str | None]] = []
    resultados: list[dict] = []
    obj = ApontamentosOee(
        sql or SqlServerConfig(servidor="FALSO", banco="MQTT", senha=SENHA_FALSA), OeeConfig(), cfg,
        logar=lambda n, m: logs.append((n, m)), ao_erro=lambda m, d=None: erros.append((m, d)),
        ao_resultado=lambda s: resultados.append(json.loads(s)), relogio=Relogio(agora), conectar=lambda _c: banco)
    return obj, logs, erros, resultados


def ligado(banco, device, canal, de, ate, valor=0, passo=60):
    """Aparelho ligado e máquina parada: leituras a cada ``passo`` s com o contador constante (não é produção)."""
    banco.leituras(device, canal, [(t, valor) for t in range(de, ate + 1, passo)])


def producao_continua(banco, device, canal, de, ate, passo=10, valor=0):
    """Leituras a cada ``passo`` s com o contador subindo: a 1ª produção é em ``de`` (a base vem ``passo`` antes)."""
    banco.leituras(device, canal, [(t, valor + k) for k, t in enumerate(range(de - passo, ate + 1, passo))])


# ============================================================================================ classe


def test_ciclo_primeira_execucao_cria_a_timeline_de_hoje_e_atualiza_a_maquina():
    b = BancoFalso()
    b.maquina(1, op=40)
    b.dados["ops"][40] = {"datrf": "", "c2_status": "", "quje": 0, "status": "aberta"}
    agora = T(9, 10)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)         # aparelho ligado desde antes da meia-noite
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))          # produz das 8 às 9, depois para
    ligado(b, "DEV1", "chd1_value", T(9) + 10, agora, valor=361)
    obj, logs, erros, res = novo(b, agora)
    r = obj.executar_ciclo()
    assert r["ok"] and not erros
    assert b.lista(1) == [(98, HOJE, T(8) - 1, True), (2, T(8), T(9) - 1, True), (98, T(9), None, True)]
    assert all(a["op"] == 40 for a in b.dados["apont"].values())
    m = b.dados["maquinas"][1]
    assert (m["situacao"], m["desde"], m["legenda"], m["op"]) == ("parada", T(9), 98, 40)
    assert b.dados["ops"][40]["status"] == "parada"
    assert (r["criados"], r["alterados"], r["excluidos"]) == (3, 0, 0)
    assert ("INFO", "Apontamentos reais: 1 máquina, 3 criados, 0 atualizados, 0 excluídos") in logs
    linha = r["maquinas"][0]
    assert linha["canais"] == "DEV1/chd1_value" and (linha["micro_s"], linha["macro_s"]) == (60, 300)
    assert linha["legenda"] == "98" and linha["legenda_descricao"] == "Macro parada"
    assert linha["desde"] == ap.iso_local(T(9)) and linha["ultima_producao"] == ap.iso_local(T(9))
    assert linha["ultima_leitura"] == ap.iso_local(T(9, 9, 10)) and linha["apontamentos_hoje"] == 3
    assert linha["aviso"] is None
    assert len(res) == 1 and res[0]["quando"] == ap.iso_local(agora) and res[0]["minutos"] == 3
    # as datas foram como datetime ingênuo UTC, sem fração
    inserts = [p for s, p in b.comandos if s.startswith("INSERT")]
    assert all(isinstance(p[3], datetime) and p[3].tzinfo is None and p[3].microsecond == 0 for p in inserts)
    assert inserts[0][5] == "[IoT] Apontamento automático do integrador MQTT"


def test_ciclo_seguinte_cresce_o_mesmo_apontamento_e_volta_a_produzir():
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))
    ligado(b, "DEV1", "chd1_value", T(9) + 10, T(9, 6), valor=361)   # parada, mas o aparelho segue mandando
    obj, logs, erros, res = novo(b, T(9, 2))
    obj.executar_ciclo()
    assert b.lista(1)[-1] == (99, T(9), None, True)                # 2 min parada: micro
    obj._relogio.t = T(9, 6)
    r = obj.executar_ciclo()
    assert b.lista(1)[-1] == (98, T(9), None, True)                # passou de 5 min: macro, mesmo apontamento
    assert (r["criados"], r["alterados"], r["excluidos"]) == (0, 1, 0)
    producao_continua(b, "DEV1", "chd1_value", T(9, 7), T(9, 8), valor=361)  # base = último valor
    obj._relogio.t = T(9, 8, 5)
    obj.executar_ciclo()
    assert b.lista(1)[-2:] == [(98, T(9), T(9, 7) - 1, True), (2, T(9, 7), None, True)]
    assert b.dados["maquinas"][1]["situacao"] == "produzindo"


def test_ciclo_manual_coberto_e_excluido_com_aviso_e_legenda_do_operador_mantida():
    b = BancoFalso()
    b.maquina(1)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))
    obj, logs, erros, _ = novo(b, T(9, 30))
    obj.executar_ciclo()
    parada = next(i for i, a in b.dados["apont"].items() if _ep(a["inicio"]) == T(9))
    b.dados["apont"][parada]["legenda"] = SETUP                      # o operador apontou o motivo
    manual = b.apontamento(1, 99, T(10), T(10, 5), iot=False)       # e um manual por cima do IoT
    b.commit()
    obj._relogio.t = T(10, 30)
    r = obj.executar_ciclo()
    assert manual not in b.dados["apont"]
    assert b.dados["apont"][parada]["legenda"] == SETUP
    assert r["excluidos"] == 1
    avisos = [m for n, m in logs if n == "AVISO"]
    assert len(avisos) == 1 and "manual" in avisos[0] and "Injetora 01" in avisos[0] and "excluído" in avisos[0]
    assert "12/11/2026 10:00:00" in avisos[0]


def test_ciclo_usa_ontem_quando_ja_ha_apontamento_do_integrador_ontem():
    b = BancoFalso()
    b.maquina(1)
    b.apontamento(1, 98, T(20, dia=11), None)                       # ficou em aberto desde ontem
    ligado(b, "DEV1", "chd1_value", ONTEM - 3600, T(7) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(7), T(8))
    obj, *_ = novo(b, T(8, 0, 30))
    obj.executar_ciclo()
    assert b.lista(1) == [(98, ONTEM, T(23, 59, 59, dia=11), True), (98, HOJE, T(7) - 1, True),
                          (2, T(7), None, True)]
    ini = [p for s, p in b.comandos if s.startswith("SELECT [ts_unix],")][0][1]
    assert ini == ONTEM - MACRO_S - ap.MARGEM_LEITURAS


def test_ciclo_dois_canais_de_dispositivos_diferentes():
    b = BancoFalso()
    b.maquina(1, canais=((1, "chd1_value"), (2, "chd3_value")))
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)         # leitura de qualquer device da máquina vale
    producao_continua(b, "DEV1", "chd1_value", T(8), T(8, 30))
    producao_continua(b, "DEV2", "chd3_value", T(8, 30), T(9))
    obj, *_ = novo(b, T(9))
    r = obj.executar_ciclo()
    assert b.lista(1) == [(98, HOJE, T(8) - 1, True), (2, T(8), None, True)]
    assert r["maquinas"][0]["canais"] == "DEV1/chd1_value, DEV2/chd3_value"


def _diag(obj, resposta):
    obj._diagnosticar = lambda devices, timeout: {d: resposta for d in devices}


def test_ciclo_buraco_de_30_min_sem_linhas_dentro_do_turno_e_98_sem_97():
    b = BancoFalso()
    _maquina_jornada(b)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))          # produz; o aparelho some das 9:00 às 9:30
    producao_continua(b, "DEV1", "chd1_value", T(9, 30), T(10), valor=500)
    obj, logs, erros, res = novo(b, T(10))
    _diag(obj, True)
    r = obj.executar_ciclo()
    assert r["ok"] and not erros
    assert b.lista(1) == [(96, HOJE, T(4, 59, 59), True), (98, T(5), T(8) - 1, True), (2, T(8), T(9) - 1, True),
                          (98, T(9), T(9, 30) - 11, True), (2, T(9, 30) - 10, None, True)]
    assert 97 not in {x[0] for x in b.lista(1)}


def test_ciclo_buraco_de_30_min_sem_linhas_fora_do_turno_e_96():
    b = BancoFalso()
    _maquina_jornada(b)                                             # turno de quinta: 05:00–14:48
    ligado(b, "DEV1", "chd1_value", T(14) - 3600, T(15) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(15), T(15, 10))
    producao_continua(b, "DEV1", "chd1_value", T(15, 40), T(16), valor=500)   # 30 min sem linhas, fora do turno
    obj, *_ = novo(b, T(16))
    _diag(obj, True)
    obj.executar_ciclo()
    lista = b.lista(1)
    assert (96, T(15, 10), T(15, 40) - 11, True) in lista and 97 not in {x[0] for x in lista}
    assert lista[-1] == (2, T(15, 40) - 10, None, True)


def test_ciclo_buraco_curto_entre_micro_e_macro_e_99():
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))
    producao_continua(b, "DEV1", "chd1_value", T(9, 2), T(9, 10), valor=500)   # 2 min sem nenhuma linha
    obj, *_ = novo(b, T(9, 10))
    obj.executar_ciclo()
    assert (99, T(9), T(9, 2) - 11, True) in b.lista(1) and 97 not in {x[0] for x in b.lista(1)}


def test_ciclo_aparelho_sumiu_sem_diag_e_parada_98_nao_97():
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))          # produzia e o aparelho caiu às 9h
    obj, logs, erros, res = novo(b, T(9, 30))
    r = obj.executar_ciclo()
    assert b.lista(1) == [(98, HOJE, T(8) - 1, True), (2, T(8), T(9) - 1, True), (98, T(9), None, True)]
    assert r["maquinas"][0]["diag"] is None


def test_ciclo_sem_leitura_nenhuma_e_98():
    b = BancoFalso()
    b.maquina(1)
    obj, *_ = novo(b, T(9))
    r = obj.executar_ciclo()
    assert b.lista(1) == [(98, HOJE, None, True)]
    assert "Nenhuma leitura" in r["maquinas"][0]["aviso"]


def test_ciclo_diag_sem_resposta_97_desde_a_ultima_noticia_e_some_quando_ele_volta():
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))          # produz até 9:00
    ligado(b, "DEV1", "chd1_value", T(9) + 10, T(9, 20), valor=361, passo=10)   # parada; aparelho cai às 9:20
    obj, logs, erros, res = novo(b, T(9, 30))
    _diag(obj, False)
    r = obj.executar_ciclo()
    assert b.lista(1) == [(98, HOJE, T(8) - 1, True), (2, T(8), T(9) - 1, True), (98, T(9), T(9, 20) - 1, True),
                          (97, T(9, 20), None, True)]
    m = b.dados["maquinas"][1]
    assert (m["situacao"], m["legenda"]) == ("parada", 97)
    linha = r["maquinas"][0]
    assert (linha["legenda"], linha["legenda_descricao"], linha["diag"]) == ("97", "Sem apontamento IOT",
                                                                             "sem resposta")
    # continua sem responder: o 97 continua do mesmo ponto
    obj._relogio.t = T(9, 33)
    obj.executar_ciclo()
    assert b.lista(1)[-1] == (97, T(9, 20), None, True)
    # volta a responder sem reenviar a memória: o buraco é parada (98), como qualquer falta de linhas
    _diag(obj, True)
    obj._relogio.t = T(9, 36)
    obj.executar_ciclo()
    assert b.lista(1)[-1] == (98, T(9), None, True) and 97 not in {x[0] for x in b.lista(1)}
    # e volta a produzir
    producao_continua(b, "DEV1", "chd1_value", T(9, 40), T(9, 50), valor=500)
    obj._relogio.t = T(9, 50, 5)
    obj.executar_ciclo()
    assert b.lista(1)[-2:] == [(98, T(9), T(9, 40) - 11, True), (2, T(9, 40) - 10, None, True)]


def test_ciclo_diag_sem_resposta_com_leitura_agora_97_comeca_agora():
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(9, 30), valor=5)   # parada o dia todo, leitura até agora
    obj, *_ = novo(b, T(9, 30))
    _diag(obj, False)
    obj.executar_ciclo()
    assert b.lista(1) == [(98, HOJE, T(9, 30) - 1, True), (97, T(9, 30), None, True)]


def test_ciclo_caso_do_print_ack_perdido_mas_dados_normais_nao_vira_97():
    """1010 INJ33: produção até 13:30, o aparelho segue mandando leituras e só o ack do diag se perde (vai para
    NOÏUS/.../ack/command). O pipeline devolve DIAG_RECENTE: sem 97, "ok (dados recentes)" na linha."""
    b = BancoFalso()
    b.maquina(1, codigo="1010", nome="INJ33")
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(12) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(12), T(13, 30))
    ligado(b, "DEV1", "chd1_value", T(13, 30) + 10, T(13, 45), valor=541, passo=10)   # contador parado
    obj, logs, erros, res = novo(b, T(13, 45))
    _diag(obj, ap.DIAG_RECENTE)
    r = obj.executar_ciclo()
    assert r["ok"] and not erros
    assert b.lista(1)[-1] == (98, T(13, 30), None, True) and 97 not in {x[0] for x in b.lista(1)}
    assert r["maquinas"][0]["diag"] == "ok (dados recentes)"
    assert b.dados["maquinas"][1]["legenda"] == 98


def test_ciclo_legenda_97_ausente_diag_sem_resposta_fica_98():
    b = BancoFalso(com_97=False)
    b.maquina(1)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))          # aparelho caiu às 9h
    obj, logs, erros, res = novo(b, T(9, 30))
    _diag(obj, False)
    r = obj.executar_ciclo()
    obj._relogio.t = T(9, 33)
    obj.executar_ciclo()
    assert r["ok"] and not erros
    assert b.lista(1) == [(98, HOJE, T(8) - 1, True), (2, T(8), T(9) - 1, True), (98, T(9), None, True)]
    avisos = [m for n, m in logs if n == "AVISO"]
    assert len(avisos) == 1 and "97" in avisos[0] and "Sem apontamento IOT" in avisos[0]
    # cadastrada a 97 no portal, o mesmo trecho vira 97
    b.dados["legendas"][97] = ("97", "Sem apontamento IOT", "parada", True)
    b.commit()
    obj._relogio.t = T(9, 36)
    obj.executar_ciclo()
    assert b.lista(1)[-1] == (97, T(9), None, True)


def test_ciclo_97_antigo_por_buraco_na_janela_e_refeito_pela_regra_nova():
    """Trechos 97 gravados pela regra antiga (buraco sem linhas) dentro da janela (ontem 00:00 até agora) são do
    integrador e o ciclo normal os refaz: viram 98/2 conforme o contador, sem botão Recalcular."""
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", ONTEM - 3600, T(4, 55) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(4, 55), T(14))
    b.apontamento(1, 98, ONTEM, T(23, 59, 59, dia=11))
    b.apontamento(1, 98, HOJE, T(4, 50) - 1)
    b.apontamento(1, 97, T(4, 50), T(4, 55) - 1)                       # 97 curto perto das 05:00 (o do print)
    b.apontamento(1, 2, T(4, 55), T(13, 33) - 1)
    b.apontamento(1, 97, T(13, 33), None)                               # 97 em aberto desde 13:33
    obj, logs, erros, res = novo(b, T(14))
    _diag(obj, True)
    obj.executar_ciclo()
    assert 97 not in {x[0] for x in b.lista(1)}
    assert b.lista(1) == [(98, ONTEM, T(23, 59, 59, dia=11), True), (98, HOJE, T(4, 55) - 1, True),
                          (2, T(4, 55), None, True)]


def _maquina_com_97_antigo(b):
    """Aparelho fora de 10/11 10:00 até agora, já apontado como 97 nos dias 10, 11 e 12."""
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", T(0, dia=10) - 3600, T(10, dia=10), valor=5)
    b.apontamento(1, 98, T(0, dia=10), T(10, dia=10) - 1)
    antigo = b.apontamento(1, 97, T(10, dia=10), T(23, 59, 59, dia=10))
    b.apontamento(1, 97, T(0, dia=11), T(23, 59, 59, dia=11))
    b.apontamento(1, 97, HOJE, None)
    return antigo


def test_correcao_retroativa_recalcula_o_97_antigo_com_os_dados_reais():
    b = BancoFalso()
    antigo = _maquina_com_97_antigo(b)
    # o aparelho voltou e reenviou a memória: estava ligado o tempo todo e a máquina produziu no dia 10 à tarde
    ligado(b, "DEV1", "chd1_value", T(10, dia=10) + 60, T(14, dia=10) - 20, valor=5)
    producao_continua(b, "DEV1", "chd1_value", T(14, dia=10), T(15, dia=10), valor=5)
    ligado(b, "DEV1", "chd1_value", T(15, dia=10) + 60, T(9), valor=366)
    obj, logs, erros, res = novo(b, T(9))
    r = obj.executar_ciclo()
    assert r["ok"] and not erros
    assert b.lista(1) == [
        (98, T(0, dia=10), T(14, dia=10) - 1, True), (2, T(14, dia=10), T(15, dia=10) - 1, True),
        (98, T(15, dia=10), T(23, 59, 59, dia=10), True), (98, ONTEM, T(23, 59, 59, dia=11), True),
        (98, HOJE, None, True)]
    assert antigo in b.dados["apont"]                                # reaproveitado, não excluído e recriado
    info = [m for n, m in logs if n == "INFO" and "retroativos" in m]
    assert info == ["Apontamentos reais: dados retroativos de DEV1 — recalculando a máquina INJ01 (Injetora 01) "
                    "desde 10/11."]
    # no ciclo seguinte não há mais 97 com leitura dentro: a janela volta ao normal
    b.comandos.clear()
    obj._relogio.t = T(9, 3)
    obj.executar_ciclo()
    desde = [p for s, p in b.comandos if s.startswith("SELECT [ts_unix],")][0][1]
    assert desde == ONTEM - MACRO_S - ap.MARGEM_LEITURAS


def test_correcao_retroativa_sem_leitura_nova_nao_muda_a_janela():
    b = BancoFalso()
    _maquina_com_97_antigo(b)
    obj, logs, erros, res = novo(b, T(9))
    obj.executar_ciclo()
    desde = [p for s, p in b.comandos if s.startswith("SELECT [ts_unix],")][0][1]
    assert desde == ONTEM - MACRO_S - ap.MARGEM_LEITURAS             # janela normal: ontem 00:00
    assert not [m for _, m in logs if "retroativos" in m]
    # o 97 de antes da janela (dia 10) fica; os da janela (ontem e hoje) são refeitos pela regra nova (98)
    assert [x[0] for x in b.lista(1)] == [98, 97, 98, 98]


def test_correcao_retroativa_respeita_dias_correcao():
    b = BancoFalso()
    _maquina_com_97_antigo(b)
    ligado(b, "DEV1", "chd1_value", T(10, dia=10) + 60, T(9), valor=5)
    obj, logs, *_ = novo(b, T(9), cfg=ApontamentosConfig(dias_correcao=1))
    obj.executar_ciclo()
    assert not [m for _, m in logs if "retroativos" in m]            # o 97 do dia 10 tem mais de 1 dia
    assert b.lista(1)[1] == (97, T(10, dia=10), T(23, 59, 59, dia=10), True)


def test_correcao_retroativa_do_97_do_diagnostico():
    """97 vindo do diagnóstico (aparelho mudo de 10/11 9:20 até 12/11): ele volta e reenvia a memória; o trecho de
    antes da janela é refeito com os dados reais (2/98), sem sobrar 97."""
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", T(0, dia=10) - 3600, T(8, dia=10) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8, dia=10), T(9, dia=10))
    ligado(b, "DEV1", "chd1_value", T(9, dia=10) + 10, T(9, 20, dia=10), valor=361, passo=10)
    obj, logs, erros, res = novo(b, T(9, 30, dia=10))
    _diag(obj, False)
    obj.executar_ciclo()
    assert b.lista(1)[-1] == (97, T(9, 20, dia=10), None, True)
    obj._relogio.t = T(12, dia=11)
    obj.executar_ciclo()                                              # continua mudo no dia seguinte
    assert [x[0] for x in b.lista(1)][-2:] == [97, 97]
    # 12/11: voltou, responde e reenviou a memória: estava parada até 14:00 do dia 10 e produziu até 15:00
    ligado(b, "DEV1", "chd1_value", T(9, 20, dia=10) + 10, T(14, dia=10) - 20, valor=361, passo=60)
    producao_continua(b, "DEV1", "chd1_value", T(14, dia=10), T(15, dia=10), valor=361)
    ligado(b, "DEV1", "chd1_value", T(15, dia=10) + 60, T(9), valor=722)
    obj._relogio.t = T(9)
    _diag(obj, True)
    obj.executar_ciclo()
    lista = b.lista(1)
    assert 97 not in {x[0] for x in lista}
    assert (98, T(9, dia=10), T(14, dia=10) - 1, True) in lista
    assert (2, T(14, dia=10), T(15, dia=10) - 1, True) in lista
    assert [m for n, m in logs if "retroativos" in m]


def test_legenda_ausente_avisa_uma_vez_e_nao_aponta():
    b = BancoFalso()
    b.maquina(1)
    del b.dados["legendas"][99]
    obj, logs, erros, res = novo(b, T(9))
    obj.executar_ciclo()
    obj.executar_ciclo()
    assert not b.dados["apont"] and not erros
    assert [n for n, m in logs if "99" in m] == ["AVISO"]
    assert res[-1]["ok"] is False and "99" in res[-1]["mensagem"]


def test_portal_ausente_e_um_info_sem_erro_e_tenta_de_novo():
    b = BancoFalso()
    b.db_existe = False
    obj, logs, erros, res = novo(b, T(9))
    obj.executar_ciclo()
    obj.executar_ciclo()
    assert not erros and [n for n, _ in logs] == ["INFO"]
    assert res[-1]["ok"] is False and "não disponível" in res[-1]["mensagem"]
    b.db_existe = True
    b.maquina(1)
    assert obj.executar_ciclo()["ok"]


def test_falha_de_sql_numa_maquina_e_erro_rollback_e_segue_para_a_proxima():
    b = BancoFalso()
    b.maquina(1, codigo="RUIM", nome="Máquina ruim")
    b.maquina(2, codigo="BOA", nome="Máquina boa", canais=((2, "chd1_value"),))
    b.falhas.append(("[maquina_id] = ? AND [inicio] <= ?", ErroOdbc("42000", "deadlock de mentira")))
    obj, logs, erros, res = novo(b, T(9))
    r = obj.executar_ciclo()
    assert len(erros) == 2      # as duas falham nessa consulta; cada uma soma 1 erro
    b.falhas.clear()
    b.maquina(3, codigo="RUIM2", canais=((1, "chd1_value"),))
    b.falhas.append(("UPDATE [OEE].[dbo].[maquinas]", ErroOdbc("42000", "sem permissão de mentira")))
    erros.clear()
    r = obj.executar_ciclo()
    assert r["ok"] is False and len(erros) == 3
    assert not b.dados["apont"]                               # tudo desfeito (rollback de cada máquina)
    assert b.rollbacks >= 3
    assert "RUIM" in erros[0][0] and erros[0][1]
    assert r["maquinas"][0]["aviso"].startswith("Falha")


def test_falha_de_conexao_e_um_erro_por_incidente():
    b = BancoFalso()
    obj, logs, erros, res = novo(b, T(9))

    def cai(_c):
        raise ErroOdbc("08001", "servidor inacessível")

    obj._conectar = cai
    obj.executar_ciclo()
    obj.executar_ciclo()
    assert len(erros) == 1 and res[-1]["ok"] is False
    obj._conectar = lambda _c: b
    b.maquina(1)
    assert obj.executar_ciclo()["ok"]
    obj._conectar = cai
    obj._fechar_conexao()
    obj.executar_ciclo()
    assert len(erros) == 2


def test_exclusao_barrada_por_fk_mantem_o_apontamento_e_avisa():
    b = BancoFalso()
    b.maquina(1)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))
    manual = b.apontamento(1, SETUP, T(8, 10), T(8, 20), iot=False)
    b.fk_ids.add(manual)
    b.falhas.append(("DELETE FROM", ErroOdbc("fk")))
    obj, logs, erros, _ = novo(b, T(9, 30))
    r = obj.executar_ciclo()
    assert r["ok"] and not erros and manual in b.dados["apont"]
    assert any("não pôde ser excluído" in m for n, m in logs if n == "AVISO")


def test_maquina_sem_micro_macro_e_ignorada_com_um_info():
    b = BancoFalso()
    b.maquina(1, micro=None)
    obj, logs, erros, _ = novo(b, T(9))
    r = obj.executar_ciclo()
    obj.executar_ciclo()
    assert not b.dados["apont"]
    assert len([m for n, m in logs if "micro/macro" in m]) == 1
    assert r["maquinas"][0]["aviso"].startswith("Micro/macro")


def test_canal_refugo_ou_dispositivo_inativo_nao_entra():
    b = BancoFalso()
    b.maquina(1)
    b.dados["maquinas"][1]["slots"][0] = (1, "chd1_value", "refugo")
    b.maquina(2, canais=((2, "chd1_value"),))
    b.dados["dispositivos"][2] = ("DEV2", False)
    obj, *_ = novo(b, T(9))
    r = obj.executar_ciclo()
    assert not b.dados["apont"]
    assert all(m["aviso"] for m in r["maquinas"])


def test_coluna_do_canal_ausente_na_iot001_vira_aviso():
    b = BancoFalso()
    b.colunas_iot -= {"chd5_value"}
    b.maquina(1, canais=((1, "chd5_value"),))
    b.maquina(2, canais=((1, "chd5_value"), (2, "chd1_value")))
    obj, *_ = novo(b, T(9))
    r = obj.executar_ciclo()
    porm = {m["maquina_id"]: m for m in r["maquinas"]}
    assert "chd5_value" in porm[1]["aviso"] and "ignorada" in porm[1]["aviso"]
    assert b.lista(1) == []                                  # sem coluna nenhuma: não aponta 98 à toa
    assert "chd5_value" in porm[2]["aviso"]                  # o outro canal basta
    assert b.lista(2) == [(98, HOJE, None, True)]            # e sem leitura nenhuma: parada (98), não 97


def test_sem_senha_nao_roda_e_responde():
    b = BancoFalso()
    obj, logs, erros, res = novo(b, T(9), sql=SqlServerConfig(servidor="FALSO", banco="MQTT", senha=""))
    r = obj.executar_ciclo()
    assert r["ok"] is False and "senha" in r["mensagem"] and not b.comandos


def test_periodicidade_e_executar_agora_pela_thread():
    b = BancoFalso()
    b.maquina(1)
    obj, logs, erros, res = novo(b, T(9))
    obj.definir_periodicidade(10)
    assert obj.minutos == 10
    obj.executar_agora()
    assert esperar(lambda: len(res) >= 1)
    assert res[0]["ok"] and res[0]["minutos"] == 10
    assert res[0]["proxima"] == ap.iso_local(T(9) + 600)
    obj.parar(2)
    assert b.fechada


# ============================================================================================ config e pipeline


def test_config_padrao_e_toml(tmp_path):
    c = Config().apontamentos
    assert (c.habilitado, c.minutos, c.legenda_produzindo, c.legenda_micro, c.legenda_macro, c.fuso) == (
        True, 3, "2", "99", "98", "America/Sao_Paulo")
    assert (c.legenda_sem_iot, c.sem_leitura_segundos, c.dias_correcao) == ("97", 120, 7)
    arq = tmp_path / "config.toml"
    arq.write_text('[apontamentos]\nminutos = 5\nlegenda_macro = "97"\n', encoding="utf-8")
    c = carregar(arq).apontamentos
    assert c.minutos == 5 and c.legenda_macro == "97"


def test_exemplo_de_config_tem_a_secao():
    import tomllib
    from pathlib import Path
    dados = tomllib.loads((Path(__file__).resolve().parent.parent / "config.example.toml").read_text("utf-8"))
    assert dados["apontamentos"]["minutos"] == 3
    assert (dados["apontamentos"]["legenda_sem_iot"], dados["apontamentos"]["sem_leitura_segundos"],
            dados["apontamentos"]["dias_correcao"]) == ("97", 120, 7)


def test_categoria_de_erro_apontamentos():
    from app.core import erros
    assert "apontamentos" in erros.CATEGORIAS
    assert erros.montar_registro("x", "apontamentos", "m")["categoria"] == "apontamentos"


def test_pipeline_liga_resultado_e_erro_dos_apontamentos(tmp_path):
    cfg = Config()
    cfg.gravacao.pasta_logs = str(tmp_path / "logs")
    resultados, erros_json = [], []
    p = Pipeline(cfg, fabrica_sink=lambda *a: None, pasta=tmp_path, relogio=Relogio(T(9)),
                 ao_apontamentos=resultados.append, ao_erro=erros_json.append)
    try:
        b = BancoFalso()
        b.maquina(1)
        b.falhas.append(("UPDATE [OEE].[dbo].[maquinas]", ErroOdbc("42000", "falha de mentira")))
        p._apontamentos._conectar = lambda _c: b
        p.definir_sql(SqlServerConfig(servidor="FALSO", banco="MQTT", senha=SENHA_FALSA))
        p.definir_periodicidade_apontamentos(7)
        p.executar_apontamentos_agora()
        assert esperar(lambda: resultados)
        dados = json.loads(resultados[0])
        assert dados["ok"] is False and dados["minutos"] == 7
        assert esperar(lambda: erros_json)
        reg = json.loads(erros_json[0])
        assert reg["categoria"] == "apontamentos" and "INJ01" in reg["mensagem"]
        assert p.contadores[2] == 1
        assert SENHA_FALSA not in resultados[0] + erros_json[0]
    finally:
        p.encerrar(2)


def test_bridge_real_tem_o_contrato_novo():
    from app.bridge import PipelineBridge
    from tests.fake_bridge import FakePipelineBridge
    for cls in (PipelineBridge, FakePipelineBridge):
        assert hasattr(cls, "apontamentos_atualizados")
        assert callable(getattr(cls, "definir_periodicidade_apontamentos"))
        assert callable(getattr(cls, "executar_apontamentos_agora"))


# ============================================================================================ fora de turno (96)

from datetime import date as _date  # noqa: E402

from app.core.apontamentos_oee import (FORA_TURNO, Jornada, intervalos_turno, jornada_de,  # noqa: E402
                                       marcar_sem_resposta, montar_jornada, recortar_fora_turno, texto_jornada)

HORARIOS = '{"1":["05:00","14:48"],"2":["05:00","14:48"],"3":["05:00","14:48"],"4":["05:00","14:48"],' \
           '"5":["05:00","13:48"]}'


def turnos_e_trechos(segs, jornada, ini, agora):
    turnos = intervalos_turno(jornada, ini, agora, FUSO)
    return dividir_por_dia(recortar_fora_turno(segs, turnos), FUSO)


def test_jornada_do_cadastro_e_texto_como_no_portal():
    h = jornada_de(HORARIOS, "12345", "05:00", "14:48")
    assert h[4] == ("05:00", "14:48") and h[5] == ("05:00", "13:48") and 6 not in h
    assert texto_jornada(h) == "Seg–Qui 05:00–14:48, Sex 05:00–13:48"
    # sem horarios_semana: dias_semana + jornada_inicio/fim
    assert jornada_de(None, "12345", "07:00", "17:00") == {d: ("07:00", "17:00") for d in range(1, 6)}
    assert texto_jornada(jornada_de("", "12345", "07:00", "17:00")) == "Seg–Sex 07:00–17:00"
    assert jornada_de(None, "12345", None, None) == {} and texto_jornada({}) is None
    assert jornada_de("{lixo", "1", "25:00", "07:00") == {}


def test_exemplo_do_usuario_96_ate_o_turno_98_ate_produzir_e_2_depois():
    j = montar_jornada(HORARIOS, "12345", "05:00", "14:48")      # 12/11 é quinta
    agora = T(6)
    prods = list(range(T(5, 22), agora + 1, 30))
    segs = segmentos_continuos(prods, HOJE, agora, MICRO_S, MACRO_S)
    assert segs[0] == (HOJE, T(5, 22), MACRO)                     # classificada pelo intervalo inteiro
    assert turnos_e_trechos(segs, j, HOJE, agora) == [
        Trecho(HOJE, T(4, 59, 59), FORA_TURNO), Trecho(T(5), T(5, 22) - 1, MACRO), Trecho(T(5, 22), None, PRODUZINDO)]


def test_feriado_no_calendario_tudo_parado_e_96():
    j = montar_jornada(HORARIOS, "12345", "05:00", "14:48", {_date(2026, 11, 12): False})
    assert intervalos_turno(j, HOJE, T(12), FUSO) == []
    segs = segmentos_continuos([], HOJE, T(12), MICRO_S, MACRO_S)
    assert turnos_e_trechos(segs, j, HOJE, T(12)) == [Trecho(HOJE, None, FORA_TURNO)]


def test_compensacao_no_sabado_e_dia_util_com_o_horario_padrao():
    sab = T(0, dia=14)
    sem = montar_jornada(HORARIOS, "12345", "05:00", "14:48")
    com = montar_jornada(HORARIOS, "12345", "05:00", "14:48", {_date(2026, 11, 14): True})
    assert intervalos_turno(sem, sab, T(12, dia=14), FUSO) == []
    assert intervalos_turno(com, sab, T(12, dia=14), FUSO) == [(T(5, dia=14), T(14, 48, dia=14))]
    # dia útil normal virando folga pelo calendário
    folga = montar_jornada(HORARIOS, "12345", "05:00", "14:48", {_date(2026, 11, 13): False})
    assert intervalos_turno(folga, T(0, dia=13), T(20, dia=13), FUSO) == []


def test_turno_que_vira_o_dia():
    j = montar_jornada(None, "1234", "22:00", "06:00")               # seg–qui, das 22h às 6h do dia seguinte
    turnos = intervalos_turno(j, HOJE, T(23), FUSO)                    # 12/11 quinta
    assert turnos == [(T(22, dia=11), T(6)), (T(22), T(6, dia=13))]   # o de quarta termina hoje às 6h
    segs = [(HOJE, T(23), MACRO)]
    assert turnos_e_trechos(segs, j, HOJE, T(23)) == [
        Trecho(HOJE, T(5, 59, 59), MACRO), Trecho(T(6), T(21, 59, 59), FORA_TURNO), Trecho(T(22), None, MACRO)]
    # sexta 13/11 às 03h ainda é o turno de quinta (pertence ao dia em que começa)
    assert intervalos_turno(j, T(3, dia=13), T(3, dia=13), FUSO) == [(T(22), T(6, dia=13))]


def test_parada_que_atravessa_o_fim_do_turno_98_vira_96():
    j = montar_jornada(HORARIOS, "12345", "05:00", "14:48")
    prods = [T(14), T(14) + 30]
    segs = segmentos_continuos(prods, T(13), T(16), MICRO_S, MACRO_S)
    trechos = turnos_e_trechos(segs, j, T(13), T(16))
    assert trechos[-2:] == [Trecho(T(14) + 30, T(14, 47, 59), MACRO), Trecho(T(14, 48), None, FORA_TURNO)]


def test_microparada_fora_do_turno_e_96_e_producao_fora_do_turno_continua_2():
    j = montar_jornada(HORARIOS, "12345", "05:00", "14:48")
    ini, agora = T(16), T(16, 30)                                      # depois do turno
    prods = [ini - 10, ini + 30, ini + 130, ini + 160, *range(ini + 190, agora - 300, 30)]
    segs = segmentos_continuos(prods, ini, agora, MICRO_S, MACRO_S)
    assert (ini + 30, ini + 130, MICRO) in segs
    estados = [t.estado for t in turnos_e_trechos(segs, j, ini, agora)]
    assert estados == [PRODUZINDO, FORA_TURNO, PRODUZINDO, FORA_TURNO]
    assert MICRO not in estados and MACRO not in estados


def test_sem_jornada_nao_ha_turno_nem_96():
    j = montar_jornada(None, "12345", None, None)
    assert j.vazia and intervalos_turno(j, HOJE, T(9), FUSO) == []


def test_diag_sem_resposta_marca_a_parada_em_aberto_como_97():
    ini, p = T(8), T(8, 30)
    prods = [ini - 10, *range(ini, p + 1, 30)]
    segs = segmentos_continuos(prods, ini, T(9), MICRO_S, MACRO_S)
    assert segs[-1] == (p, T(9), MACRO)
    assert marcar_sem_resposta(segs, prods, ini)[-1] == (p, T(9), SEM_IOT)
    assert marcar_sem_resposta(segs, prods, ini)[0] == segs[0]                 # o que já passou não muda
    produzindo = segmentos_continuos(prods, ini, p + 10, MICRO_S, MACRO_S)
    assert marcar_sem_resposta(produzindo, prods, ini) == produzindo           # sem parada em aberto
    # com a última notícia do aparelho depois da última produção: 97 só a partir dela
    assert marcar_sem_resposta(segs, prods, ini, p + 600)[-2:] == [(p, p + 600, MACRO), (p + 600, T(9), SEM_IOT)]
    assert marcar_sem_resposta(segs, prods, ini, T(9))[-1] == (T(9), T(9), SEM_IOT)   # leitura agora: 97 de agora
    assert marcar_sem_resposta(segs, prods, ini, p - 100)[-1] == (p, T(9), SEM_IOT)   # leitura antiga: desde p


def _maquina_jornada(b, **kw):
    b.maquina(1, horarios_semana=HORARIOS, dias_semana="12345", jornada_inicio="05:00", jornada_fim="14:48", **kw)


def test_ciclo_exemplo_do_usuario_e_json_novo():
    b = BancoFalso()
    _maquina_jornada(b, calendario_id=3)
    agora = T(6)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(5, 22) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(5, 22), agora)
    obj, logs, erros, res = novo(b, agora)
    obj._diagnosticar = lambda devices, timeout: {d: True for d in devices}
    r = obj.executar_ciclo()
    assert r["ok"] and not erros
    assert b.lista(1) == [(96, HOJE, T(4, 59, 59), True), (98, T(5), T(5, 22) - 1, True), (2, T(5, 22), None, True)]
    linha = r["maquinas"][0]
    assert (linha["diag"], linha["turno"], linha["jornada"]) == ("ok", "dentro", "Seg–Qui 05:00–14:48, Sex 05:00–13:48")


def test_ciclo_feriado_pelo_calendario_tudo_96_e_turno_fora():
    b = BancoFalso()
    _maquina_jornada(b, calendario_id=3)
    b.dados["calendario"].append((3, _date(2026, 11, 12), "feriado"))
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(9))
    obj, *_ = novo(b, T(9))
    r = obj.executar_ciclo()
    assert b.lista(1) == [(96, HOJE, None, True)]
    assert r["maquinas"][0]["turno"] == "fora" and r["maquinas"][0]["diag"] is None   # sem callback de diag
    assert b.dados["maquinas"][1]["situacao"] == "parada"


def test_ciclo_sem_jornada_sem_96():
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(3))
    obj, *_ = novo(b, T(3))
    r = obj.executar_ciclo()
    assert b.lista(1) == [(98, HOJE, None, True)]
    assert (r["maquinas"][0]["turno"], r["maquinas"][0]["jornada"]) == ("sem jornada", None)


def test_ciclo_portal_sem_colunas_de_jornada_sem_96():
    b = BancoFalso(jornada=False)
    _maquina_jornada(b)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(3))
    obj, *_ = novo(b, T(3))
    r = obj.executar_ciclo()
    assert b.lista(1) == [(98, HOJE, None, True)] and r["maquinas"][0]["turno"] == "sem jornada"


def test_ciclo_legenda_96_ausente_avisa_uma_vez_e_segue_sem_96():
    b = BancoFalso(com_96=False)
    _maquina_jornada(b)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(3))
    obj, logs, erros, _ = novo(b, T(3))
    r = obj.executar_ciclo()
    obj.executar_ciclo()
    assert r["ok"] and b.lista(1) == [(98, HOJE, None, True)]
    avisos = [m for n, m in logs if n == "AVISO"]
    assert len(avisos) == 1 and "96" in avisos[0] and "Fora de turno" in avisos[0]
    assert r["maquinas"][0]["turno"] == "fora"                 # a tela ainda mostra o turno


# ============================================================================================ diagnóstico


def _parada_em_aberto(b):
    _maquina_jornada(b)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))
    ligado(b, "DEV1", "chd1_value", T(9) + 10, T(9, 20), valor=361, passo=10)   # parada; última leitura 9:20


@pytest.mark.parametrize("resposta, diag", [(False, "sem resposta"), (True, "ok"), (None, "sem MQTT"),
                                             (ap.DIAG_RECENTE, "ok (dados recentes)")])
def test_ciclo_diag_decide_a_parada_em_aberto(resposta, diag):
    b = BancoFalso()
    _parada_em_aberto(b)
    pedidos = []
    obj, logs, erros, res = novo(b, T(9, 30))
    obj._diagnosticar = lambda devices, timeout: pedidos.append((devices, timeout)) or {d: resposta for d in devices}
    r = obj.executar_ciclo()
    assert pedidos == [(["DEV1"], 15.0)]
    if resposta is False:     # 97 a partir da última notícia do aparelho (9:20); antes disso continua 98
        assert b.lista(1)[-2:] == [(98, T(9), T(9, 20) - 1, True), (97, T(9, 20), None, True)]
    else:
        assert b.lista(1)[-1] == (98, T(9), None, True)
    assert (2, T(8), T(9) - 1, True) in b.lista(1)                  # o passado não muda
    linha = r["maquinas"][0]
    assert linha["diag"] == diag
    assert ("Sem MQTT" in (linha["aviso"] or "")) == (resposta is None)


def test_ciclo_diag_sem_resposta_fora_do_turno_continua_96():
    b = BancoFalso()
    _maquina_jornada(b)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(15))
    obj, *_ = novo(b, T(15))
    obj._diagnosticar = lambda devices, timeout: {d: False for d in devices}
    obj.executar_ciclo()
    assert b.lista(1)[-1] == (96, T(14, 48), None, True)


# ============================================================================================ recalcular


def test_recalcular_dia_comeca_no_dia_escolhido_e_apaga_os_manuais():
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", T(0, dia=10) - 3600, T(9))
    manual = b.apontamento(1, SETUP, T(10, dia=10), T(11, dia=10), iot=False)
    fora = b.apontamento(1, SETUP, T(10, dia=9), T(11, dia=9), iot=False)      # antes do dia escolhido
    obj, logs, erros, res = novo(b, T(9))
    respostas = []
    obj._ao_recalculo = lambda *a: respostas.append(a)
    obj.executar_ciclo()
    assert manual in b.dados["apont"]                                # o ciclo normal só olha de hoje
    ini = T(0, dia=10)
    assert obj.executar_recalculo(1, "2026-11-10", ini)
    assert manual not in b.dados["apont"] and fora in b.dados["apont"]
    assert [x[0] for x in b.lista(1) if x[3]] == [98, 98, 98]
    mid, dia, ok, msg = respostas[-1]
    assert (mid, dia, ok) == (1, "2026-11-10", True)
    assert msg.startswith("Recalculado desde 10/11:") and "(1 manual apagado)" in msg
    desde = [p for s, p in b.comandos if s.startswith("SELECT [ts_unix],")][-1][1]
    assert desde == ini - MACRO_S - ap.MARGEM_LEITURAS
    assert res[-1]["maquinas"][0]["maquina_id"] == 1 and res[-1]["maquinas"][0]["legenda"] == "98"


def test_recalcular_dia_fora_do_limite_ou_invalido_responde_falha():
    b = BancoFalso()
    b.maquina(1)
    obj, logs, erros, res = novo(b, T(9))
    respostas = []
    obj._ao_recalculo = lambda *a: respostas.append(a)
    obj.recalcular(1, "2026-11-04")                                   # 8 dias atrás (limite: 7)
    obj.recalcular(1, "2026-11-13")                                   # amanhã
    obj.recalcular(1, "12/11/2026")
    assert [r[2] for r in respostas] == [False, False, False]
    assert "05/11 a 12/11" in respostas[0][3] and "AAAA-MM-DD" in respostas[2][3]
    assert not b.comandos
    assert obj.executar_recalculo(99, "2026-11-12", HOJE) is False
    assert "não encontrada" in respostas[-1][3]


def test_recalcular_pela_fila_da_thread():
    b = BancoFalso()
    b.maquina(1)
    obj, logs, erros, res = novo(b, T(9))
    respostas = []
    obj._ao_recalculo = lambda *a: respostas.append(a)
    obj.recalcular(1, "2026-11-12")
    assert esperar(lambda: respostas)
    assert respostas[0][:3] == (1, "2026-11-12", True)
    obj.parar(2)


# ============================================================================================ pipeline: diag do ciclo

from tests.test_digirail import DEV, inicio, montar, resposta_diag  # noqa: E402,F401


def test_pipeline_diagnosticar_dispositivos(montar):
    p, cli, relogio, col, _con = montar(intervalo=0)
    # responde
    resultado = {}
    t = threading.Thread(target=lambda: resultado.update(p.diagnosticar_dispositivos([DEV], 5)))
    t.start()
    assert esperar(lambda: cli.publicados)
    ts = json.loads(cli.publicados[-1][1])["timestamp"]
    cli.entregar(f"NOVUS/{DEV}/ack/command", resposta_diag(ts))
    t.join(5)
    assert resultado == {DEV: True}
    # não responde (e nenhuma mensagem dele há mais de 60 s): False, 1 erro "comando" por incidente,
    # resposta_comando a cada vez
    relogio.ir(relogio() + 61)
    assert p.diagnosticar_dispositivos([DEV], 0.2) == {DEV: False}
    assert p.diagnosticar_dispositivos([DEV], 0.2) == {DEV: False}
    registros = [json.loads(e) for e in col.erros]
    assert [r["categoria"] for r in registros] == ["comando"] and DEV in registros[0]["mensagem"]
    assert sum(1 for r in col.respostas if r[:3] == (DEV, "diag", False)) == 2
    assert len(col.com("não responde", "AVISO") + col.com("A parada em aberto", "AVISO")) >= 1
    assert not p._comandos                                            # nada pendente sobrou
    # volta a responder: fecha o incidente; a próxima queda soma outro erro
    resultado.clear()
    t = threading.Thread(target=lambda: resultado.update(p.diagnosticar_dispositivos([DEV], 5)))
    n = len(cli.publicados)
    t.start()
    assert esperar(lambda: len(cli.publicados) > n)
    cli.entregar(f"NOVUS/{DEV}/ack/command", resposta_diag(json.loads(cli.publicados[-1][1])["timestamp"]))
    t.join(5)
    assert resultado == {DEV: True} and col.com("voltou a responder", "INFO")
    relogio.ir(relogio() + 61)
    assert p.diagnosticar_dispositivos([DEV], 0.1) == {DEV: False}
    assert len(col.erros) == 2
    # sem MQTT: None, sem publicar nada
    p.desconectar().result(5)
    n = len(cli.publicados)
    assert p.diagnosticar_dispositivos([DEV], 0.1) == {DEV: None}
    assert len(cli.publicados) == n


def test_pipeline_diag_sem_ack_mas_com_mensagem_recente_conta_como_respondeu(montar):
    """Caso real: o ack vai para NOÏUS/.../ack/command (não ouvido), mas as leituras chegam em NOVUS/.../events."""
    from tests.test_amostragem import novus
    p, cli, relogio, col, _con = montar(intervalo=0)
    t0 = relogio()
    # nenhuma mensagem do device ainda: sem resposta = False (97), 1 erro
    assert p.diagnosticar_dispositivos([DEV], 0.1) == {DEV: False}
    assert len(col.erros) == 1
    # chega uma leitura normal; o ack do diag continua sem chegar
    cli.entregar(f"NOVUS/{DEV}/events", novus(51584019, ts=int(t0)))
    assert esperar(lambda: p.segundos_desde_atividade(DEV) is not None)
    relogio.ir(t0 + 60)                                               # exatamente 60 s depois: ainda vale
    assert p.diagnosticar_dispositivos([DEV], 0.1) == {DEV: ap.DIAG_RECENTE}
    assert col.com("voltou a mandar mensagens", "INFO")               # fecha o incidente de device mudo
    assert len(col.com("sem confirmação", "AVISO")) == 1
    assert p.diagnosticar_dispositivos([DEV], 0.1) == {DEV: ap.DIAG_RECENTE}
    assert len(col.com("sem confirmação", "AVISO")) == 1              # AVISO 1 vez (até o ack voltar)
    assert len(col.erros) == 1                                         # nenhum erro novo
    assert not [r for r in col.respostas if r[:3] == (DEV, "diag", False) and "Sem resposta" in r[3]][1:]
    # mais de 60 s sem nada: volta a ser sem resposta (novo incidente, novo erro)
    relogio.ir(t0 + 61)
    assert p.diagnosticar_dispositivos([DEV], 0.1) == {DEV: False}
    assert len(col.erros) == 2
    # mensagem retida (antiga, do broker) não conta como atividade
    relogio.ir(t0 + 200)
    cli.entregar(f"NOVUS/{DEV}/events", novus(51584019, ts=int(t0) + 1), retida=True)
    time.sleep(0.2)
    assert p.diagnosticar_dispositivos([DEV], 0.1) == {DEV: False}


def test_bridge_tem_recalcular_e_sinal():
    from app.bridge import PipelineBridge
    from tests.fake_bridge import FakePipelineBridge
    for cls in (PipelineBridge, FakePipelineBridge):
        assert hasattr(cls, "recalculo_concluido") and callable(getattr(cls, "recalcular_dia_apontamentos"))


def test_config_legenda_fora_turno():
    assert Config().apontamentos.legenda_fora_turno == "96"


# ============================================================================================ ciclo médio

from app.core.apontamentos_oee import alteracoes, ciclo_medio  # noqa: E402


def test_alteracoes_guardam_o_valor_e_marcam_o_reinicio():
    alts = alteracoes([(0, 5), (10, 6), (20, 6), (30, 9), (40, 0), (50, 1), (60, None)])
    assert alts[:2] == [(10, 6.0), (30, 9.0)]
    assert alts[2][0] == 40 and alts[2][1] != alts[2][1]          # marcador de reinício (nan)
    assert alts[3] == (50, 1.0)
    assert producoes([(0, 5), (10, 6), (20, 6), (30, 9), (40, 0), (50, 1)]) == [10, 30, 50]


def test_ciclo_medio_contador_de_1_em_1_e_a_media_dos_intervalos():
    # 15 alterações com intervalos variados; valem as 10 últimas (11 registros)
    tss = [0, 30, 50, 80, 100, 130, 150, 180, 200, 230, 250, 280, 300, 330, 350]
    alts = [(t, float(i)) for i, t in enumerate(tss)]
    ultimos = tss[-11:]
    assert ciclo_medio(alts, 10_000) == round((ultimos[-1] - ultimos[0]) / 10)   # 250/10 = 25
    assert ciclo_medio(alts, 10_000, n=2) == round((350 - 300) / 2)


def test_ciclo_medio_contador_subindo_3_por_leitura_e_o_tempo_por_peca():
    leituras = [(t, 3 * k) for k, t in enumerate(range(0, 200, 10))]   # leitura a cada 10 s, +3 peças
    assert ciclo_medio(alteracoes(leituras), 10_000) == 3                # 100 s / 30 peças = 3,33 → 3


def test_ciclo_medio_menos_de_2_alteracoes_e_none_e_teto_999():
    assert ciclo_medio([], 100) is None
    assert ciclo_medio([(10, 1.0)], 100) is None
    assert ciclo_medio([(10, 1.0), (20, 2.0)], 15) is None                 # só 1 alteração até "ate"
    assert ciclo_medio([(0, 1.0), (5000, 2.0)], 9999) == 999
    assert ciclo_medio([(0, 1.0), (5000, 2.0)], 9999, teto=100) == 100


def test_ciclo_medio_reinicio_do_contador_quebra_a_sequencia():
    leituras = [(0, 100), (10, 101), (20, 102), (30, 103), (40, 0), (100, 1), (160, 2), (220, 3)]
    alts = alteracoes(leituras)
    assert ciclo_medio(alts, 1000) == 60                                 # só depois do reinício: 1→3 em 120 s
    assert ciclo_medio(alts, 35) == 10                                   # antes do reinício: 101→103 em 20 s
    assert ciclo_medio(alts, 120) is None                                # depois do reinício só 1 alteração


def test_ciclo_medio_em_aberto_ate_agora_e_fechado_ate_o_fim():
    alts = [(t, float(i)) for i, t in enumerate([0, 10, 20, 30, 40, 100, 160, 220])]
    assert ciclo_medio(alts, 35, n=3) == 10              # fechado em 35: só o que veio até o fim
    assert ciclo_medio(alts, 500, n=3) == 60             # em aberto: até agora


def _maquina_com_ritmos(b):
    """Produz a cada 30 s das 8h às 8h30, microparada de 2 min, a cada 20 s até 9h e para (aparelho ligado)."""
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 60)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(8, 30), passo=30)            # valores 0..61
    producao_continua(b, "DEV1", "chd1_value", T(8, 32), T(9), passo=20, valor=61)  # base 61 em 8h31m40
    ligado(b, "DEV1", "chd1_value", T(9) + 20, T(9, 30), valor=61 + 85)


def test_ciclo_cada_apontamento_tem_o_ciclo_ate_o_fim_dele_e_o_aberto_ate_agora():
    b = BancoFalso()
    _maquina_com_ritmos(b)
    obj, logs, erros, res = novo(b, T(9, 30))
    r = obj.executar_ciclo()
    assert r["ok"] and not erros
    assert [x[0] for x in b.lista(1)] == [98, 2, 99, 2, 98]
    assert b.ciclos(1) == [(HOJE, None), (T(8), 30), (T(8, 30), 30), (T(8, 32), 20), (T(9), 20)]
    assert r["maquinas"][0]["ciclo_medio"] == 20                          # do apontamento mais recente
    inserts = [s for s, _p in b.comandos if s.startswith("INSERT INTO [OEE].[dbo].[apontamentos_estado]")]
    assert inserts and all("[ciclo_medio]" in s for s in inserts)
    # só o ciclo do aberto muda (nova produção mais lenta): é uma atualização normal
    producao_continua(b, "DEV1", "chd1_value", T(9, 31), T(9, 33), passo=50, valor=146)
    obj._relogio.t = T(9, 33, 30)
    r = obj.executar_ciclo()
    assert b.ciclos(1)[-1][0] == T(9, 31) and r["alterados"] >= 1


def test_ciclo_usa_alteracoes_de_antes_da_janela():
    b = BancoFalso()
    b.maquina(1)
    producao_continua(b, "DEV1", "chd1_value", T(22, dia=11), T(23, dia=11), passo=40)  # ontem: 40 s por peça
    ligado(b, "DEV1", "chd1_value", T(23, dia=11) + 60, T(1), valor=91)                  # parada desde 23h
    obj, *_ = novo(b, T(1))
    r = obj.executar_ciclo()
    assert b.lista(1) == [(98, HOJE, None, True)]
    assert b.ciclos(1) == [(HOJE, 40)] and r["maquinas"][0]["ciclo_medio"] == 40
    antes = [(s, p) for s, p in b.comandos if s.startswith("SELECT TOP (500) [ts_unix], [chd1_value]")]
    assert antes and antes[0][1][1] == HOJE - MACRO_S - ap.MARGEM_LEITURAS


def test_ciclo_consulta_de_antes_da_janela_e_limitada():
    b = BancoFalso()
    b.maquina(1)
    producao_continua(b, "DEV1", "chd1_value", T(12, dia=11), T(13, dia=11), passo=40)
    ligado(b, "DEV1", "chd1_value", T(13, dia=11) + 5, T(1), valor=91, passo=5)          # 12 h de linhas paradas
    obj, *_ = novo(b, T(1))
    obj.executar_ciclo()
    antes = [s for s, _p in b.comandos if s.startswith("SELECT TOP (500)")]
    assert len(antes) == ap.MAX_BLOCOS_CICLO
    assert b.ciclos(1) == [(HOJE, None)]                                  # produção longe demais: sem dado


def test_ciclo_coluna_ausente_segue_sem_ela_com_um_info():
    b = BancoFalso(com_ciclo=False)
    _maquina_com_ritmos(b)
    obj, logs, erros, res = novo(b, T(9, 30))
    r = obj.executar_ciclo()
    obj.executar_ciclo()
    assert r["ok"] and [x[0] for x in b.lista(1)] == [98, 2, 99, 2, 98]
    assert not any("ciclo_medio" in s or "ORDER BY [ts_unix] DESC" in s for s, _p in b.comandos)
    assert len([m for n, m in logs if "ciclo_medio" in m]) == 1
    assert r["maquinas"][0]["ciclo_medio"] is None


def test_ciclo_manual_aparado_nao_recebe_ciclo_medio():
    b = BancoFalso()
    b.maquina(1)
    manual = b.apontamento(1, SETUP, T(23, dia=11), None, iot=False)     # em aberto desde ontem
    _maquina_com_ritmos(b)
    obj, *_ = novo(b, T(9, 30))
    obj.executar_ciclo()
    assert b.dados["apont"][manual].get("ciclo") is None
    updates = [s for s, p in b.comandos if s.startswith("UPDATE [OEE].[dbo].[apontamentos_estado]") and p[-1] == manual]
    assert updates and "[ciclo_medio]" not in updates[0]


def test_planejar_com_ciclo_compara_e_leva_o_ciclo():
    d = [Trecho(HOJE, None, PRODUZINDO, 25)]
    existentes = [apont(1, 2, HOJE, None)]
    assert planejar(d, existentes, HOJE, T(9), LEG, None, FUSO).vazio               # sem a coluna: igual
    plano = planejar(d, existentes, HOJE, T(9), LEG, None, FUSO, com_ciclo=True)
    assert [(x["id"], x["ciclo_medio"]) for x in plano.atualizar] == [(1, 25)]
    assert planejar(d, [{**existentes[0], "ciclo_medio": 25}], HOJE, T(9), LEG, None, FUSO, com_ciclo=True).vazio
    plano = planejar(d, [], HOJE, T(9), LEG, None, FUSO, com_ciclo=True)
    assert plano.inserir[0]["ciclo_medio"] == 25


# ============================================================================================ auditoria e origem IoT


def _comandos_apont(b, inicio="UPDATE"):
    return [(s, p) for s, p in b.comandos if s.startswith(f"{inicio}") and "[apontamentos_estado]" in s]


def test_update_preserva_atualizado_em_de_quem_editou_e_grava_quando_ninguem_editou():
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", ONTEM - 3600, T(3))
    editado_em = _dt(T(1))
    editado = b.apontamento(1, SETUP, HOJE, None, por=7, em=editado_em)            # o operador trocou o motivo
    manual = b.apontamento(1, SETUP, T(22, dia=11), None, iot=False, por=5, em=editado_em)   # aberto desde ontem
    obj, logs, erros, _ = novo(b, T(3))
    obj.executar_ciclo()
    a = b.dados["apont"][editado]
    assert a["legenda"] == SETUP and a["dev"] == 1                    # atualizado (origem), motivo mantido
    assert a["em"] == editado_em and a["por"] == 7                    # auditoria humana intacta
    m = b.dados["apont"][manual]
    assert _ep(m["fim"]) == T(23, 59, 59, dia=11) and m["em"] == editado_em and m["por"] == 5
    assert m.get("dev") is None and m.get("canal") is None           # manual aparado não recebe origem
    ups = _comandos_apont(b)
    assert ups and all("CASE WHEN [atualizado_por_id] IS NULL THEN ? ELSE [atualizado_em] END" in s for s, _p in ups)
    assert not any("[atualizado_por_id] = ?" in s for s, _p in ups)
    # sem edição humana: o integrador grava atualizado_em
    b.dados["apont"][editado]["por"] = None
    b.commit()
    obj._relogio.t = T(3, 5)
    ligado(b, "DEV1", "chd1_value", T(3) + 60, T(3, 5))
    b.dados["apont"][editado]["dev"] = None                           # força uma atualização
    b.commit()
    obj.executar_ciclo()
    assert b.dados["apont"][editado]["em"] == ap.utc_sql(T(3, 5))


def test_sem_a_coluna_atualizado_por_id_update_como_antes():
    b = BancoFalso(com_auditoria=False)
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(3))
    b.apontamento(1, 98, HOJE, None)
    obj, *_ = novo(b, T(3))
    obj.executar_ciclo()
    ups = _comandos_apont(b)
    assert ups and all("CASE" not in s and "[atualizado_em] = ?" in s for s, _p in ups)


def test_origem_iot_no_insert_no_update_e_em_apontamento_antigo_sem_origem():
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))
    antigo = b.apontamento(1, 98, HOJE, T(8) - 1)                     # do integrador, de antes da origem existir
    obj, logs, erros, res = novo(b, T(9, 30))
    r = obj.executar_ciclo()
    assert r["ok"]
    assert all((a["dev"], a["canal"]) == (1, "chd1_value") for a in b.dados["apont"].values())
    assert b.dados["apont"][antigo]["dev"] == 1                       # ganhou a origem no ciclo
    ins = _comandos_apont(b, "INSERT")
    assert ins and all("[iot_dispositivo_id], [iot_canal]" in s for s, _p in ins)
    # nada mudou: o ciclo seguinte não atualiza só por causa da origem
    b.comandos.clear()
    obj._relogio.t = T(9, 30, 10)
    r = obj.executar_ciclo()
    assert (r["criados"], r["alterados"], r["excluidos"]) == (0, 0, 0)
    assert not _comandos_apont(b) and not _comandos_apont(b, "INSERT")


def test_colunas_de_origem_ausentes_um_info_e_nenhum_sql_as_menciona():
    b = BancoFalso(com_origem=False)
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(3))
    obj, logs, erros, _ = novo(b, T(3))
    obj.executar_ciclo()
    obj.executar_ciclo()
    assert not any("iot_canal" in s or "[iot_dispositivo_id]" in s for s, _p in b.comandos
                   if "[apontamentos_estado]" in s)
    assert len([m for n, m in logs if "iot_dispositivo_id" in m]) == 1
    assert b.lista(1) == [(98, HOJE, None, True)]


def test_maquina_com_dois_canais_vale_o_primeiro_com_coluna():
    b = BancoFalso()
    b.colunas_iot -= {"chd5_value"}
    b.maquina(1, canais=((2, "chd5_value"), (1, "chd1_value")))       # iot1 sem coluna na IOT001
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(3))
    obj, *_ = novo(b, T(3))
    obj.executar_ciclo()
    assert [(a["dev"], a["canal"]) for a in b.dados["apont"].values()] == [(1, "chd1_value")]
    b2 = BancoFalso()
    b2.maquina(1, canais=((2, "chd5_value"), (1, "chd1_value")))      # as duas colunas existem: vale iot1
    ligado(b2, "DEV1", "chd1_value", HOJE - 3600, T(3))
    obj2, *_ = novo(b2, T(3))
    obj2.executar_ciclo()
    assert [(a["dev"], a["canal"]) for a in b2.dados["apont"].values()] == [(2, "chd5_value")]


def test_planejar_com_origem_compara_e_leva_a_origem():
    d = [Trecho(HOJE, None, MACRO)]
    existentes = [apont(1, 98, HOJE, None)]
    plano = planejar(d, existentes, HOJE, T(9), LEG, None, FUSO, origem=(4, "chd2_value"))
    assert [(x["id"], x["iot_dispositivo_id"], x["iot_canal"]) for x in plano.atualizar] == [(1, 4, "chd2_value")]
    feito = {**existentes[0], "iot_dispositivo_id": 4, "iot_canal": "chd2_value"}
    assert planejar(d, [feito], HOJE, T(9), LEG, None, FUSO, origem=(4, "chd2_value")).vazio
    assert planejar(d, [], HOJE, T(9), LEG, None, FUSO, origem=(4, "chd2_value")).inserir[0]["iot_canal"] == \
        "chd2_value"


def test_ciclo_avisa_o_portal_dos_criados_e_da_legenda_trocada_depois_do_commit():
    """SS automática: o portal recebe os apontamentos criados e os que trocaram de legenda (não os que só cresceram)."""
    b = BancoFalso()
    b.maquina(1)
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))
    ligado(b, "DEV1", "chd1_value", T(9) + 10, T(9, 6), valor=361)
    obj, logs, erros, _ = novo(b, T(9, 2))
    avisados: list[list[int]] = []
    obj._avisar_portal = lambda ids, *_resto: avisados.append(list(ids))
    obj.executar_ciclo()
    assert sorted(avisados[-1]) == sorted(b.dados["apont"].keys())          # os 3 criados
    em_aberto = max(b.dados["apont"])
    obj._relogio.t = T(9, 4)
    obj.executar_ciclo()
    assert avisados[-1] == []                                               # só cresceu (99 continua): nada
    obj._relogio.t = T(9, 6)
    obj.executar_ciclo()
    assert avisados[-1] == [em_aberto]                                      # 99 → 98: legenda trocada


def test_falha_no_aviso_ao_portal_nao_derruba_o_ciclo():
    b = BancoFalso()
    b.maquina(1)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))
    obj, logs, erros, _ = novo(b, T(9, 30))

    def quebra(_ids):
        raise RuntimeError("sem rede")
    obj._avisar_portal = quebra
    r = obj.executar_ciclo()
    assert r["ok"] and not erros
    assert any("falha ao avisar o portal OEE" in m for _n, m in logs)


def test_incrementos_somam_pulsos_e_reinicio_conta_o_valor_novo():
    from app.core.apontamentos_oee import ciclos_no_trecho, incrementos
    incs = incrementos([(0, 10), (10, 11), (20, 14), (30, 14), (40, None), (50, 2), (60, 5)])
    assert incs == [(10, 1), (20, 3), (50, 2), (60, 3)]
    assert ciclos_no_trecho(incs, 10, 50) == 6


def test_ciclo_conta_os_pulsos_de_cada_trecho_de_producao_e_avisa_o_portal_das_pecas():
    """Peças: cada pulso do contador no trecho de produção (o portal multiplica pelas cavidades do molde)."""
    b = BancoFalso()
    b.maquina(1, op=40)
    b.dados["ops"][40] = {"datrf": "", "c2_status": "", "quje": 0, "status": "aberta"}
    ligado(b, "DEV1", "chd1_value", HOJE - 3600, T(8) - 20)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))           # 1 pulso a cada 10 s de 08:00 a 09:00
    ligado(b, "DEV1", "chd1_value", T(9) + 10, T(9, 10), valor=361)
    obj, logs, erros, _ = novo(b, T(9, 10))
    avisos: list = []
    obj._avisar_portal = lambda ids, producao=None, ops=None: avisos.append((list(ids), producao, ops))
    r = obj.executar_ciclo()
    assert r["ok"] and not erros
    por_inicio = {_ep(a["inicio"]): (i, a) for i, a in b.dados["apont"].items()}
    i_prod, prod = por_inicio[T(8)]
    assert prod["legenda"] == 2 and prod["ciclos"] == 361                  # 08:00:00 até 09:00:00 (inclusive)
    assert por_inicio[T(9)][1]["ciclos"] is None                          # parada não conta
    assert avisos[-1][1] == [i_prod] and avisos[-1][2] == [40]
    # Próximo ciclo sem pulsos novos: nada a refazer de peças.
    obj._relogio.t = T(9, 12)
    obj.executar_ciclo()
    assert avisos[-1][1] in (None, [])


def test_sem_a_coluna_ciclos_no_portal_nao_grava_nem_avisa_pecas():
    b = BancoFalso(com_ciclos=False)
    b.maquina(1)
    producao_continua(b, "DEV1", "chd1_value", T(8), T(9))
    obj, logs, erros, _ = novo(b, T(9, 30))
    avisos: list = []
    obj._avisar_portal = lambda ids, *resto: avisos.append(resto)
    assert obj.executar_ciclo()["ok"]
    assert avisos[-1] == () and all("ciclos" not in s for s, _p in b.comandos if s.startswith("INSERT"))
