"""Aba Apontamentos reais: posição na janela, tabela a partir do JSON do ciclo, cores por legenda, periodicidade
salva e enviada ao bridge, Executar agora e ciclo com falha (QT_QPA_PLATFORM=offscreen).

O bridge de teste é local (subclasse do falso de sempre com o sinal e os métodos novos), para não depender de
tests/fake_bridge.py já ter o contrato dos apontamentos."""
from __future__ import annotations

import json
import os
from datetime import datetime

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QSettings, Qt, Signal  # noqa: E402

from app.config import Config  # noqa: E402
from app.gui import estilo  # noqa: E402
from app.gui.apontamentos import (  # noqa: E402
    CHAVE_SETTINGS,
    COL_ACAO,
    COL_CANAIS,
    COL_DESDE,
    COL_DIAG,
    COL_ESTADO,
    COL_HOJE,
    COL_LEITURA,
    COL_LIMITES,
    COL_MAQUINA,
    COL_PRODUCAO,
    COL_TURNO,
    PainelApontamentos,
    TEXTO_AGUARDANDO,
    formatar_data,
    interpretar_ciclo,
    tom_legenda,
)
from app.gui.janela import JanelaPrincipal  # noqa: E402
from app.gui.widgets import PAPEL_PILULA, DelegadoPilula  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_clientes import destruir  # noqa: E402
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401

T0 = datetime(2026, 11, 12, 14, 0, 0).timestamp()


class Relogio:
    def __init__(self, t: float = T0):
        self.t = t

    def __call__(self) -> float:
        return self.t


class BridgeApontamentos(FakePipelineBridge):
    """Falso com o contrato dos apontamentos: registra as chamadas, não roda nada."""

    apontamentos_atualizados = Signal(str)
    recalculo_concluido = Signal(int, str, bool, str)

    def __init__(self, config, **kw):
        super().__init__(config, **kw)
        self.periodicidades: list[float] = []
        self.execucoes = 0
        self.recalculos: list[tuple[int, str]] = []

    def recalcular_dia_apontamentos(self, maquina_id: int, dia: str) -> None:
        self.recalculos.append((maquina_id, dia))

    def definir_periodicidade_apontamentos(self, minutos: float) -> None:
        self.periodicidades.append(minutos)

    def executar_apontamentos_agora(self) -> None:
        self.execucoes += 1


def maquina(**kw) -> dict:
    base = {"maquina_id": 1, "codigo": "INJ01", "nome": "Injetora 01", "canais": "Teste_Alumbra/chd1_value",
            "micro_s": 60, "macro_s": 300, "legenda": "2", "legenda_descricao": "Produção Normal",
            "desde": "2026-11-12T13:40:05", "ultima_producao": "2026-11-12T13:59:50", "apontamentos_hoje": 7,
            "ultima_leitura": "2026-11-12T13:59:58", "aviso": None}
    base.update(kw)
    return base


def ciclo(maquinas=None, ok=True, mensagem="", proxima="2026-11-12T14:03:00", **kw) -> str:
    d = {"quando": "2026-11-12T14:00:00", "proxima": proxima, "minutos": 3.0, "ok": ok, "mensagem": mensagem,
         "criados": 2, "alterados": 5, "excluidos": 1, "maquinas": maquinas if maquinas is not None else []}
    d.update(kw)
    return json.dumps(d)


TRES = [
    maquina(),
    maquina(maquina_id=2, codigo="INJ02", nome="Injetora 02", legenda="99", legenda_descricao="Microparada"),
    maquina(maquina_id=3, codigo="INJ03", nome="Injetora 03", legenda="98", legenda_descricao="Macro parada",
            aviso="Sem leitura há 20 min"),
    maquina(maquina_id=4, codigo="INJ04", nome="Injetora 04", legenda="7", legenda_descricao="Setup"),
]


@pytest.fixture
def novo_painel(qapp, tmp_path):
    criados = []

    def _novo(**kw):
        kw.setdefault("relogio", Relogio())
        kw.setdefault("settings", QSettings(str(tmp_path / "p.ini"), QSettings.Format.IniFormat))
        p = PainelApontamentos(**kw)
        criados.append(p)
        return p

    yield _novo
    destruir(criados)


@pytest.fixture
def janela(qapp, tmp_path):
    criadas = []

    def _montar(bridge_cls=BridgeApontamentos, settings=None, config=None):
        config = config or Config()
        config.mosquitto.iniciar_automaticamente = False
        bridge = bridge_cls(config)
        settings = settings or QSettings(str(tmp_path / "g.ini"), QSettings.Format.IniFormat)
        j = JanelaPrincipal(bridge, config, settings=settings, tema="claro", cofre=CofreFalso(),
                            executor_arp=lambda: "", enderecos_locais=set(), relogio=Relogio())
        j.show()
        criadas.append(j)
        return j, bridge, settings

    yield _montar
    destruir(criadas)


# ---------------------------------------------------------------------- funções puras
def test_formatar_data_e_tons():
    assert formatar_data("2026-11-12T13:40:05") == "12/11 13:40:05"
    assert formatar_data(None) == "—"
    assert formatar_data("ontem") == "ontem"
    assert [tom_legenda(c) for c in ("2", "99", "98", "7", None)] == ["sucesso", "aviso", "erro", "neutro", "neutro"]


def test_interpretar_ciclo_tolera_lixo():
    c = interpretar_ciclo("{não é json")
    assert not c.ok and "ilegível" in c.mensagem
    c = interpretar_ciclo(json.dumps({"ok": True, "maquinas": [{"codigo": "X", "micro_s": "45"}, 3]}))
    assert c.ok and len(c.maquinas) == 1 and c.maquinas[0].micro_s == 45 and c.maquinas[0].legenda == ""


# ---------------------------------------------------------------------- painel
def test_estado_vazio_e_json_popula_tabela(novo_painel):
    p = novo_painel()
    assert p.pilha.currentWidget() is p.vazio and TEXTO_AGUARDANDO in p.vazio.text()
    assert p.chip_ultima.valor() == "—"
    p.ao_ciclo(ciclo(TRES))
    assert p.pilha.currentWidget() is p.tabela
    linhas = p.linhas_visiveis()
    assert [l[COL_MAQUINA] for l in linhas] == ["INJ01 – Injetora 01", "INJ02 – Injetora 02", "INJ03 – Injetora 03",
                                                "INJ04 – Injetora 04"]
    primeira = linhas[0]
    assert primeira[COL_CANAIS] == "Teste_Alumbra/chd1_value"
    assert primeira[COL_LIMITES] == "60 / 300" and primeira[COL_ESTADO] == "2 · Produção Normal"
    assert primeira[COL_DESDE] == "12/11 13:40:05" and primeira[COL_PRODUCAO] == "12/11 13:59:50"
    assert primeira[COL_HOJE] == "7" and primeira[COL_LEITURA] == "12/11 13:59:58"
    assert p.resumo.isVisibleTo(p) and p.resumo.text() == "Último ciclo: 2 criados · 5 atualizados · 1 excluído"
    assert p.chip_ultima.valor() == "14:00:00 · ok" and p.chip_ultima.estado == "ok"
    assert not p.falha.isVisibleTo(p)


def test_cores_por_legenda_e_aviso(novo_painel):
    p = novo_painel()
    p.ao_ciclo(ciclo(TRES))
    m = p.filtro
    tons = {m.index(r, COL_MAQUINA).data(): m.index(r, COL_ESTADO).data(PAPEL_PILULA) for r in range(m.rowCount())}
    assert tons == {"INJ01 – Injetora 01": "sucesso", "INJ02 – Injetora 02": "aviso",
                    "INJ03 – Injetora 03": "erro", "INJ04 – Injetora 04": "neutro"}
    # Cores do tema (claro e escuro) vêm da paleta, via DelegadoPilula
    for paleta in (estilo.CLARA, estilo.ESCURA):
        p.definir_paleta(paleta)
        d: DelegadoPilula = p.delegado
        assert d.cores("sucesso") == (paleta.sucesso, paleta.sucesso_suave)
        assert d.cores("aviso") == (paleta.aviso, paleta.aviso_suave)
        assert d.cores("erro") == (paleta.erro, paleta.erro_suave)
        assert d.cores("neutro") == (paleta.texto_sec, paleta.neutro_suave)
    # Aviso: ícone na coluna Máquina e texto na dica
    linha_aviso = next(r for r in range(m.rowCount()) if m.index(r, COL_MAQUINA).data() == "INJ03 – Injetora 03")
    assert m.index(linha_aviso, COL_MAQUINA).data(Qt.ItemDataRole.DecorationRole) is not None
    assert "Sem leitura há 20 min" in m.index(linha_aviso, COL_DESDE).data(Qt.ItemDataRole.ToolTipRole)
    assert m.index(0, COL_MAQUINA).data(Qt.ItemDataRole.DecorationRole) is None


def test_ordenar_por_estado(novo_painel):
    p = novo_painel()
    p.ao_ciclo(ciclo(TRES))
    p.tabela.sortByColumn(COL_ESTADO, Qt.SortOrder.AscendingOrder)
    assert [l[COL_ESTADO].split(" ")[0] for l in p.linhas_visiveis()] == ["98", "99", "7", "2"]


def test_97_sem_apontamento_iot_tem_cor_propria_e_fica_com_as_paradas(novo_painel):
    from app.gui.apontamentos import DelegadoLegenda
    assert tom_legenda("97") == "sem_iot"
    p = novo_painel()
    assert isinstance(p.delegado, DelegadoLegenda)
    for paleta in (estilo.CLARA, estilo.ESCURA):
        p.definir_paleta(paleta)
        cor_97 = p.delegado.cores("sem_iot")
        assert cor_97 == (paleta.texto, paleta.borda_forte)
        assert cor_97 != p.delegado.cores("neutro")                  # diferente do "outro" (motivo do operador)
        assert cor_97 not in {p.delegado.cores(t) for t in ("sucesso", "aviso", "erro")}
    sem_iot = maquina(maquina_id=5, codigo="INJ05", nome="Injetora 05", legenda="97",
                      legenda_descricao="Sem apontamento IOT")
    p.ao_ciclo(ciclo(TRES + [sem_iot]))
    p.tabela.sortByColumn(COL_ESTADO, Qt.SortOrder.AscendingOrder)
    assert [l[COL_ESTADO].split(" ")[0] for l in p.linhas_visiveis()] == ["98", "97", "99", "7", "2"]


def test_contagem_regressiva_da_proxima(novo_painel):
    relogio = Relogio()
    p = novo_painel(relogio=relogio)
    p.ao_ciclo(ciclo(TRES, proxima="2026-11-12T14:03:00"))
    assert p.chip_proxima.valor() == "3:00"
    relogio.t += 75
    p.tique()
    assert p.chip_proxima.valor() == "1:45"
    relogio.t += 200
    p.tique()
    assert p.chip_proxima.valor() == "em instantes"


def test_ciclo_com_falha_mostra_mensagem_e_mantem_tabela(novo_painel):
    p = novo_painel()
    p.ao_ciclo(ciclo([], ok=False, mensagem="Sem permissão no banco OEE."))
    assert p.pilha.currentWidget() is p.vazio
    assert TEXTO_AGUARDANDO in p.vazio.text() and "Sem permissão no banco OEE." in p.vazio.text()
    assert p.falha.isVisibleTo(p) and "Sem permissão" in p.falha.texto()
    assert p.chip_ultima.valor() == "14:00:00 · falhou" and p.chip_ultima.estado == "erro"
    # Um ciclo bom e depois uma falha sem máquinas: a tabela anterior continua
    p.ao_ciclo(ciclo(TRES))
    assert not p.falha.isVisibleTo(p)
    p.ao_ciclo(ciclo([], ok=False, mensagem="Tempo esgotado."))
    assert p.pilha.currentWidget() is p.tabela and len(p.linhas_visiveis()) == 4
    assert "Tempo esgotado." in p.falha.texto()


def test_ok_sem_maquinas_explica_o_que_falta(novo_painel):
    p = novo_painel()
    p.ao_ciclo(ciclo([]))
    assert p.pilha.currentWidget() is p.vazio and "Nenhuma máquina" in p.vazio.text()


def test_periodicidade_padrao_da_config_e_salva(novo_painel, tmp_path):
    settings = QSettings(str(tmp_path / "x.ini"), QSettings.Format.IniFormat)
    p = novo_painel(settings=settings, minutos_padrao=5)
    assert p.minutos() == 5
    pedidos = []
    p.periodicidade_alterada.connect(pedidos.append)
    p.campo_minutos.setValue(12)
    assert esperar(lambda: pedidos == [12.0])
    assert int(settings.value(CHAVE_SETTINGS)) == 12
    p2 = novo_painel(settings=settings, minutos_padrao=5)
    assert p2.minutos() == 12  # QSettings vale mais que o config
    assert novo_painel(settings=QSettings(str(tmp_path / "y.ini"), QSettings.Format.IniFormat)).minutos() == 3


# ---------------------------------------------------------------------- na janela
def test_aba_depois_de_clientes(janela):
    j, _bridge, _ = janela()
    nomes = [j.abas.tabText(i) for i in range(j.abas.count())]
    assert nomes.index("Apontamentos reais") == nomes.index("Clientes") + 1
    assert j.abas.widget(nomes.index("Apontamentos reais")) is j.painel_apontamentos


def test_janela_liga_sinal_e_reaplica_periodicidade(janela, tmp_path):
    settings = QSettings(str(tmp_path / "s.ini"), QSettings.Format.IniFormat)
    settings.setValue(CHAVE_SETTINGS, 7)
    j, bridge, _ = janela(settings=settings)
    assert bridge.periodicidades == [7.0]  # reaplicada ao abrir
    assert j.painel_apontamentos.minutos() == 7
    j.painel_apontamentos.campo_minutos.setValue(10)
    assert esperar(lambda: bridge.periodicidades == [7.0, 10.0])
    assert int(settings.value(CHAVE_SETTINGS)) == 10
    bridge.apontamentos_atualizados.emit(ciclo(TRES))
    assert esperar(lambda: len(j.painel_apontamentos.linhas_visiveis()) == 4)


def test_executar_agora_desabilita_ate_proximo_sinal(janela):
    j, bridge, _ = janela()
    p = j.painel_apontamentos
    assert p.botao_executar.isEnabled()
    p.botao_executar.click()
    assert bridge.execucoes == 1
    assert not p.botao_executar.isEnabled() and p.botao_executar.text() == "Executando…"
    assert p.chip_proxima.valor() == "executando…"
    p.botao_executar.click()
    assert bridge.execucoes == 1  # desabilitado: não pede de novo
    bridge.apontamentos_atualizados.emit(ciclo(TRES))
    assert esperar(lambda: p.botao_executar.isEnabled())
    assert p.botao_executar.text() == "Executar agora"


def test_executar_agora_tempo_limite(novo_painel):
    p = novo_painel()
    assert p.executar_agora()
    assert p._timer_limite.isActive() and p._timer_limite.interval() == 120_000
    p._sem_resposta()
    assert p.botao_executar.isEnabled() and not p.executando


def test_bridge_sem_apontamentos_desabilita(janela):
    j, bridge, _ = janela(bridge_cls=FakePipelineBridge)
    p = j.painel_apontamentos
    if hasattr(bridge, "executar_apontamentos_agora"):
        pytest.skip("o bridge falso já tem o contrato dos apontamentos")
    assert not p.botao_executar.isEnabled() and not p.campo_minutos.isEnabled()
    assert "não tem o processo" in p.vazio.text()


# ---------------------------------------------------------------------- 96, turno, diagnóstico e Recalcular…
def test_96_fora_de_turno_cor_propria_ordem_e_descricao(novo_painel):
    from app.gui.apontamentos import DelegadoLegenda
    assert tom_legenda("96") == "fora_turno"
    p = novo_painel()
    assert isinstance(p.delegado, DelegadoLegenda)
    for paleta in (estilo.CLARA, estilo.ESCURA):
        p.definir_paleta(paleta)
        cor_96 = p.delegado.cores("fora_turno")
        assert cor_96 == (paleta.indigo, paleta.indigo_suave)
        outras = {p.delegado.cores(t) for t in ("sucesso", "aviso", "erro", "neutro", "sem_iot", "destaque")}
        assert cor_96 not in outras
    linhas = TRES + [
        maquina(maquina_id=5, codigo="INJ05", nome="Injetora 05", legenda="97", legenda_descricao="Sem apontamento IOT"),
        maquina(maquina_id=6, codigo="INJ06", nome="Injetora 06", legenda="96", legenda_descricao=""),
    ]
    p.ao_ciclo(ciclo(linhas))
    p.tabela.sortByColumn(COL_ESTADO, Qt.SortOrder.AscendingOrder)
    estados = [l[COL_ESTADO] for l in p.linhas_visiveis()]
    assert [e.split(" ")[0] for e in estados] == ["98", "97", "96", "99", "7", "2"]
    assert "96 · Fora de turno" in estados  # descrição de reserva quando o core não manda
    m = p.filtro
    linha_96 = next(r for r in range(m.rowCount()) if m.index(r, COL_ESTADO).data().startswith("96"))
    assert m.index(linha_96, COL_ESTADO).data(PAPEL_PILULA) == "fora_turno"


def test_colunas_turno_e_diagnostico(novo_painel):
    p = novo_painel()
    linhas = [
        maquina(maquina_id=1, codigo="A", nome="A", turno="dentro", jornada="Seg–Sex 05:00–14:48", diag="ok"),
        maquina(maquina_id=2, codigo="B", nome="B", turno="fora", jornada="Seg–Sex 05:00–14:48",
                diag="sem resposta"),
        maquina(maquina_id=3, codigo="C", nome="C", turno="sem jornada", jornada=None, diag="sem MQTT"),
        maquina(maquina_id=4, codigo="D", nome="D", diag=None),
    ]
    p.ao_ciclo(ciclo(linhas))
    m = p.filtro
    por_nome = {m.index(r, COL_MAQUINA).data(): r for r in range(m.rowCount())}

    def celula(nome, col, papel=Qt.ItemDataRole.DisplayRole):
        return m.index(por_nome[nome], col).data(papel)

    assert [celula(n, COL_TURNO) for n in "ABC"] == ["dentro", "fora", "sem jornada"]
    assert [celula(n, COL_TURNO, PAPEL_PILULA) for n in "ABC"] == ["destaque", "fora_turno", "neutro"]
    assert "Seg–Sex 05:00–14:48" in celula("A", COL_TURNO, Qt.ItemDataRole.ToolTipRole)
    assert "sem horário" in celula("C", COL_TURNO, Qt.ItemDataRole.ToolTipRole)
    assert [celula(n, COL_DIAG) for n in "ABCD"] == ["ok", "sem resposta", "sem MQTT", ""]
    assert [celula(n, COL_DIAG, PAPEL_PILULA) for n in "ABCD"] == ["sucesso", "erro", "neutro", None]
    assert "97" in celula("B", COL_DIAG, Qt.ItemDataRole.ToolTipRole)
    p.tabela.sortByColumn(COL_DIAG, Qt.SortOrder.AscendingOrder)
    assert [l[COL_MAQUINA] for l in p.linhas_visiveis()][:3] == ["B", "C", "A"]


def test_dialogo_recalcular_limite_de_dias_e_aviso(qapp):
    from datetime import date

    from app.gui.apontamentos import DialogoRecalcular
    hoje = date(2026, 11, 13)
    dlg = DialogoRecalcular("INJ01 – Injetora 01", hoje, 7)
    try:
        assert dlg.dia() == hoje
        assert dlg.campo_dia.displayFormat() == "dd/MM/yyyy"
        assert dlg.campo_dia.minimumDate().toPython() == date(2026, 11, 6)
        assert dlg.campo_dia.maximumDate().toPython() == hoje
        assert dlg.aviso.text() == ("Os apontamentos MANUAIS desta máquina a partir de 13/11/2026 serão apagados e "
                                    "a timeline será refeita pelo IoT.")
        dlg.definir_dia(date(2026, 11, 1))  # antes do limite: fica no mínimo
        assert dlg.dia() == date(2026, 11, 6) and "06/11/2026" in dlg.aviso.text()
        dlg.definir_dia(date(2026, 11, 20))  # depois de hoje: fica em hoje
        assert dlg.dia() == hoje
        assert dlg.botao_cancelar.isDefault() and dlg.botao_recalcular.text() == "Recalcular"
    finally:
        dlg.deleteLater()


def test_recalcular_chama_bridge_e_fica_recalculando_ate_o_sinal(janela):
    from datetime import date

    from app.gui.apontamentos import TEXTO_RECALCULANDO
    from app.gui.comandos import PAPEL_ACAO

    j, bridge, _ = janela()
    p = j.painel_apontamentos
    assert p.dias_correcao == 7
    bridge.apontamentos_atualizados.emit(ciclo(TRES))
    assert esperar(lambda: p.modelo.rowCount() == 4)
    vistos = []

    def escolher(dlg):
        vistos.append((dlg.campo_dia.minimumDate().toPython(), dlg.aviso.text()))
        dlg.definir_dia(date(2026, 11, 10))
        return 1  # Aceito

    p.executar_dialogo = escolher
    assert p.recalcular(2)
    assert vistos[0][0] == date(2026, 11, 5)  # hoje (12/11) − 7 dias
    assert bridge.recalculos == [(2, "2026-11-10")]
    m = p.filtro
    linha = next(r for r in range(m.rowCount()) if m.index(r, COL_MAQUINA).data() == "INJ02 – Injetora 02")
    assert m.index(linha, COL_ACAO).data(PAPEL_ACAO) == ("enviando", TEXTO_RECALCULANDO)
    assert p.recalculando(2) and not p.recalcular(2)  # não pede de novo enquanto recalcula
    # sinal de outra máquina não libera esta
    bridge.recalculo_concluido.emit(1, "2026-11-12", True, "ok")
    assert esperar(lambda: not p.recalculando(1))
    assert p.recalculando(2)
    bridge.recalculo_concluido.emit(2, "2026-11-10", True, "Timeline refeita: 12 apontamentos.")
    assert esperar(lambda: not p.recalculando(2))
    estado, _texto = m.index(linha, COL_ACAO).data(PAPEL_ACAO)
    assert estado == "ok"
    assert "12 apontamentos" in m.index(linha, COL_ACAO).data(Qt.ItemDataRole.ToolTipRole)
    assert p.resultado_recalculo.estado == "ok" and "desde 10/11/2026" in p.resultado_recalculo.texto()


def test_recalcular_cancelado_nao_chama_bridge(janela):
    j, bridge, _ = janela()
    p = j.painel_apontamentos
    bridge.apontamentos_atualizados.emit(ciclo(TRES))
    assert esperar(lambda: p.modelo.rowCount() == 4)
    p.executar_dialogo = lambda dlg: 0
    assert not p.recalcular(1)
    assert bridge.recalculos == [] and not p.recalculando(1)


def test_recalculo_com_falha_e_tempo_limite(novo_painel):
    from app.gui.comandos import PAPEL_ACAO

    relogio = Relogio()
    p = novo_painel(relogio=relogio, dias_correcao=3)
    p.definir_recalculo_disponivel(True)
    p.ao_ciclo(ciclo(TRES))
    pedidos = []
    p.recalculo_pedido.connect(lambda mid, dia: pedidos.append((mid, dia)))
    p.executar_dialogo = lambda dlg: 1
    assert p.recalcular(3) and pedidos == [(3, "2026-11-12")]
    p.ao_recalculo_concluido(3, "2026-11-12", False, "Sem permissão em apontamentos_estado.")
    assert p.resultado_recalculo.estado == "falha"
    assert "Sem permissão" in p.resultado_recalculo.texto()
    m = p.filtro
    linha = next(r for r in range(m.rowCount()) if m.index(r, COL_MAQUINA).data() == "INJ03 – Injetora 03")
    assert m.index(linha, COL_ACAO).data(PAPEL_ACAO) == ("falha", "Falhou")
    assert "Sem permissão" in m.index(linha, COL_ACAO).data(Qt.ItemDataRole.ToolTipRole)
    # Tempo-limite de 2 min sem resposta
    assert p.recalcular(1)
    relogio.t += 119
    p.tique()
    assert p.recalculando(1)
    relogio.t += 2
    p.tique()
    assert not p.recalculando(1) and p.resultado_recalculo.estado == "falha"
    assert "Sem resposta" in p.resultado_recalculo.texto()


def test_sem_recalculo_no_bridge_botao_desabilitado(janela):
    from app.gui.comandos import PAPEL_ACAO

    j, bridge, _ = janela(bridge_cls=FakePipelineBridge)
    if hasattr(bridge, "recalcular_dia_apontamentos"):
        pytest.skip("o bridge falso já tem o recálculo")
    p = j.painel_apontamentos
    p.ao_ciclo(ciclo(TRES))
    assert p.filtro.index(0, COL_ACAO).data(PAPEL_ACAO)[0] == "desabilitado"
    p.executar_dialogo = lambda dlg: 1
    assert not p.recalcular(1)
