"""Aba Clientes (v0.1.19): botão Reiniciar pelo relé e dispositivos do cadastro OEE (QT_QPA_PLATFORM=offscreen)."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QSettings, Qt  # noqa: E402
from PySide6.QtTest import QTest  # noqa: E402

from app.config import Config  # noqa: E402
from app.gui.clientes import (  # noqa: E402
    CHAVE_MOSTRAR_OEE,
    COL_HA,
    COL_IP,
    COL_MAQUINA,
    COL_NOME,
    COL_REINICIO,
    COL_STATUS,
    COL_TOPICO,
    COL_ULTIMO,
    ORDEM_VISUAL,
    STATUS_OEE,
    PainelClientes,
    epoch_de_iso_oee,
)
from app.gui.comandos import (  # noqa: E402
    AVISO_REINICIO_DESCONECTADO,
    PAPEL_REINICIO,
    texto_confirmar_reinicio,
    texto_etapa_curto,
)
from app.gui.janela import JanelaPrincipal  # noqa: E402
from app.gui.widgets import PAPEL_PILULA, PAPEL_PILULA_SELO  # noqa: E402
from tests.fake_bridge import FakePipelineBridge, dispositivos_oee_exemplo  # noqa: E402
from tests.test_clientes import SAIDA, T0, Relogio, destruir, novo_painel  # noqa: E402,F401
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401


def iso(t: float) -> str:
    return datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")




def indice_de(painel: PainelClientes, nome: str, coluna: int):
    for r in range(painel.filtro.rowCount()):
        if painel.filtro.index(r, COL_NOME).data() == nome:
            return painel.filtro.index(r, coluna)
    raise AssertionError(f"{nome} não está na lista")


def nomes(painel: PainelClientes) -> list[str]:
    return [painel.filtro.index(r, COL_NOME).data() for r in range(painel.filtro.rowCount())]


def item_oee(codigo: str, **kw) -> dict:
    base = {"codigo": codigo, "nome": codigo, "endereco": "192.168.100.64", "topico": f"NOVUS/{codigo}/events",
            "ultimo_sinal_em": iso(T0 - 3600), "protocolo": "mqtt", "maquina": f"M-{codigo}", "unidade": "manutencao"}
    base.update(kw)
    return base


# ---------------------------------------------------------------------- painel isolado (relógio falso)
class Cenario:
    def __init__(self, painel: PainelClientes, relogio: Relogio):
        self.painel, self.relogio = painel, relogio
        self.pedidos: list[str] = []
        self.logs: list[tuple[str, str]] = []
        self.confirmacoes: list[tuple] = []
        self.resposta = True
        painel.reinicio_pedido.connect(self.pedidos.append)
        painel.registrar_log.connect(lambda nivel, msg: self.logs.append((nivel, msg)))

        def confirmar(parent, device, pergunta, explicacao):
            self.confirmacoes.append((device, pergunta, explicacao))
            return self.resposta

        painel.confirmar_reinicio = confirmar


@pytest.fixture
def cenario(novo_painel):
    def _montar(conectado: bool = True, saida: int = 1, dispositivos=("dev",), settings=None) -> Cenario:
        relogio = Relogio()
        painel = novo_painel(relogio=relogio, executor_arp=lambda: "", locais=set(), settings=settings)
        painel.definir_reinicio(True, saida)
        painel.definir_conectado(conectado)
        for d in dispositivos:
            painel.ao_dispositivo_visto(d, f"NOVUS/{d}/events", iso(T0 - 5))
        return Cenario(painel, relogio)

    return _montar


def estado_reinicio(painel, nome):
    return indice_de(painel, nome, COL_REINICIO).data(PAPEL_REINICIO)


def test_textos_puros():
    pergunta, explicacao = texto_confirmar_reinicio("Prensa", 2)
    assert pergunta == "Reiniciar Prensa?"
    assert explicacao == ("O integrador vai ligar a saída 2 do aparelho, que aciona o relé e corta a alimentação por "
                          "alguns segundos. A contagem para durante o reinício.")
    assert AVISO_REINICIO_DESCONECTADO not in explicacao
    assert texto_confirmar_reinicio("Prensa", 1, desconectado=True)[1].endswith(AVISO_REINICIO_DESCONECTADO)
    assert AVISO_REINICIO_DESCONECTADO == ("O aparelho parece desconectado; o comando só chega se ele ainda estiver "
                                           "ouvindo o broker.")
    assert texto_etapa_curto("Aguardando o aparelho voltar…") == "Aguardando voltar…"
    assert texto_etapa_curto("Relé acionado") == "Relé acionado"
    assert texto_etapa_curto("") == "Reiniciando…"


def test_confirmar_chama_o_bridge_e_texto_da_confirmacao(cenario):
    c = cenario(saida=2)
    assert estado_reinicio(c.painel, "dev") == ("pronto", "Reiniciar")
    assert c.painel.reiniciar("dev") is True
    assert c.pedidos == ["dev"]
    device, pergunta, explicacao = c.confirmacoes[0]
    assert device == "dev" and pergunta == "Reiniciar dev?"
    assert "ligar a saída 2 do aparelho" in explicacao and "A contagem para durante o reinício." in explicacao
    assert AVISO_REINICIO_DESCONECTADO not in explicacao  # enviou há 5 s: conectado
    assert estado_reinicio(c.painel, "dev") == ("enviando", "Reiniciando…")
    # durante o reinício o botão não é clicável e não pede de novo
    assert c.painel.reiniciar("dev") is False and c.pedidos == ["dev"]


def test_cancelar_nao_chama_o_bridge(cenario):
    c = cenario()
    c.resposta = False
    assert c.painel.reiniciar("dev") is False
    assert len(c.confirmacoes) == 1 and c.pedidos == []
    assert estado_reinicio(c.painel, "dev") == ("pronto", "Reiniciar")


def test_etapa_muda_o_texto_e_a_dica_tem_a_etapa_completa(cenario):
    c = cenario()
    c.painel.reiniciar("dev")
    c.painel.ao_reinicio_etapa("dev", "Relé acionado")
    assert estado_reinicio(c.painel, "dev") == ("enviando", "Relé acionado")
    c.painel.ao_reinicio_etapa("dev", "Aguardando o aparelho voltar…")
    assert estado_reinicio(c.painel, "dev") == ("enviando", "Aguardando voltar…")
    dica = indice_de(c.painel, "dev", COL_REINICIO).data(Qt.ItemDataRole.ToolTipRole)
    assert "Etapa: Aguardando o aparelho voltar…" in dica
    assert indice_de(c.painel, "dev", COL_REINICIO).data() == "Aguardando voltar…"


def test_sucesso_e_falha(cenario):
    c = cenario()
    c.painel.reiniciar("dev")
    c.relogio.t = datetime(2026, 11, 13, 14, 31, 20).timestamp()
    c.painel.ao_resposta_comando("dev", "reiniciar", True, "Reiniciado: voltou em 42 s")
    assert estado_reinicio(c.painel, "dev") == ("ok", "Reiniciado às 14:31")
    dica = indice_de(c.painel, "dev", COL_REINICIO).data(Qt.ItemDataRole.ToolTipRole)
    assert "Reiniciado: voltou em 42 s" in dica
    cor = indice_de(c.painel, "dev", COL_REINICIO)
    # verde/vermelho vêm do delegate pelo estado; o texto nunca depende só da cor
    assert c.painel.delegado_reinicio.cores("ok", False)[2] == c.painel.modelo.paleta.sucesso
    assert cor.data(PAPEL_REINICIO)[0] == "ok"

    assert c.painel.reiniciar("dev") is True  # "Reiniciado" é clicável: reinicia de novo
    msg = "A saída ligou mas o aparelho não reiniciou: confira a ligação do relé"
    c.painel.ao_resposta_comando("dev", "reiniciar", False, msg)
    assert estado_reinicio(c.painel, "dev") == ("falha", "Falhou")
    assert msg in indice_de(c.painel, "dev", COL_REINICIO).data(Qt.ItemDataRole.ToolTipRole)
    assert c.painel.delegado_reinicio.cores("falha", False)[2] == c.painel.modelo.paleta.erro
    # o resultado é logado pelo core; a tela não duplica
    assert c.logs == []


def test_desabilitado_sem_broker(cenario):
    c = cenario(conectado=False)
    assert estado_reinicio(c.painel, "dev") == ("desabilitado", "Reiniciar")
    assert "Conecte ao broker" in indice_de(c.painel, "dev", COL_REINICIO).data(Qt.ItemDataRole.ToolTipRole)
    assert c.painel.reiniciar("dev") is False
    assert c.confirmacoes == [] and c.pedidos == []
    menu = c.painel.menu_da_linha(indice_de(c.painel, "dev", COL_NOME))
    acao = next(a for a in menu.actions() if a.text() == "Reiniciar…")
    assert not acao.isEnabled()
    menu.deleteLater()
    c.painel.definir_conectado(True)
    assert estado_reinicio(c.painel, "dev") == ("pronto", "Reiniciar")
    # bridge sem reiniciar_dispositivo: desabilitado mesmo conectado
    c.painel.definir_reinicio(False)
    assert estado_reinicio(c.painel, "dev")[0] == "desabilitado"


def test_aparelho_desconectado_continua_habilitado_e_avisa(cenario):
    c = cenario(dispositivos=())
    c.painel.ao_dispositivo_visto("parado", "NOVUS/parado/events", iso(T0 - 5))
    # sem leitura há mais de 10 min: parece desconectado
    c.relogio.t = T0 + 900
    assert estado_reinicio(c.painel, "parado") == ("pronto", "Reiniciar")
    assert c.painel.reiniciar("parado") is True
    assert c.confirmacoes[-1][2].endswith(AVISO_REINICIO_DESCONECTADO)
    assert c.pedidos == ["parado"]


def test_menu_de_contexto_reiniciar(cenario):
    c = cenario()
    menu = c.painel.menu_da_linha(indice_de(c.painel, "dev", COL_NOME))
    acao = next(a for a in menu.actions() if a.text() == "Reiniciar…")
    assert acao.isEnabled()
    acao.trigger()
    assert c.pedidos == ["dev"]
    menu.deleteLater()


def test_protecao_de_5_minutos_com_relogio_falso(cenario):
    c = cenario()
    c.painel.reiniciar("dev")
    c.relogio.t += 200
    c.painel.ao_reinicio_etapa("dev", "Aguardando o aparelho voltar…")  # sinal do core: recomeça a contagem
    c.relogio.t += 299
    c.painel.tique()
    assert estado_reinicio(c.painel, "dev")[0] == "enviando" and c.logs == []
    c.relogio.t += 2
    c.painel.tique()
    assert estado_reinicio(c.painel, "dev") == ("falha", "Falhou")
    assert "Sem resposta do integrador em 5 min." in indice_de(c.painel, "dev", COL_REINICIO).data(
        Qt.ItemDataRole.ToolTipRole)
    assert len(c.logs) == 1 and c.logs[0][0] == "AVISO" and "Reiniciar dev" in c.logs[0][1]
    c.painel.tique()
    assert len(c.logs) == 1  # não repete


def test_botao_no_fim_da_linha_e_clique(cenario):
    c = cenario()
    c.painel.resize(1000, 400)
    c.painel.show()
    c.painel._ajustar_colunas()
    cab = c.painel.tabela.horizontalHeader()
    visiveis = [col for col in ORDEM_VISUAL if not c.painel.tabela.isColumnHidden(col)]
    assert visiveis[-1] == COL_REINICIO
    assert cab.visualIndex(COL_REINICIO) == max(cab.visualIndex(col) for col in visiveis)
    rect = c.painel.tabela.visualRect(indice_de(c.painel, "dev", COL_REINICIO))
    pos = rect.center()
    pos.setX(rect.left() + 20)
    QTest.mouseClick(c.painel.tabela.viewport(), Qt.MouseButton.LeftButton, pos=pos)
    assert c.pedidos == ["dev"]


# ---------------------------------------------------------------------- cadastro OEE
def test_linha_nova_do_cadastro_oee(cenario):
    c = cenario(dispositivos=("dev",))
    c.painel.ao_dispositivos_oee(json.dumps([item_oee("Prensa_07", nome="Prensa 07", maquina="PRE07 — Prensa 07",
                                                      ultimo_sinal_em=iso(T0 - 7200))]))
    assert sorted(nomes(c.painel)) == ["Prensa 07", "dev"]
    p = c.painel
    assert indice_de(p, "Prensa 07", COL_STATUS).data() == STATUS_OEE == "Desconectado (cadastro OEE)"
    assert indice_de(p, "Prensa 07", COL_STATUS).data(PAPEL_PILULA) == "neutro"
    assert indice_de(p, "Prensa 07", COL_STATUS).data(PAPEL_PILULA_SELO) == "OEE"
    assert indice_de(p, "Prensa 07", COL_IP).data() == "192.168.100.64"
    assert indice_de(p, "Prensa 07", COL_TOPICO).data() == "NOVUS/Prensa_07/events"
    assert indice_de(p, "Prensa 07", COL_HA).data() == "—"
    assert indice_de(p, "Prensa 07", COL_ULTIMO).data() == "—"
    esperado = datetime.fromtimestamp(T0 - 7200).strftime("%d/%m %H:%M")  # ISO local vindo do core
    assert indice_de(p, "Prensa 07", COL_HA).data(Qt.ItemDataRole.ToolTipRole) == f"Último sinal no OEE: {esperado}"
    assert indice_de(p, "Prensa 07", COL_MAQUINA).data() == "PRE07 — Prensa 07"
    # cinza
    cor = indice_de(p, "Prensa 07", COL_NOME).data(Qt.ItemDataRole.ForegroundRole)
    assert cor.name() == p.modelo.paleta.texto_sec.lower() or cor.name() == p.modelo.paleta.texto_sec
    # chip
    assert p.texto_chip() == "1 dispositivo enviando · 1 do cadastro OEE desconectado"
    # o device_id (codigo) continua sendo o alvo dos comandos
    assert p.reiniciar("Prensa_07") is True and c.pedidos == ["Prensa_07"]
    assert c.confirmacoes[-1][2].endswith(AVISO_REINICIO_DESCONECTADO)
    # não entra no "Atualizar diagnóstico" de todos
    assert p.dispositivos_listados() == ["dev"]


def test_juncao_pelo_codigo_sem_duplicar(cenario):
    c = cenario(dispositivos=("Teste_Alumbra",))
    lista = [item_oee("Teste_Alumbra", maquina="INJ01 — Injetora 01"), item_oee("Outro")]
    c.painel.ao_dispositivos_oee(json.dumps(lista))
    c.painel.ao_dispositivos_oee(json.dumps(lista))  # a cada 1 min chega a lista inteira de novo
    assert sorted(nomes(c.painel)) == ["Outro", "Teste_Alumbra"]
    assert indice_de(c.painel, "Teste_Alumbra", COL_STATUS).data() != STATUS_OEE
    assert indice_de(c.painel, "Teste_Alumbra", COL_MAQUINA).data() == "INJ01 — Injetora 01"
    assert "INJ01" in indice_de(c.painel, "Teste_Alumbra", COL_NOME).data(Qt.ItemDataRole.ToolTipRole)
    # saiu do cadastro: a linha do OEE some; a ao vivo fica, sem máquina
    c.painel.ao_dispositivos_oee(json.dumps([]))
    assert nomes(c.painel) == ["Teste_Alumbra"]
    assert indice_de(c.painel, "Teste_Alumbra", COL_MAQUINA).data() == "—"


def test_transicao_para_ao_vivo(cenario):
    c = cenario(dispositivos=())
    c.painel.ao_dispositivos_oee(json.dumps([item_oee("Prensa_07", nome="Prensa 07"), item_oee("Forno")]))
    assert sorted(nomes(c.painel)) == ["Forno", "Prensa 07"]
    # leitura de contagem: vira a linha ao vivo normal (nome = device_id), sem duplicar
    c.painel.ao_dispositivo_visto("Prensa_07", "NOVUS/Prensa_07/events", iso(T0 - 2))
    assert sorted(nomes(c.painel)) == ["Forno", "Prensa_07"]
    assert indice_de(c.painel, "Prensa_07", COL_STATUS).data() != STATUS_OEE
    assert indice_de(c.painel, "Prensa_07", COL_HA).data() == "há 2 s"
    assert indice_de(c.painel, "Prensa_07", COL_MAQUINA).data() == "M-Prensa_07"
    # a lista seguinte do OEE não recria a linha desconectada
    c.painel.ao_dispositivos_oee(json.dumps([item_oee("Prensa_07", nome="Prensa 07"), item_oee("Forno")]))
    assert sorted(nomes(c.painel)) == ["Forno", "Prensa_07"]
    assert c.painel.texto_chip() == "1 dispositivo enviando · 1 do cadastro OEE desconectado"
    # diagnóstico também mostra que está conectado
    c.painel.ao_diagnostico("Forno", json.dumps({"sn": "1", "fw_v": "1.23", "ipv4": [192, 168, 100, 9]}))
    assert sorted(nomes(c.painel)) == ["Forno", "Prensa_07"]
    assert indice_de(c.painel, "Forno", COL_STATUS).data() != STATUS_OEE
    assert c.painel.registro.desconectados_oee() == 0
    assert c.painel.texto_chip() == "1 dispositivo enviando"


def test_transicao_com_cliente_do_mosquitto(cenario):
    c = cenario(dispositivos=())
    c.painel.ao_dispositivos_oee(json.dumps([item_oee("Teste_Alumbra")]))
    c.painel.ao_saida_mosquitto("saida", SAIDA)
    c.painel.ao_dispositivo_visto("Teste_Alumbra", "NOVUS/Teste_Alumbra/events", iso(T0 - 1))
    linhas = [l for l in c.painel.registro.linhas if l.nome == "Teste_Alumbra"]
    assert len(linhas) == 1 and not linhas[0].so_oee


def test_caixa_mostrar_cadastro_oee_e_assinantes(cenario, tmp_path):
    settings = QSettings(str(tmp_path / "c.ini"), QSettings.Format.IniFormat)
    c = cenario(settings=settings)
    p = c.painel
    assert p.opcao_oee.isChecked()  # marcada por padrão
    p.ao_dispositivos_oee(json.dumps([item_oee("Prensa_07")]))
    assert "Prensa_07" in nomes(p)
    # "Mostrar assinantes" não afeta as linhas do OEE
    p.opcao_assinantes.setChecked(True)
    assert "Prensa_07" in nomes(p)
    p.opcao_assinantes.setChecked(False)
    assert "Prensa_07" in nomes(p)
    p.opcao_oee.setChecked(False)
    assert nomes(p) == ["dev"]
    assert "do cadastro OEE" in p.texto_chip()  # a contagem continua no chip
    assert str(settings.value(CHAVE_MOSTRAR_OEE)).lower() == "false"
    outro = PainelClientes(relogio=Relogio(), executor_arp=lambda: "", locais=set(), settings=settings)
    try:
        assert not outro.opcao_oee.isChecked()
    finally:
        destruir([outro])


def test_coluna_maquina_e_formatos(cenario):
    c = cenario(dispositivos=("a", "b", "x"))
    c.painel.ao_dispositivos_oee(json.dumps([
        item_oee("a", maquina={"codigo": "INJ01", "nome": "Injetora 01"}),
        item_oee("b", maquina=None),
    ]))
    p = c.painel
    assert not p.tabela.isColumnHidden(COL_MAQUINA)
    assert ORDEM_VISUAL.index(COL_MAQUINA) == 1
    assert indice_de(p, "a", COL_MAQUINA).data() == "INJ01 — Injetora 01"
    assert indice_de(p, "b", COL_MAQUINA).data() == "—"
    assert "sem máquina" in indice_de(p, "b", COL_MAQUINA).data(Qt.ItemDataRole.ToolTipRole)
    assert "Não cadastrado" in indice_de(p, "x", COL_MAQUINA).data(Qt.ItemDataRole.ToolTipRole)


def test_ultimo_sinal_em_hora_local():
    # o core manda ISO local (já convertido do UTC do OEE); com fuso explícito, o fuso vale
    assert epoch_de_iso_oee("2026-11-13T14:31:00") == datetime(2026, 11, 13, 14, 31).timestamp()
    assert epoch_de_iso_oee("2026-11-13T14:31:00-03:00") == epoch_de_iso_oee("2026-11-13T17:31:00Z")
    assert epoch_de_iso_oee("2026-11-13T17:31:00Z") == datetime(2026, 11, 13, 17, 31, tzinfo=timezone.utc).timestamp()
    assert epoch_de_iso_oee("2026-11-13 14:31:00.123") is not None
    assert epoch_de_iso_oee(None) is None and epoch_de_iso_oee("lixo") is None


def test_json_invalido_nao_derruba(cenario):
    c = cenario()
    c.painel.ao_dispositivos_oee("{nada")
    assert nomes(c.painel) == ["dev"]
    assert c.logs and c.logs[0][0] == "AVISO"


# ---------------------------------------------------------------------- na janela, com o bridge falso
@pytest.fixture
def janela(qapp, tmp_path):
    criadas = []

    def _montar(**kw_bridge):
        config = Config()
        config.mosquitto.iniciar_automaticamente = False
        config.reinicio.saida = 2
        bridge = FakePipelineBridge(config, intervalo=10.0, **kw_bridge)
        j = JanelaPrincipal(bridge, config, settings=QSettings(str(tmp_path / "g.ini"), QSettings.Format.IniFormat),
                            tema="claro", cofre=CofreFalso(), executor_arp=lambda: "", enderecos_locais=set())
        j.resize(1440, 900)
        j.show()
        j.abas.setCurrentIndex(2)
        criadas.append(j)
        return j, bridge

    yield _montar
    destruir(criadas)


def test_janela_fluxo_completo_com_o_bridge_falso(janela):
    j, bridge = janela(responder_comandos=True, atraso_resposta=0.05)
    painel = j.painel_clientes
    j.botao_conectar.click()
    assert esperar(lambda: j.estado == "conectado", 3)
    bridge.emitir_dispositivo("dev", "NOVUS/dev/events")
    assert esperar(lambda: "dev" in nomes(painel))
    vistos = []
    painel.confirmar_reinicio = lambda parent, device, pergunta, explicacao: (vistos.append(explicacao), True)[1]
    assert painel.reiniciar("dev")
    assert "saída 2" in vistos[0]  # vem de config.reinicio.saida
    assert ("reiniciar_dispositivo", "dev") in bridge.chamadas
    assert esperar(lambda: estado_reinicio(painel, "dev")[1] == "Aguardando voltar…", 2)
    assert esperar(lambda: estado_reinicio(painel, "dev")[0] == "ok", 2)
    # desconectou: desabilitado
    j.botao_conectar.click()
    assert esperar(lambda: j.estado == "desconectado", 3)
    assert estado_reinicio(painel, "dev")[0] == "desabilitado"


def test_janela_recebe_o_cadastro_oee(janela):
    j, bridge = janela()
    painel = j.painel_clientes
    bridge.emitir_dispositivo("Teste_Alumbra", "NOVUS/Teste_Alumbra/events")
    bridge.emitir_oee(dispositivos_oee_exemplo())
    assert esperar(lambda: sorted(nomes(painel)) == ["Embaladora 02", "Prensa 07", "Teste_Alumbra"])
    assert indice_de(painel, "Teste_Alumbra", COL_MAQUINA).data() == "INJ01 – Injetora 01"
    assert indice_de(painel, "Prensa 07", COL_STATUS).data() == STATUS_OEE
    assert "Último sinal no OEE" in indice_de(painel, "Prensa 07", COL_HA).data(Qt.ItemDataRole.ToolTipRole)
    assert "Sem sinal registrado no OEE." in indice_de(painel, "Embaladora 02", COL_HA).data(
        Qt.ItemDataRole.ToolTipRole)
    assert painel.texto_chip() == "1 dispositivo enviando · 2 do cadastro OEE desconectados"


def test_colunas_cabem_em_1440(janela):
    j, bridge = janela()
    painel = j.painel_clientes
    for n in ("Teste_Alumbra", "Linha2_Forno"):
        bridge.emitir_dispositivo(n, f"NOVUS/{n}/events")
    bridge.emitir_oee(dispositivos_oee_exemplo())
    assert esperar(lambda: len(nomes(painel)) == 4)
    painel._ajustar_colunas()
    cab = painel.tabela.horizontalHeader()
    total = sum(cab.sectionSize(c) for c in range(cab.count()) if not painel.tabela.isColumnHidden(c))
    assert total <= painel.tabela.viewport().width() + 1
    assert not painel.tabela.horizontalScrollBar().isVisible()
