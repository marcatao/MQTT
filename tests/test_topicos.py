"""Tópicos por dispositivo e tópicos não ouvidos na aba Clientes (QT_QPA_PLATFORM=offscreen).

Caso real: o NXperience do `Teste_Alumbra` grava a confirmação de comando como `NOÏUS/Teste_Alumbra/ack/command`.
"""
from __future__ import annotations

import os
from datetime import datetime

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")
pytest.importorskip("paho.mqtt")

from PySide6.QtCore import QSettings, Qt  # noqa: E402
from PySide6.QtWidgets import QDialog  # noqa: E402
from PySide6.QtTest import QTest  # noqa: E402

from app.config import Config  # noqa: E402
from app.gui.clientes import COL_ACAO, COL_DIAG, COL_NOME, PainelClientes  # noqa: E402
from app.gui.comandos import PAPEL_TOPICOS  # noqa: E402
from app.gui.janela import JanelaPrincipal  # noqa: E402
from app.gui.mosquitto import _Leitor, decodificar  # noqa: E402
from app.gui.topicos import (  # noqa: E402
    DialogoTopicos,
    caracteres_especiais,
    campo_sugerido,
    validar_topico,
)
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_clientes import T0, Relogio, destruir, novo_painel  # noqa: E402,F401
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401

NOIUS = "NOÏUS/Teste_Alumbra/ack/command"
CONEXAO = (f"{int(T0) - 60}: New client connected from 192.168.100.50:51544 as DigiRail_01 (p2, c1, k60).\n"
           f"{int(T0) - 60}: New client connected from 127.0.0.1:51551 as mqtt-sqlserver-4f2a (p5, c1, k60).")


def publish(topico: str, cliente: str = "DigiRail_01", t: float = T0) -> str:
    return f"{int(t)}: Received PUBLISH from {cliente} (d0, q1, r0, m3, '{topico}', ... (96 bytes))"


def iso(t: float) -> str:
    return datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S")


def indice_de(painel: PainelClientes, nome: str, coluna: int):
    for r in range(painel.filtro.rowCount()):
        if painel.filtro.index(r, COL_NOME).data() == nome:
            return painel.filtro.index(r, coluna)
    raise AssertionError(nome)


@pytest.fixture
def cenario(novo_painel):
    """Painel com o bridge falso, conectado, e o Teste_Alumbra publicando leituras em NOVUS/Teste_Alumbra/events."""

    def _criar(conectado: bool = True):
        painel = novo_painel(relogio=Relogio(), executor_arp=lambda: "", locais=set())
        bridge = FakePipelineBridge(Config())
        painel.definir_api_topicos(bridge)
        painel.definir_conectado(conectado)
        logs: list[tuple[str, str]] = []
        painel.registrar_log.connect(lambda nivel, msg: logs.append((nivel, msg)))
        painel.ao_saida_mosquitto("saida", CONEXAO + "\n" + publish("NOVUS/Teste_Alumbra/events", t=T0 - 30))
        painel.ao_dispositivo_visto("Teste_Alumbra", "NOVUS/Teste_Alumbra/events", iso(T0 - 30))
        return painel, bridge, logs

    return _criar


def avisos(logs):
    return [m for n, m in logs if n == "AVISO"]


# ---------------------------------------------------------------------- funções puras
def test_validar_topico_e_sugestao():
    assert validar_topico("") == "Preencha o tópico."
    assert validar_topico("   ") == "Preencha o tópico."
    assert "+ ou #" in validar_topico("NOVUS/+/ack/command")
    assert "+ ou #" in validar_topico("NOVUS/x/#")
    assert validar_topico(NOIUS) is None
    assert campo_sugerido(NOIUS) == "ack_command"
    assert campo_sugerido("NOVUS/x/ack/config") == "ack_config"
    assert campo_sugerido("NOVUS/x/events") == "events"
    assert campo_sugerido("outra/coisa") is None
    assert "U+00CF" in caracteres_especiais(NOIUS)
    assert caracteres_especiais("NOVUS/x") is None


def test_fake_bridge_topico_coberto_usa_topic_matches_sub():
    bridge = FakePipelineBridge(Config())
    assert bridge.topico_coberto("NOVUS/Teste_Alumbra/ack/command")
    assert not bridge.topico_coberto(NOIUS)
    t = bridge.topicos_do_dispositivo("Teste_Alumbra")
    assert t["ack_command"] == "NOVUS/Teste_Alumbra/ack/command" and t["command"] == "NOVUS/Teste_Alumbra/command"
    bridge.salvar_topicos_dispositivo("Teste_Alumbra", {**t, "ack_command": f"  {NOIUS} "})
    assert bridge.topicos_do_dispositivo("Teste_Alumbra")["ack_command"] == NOIUS
    assert bridge.topico_coberto(NOIUS)  # desconectado: pela última assinatura + extras
    assert bridge.topicos_ouvidos() == []  # desconectado: nada assinado agora
    bridge.conectado_broker = True
    assert bridge.topicos_ouvidos() == ["NOVUS/Teste_Alumbra/#", NOIUS]
    bridge.restaurar_topicos_padrao("Teste_Alumbra")
    assert not bridge.topico_coberto(NOIUS)


# ---------------------------------------------------------------------- decodificação da saída do mosquitto
def test_decodificar_preserva_o_trema():
    texto = publish(NOIUS)
    assert NOIUS in decodificar(texto.encode("utf-8"))
    # Se o processo devolver bytes na página de código do console, o texto continua exato.
    assert NOIUS in decodificar(texto.encode("cp850"), "cp850")
    assert NOIUS in decodificar(texto.encode("cp1252"), "cp1252")


def test_leitor_decodifica_por_linha_sem_estragar_as_vizinhas():
    leitor = _Leitor(alternativa="cp850")
    utf8 = publish(NOIUS).encode("utf-8")
    console = "Aviso do Windows: acesso negado à pasta".encode("cp850")  # não é UTF-8 válido
    # Um caractere UTF-8 partido entre duas leituras também chega inteiro.
    meio = utf8.index("Ï".encode("utf-8")) + 1
    assert leitor.ler(utf8[:meio]) is None
    saida = leitor.ler(utf8[meio:] + b"\r\n" + console + b"\r\n")
    linhas = saida.split("\n")
    assert NOIUS in linhas[0]
    assert linhas[1] == "Aviso do Windows: acesso negado à pasta"


# ---------------------------------------------------------------------- diálogo
def test_dialogo_preenche_e_salva_com_o_trema_preservado(cenario):
    painel, bridge, _logs = cenario()
    salvos = {}

    def executar(dlg: DialogoTopicos):
        assert dlg.windowTitle() == "Tópicos — Teste_Alumbra"
        assert dlg.valores() == bridge.topicos_padrao("Teste_Alumbra")
        dlg.campos["ack_command"].setText(f"  {NOIUS}  ")
        dlg.botao_salvar.click()
        salvos.update(bridge.topicos_do_dispositivo("Teste_Alumbra"))
        return dlg.result()

    painel.executar_dialogo = executar
    assert painel.abrir_topicos("Teste_Alumbra")
    assert salvos["ack_command"] == NOIUS  # só strip nas pontas; o Ï continua Ï
    assert salvos["ack_command"].encode("utf-8") == NOIUS.encode("utf-8")
    chamada = next(c for c in bridge.chamadas if c[0] == "salvar_topicos_dispositivo")
    assert chamada[2]["ack_command"] == NOIUS
    # Ao abrir de novo, vem o que foi salvo.
    vistos = []
    painel.executar_dialogo = lambda dlg: (vistos.append(dlg.valores()), QDialog.DialogCode.Rejected)[1]
    assert not painel.abrir_topicos("Teste_Alumbra")
    assert vistos[0]["ack_command"] == NOIUS


def test_validacao_mostra_erro_no_campo(cenario):
    painel, bridge, _logs = cenario()
    dlg = DialogoTopicos("Teste_Alumbra", bridge, [], painel)
    dlg.campos["events"].setText("   ")
    dlg.campos["command"].setText("NOVUS/+/command")
    assert not dlg.salvar()
    assert dlg.result() != QDialog.DialogCode.Accepted
    assert not any(c[0] == "salvar_topicos_dispositivo" for c in bridge.chamadas)
    assert dlg.campos["events"].property("invalido") == "true"
    assert dlg.erros["events"].text() == "Preencha o tópico." and not dlg.erros["events"].isHidden()
    assert "+ ou #" in dlg.erros["command"].text()
    assert dlg.campos["ack_command"].property("invalido") == "false"
    # Ao corrigir o campo, o erro some.
    dlg.campos["events"].setText("NOVUS/Teste_Alumbra/events")
    assert dlg.erros["events"].isHidden() and dlg.campos["events"].property("invalido") == "false"
    dlg.campos["command"].setText("NOVUS/Teste_Alumbra/command")
    assert dlg.salvar()
    dlg.deleteLater()


def test_restaurar_padrao_com_confirmacao(cenario):
    painel, bridge, logs = cenario()
    t = bridge.topicos_do_dispositivo("Teste_Alumbra")
    bridge.salvar_topicos_dispositivo("Teste_Alumbra", {**t, "ack_command": NOIUS})
    perguntas = []

    # Primeiro o usuário desiste na confirmação: nada muda.
    def nao(parent, device):
        perguntas.append(device)
        return False

    dlg = DialogoTopicos("Teste_Alumbra", bridge, [], painel, confirmar=nao)
    assert dlg.campos["ack_command"].text() == NOIUS
    assert not dlg.restaurar()
    assert perguntas == ["Teste_Alumbra"]
    assert bridge.topicos_do_dispositivo("Teste_Alumbra")["ack_command"] == NOIUS
    dlg.deleteLater()

    painel.confirmar_restaurar = lambda parent, device: True
    painel.executar_dialogo = lambda dlg: (dlg.botao_restaurar.click(), dlg.result())[1]
    assert painel.abrir_topicos("Teste_Alumbra")
    assert ("restaurar_topicos_padrao", "Teste_Alumbra") in bridge.chamadas
    assert bridge.topicos_do_dispositivo("Teste_Alumbra") == bridge.topicos_padrao("Teste_Alumbra")
    assert not any("Tópicos de" in m for _n, m in logs)  # o log de restaurar vem do core


def test_indicador_ouvido_muda_ao_digitar(cenario):
    painel, bridge, _logs = cenario()
    dlg = DialogoTopicos("Teste_Alumbra", bridge, [], painel)
    ind = dlg.indicadores["ack_command"]
    assert set(dlg.indicadores) == {"events", "ack_config", "ack_command"}  # só os que o aparelho publica
    assert ind.property("estado") == "ouvido" and "não" not in ind.text()
    dlg.campos["ack_command"].setText(NOIUS)
    assert ind.property("estado") == "nao_ouvido" and ind.text() == "● não ouvido"
    assert "Ao salvar, o integrador passa a assinar" in ind.toolTip()
    dlg.campos["ack_command"].setText("NOVUS/Teste_Alumbra/#")
    assert ind.property("estado") == "invalido"
    dlg.campos["ack_command"].setText("")
    assert ind.property("estado") == "vazio"
    dlg.campos["ack_command"].setText("NOVUS/Teste_Alumbra/ack/command")
    assert ind.property("estado") == "ouvido"
    dlg.deleteLater()


def test_usar_como_preenche_o_campo(cenario):
    painel, bridge, _logs = cenario()
    dlg = DialogoTopicos("Teste_Alumbra", bridge, [NOIUS], painel)
    assert [t for t, _b in dlg.itens_nao_ouvidos] == [NOIUS]
    menu = dlg.itens_nao_ouvidos[0][1].menu()
    textos = [a.text() for a in menu.actions()]
    assert textos == ["Usar como publicação de dados periódicos e eventos", "Usar como confirmação de configuração",
                      "Usar como confirmação de comando", "Usar como receber configurações",
                      "Usar como receber comandos"]
    assert menu.defaultAction().data() == "ack_command"  # sugerido pelo fim do tópico
    next(a for a in menu.actions() if a.data() == "ack_command").trigger()
    assert dlg.campos["ack_command"].text() == NOIUS
    assert dlg.indicadores["ack_command"].property("estado") == "nao_ouvido"
    # Outro campo também pode receber o texto exato.
    next(a for a in menu.actions() if a.data() == "ack_config").trigger()
    assert dlg.campos["ack_config"].text() == NOIUS
    dlg.deleteLater()


# ---------------------------------------------------------------------- tópicos não ouvidos (mosquitto -v)
def test_topico_nao_ouvido_gera_um_aviso_e_o_alerta_na_linha(cenario):
    painel, _bridge, logs = cenario()
    assert avisos(logs) == []
    indice = indice_de(painel, "Teste_Alumbra", COL_NOME)
    assert indice.data(Qt.ItemDataRole.DecorationRole) is None
    painel.ao_saida_mosquitto("saida", publish(NOIUS))
    painel.ao_saida_mosquitto("saida", publish(NOIUS, t=T0 + 10) + "\n" + publish(NOIUS, t=T0 + 20))
    assert avisos(logs) == [f"Teste_Alumbra publica em '{NOIUS}', que o integrador não está ouvindo. "
                            "Abra Clientes → Tópicos… para usar esse tópico."]
    indice = indice_de(painel, "Teste_Alumbra", COL_NOME)
    assert indice.data(Qt.ItemDataRole.DecorationRole) is not None  # o ⚠
    dica = indice.data(Qt.ItemDataRole.ToolTipRole)
    assert "não ouve" in dica and NOIUS in dica
    assert indice_de(painel, "Teste_Alumbra", COL_ACAO).data(PAPEL_TOPICOS) == "aviso"
    assert painel.registro.nao_ouvidos_do_dispositivo("Teste_Alumbra") == [NOIUS]
    # O texto do nome continua limpo (ordenação, filtro e CSV não mudam).
    assert indice.data() == "Teste_Alumbra"
    # O próprio integrador publicando comandos não gera aviso.
    painel.ao_saida_mosquitto("saida", publish("NOVUS/Outro/command", cliente="mqtt-sqlserver-4f2a"))
    assert len(avisos(logs)) == 1


def test_antes_de_conectar_nao_avisa_e_confere_ao_conectar(cenario):
    painel, _bridge, logs = cenario(conectado=False)
    painel.ao_saida_mosquitto("saida", publish(NOIUS))
    assert avisos(logs) == []
    painel.definir_conectado(True)
    assert len(avisos(logs)) == 1 and NOIUS in avisos(logs)[0]
    assert indice_de(painel, "Teste_Alumbra", COL_NOME).data(Qt.ItemDataRole.DecorationRole) is not None


def test_depois_de_salvar_o_alerta_some(cenario):
    painel, bridge, logs = cenario()
    painel.ao_saida_mosquitto("saida", publish(NOIUS))
    listados = []

    def executar(dlg: DialogoTopicos):
        listados.extend(t for t, _b in dlg.itens_nao_ouvidos)
        menu = dlg.itens_nao_ouvidos[0][1].menu()
        next(a for a in menu.actions() if a.data() == "ack_command").trigger()
        dlg.botao_salvar.click()
        return dlg.result()

    painel.executar_dialogo = executar
    assert painel.abrir_topicos("Teste_Alumbra")
    assert listados == [NOIUS]
    assert bridge.topico_coberto(NOIUS)
    indice = indice_de(painel, "Teste_Alumbra", COL_NOME)
    assert indice.data(Qt.ItemDataRole.DecorationRole) is None
    assert "não ouve" not in indice.data(Qt.ItemDataRole.ToolTipRole)
    assert indice_de(painel, "Teste_Alumbra", COL_ACAO).data(PAPEL_TOPICOS) == "normal"
    assert painel.registro.nao_ouvidos_do_dispositivo("Teste_Alumbra") == []
    # Publicar de novo no tópico (agora ouvido) não avisa.
    painel.ao_saida_mosquitto("saida", publish(NOIUS, t=T0 + 30))
    assert len(avisos(logs)) == 1
    # Restaurar o padrão faz o tópico voltar a não ser ouvido: o ⚠ volta, mas o aviso não se repete na sessão.
    painel.confirmar_restaurar = lambda parent, device: True
    painel.executar_dialogo = lambda dlg: (dlg.botao_restaurar.click(), dlg.result())[1]
    assert painel.abrir_topicos("Teste_Alumbra")
    assert indice_de(painel, "Teste_Alumbra", COL_NOME).data(Qt.ItemDataRole.DecorationRole) is not None
    assert len(avisos(logs)) == 1


def test_menu_da_linha_e_botao_topicos_abrem_o_dialogo(cenario):
    painel, _bridge, _logs = cenario()
    painel.resize(1400, 400)
    painel.show()
    painel._ajustar_colunas()
    abertos = []
    painel.executar_dialogo = lambda dlg: (abertos.append(dlg.device_id), QDialog.DialogCode.Rejected)[1]
    menu = painel.menu_da_linha(indice_de(painel, "Teste_Alumbra", COL_NOME))
    acao = next(a for a in menu.actions() if a.text() == "Tópicos…")
    assert acao.isEnabled()
    acao.trigger()
    assert abertos == ["Teste_Alumbra"]
    # Botão "Tópicos" ao lado de Zerar contadores, na coluna Ação.
    indice = indice_de(painel, "Teste_Alumbra", COL_ACAO)
    from PySide6.QtWidgets import QStyleOptionViewItem

    opcao = QStyleOptionViewItem()
    opcao.initFrom(painel.tabela)
    opcao.rect = painel.tabela.visualRect(indice)
    opcao.font = painel.tabela.font()
    caixa = painel.delegado_acao.retangulo_topicos(opcao, indice.data())
    principal = painel.delegado_acao.retangulo_botao(opcao, indice.data(), com_topicos=True)
    assert caixa.width() >= 28 and caixa.left() > principal.right()
    assert caixa.right() <= opcao.rect.right()  # cabe na coluna, nada cortado
    QTest.mouseClick(painel.tabela.viewport(), Qt.MouseButton.LeftButton, pos=caixa.center().toPoint())
    assert abertos == ["Teste_Alumbra", "Teste_Alumbra"]


def test_sem_api_de_topicos_nao_ha_botao_nem_menu(novo_painel):
    painel = novo_painel(relogio=Relogio(), executor_arp=lambda: "", locais=set())
    painel.definir_conectado(True)
    painel.ao_saida_mosquitto("saida", CONEXAO + "\n" + publish(NOIUS))
    painel.ao_dispositivo_visto("Teste_Alumbra", NOIUS, iso(T0))
    assert indice_de(painel, "Teste_Alumbra", COL_ACAO).data(PAPEL_TOPICOS) is None
    menu = painel.menu_da_linha(indice_de(painel, "Teste_Alumbra", COL_NOME))
    assert not next(a for a in menu.actions() if a.text() == "Tópicos…").isEnabled()
    assert not painel.abrir_topicos("Teste_Alumbra")


# ---------------------------------------------------------------------- Diagnóstico "sem resposta"
def test_diagnostico_sem_resposta_e_limpeza(cenario):
    painel, bridge, _logs = cenario()
    painel.ao_resposta_comando("Teste_Alumbra", "diag", False, "Sem resposta do dispositivo em 15 s.")
    indice = indice_de(painel, "Teste_Alumbra", COL_DIAG)
    assert indice.data() == "sem resposta"
    assert indice.data(Qt.ItemDataRole.ForegroundRole).name().lower() == painel.modelo.paleta.aviso.lower()
    dica = indice.data(Qt.ItemDataRole.ToolTipRole)
    hora = datetime.fromtimestamp(T0).strftime("%H:%M:%S")
    assert "Sem resposta do dispositivo em 15 s." in dica and hora in dica
    # Um diagnóstico recebido depois limpa o estado.
    painel.ao_diagnostico("Teste_Alumbra", '{"sn": "21004417", "fw_v": "1.23", "curr_rssi": "55"}')
    indice = indice_de(painel, "Teste_Alumbra", COL_DIAG)
    assert indice.data() == hora
    assert "Sem resposta" not in (indice.data(Qt.ItemDataRole.ToolTipRole) or "")


# ---------------------------------------------------------------------- ordenação e filtro
def test_ordenacao_e_filtro_continuam_ok(cenario):
    painel, bridge, _logs = cenario()
    bridge._assinatura = "NOVUS/#"  # como se o usuário assinasse tudo de NOVUS: só o NOÏUS fica de fora
    painel.reavaliar_topicos()
    outros = [
        f"{int(T0) - 50}: New client connected from 192.168.100.57:49152 as DigiRail_02 (p2, c1, k60).",
        publish("NOVUS/Alfa/events", cliente="DigiRail_02"),
        f"{int(T0) - 50}: New client connected from 127.0.0.1:51600 as mosq-Zx81kPq (p2, c1, k60).",
        f"{int(T0) - 50}: Received SUBSCRIBE from mosq-Zx81kPq",
        publish(NOIUS),
        publish("NOVUS/Zeta/events", cliente="DigiRail_03"),
    ]
    painel.ao_saida_mosquitto("saida", "\n".join(outros))
    painel.ao_dispositivo_visto("Alfa", "NOVUS/Alfa/events", iso(T0))
    painel.ao_dispositivo_visto("Zeta", "NOVUS/Zeta/events", iso(T0))
    painel.ao_resposta_comando("Zeta", "diag", False, "Sem resposta do dispositivo em 15 s.")
    for ordem in (Qt.SortOrder.AscendingOrder, Qt.SortOrder.DescendingOrder):
        painel.tabela.sortByColumn(COL_NOME, ordem)
        nomes = [painel.filtro.index(r, COL_NOME).data() for r in range(painel.filtro.rowCount())]
        assert nomes == sorted(["Alfa", "Teste_Alumbra", "Zeta"], reverse=ordem == Qt.SortOrder.DescendingOrder)
        for r, nome in enumerate(nomes):
            alerta = painel.filtro.index(r, COL_NOME).data(Qt.ItemDataRole.DecorationRole) is not None
            assert alerta == (nome == "Teste_Alumbra")
            assert (painel.filtro.index(r, COL_DIAG).data() == "sem resposta") == (nome == "Zeta")
    painel.tabela.sortByColumn(COL_DIAG, Qt.SortOrder.AscendingOrder)
    assert painel.filtro.rowCount() == 3
    # O assinante (mosquitto_sub) continua escondido por padrão e aparece com "Mostrar assinantes".
    painel.opcao_assinantes.setChecked(True)
    assert painel.filtro.rowCount() == 5  # + mosq-… e o próprio integrador
    painel.opcao_assinantes.setChecked(False)
    assert painel.filtro.rowCount() == 3


# ---------------------------------------------------------------------- janela
def test_janela_liga_a_api_de_topicos(qapp, tmp_path):
    config = Config()
    config.mosquitto.iniciar_automaticamente = False
    bridge = FakePipelineBridge(config, intervalo=10.0, responder_comandos=False)
    j = JanelaPrincipal(bridge, config, settings=QSettings(str(tmp_path / "g.ini"), QSettings.Format.IniFormat),
                        tema="claro", cofre=CofreFalso(), executor_arp=lambda: "", enderecos_locais=set())
    try:
        painel = j.painel_clientes
        assert painel.api_topicos is bridge
        j.botao_conectar.click()
        assert esperar(lambda: j.estado == "conectado", 3)
        j.mosquitto.saida.emit("saida", CONEXAO + "\n" + publish("NOVUS/Teste_Alumbra/events") + "\n"
                               + publish(NOIUS))
        bridge.emitir_dispositivo("Teste_Alumbra", "NOVUS/Teste_Alumbra/events", iso(T0))
        assert esperar(lambda: NOIUS in j.logs.caixa.toPlainText())
        assert painel.registro.nao_ouvidos_do_dispositivo("Teste_Alumbra") == [NOIUS]
    finally:
        destruir([j])


def test_erro_do_bridge_ao_salvar_mantem_o_dialogo_aberto(cenario):
    """O bridge levanta ValueError (tópico inválido ou arquivo que não pode ser gravado): nada é salvo, a mensagem
    aparece no diálogo e ele continua aberto."""
    painel, bridge, _logs = cenario()
    # Tópico inválido que passa pela validação da tela (o bridge é a palavra final): o fake levanta ValueError.
    with pytest.raises(ValueError):
        bridge.salvar_topicos_dispositivo("Teste_Alumbra", {**bridge.topicos_padrao("Teste_Alumbra"),
                                                           "command": "NOVUS/+/command"})
    dlg = DialogoTopicos("Teste_Alumbra", bridge, [], painel)
    original = bridge.salvar_topicos_dispositivo

    def falhar(device_id, topicos):
        original(device_id, topicos)  # valida como o real...
        raise ValueError("Não foi possível gravar dispositivos.json: acesso negado.")

    bridge.salvar_topicos_dispositivo = falhar
    bridge._topicos_salvos.clear()
    dlg.campos["ack_command"].setText(NOIUS)
    assert not dlg.salvar()
    assert dlg.result() != QDialog.DialogCode.Accepted and dlg.resultado == ""
    assert not dlg.erro_geral.isHidden()
    assert "acesso negado" in dlg.erro_geral.text()
    # Editar de novo esconde a mensagem; Restaurar padrão com erro também mantém o diálogo aberto.
    dlg.campos["ack_command"].setText(NOIUS + "x")
    assert dlg.erro_geral.isHidden()

    def falhar_restaurar(device_id):
        raise ValueError("Não foi possível gravar dispositivos.json: disco cheio.")

    bridge.restaurar_topicos_padrao = falhar_restaurar
    dlg._confirmar = lambda parent, device: True
    assert not dlg.restaurar()
    assert "disco cheio" in dlg.erro_geral.text() and dlg.result() != QDialog.DialogCode.Accepted
    dlg.deleteLater()
