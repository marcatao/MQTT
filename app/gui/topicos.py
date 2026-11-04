"""Diálogo "Tópicos — <device_id>" da aba Clientes (CLAUDE.md, "Tópicos por dispositivo e tópicos não ouvidos").

Cada aparelho tem 5 tópicos, com os mesmos rótulos do NXperience. O diálogo não fala com o broker: lê e grava pelo
bridge (`topicos_do_dispositivo`, `salvar_topicos_dispositivo`, `restaurar_topicos_padrao`) e pergunta a
`topico_coberto` se o integrador ouve cada tópico que o aparelho publica.

O texto do tópico é guardado exatamente como digitado ou colado: só se tira o espaço das pontas. Nada de
normalização Unicode — o NXperience do `Teste_Alumbra` grava `NOÏUS/...` no lugar de `NOVUS/...`, e o integrador
precisa usar esse texto exato para ouvir o aparelho.
"""
from __future__ import annotations

import unicodedata
from typing import Callable, Protocol

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from . import estilo

CHAVES = ("events", "config", "ack_config", "command", "ack_command")
PUBLICADOS = ("events", "ack_config", "ack_command")   # o aparelho publica, o integrador precisa ouvir
RECEBIDOS = ("config", "command")                      # o aparelho recebe, o integrador publica
ROTULOS = {
    "events": "Publicação de dados periódicos e eventos",
    "ack_config": "Confirmação de configuração",
    "ack_command": "Confirmação de comando",
    "config": "Receber configurações",
    "command": "Receber comandos",
}
DICAS = {
    "events": "Onde o aparelho publica as leituras (channels) e os eventos.",
    "ack_config": "Onde o aparelho confirma uma configuração recebida.",
    "ack_command": "Onde o aparelho responde aos comandos (diagnóstico, Zerar contadores).",
    "config": "Onde o aparelho espera receber configurações.",
    "command": "Onde o integrador publica os comandos (diagnóstico, Zerar contadores).",
}
# Sufixo do tópico → campo sugerido no menu "Usar como…" (o mais específico primeiro).
SUGESTAO_POR_SUFIXO = (("/ack/command", "ack_command"), ("/ack/config", "ack_config"), ("/events", "events"),
                       ("/command", "command"), ("/config", "config"))
DICA_NAO_OUVIDO = ("O integrador não assina este tópico agora, então não recebe o que o aparelho publica nele.\n"
                   "Ao salvar, o integrador passa a assinar o tópico.")
DICA_OUVIDO = "O integrador já recebe as mensagens publicadas neste tópico."


class ApiTopicos(Protocol):
    def topicos_do_dispositivo(self, device_id: str) -> dict[str, str]: ...
    def salvar_topicos_dispositivo(self, device_id: str, topicos: dict[str, str]) -> None: ...
    def restaurar_topicos_padrao(self, device_id: str) -> None: ...
    def topico_coberto(self, topico: str) -> bool: ...


def validar_topico(texto: str) -> str | None:
    """Mensagem de erro (português) ou None. O tópico de um aparelho é um nome concreto: sem curingas."""
    t = texto.strip()
    if not t:
        return "Preencha o tópico."
    if "+" in t or "#" in t:
        return "Use o tópico exato, sem + ou # (curingas só valem para assinar)."
    return None


def campo_sugerido(topico: str) -> str | None:
    t = topico.strip().lower()
    return next((chave for sufixo, chave in SUGESTAO_POR_SUFIXO if t.endswith(sufixo)), None)


def caracteres_especiais(texto: str) -> str | None:
    """'NOÏUS/x' → "Ï (U+00CF, LATIN CAPITAL LETTER I WITH DIAERESIS)". Ajuda a ver que não é um V."""
    vistos: list[str] = []
    for ch in texto:
        if ord(ch) > 126 or ord(ch) < 32:
            nome = unicodedata.name(ch, "sem nome")
            item = f"{ch if ch.isprintable() else ' '} (U+{ord(ch):04X}, {nome})"
            if item not in vistos:
                vistos.append(item)
    return "; ".join(vistos) if vistos else None


def confirmar_restaurar(parent: QWidget | None, device_id: str) -> bool:
    caixa = QMessageBox(parent)
    caixa.setIcon(QMessageBox.Icon.Question)
    caixa.setWindowTitle("Restaurar padrão")
    caixa.setText(f"Voltar os tópicos de {device_id} para o padrão?")
    caixa.setInformativeText("Os 5 tópicos voltam a NOVUS/{id}/… e o que foi salvo para este aparelho é apagado.")
    restaurar = caixa.addButton("Restaurar", QMessageBox.ButtonRole.AcceptRole)
    cancelar = caixa.addButton("Cancelar", QMessageBox.ButtonRole.RejectRole)
    caixa.setDefaultButton(cancelar)
    caixa.setEscapeButton(cancelar)
    caixa.exec()
    return caixa.clickedButton() is restaurar


class DialogoTopicos(QDialog):
    """Editar os 5 tópicos de um aparelho. `resultado` depois de aceito: "salvo" ou "restaurado"."""

    def __init__(self, device_id: str, api: ApiTopicos, nao_ouvidos: list[str] | None = None,
                 parent: QWidget | None = None,
                 confirmar: Callable[[QWidget | None, str], bool] | None = None):
        super().__init__(parent)
        self.device_id = device_id
        self.api = api
        self.nao_ouvidos = list(nao_ouvidos or [])
        self._confirmar = confirmar or confirmar_restaurar
        self.resultado = ""
        self.setObjectName("dialogoTopicos")
        self.setWindowTitle(f"Tópicos — {device_id}")
        self.setModal(True)
        self.setMinimumWidth(720)

        # Fonte monoespaçada nos tópicos: cada caractere com a mesma largura facilita comparar com o NXperience, e
        # Cascadia Mono/Consolas (Windows) desenham bem acentos e o trema (Ï).
        self._fonte_topico = estilo.fonte_mono()
        self._fonte_topico.setPointSizeF(10)

        corpo = QVBoxLayout(self)
        corpo.setContentsMargins(24, 24, 24, 24)
        corpo.setSpacing(16)

        titulo = QLabel(f"Tópicos — {device_id}", self)
        titulo.setObjectName("tituloDialogo")
        titulo.setWordWrap(True)
        descricao = QLabel("Os mesmos tópicos configurados no NXperience para este aparelho. Copie e cole o texto "
                           "exato: letras com acento ou trema contam.", self)
        descricao.setObjectName("descricaoDialogo")
        descricao.setWordWrap(True)
        cab = QVBoxLayout()
        cab.setSpacing(4)
        cab.addWidget(titulo)
        cab.addWidget(descricao)
        corpo.addLayout(cab)

        self.campos: dict[str, QLineEdit] = {}
        self.erros: dict[str, QLabel] = {}
        self.indicadores: dict[str, QLabel] = {}
        corpo.addWidget(self._grupo("O APARELHO PUBLICA", PUBLICADOS, indicador=True))
        corpo.addWidget(self._grupo("O APARELHO RECEBE", RECEBIDOS, indicador=False))
        corpo.addWidget(self._secao_nao_ouvidos())

        self.erro_geral = QLabel("", self)
        self.erro_geral.setObjectName("erroValidacao")
        self.erro_geral.setWordWrap(True)
        self.erro_geral.hide()
        corpo.addWidget(self.erro_geral)

        botoes = QHBoxLayout()
        botoes.setSpacing(8)
        self.botao_restaurar = QPushButton("Restaurar padrão", self)
        self.botao_restaurar.setObjectName("botaoDiscreto")
        self.botao_restaurar.setToolTip("Voltar os 5 tópicos para o padrão NOVUS/{id}/… (pede confirmação).")
        self.botao_restaurar.clicked.connect(self.restaurar)
        self.botao_cancelar = QPushButton("Cancelar", self)
        self.botao_cancelar.clicked.connect(self.reject)
        self.botao_salvar = QPushButton("Salvar", self)
        self.botao_salvar.setObjectName("botaoPrincipal")
        self.botao_salvar.setCursor(Qt.CursorShape.PointingHandCursor)
        self.botao_salvar.setToolTip("Gravar os tópicos deste aparelho. O integrador passa a usá-los na hora.")
        self.botao_salvar.setDefault(True)
        self.botao_salvar.clicked.connect(self.salvar)
        botoes.addWidget(self.botao_restaurar)
        botoes.addStretch(1)
        botoes.addWidget(self.botao_cancelar)
        botoes.addWidget(self.botao_salvar)
        corpo.addLayout(botoes)

        self.preencher(self.api.topicos_do_dispositivo(device_id))
        ordem = [self.campos[k] for k in (*PUBLICADOS, *RECEBIDOS)] + self._botoes_usar
        ordem += [self.botao_restaurar, self.botao_cancelar, self.botao_salvar]
        for a, b in zip(ordem, ordem[1:]):
            QWidget.setTabOrder(a, b)
        self.campos["events"].setFocus()

    # ----- montagem -----
    def _grupo(self, titulo: str, chaves: tuple[str, ...], indicador: bool) -> QFrame:
        grupo = QFrame(self)
        grupo.setObjectName("grupoCanais")
        lay = QVBoxLayout(grupo)
        lay.setContentsMargins(16, 12, 16, 12)
        lay.setSpacing(8)
        rotulo = QLabel(titulo, grupo)
        rotulo.setObjectName("secaoCanais")
        lay.addWidget(rotulo)
        # Rótulo em cima do campo: o tópico fica com a largura toda (tópicos longos aparecem inteiros).
        grade = QGridLayout()
        grade.setHorizontalSpacing(8)
        grade.setVerticalSpacing(4)
        grade.setColumnStretch(0, 1)
        for i, chave in enumerate(chaves):
            linha = i * 3
            nome = QLabel(ROTULOS[chave], grupo)
            nome.setObjectName("rotuloTopico")
            nome.setToolTip(DICAS[chave])
            campo = QLineEdit(grupo)
            campo.setFont(self._fonte_topico)
            campo.setPlaceholderText(f"ex.: NOVUS/{self.device_id}/…")
            campo.setToolTip(DICAS[chave])
            campo.setAccessibleName(ROTULOS[chave])
            campo.setClearButtonEnabled(True)
            nome.setBuddy(campo)
            campo.textChanged.connect(lambda _t, k=chave: self._ao_editar(k))
            erro = QLabel("", grupo)
            erro.setObjectName("erroValidacao")
            erro.setWordWrap(True)
            erro.hide()
            self.campos[chave] = campo
            self.erros[chave] = erro
            grade.addWidget(nome, linha, 0, 1, 2)
            grade.addWidget(campo, linha + 1, 0)
            if indicador:
                ind = QLabel("", grupo)
                ind.setObjectName("indicadorTopico")
                ind.setMinimumWidth(112)
                ind.setAlignment(Qt.AlignmentFlag.AlignCenter)
                self.indicadores[chave] = ind
                grade.addWidget(ind, linha + 1, 1)
            grade.addWidget(erro, linha + 2, 0, 1, 2)
            if i < len(chaves) - 1:
                grade.setRowMinimumHeight(linha + 2, 8)
        lay.addLayout(grade)
        if indicador:
            nota = QLabel("Ao salvar, o integrador passa a assinar os tópicos marcados como não ouvidos.", grupo)
            nota.setObjectName("descricaoDialogo")
            nota.setWordWrap(True)
            lay.addWidget(nota)
        return grupo

    def _secao_nao_ouvidos(self) -> QFrame:
        grupo = QFrame(self)
        grupo.setObjectName("grupoNaoOuvidos" if self.nao_ouvidos else "grupoCanais")
        lay = QVBoxLayout(grupo)
        lay.setContentsMargins(16, 12, 16, 12)
        lay.setSpacing(8)
        rotulo = QLabel("TÓPICOS PUBLICADOS POR ESTE APARELHO QUE O INTEGRADOR NÃO OUVE", grupo)
        rotulo.setObjectName("secaoCanais")
        lay.addWidget(rotulo)
        self._botoes_usar: list[QPushButton] = []
        self.itens_nao_ouvidos: list[tuple[str, QPushButton]] = []
        if not self.nao_ouvidos:
            vazio = QLabel("Nenhum. Tudo o que este aparelho publicou desde que o programa abriu é ouvido.", grupo)
            vazio.setObjectName("descricaoDialogo")
            vazio.setWordWrap(True)
            lay.addWidget(vazio)
            return grupo
        lista = QWidget(grupo)
        lay_lista = QVBoxLayout(lista)
        lay_lista.setContentsMargins(0, 0, 0, 0)
        lay_lista.setSpacing(4)
        for topico in self.nao_ouvidos:
            linha = QHBoxLayout()
            linha.setSpacing(8)
            texto = QLabel(topico, lista)
            texto.setObjectName("topicoNaoOuvido")
            texto.setFont(self._fonte_topico)
            texto.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            dica = "Visto na saída do mosquitto -v (aba Mosquitto)."
            especiais = caracteres_especiais(topico)
            if especiais:
                dica += f"\nCaracteres fora do ASCII: {especiais}"
            texto.setToolTip(dica)
            botao = QPushButton("Usar como…", lista)
            botao.setObjectName("botaoUsarComo")
            botao.setToolTip("Copiar este tópico, exatamente como está, para um dos campos acima.")
            botao.setMenu(self.menu_usar_como(topico, botao))
            linha.addWidget(texto, 1)
            linha.addWidget(botao)
            lay_lista.addLayout(linha)
            self._botoes_usar.append(botao)
            self.itens_nao_ouvidos.append((topico, botao))
        if len(self.nao_ouvidos) > 4:
            rolagem = QScrollArea(grupo)
            rolagem.setWidget(lista)
            rolagem.setWidgetResizable(True)
            rolagem.setFrameShape(QFrame.Shape.NoFrame)
            rolagem.setMaximumHeight(160)
            lay.addWidget(rolagem)
        else:
            lay.addWidget(lista)
        return grupo

    def menu_usar_como(self, topico: str, parent: QWidget) -> QMenu:
        menu = QMenu(parent)
        sugerido = campo_sugerido(topico)
        for chave in (*PUBLICADOS, *RECEBIDOS):
            acao = menu.addAction(f"Usar como {ROTULOS[chave][0].lower()}{ROTULOS[chave][1:]}")
            acao.setData(chave)
            acao.triggered.connect(lambda _c=False, k=chave, t=topico: self.usar_como(t, k))
            if chave == sugerido:
                menu.setDefaultAction(acao)  # em negrito: o campo que combina com o fim do tópico
        return menu

    # ----- dados -----
    def preencher(self, topicos: dict[str, str]) -> None:
        for chave in CHAVES:
            self.campos[chave].setText(str(topicos.get(chave) or ""))
            self.campos[chave].setCursorPosition(0)  # começo do tópico visível
        for chave in CHAVES:
            self._mostrar_erro(chave, None)
        self._atualizar_indicadores()

    def valores(self) -> dict[str, str]:
        return {chave: self.campos[chave].text().strip() for chave in CHAVES}

    def usar_como(self, topico: str, chave: str) -> None:
        self.campos[chave].setText(topico)  # o texto exato, sem normalizar
        self.campos[chave].setCursorPosition(0)
        self.campos[chave].setFocus(Qt.FocusReason.OtherFocusReason)
        self.campos[chave].deselect()

    def validar(self) -> bool:
        ok = True
        primeiro: QLineEdit | None = None
        for chave in (*PUBLICADOS, *RECEBIDOS):
            erro = validar_topico(self.campos[chave].text())
            self._mostrar_erro(chave, erro)
            if erro:
                ok = False
                primeiro = primeiro or self.campos[chave]
        if primeiro is not None:
            primeiro.setFocus()
        return ok

    def salvar(self) -> bool:
        self.erro_geral.hide()
        if not self.validar():
            return False
        try:
            self.api.salvar_topicos_dispositivo(self.device_id, self.valores())
        except Exception as exc:  # noqa: BLE001
            self.erro_geral.setText(str(exc) or "Não foi possível salvar os tópicos.")
            self.erro_geral.show()
            return False
        self.resultado = "salvo"
        self.accept()
        return True

    def restaurar(self) -> bool:
        if not self._confirmar(self, self.device_id):
            return False
        try:
            self.api.restaurar_topicos_padrao(self.device_id)
        except Exception as exc:  # noqa: BLE001
            self.erro_geral.setText(str(exc) or "Não foi possível restaurar o padrão.")
            self.erro_geral.show()
            return False
        self.preencher(self.api.topicos_do_dispositivo(self.device_id))
        self.resultado = "restaurado"
        self.accept()
        return True

    # ----- estado visual -----
    def estado_indicador(self, chave: str) -> str:
        """"ouvido" | "nao_ouvido" | "invalido" | "vazio"."""
        texto = self.campos[chave].text().strip()
        if not texto:
            return "vazio"
        if validar_topico(texto):
            return "invalido"
        try:
            coberto = bool(self.api.topico_coberto(texto))
        except Exception:  # noqa: BLE001
            return "vazio"
        return "ouvido" if coberto else "nao_ouvido"

    def _atualizar_indicadores(self) -> None:
        for chave, ind in self.indicadores.items():
            estado = self.estado_indicador(chave)
            texto, dica = {
                "ouvido": ("● ouvido", DICA_OUVIDO),
                "nao_ouvido": ("● não ouvido", DICA_NAO_OUVIDO),
                "invalido": ("tópico inválido", "Use o tópico exato, sem + ou #."),
                "vazio": ("—", "Preencha o tópico."),
            }[estado]
            ind.setText(texto)
            ind.setToolTip(dica)
            ind.setAccessibleName(f"{ROTULOS[chave]}: {texto.lstrip('● ')}")
            if ind.property("estado") != estado:
                ind.setProperty("estado", estado)
                ind.style().unpolish(ind)
                ind.style().polish(ind)

    def _ao_editar(self, chave: str) -> None:
        self._mostrar_erro(chave, None)
        self.erro_geral.hide()
        if chave in self.indicadores:
            self._atualizar_indicadores()

    def _mostrar_erro(self, chave: str, erro: str | None) -> None:
        campo, rotulo = self.campos[chave], self.erros[chave]
        rotulo.setText(erro or "")
        rotulo.setVisible(bool(erro))
        invalido = "true" if erro else "false"
        if campo.property("invalido") != invalido:
            campo.setProperty("invalido", invalido)
            campo.style().unpolish(campo)
            campo.style().polish(campo)
