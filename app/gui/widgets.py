"""Componentes da tela: indicador de status, cartão de contador, seletor de QoS, chips, painel de logs e console."""
from __future__ import annotations

import threading
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Callable

from PySide6.QtCore import QLocale, QModelIndex, QObject, QRectF, QTimer, Signal, Qt
from PySide6.QtGui import QColor, QFont, QFontMetrics, QGuiApplication, QKeyEvent, QMouseEvent, QPainter, QPen, QTextCharFormat, QTextCursor
from PySide6.QtWidgets import (
    QApplication,
    QBoxLayout,
    QButtonGroup,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QToolTip,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from .estilo import CLARA, Paleta, fonte_mono

LOCALE_BR = QLocale(QLocale.Language.Portuguese, QLocale.Country.Brazil)
# Papel do modelo com o "tom" de uma pílula (DelegadoPilula):
# "sucesso" | "aviso" | "erro" | "destaque" | "roxo" | "ciano" | "neutro"
PAPEL_PILULA = Qt.ItemDataRole.UserRole + 1
PAPEL_PILULA_TEXTO = Qt.ItemDataRole.UserRole + 41  # texto da pílula, quando mais curto que o da célula (opcional)
PAPEL_PILULA_SELO = Qt.ItemDataRole.UserRole + 42   # selo contornado depois da pílula, ex.: "OEE" (opcional)


class _Mensageiro(QObject):
    """Leva o resultado de uma thread de trabalho à thread da GUI. Sem pai e guardado em `_MENSAGEIROS` até a
    entrega: a thread nunca emite num objeto que a tela possa estar destruindo."""

    pronto = Signal(object, str)


_MENSAGEIROS: set[_Mensageiro] = set()


def rodar_em_thread(funcao: Callable[[], object], ao_terminar: Callable[[object, str], None],
                    nome: str = "trabalho") -> None:
    """Roda `funcao()` numa thread daemon (nunca na thread da GUI) e chama `ao_terminar(resultado, erro)` na thread
    da GUI. `ao_terminar` deve ser método de um QObject: se ele for destruído antes, a entrega é descartada."""
    m = _Mensageiro()
    _MENSAGEIROS.add(m)
    m.pronto.connect(ao_terminar)

    def limpar(*_args) -> None:
        _MENSAGEIROS.discard(m)
        m.deleteLater()

    m.pronto.connect(limpar)

    def trabalho() -> None:
        try:
            resultado, erro = funcao(), ""
        except Exception as exc:  # noqa: BLE001 - qualquer falha vira mensagem, nunca derruba a tela
            resultado, erro = None, str(exc) or type(exc).__name__
        m.pronto.emit(resultado, erro)

    threading.Thread(target=trabalho, name=nome, daemon=True).start()


def refiltrar(proxy) -> None:
    """Reaplica o filtro de linhas de um QSortFilterProxyModel (API nova do Qt 6.10+; antiga como reserva)."""
    if hasattr(proxy, "beginFilterChange"):
        proxy.beginFilterChange()
        try:
            from PySide6.QtCore import QSortFilterProxyModel

            proxy.endFilterChange(QSortFilterProxyModel.Direction.Rows)
        except (AttributeError, TypeError):
            proxy.endFilterChange()
    else:
        proxy.invalidateFilter()


def avisar(widget: QWidget, texto: str) -> None:
    """Aviso curto junto do widget (sem caixa de diálogo modal)."""
    QToolTip.showText(widget.mapToGlobal(widget.rect().bottomLeft()), texto, widget)


def repolir(*widgets: QWidget) -> None:
    """Reaplica o QSS depois de mudar uma propriedade dinâmica usada em seletores."""
    for w in widgets:
        w.style().unpolish(w)
        w.style().polish(w)
        w.update()


class Cartao(QFrame):
    """Superfície branca (ou escura) com borda e cantos arredondados."""

    def __init__(self, parent: QWidget | None = None, margem: int = 16, espaco: int = 12):
        super().__init__(parent)
        self.setObjectName("cartao")
        self.corpo = QVBoxLayout(self)
        self.corpo.setContentsMargins(margem, margem, margem, margem)
        self.corpo.setSpacing(espaco)


class IndicadorStatus(QFrame):
    """Pílula com ponto colorido e texto. Nunca depende só da cor."""

    TEXTOS = {
        "desconectado": "Desconectado",
        "conectando": "Conectando…",
        "conectado": "Conectado",
        "erro": "Erro",
    }

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("statusPill")
        self.setAccessibleName("Status da conexão")
        lay = QHBoxLayout(self)
        lay.setContentsMargins(12, 0, 14, 0)
        lay.setSpacing(8)
        self._ponto = QLabel(self)
        self._ponto.setObjectName("statusPonto")
        self._ponto.setFixedSize(10, 10)
        self._texto = QLabel(self)
        self._texto.setObjectName("statusTexto")
        lay.addWidget(self._ponto)
        lay.addWidget(self._texto)
        self.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)

        self._pulso = False
        self._timer_pulso = QTimer(self)
        self._timer_pulso.setInterval(550)
        self._timer_pulso.timeout.connect(self._pulsar)
        self.estado = ""
        self.definir("desconectado")

    def definir(self, estado: str) -> None:
        estado = estado if estado in self.TEXTOS else "erro"
        self.estado = estado
        self._texto.setText(self.TEXTOS[estado])
        self.setAccessibleDescription(self.TEXTOS[estado])
        for w in (self, self._ponto):
            w.setProperty("estado", estado)
        self._pulso = False
        self._ponto.setProperty("pulso", False)
        if estado == "conectando":
            self._timer_pulso.start()
        else:
            self._timer_pulso.stop()
        repolir(self, self._ponto, self._texto)

    def texto(self) -> str:
        return self._texto.text()

    def _pulsar(self) -> None:
        self._pulso = not self._pulso
        self._ponto.setProperty("pulso", self._pulso)
        repolir(self._ponto)


class CartaoContador(Cartao):
    """Contador grande. Com `clicavel`, vira um botão: mãozinha, hover, foco, Enter/Espaço e o sinal `clicado`."""

    clicado = Signal()

    def __init__(self, titulo: str, dica: str, tipo: str, parent: QWidget | None = None,
                 clicavel: bool = False, dica_clique: str = "", texto_acao: str = ""):
        super().__init__(parent, margem=16, espaco=2)
        self._tipo = tipo
        self.clicavel = clicavel
        topo = QHBoxLayout()
        topo.setSpacing(8)
        marca = QFrame(self)
        marca.setObjectName("marcaContador")
        marca.setProperty("tipo", tipo)
        marca.setFixedSize(4, 14)
        rotulo = QLabel(titulo.upper(), self)
        rotulo.setObjectName("rotuloCampo")
        topo.addWidget(marca)
        topo.addWidget(rotulo)
        topo.addStretch(1)
        self.acao: QLabel | None = None
        if clicavel:
            self.setProperty("clicavel", True)
            self.setCursor(Qt.CursorShape.PointingHandCursor)
            self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
            self.setAttribute(Qt.WidgetAttribute.WA_Hover, True)
            self.setToolTip(dica_clique)
            self.setAccessibleDescription(dica_clique)
            if texto_acao:
                self.acao = QLabel(texto_acao, self)
                self.acao.setObjectName("acaoContador")
                topo.addWidget(self.acao)
        self._valor = QLabel("0", self)
        self._valor.setObjectName("valorContador")
        if not clicavel:  # texto selecionável "engoliria" o clique
            self._valor.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        dica_lbl = QLabel(dica, self)
        dica_lbl.setObjectName("dicaContador")
        self.corpo.addLayout(topo)
        self.corpo.addSpacing(4)
        self.corpo.addWidget(self._valor)
        self.corpo.addWidget(dica_lbl)
        self.setAccessibleName(titulo)
        self.valor = 0

    def definir(self, valor: int) -> None:
        if valor == self.valor and self._valor.text() != "":
            return
        self.valor = valor
        self._valor.setText(LOCALE_BR.toString(int(valor)))
        alerta = self._tipo == "erros" and valor > 0
        if bool(self._valor.property("alerta")) != alerta:
            self._valor.setProperty("alerta", alerta)
            repolir(self._valor)

    def mouseReleaseEvent(self, evento: QMouseEvent) -> None:  # noqa: N802
        if (self.clicavel and evento.button() == Qt.MouseButton.LeftButton
                and self.rect().contains(evento.position().toPoint())):
            self.clicado.emit()
            evento.accept()
            return
        super().mouseReleaseEvent(evento)

    def keyPressEvent(self, evento: QKeyEvent) -> None:  # noqa: N802
        if self.clicavel and evento.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter, Qt.Key.Key_Space):
            self.clicado.emit()
            evento.accept()
            return
        super().keyPressEvent(evento)


class SeletorSegmentado(QWidget):
    """Controle segmentado (botões encostados, um marcado), alternativa limpa ao QComboBox."""

    alterado = Signal(object)

    def __init__(self, opcoes: list[tuple[object, str, str]], padrao: object, parent: QWidget | None = None,
                 largura_min: int = 40, nome: str = ""):
        super().__init__(parent)
        lay = QHBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)
        self._grupo = QButtonGroup(self)
        self._grupo.setExclusive(True)
        self._valores = [v for v, _, _ in opcoes]
        self.botoes: list[QPushButton] = []
        for i, (valor, texto, dica) in enumerate(opcoes):
            posicao = "primeiro" if i == 0 else ("ultimo" if i == len(opcoes) - 1 else "meio")
            b = QPushButton(texto, self)
            b.setObjectName("segmento")
            b.setProperty("posicao", posicao)
            b.setCheckable(True)
            b.setToolTip(dica)
            b.setAccessibleName(f"{nome} {texto}".strip())
            b.setCursor(Qt.CursorShape.PointingHandCursor)
            b.setMinimumWidth(largura_min)
            b.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            self._grupo.addButton(b, i)
            lay.addWidget(b)
            self.botoes.append(b)
        self._grupo.idClicked.connect(lambda i: self.alterado.emit(self._valores[i]))
        self._padrao = padrao
        self.definir(padrao)

    def valor(self) -> object:
        i = self._grupo.checkedId()
        return self._valores[i] if i >= 0 else self._padrao

    def definir(self, valor: object) -> None:
        if valor not in self._valores:
            valor = self._padrao
        self._grupo.button(self._valores.index(valor)).setChecked(True)


class SeletorQoS(SeletorSegmentado):
    DICAS = {
        0: "QoS 0 — no máximo uma vez: mais leve, pode perder mensagens.",
        1: "QoS 1 — pelo menos uma vez: não perde, mas pode duplicar.",
        2: "QoS 2 — exatamente uma vez: o mais seguro (padrão).",
    }

    def __init__(self, parent: QWidget | None = None):
        super().__init__([(n, str(n), self.DICAS[n]) for n in (0, 1, 2)], 2, parent, nome="QoS")

    def definir(self, valor: object) -> None:
        try:
            valor = int(valor)
        except (TypeError, ValueError):
            valor = 2
        super().definir(valor)


class ResultadoTeste(QFrame):
    """Faixa com ícone e texto mostrando o resultado do teste de conexão (verde/vermelho/neutro)."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("resultadoTeste")
        lay = QHBoxLayout(self)
        lay.setContentsMargins(10, 8, 12, 8)
        lay.setSpacing(8)
        self._icone = QLabel(self)
        self._icone.setObjectName("resultadoIcone")
        self._icone.setFixedSize(20, 20)
        self._icone.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._texto = QLabel(self)
        self._texto.setObjectName("resultadoTexto")
        self._texto.setWordWrap(True)
        self._texto.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        lay.addWidget(self._icone, 0, Qt.AlignmentFlag.AlignTop)
        lay.addWidget(self._texto, 1)
        self.estado = ""
        self.hide()

    def mostrar(self, estado: str, texto: str) -> None:
        """estado: "ok" | "falha" | "testando"."""
        simbolos = {"ok": "✓", "falha": "✕", "testando": "…"}
        self.estado = estado
        self._icone.setText(simbolos.get(estado, ""))
        # Mensagem inteira: o teste pode falhar com a conexão funcionando (falta tabela ou permissão de INSERT).
        self._texto.setText("\n".join(l.rstrip() for l in texto.strip().splitlines() if l.strip()))
        for w in (self, self._icone, self._texto):
            w.setProperty("estado", estado)
        repolir(self, self._icone, self._texto)
        self.show()

    def texto(self) -> str:
        return self._texto.text()

    def ocultar(self) -> None:
        self.estado = ""
        self.hide()


class ChipInfo(QFrame):
    """Pílula discreta com ponto, prefixo (texto secundário) e valor (destacado). Ex.: "Próxima gravação em 0:42".

    `estado` (propriedade dinâmica usada no QSS): "neutro" | "ativo" | "ok" | "ocupado" | "aviso" | "erro".
    `sufixo` (opcional): texto secundário depois do valor ("Reinício automático em 59:12 se nada for gravado").
    """

    def __init__(self, nome_objeto: str, nome_acessivel: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("chipInfo")
        self.setProperty("tipo", nome_objeto)
        self.setAccessibleName(nome_acessivel)
        lay = QHBoxLayout(self)
        lay.setContentsMargins(10, 0, 12, 0)
        lay.setSpacing(6)
        self._ponto = QLabel(self)
        self._ponto.setObjectName("chipPonto")
        self._ponto.setFixedSize(8, 8)
        self._prefixo = QLabel(self)
        self._prefixo.setObjectName("chipPrefixo")
        self._valor = QLabel(self)
        self._valor.setObjectName("chipValor")
        self._valor.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._sufixo = QLabel(self)
        self._sufixo.setObjectName("chipPrefixo")
        self._sufixo.hide()
        lay.addWidget(self._ponto)
        lay.addWidget(self._prefixo)
        lay.addWidget(self._valor)
        lay.addWidget(self._sufixo)
        self.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        self.estado = ""
        self.definir("", "", "neutro")

    def definir(self, prefixo: str, valor: str, estado: str = "neutro", sufixo: str = "") -> None:
        if self._prefixo.text() != prefixo:
            self._prefixo.setText(prefixo)
            self._prefixo.setVisible(bool(prefixo))
        if self._valor.text() != valor:
            self._valor.setText(valor)
        if self._sufixo.text() != sufixo:
            self._sufixo.setText(sufixo)
            self._sufixo.setVisible(bool(sufixo))
        self.setAccessibleDescription(self.texto())
        if estado != self.estado:
            self.estado = estado
            for w in (self, self._ponto, self._valor):
                w.setProperty("estado", estado)
            repolir(self, self._ponto, self._valor)

    def texto(self) -> str:
        return " ".join(t for t in (self._prefixo.text(), self._valor.text(), self._sufixo.text()) if t)

    def valor(self) -> str:
        return self._valor.text()


class PainelLogs(Cartao):
    """Caixa de logs com nível colorido, atualização em lote, rolagem inteligente e limite de linhas."""

    NIVEIS = ("INFO", "AVISO", "ERRO")

    def __init__(self, limite_linhas: int = 5000, intervalo_ms: int = 150, parent: QWidget | None = None):
        super().__init__(parent, margem=16, espaco=12)
        self._limite = limite_linhas
        self._historico: deque[tuple[str, str, str]] = deque(maxlen=limite_linhas)
        self._pendentes: deque[tuple[str, str, str]] = deque(maxlen=limite_linhas)
        self._seguir = True
        self._formatos: dict[str, QTextCharFormat] = {}

        # Cabeçalho
        topo = QHBoxLayout()
        topo.setSpacing(8)
        titulo = QLabel("Atividade", self)
        titulo.setObjectName("tituloCartao")
        self.titulo = titulo
        self.chip_pausa = QPushButton("Rolagem pausada · ir para o fim ↓", self)
        self.chip_pausa.setObjectName("chipPausa")
        self.chip_pausa.setCursor(Qt.CursorShape.PointingHandCursor)
        self.chip_pausa.setToolTip("Novas linhas continuam chegando. Clique para voltar ao fim e seguir o log.")
        self.chip_pausa.clicked.connect(self.ir_para_fim)
        self.chip_pausa.hide()
        self.botao_limpar = self._botao("Limpar", "Apagar as linhas da tela (Ctrl+L)")
        self.botao_copiar = self._botao("Copiar", "Copiar todo o log para a área de transferência")
        self.botao_salvar = self._botao("Salvar…", "Salvar o log em um arquivo .txt (Ctrl+S)")
        self.botao_limpar.clicked.connect(self.limpar)
        self.botao_copiar.clicked.connect(self.copiar)
        self.botao_salvar.clicked.connect(self.salvar_dialogo)
        topo.addWidget(titulo)
        topo.addSpacing(8)
        topo.addWidget(self.chip_pausa)
        topo.addStretch(1)
        topo.addWidget(self.botao_limpar)
        topo.addWidget(self.botao_copiar)
        topo.addWidget(self.botao_salvar)

        # Faixa de tempo: contagem até a próxima gravação e a última gravação com sucesso no SQL.
        faixa = QHBoxLayout()
        faixa.setSpacing(8)
        self.chip_proxima = ChipInfo("proxima", "Próxima gravação", self)
        self.chip_proxima.setToolTip("Tempo até o próximo ciclo de gravação no SQL Server (uma leitura por minuto).")
        self.chip_reinicio = ChipInfo("reinicio", "Reinício automático", self)
        self.chip_reinicio.setToolTip("Se nada for gravado no SQL Server nesse tempo, o programa se reinicia e "
                                      "reconecta sozinho. A contagem recomeça a cada gravação com sucesso.")
        self.chip_ultima = ChipInfo("ultima", "Última gravação", self)
        self.chip_ultima.setToolTip("Dia e hora da última gravação bem-sucedida no SQL Server "
                                    "(o que foi para a contingência não conta).")
        for chip in (self.chip_reinicio,
                     self.chip_proxima,  # só com amostragem ([gravacao] intervalo_gravacao_segundos > 0)
                     self.chip_ultima):
            faixa.addWidget(chip, 0, Qt.AlignmentFlag.AlignLeft)
        faixa.addStretch(1)
        self._faixa_chips = faixa  # empilha os chips quando não cabem lado a lado (janela estreita)

        # Caixa
        self.caixa = QPlainTextEdit(self)
        self.caixa.setObjectName("caixaLogs")
        self.caixa.setReadOnly(True)
        self.caixa.setUndoRedoEnabled(False)
        self.caixa.setLineWrapMode(QPlainTextEdit.LineWrapMode.WidgetWidth)
        self.caixa.setMaximumBlockCount(limite_linhas)
        self.caixa.setFont(fonte_mono())
        self.caixa.setPlaceholderText("Os eventos da conexão e das mensagens aparecem aqui.")
        self.caixa.setAccessibleName("Caixa de logs")
        self.caixa.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse | Qt.TextInteractionFlag.TextSelectableByKeyboard
        )
        self.caixa.verticalScrollBar().valueChanged.connect(self._rolagem_mudou)

        self.corpo.addLayout(topo)
        self.corpo.addLayout(faixa)
        self.corpo.addWidget(self.caixa, 1)

        self._timer = QTimer(self)
        self._timer.setInterval(intervalo_ms)
        self._timer.timeout.connect(self.descarregar)
        self._timer.start()
        self.definir_paleta(CLARA)

    def ajustar_chips(self) -> None:
        """Chips lado a lado quando cabem; um embaixo do outro na janela estreita (nada fica cortado).

        Chamado no resize e quando o texto dos chips muda (a contagem cresce/encolhe a cada segundo)."""
        chips = [c for c in (self.chip_reinicio, self.chip_proxima, self.chip_ultima) if not c.isHidden()]
        m = self.corpo.contentsMargins()
        disponivel = self.contentsRect().width() - m.left() - m.right()
        necessario = sum(c.sizeHint().width() for c in chips) + self._faixa_chips.spacing() * max(0, len(chips) - 1)
        direcao = (QBoxLayout.Direction.LeftToRight if necessario <= disponivel
                   else QBoxLayout.Direction.TopToBottom)
        if self._faixa_chips.direction() != direcao:
            self._faixa_chips.setDirection(direcao)

    def resizeEvent(self, evento) -> None:  # noqa: N802 - API do Qt
        super().resizeEvent(evento)
        self.ajustar_chips()

    def _botao(self, texto: str, dica: str) -> QPushButton:
        b = QPushButton(texto, self)
        b.setObjectName("botaoDiscreto")
        b.setToolTip(dica)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        return b

    # ----- API -----
    def adicionar(self, hora: str, nivel: str, mensagem: str) -> None:
        """Enfileira uma linha. Seguro para alta frequência: a tela é atualizada em lote pelo QTimer."""
        nivel = (nivel or "INFO").upper()
        self._pendentes.append((hora, nivel, mensagem))

    def adicionar_agora(self, nivel: str, mensagem: str) -> None:
        self.adicionar(datetime.now().strftime("%H:%M:%S"), nivel, mensagem)

    def definir_paleta(self, p: Paleta) -> None:
        cores = {"INFO": p.destaque, "AVISO": p.aviso, "ERRO": p.erro}
        hora = QTextCharFormat()
        hora.setForeground(QColor(p.texto_fraco))
        self._formatos = {"hora": hora, "texto": QTextCharFormat()}
        for nivel, cor in cores.items():
            f = QTextCharFormat()
            f.setForeground(QColor(cor))
            f.setFontWeight(QFont.Weight.Bold)
            self._formatos[nivel] = f
        erro_txt = QTextCharFormat()
        erro_txt.setForeground(QColor(p.erro))
        self._formatos["texto_ERRO"] = erro_txt
        # Repinta o que já está na tela com as cores do novo tema.
        if self._historico:
            linhas = list(self._historico)
            self._historico.clear()
            self.caixa.clear()
            self._inserir(linhas)

    def descarregar(self) -> None:
        if not self._pendentes:
            return
        linhas = list(self._pendentes)
        self._pendentes.clear()
        self._inserir(linhas)

    def limpar(self) -> None:
        self._pendentes.clear()
        self._historico.clear()
        self.caixa.clear()
        self._seguir = True
        self.chip_pausa.hide()

    def texto(self) -> str:
        self.descarregar()
        return "\n".join(self._formatar(*l) for l in self._historico)

    def copiar(self) -> None:
        QGuiApplication.clipboard().setText(self.texto())

    def salvar_em(self, caminho: Path) -> Path:
        caminho = Path(caminho)
        caminho.write_text(self.texto() + "\n", encoding="utf-8")
        return caminho

    def salvar_dialogo(self) -> None:
        nome = f"log-mqtt-{datetime.now():%Y%m%d-%H%M%S}.txt"
        caminho, _ = QFileDialog.getSaveFileName(self, "Salvar log", nome, "Texto (*.txt);;Todos os arquivos (*)")
        if not caminho:
            return
        try:
            self.salvar_em(Path(caminho))
        except OSError as exc:
            self.adicionar_agora("ERRO", f"Não foi possível salvar o log: {exc}")
        else:
            self.adicionar_agora("INFO", f"Log salvo em {caminho}")

    def ir_para_fim(self) -> None:
        barra = self.caixa.verticalScrollBar()
        barra.setValue(barra.maximum())
        self._seguir = True
        self.chip_pausa.hide()

    @property
    def seguindo(self) -> bool:
        return self._seguir

    @property
    def linhas(self) -> int:
        return len(self._historico)

    # ----- interno -----
    @staticmethod
    def _formatar(hora: str, nivel: str, msg: str) -> str:
        return f"{hora}  {nivel:<5}  {msg}"

    def _rolagem_mudou(self, valor: int) -> None:
        barra = self.caixa.verticalScrollBar()
        seguir = valor >= barra.maximum() - 2
        if seguir != self._seguir:
            self._seguir = seguir
            self.chip_pausa.setVisible(not seguir)

    def _inserir(self, linhas: list[tuple[str, str, str]]) -> None:
        if len(linhas) > self._limite:
            linhas = linhas[-self._limite:]
        seguir = self._seguir
        doc = self.caixa.document()
        cursor = QTextCursor(doc)
        cursor.movePosition(QTextCursor.MoveOperation.End)
        cursor.beginEditBlock()
        vazio = doc.isEmpty()
        f = self._formatos
        for hora, nivel, msg in linhas:
            if vazio:
                vazio = False
            else:
                cursor.insertBlock()
            cursor.insertText(hora + "  ", f["hora"])
            cursor.insertText(f"{nivel:<5}", f.get(nivel, f["INFO"]))
            cursor.insertText("  " + msg, f["texto_ERRO"] if nivel == "ERRO" else f["texto"])
        cursor.endEditBlock()
        self._historico.extend(linhas)
        if seguir:
            barra = self.caixa.verticalScrollBar()
            barra.setValue(barra.maximum())


class CaixaConsole(QPlainTextEdit):
    """Console somente leitura com fundo de terminal (aba Mosquitto).

    Mesmo comportamento da caixa de atividade: atualização em lote por QTimer (milhares de linhas não travam a
    tela), limite de linhas e rolagem automática que pausa quando o usuário rola para cima (`seguindo_alterado`).

    Tipos de linha: "prompt" (C:\\pasta>comando), "saida" (o que o processo escreve), "info", "ok", "aviso", "erro"
    (mensagens do próprio app sobre o processo).
    """

    TIPOS = ("prompt", "saida", "info", "ok", "aviso", "erro")
    seguindo_alterado = Signal(bool)

    def __init__(self, limite_linhas: int = 5000, intervalo_ms: int = 150, parent: QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("consoleMosquitto")
        self._limite = limite_linhas
        self._historico: deque[tuple[str, str]] = deque(maxlen=limite_linhas)
        self._pendentes: deque[tuple[str, str]] = deque(maxlen=limite_linhas)
        self._seguir = True
        self._formatos: dict[str, QTextCharFormat] = {}
        self.setReadOnly(True)
        self.setUndoRedoEnabled(False)
        self.setLineWrapMode(QPlainTextEdit.LineWrapMode.WidgetWidth)
        self.setMaximumBlockCount(limite_linhas)
        self.setFont(fonte_mono())
        self.setAccessibleName("Console do Mosquitto")
        self.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse | Qt.TextInteractionFlag.TextSelectableByKeyboard
        )
        self.verticalScrollBar().valueChanged.connect(self._rolagem_mudou)
        self._timer = QTimer(self)
        self._timer.setInterval(intervalo_ms)
        self._timer.timeout.connect(self.descarregar)
        self._timer.start()
        self.definir_paleta(CLARA)

    # ----- API -----
    def adicionar(self, tipo: str, texto: str) -> None:
        """Enfileira um bloco de texto (uma ou várias linhas). A tela é atualizada em lote pelo QTimer."""
        tipo = tipo if tipo in self.TIPOS else "saida"
        if tipo == "prompt" and (self._historico or self._pendentes):
            self._pendentes.append(("saida", ""))  # linha em branco antes de um novo prompt, como no cmd
        for linha in str(texto).split("\n"):
            self._pendentes.append((tipo, linha.rstrip("\r")))

    def descarregar(self) -> None:
        if not self._pendentes:
            return
        linhas = list(self._pendentes)
        self._pendentes.clear()
        self._inserir(linhas)

    def limpar(self) -> None:
        self._pendentes.clear()
        self._historico.clear()
        self.clear()
        self._definir_seguir(True)

    def texto(self) -> str:
        self.descarregar()
        return "\n".join(l for _, l in self._historico)

    def linhas_texto(self) -> list[str]:
        self.descarregar()
        return [l for _, l in self._historico]

    def ir_para_fim(self) -> None:
        barra = self.verticalScrollBar()
        barra.setValue(barra.maximum())
        self._definir_seguir(True)

    @property
    def seguindo(self) -> bool:
        return self._seguir

    def definir_paleta(self, p: Paleta) -> None:
        cores = {"prompt": p.terminal_prompt, "saida": p.terminal_texto, "info": p.terminal_fraco,
                 "ok": p.terminal_ok, "aviso": p.terminal_aviso, "erro": p.terminal_erro}
        self._formatos = {}
        for tipo, cor in cores.items():
            f = QTextCharFormat()
            f.setForeground(QColor(cor))
            if tipo == "prompt":
                f.setFontWeight(QFont.Weight.Bold)
            self._formatos[tipo] = f
        if self._historico:
            linhas = list(self._historico)
            self._historico.clear()
            self.clear()
            self._inserir(linhas)

    # ----- interno -----
    def _definir_seguir(self, seguir: bool) -> None:
        if seguir != self._seguir:
            self._seguir = seguir
            self.seguindo_alterado.emit(seguir)

    def _rolagem_mudou(self, valor: int) -> None:
        self._definir_seguir(valor >= self.verticalScrollBar().maximum() - 2)

    def _inserir(self, linhas: list[tuple[str, str]]) -> None:
        if len(linhas) > self._limite:
            linhas = linhas[-self._limite:]
        seguir = self._seguir
        doc = self.document()
        cursor = QTextCursor(doc)
        cursor.movePosition(QTextCursor.MoveOperation.End)
        cursor.beginEditBlock()
        vazio = doc.isEmpty() and not self._historico
        f = self._formatos
        for tipo, linha in linhas:
            if vazio:
                vazio = False
            else:
                cursor.insertBlock()
            cursor.insertText(linha, f.get(tipo, f["saida"]))
        cursor.endEditBlock()
        self._historico.extend(linhas)
        if seguir:
            barra = self.verticalScrollBar()
            barra.setValue(barra.maximum())


class DelegadoPilula(QStyledItemDelegate):
    """Desenha o texto da célula como uma pílula colorida (fundo suave + texto forte), conforme `PAPEL_PILULA`.

    Usado na categoria dos erros e no status dos clientes. Sem tom, a célula é desenhada normalmente.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.paleta: Paleta = CLARA

    def definir_paleta(self, p: Paleta) -> None:
        self.paleta = p

    def cores(self, tom: str) -> tuple[str, str]:
        p = self.paleta
        return {
            "sucesso": (p.sucesso, p.sucesso_suave),
            "aviso": (p.aviso, p.aviso_suave),
            "erro": (p.erro, p.erro_suave),
            "destaque": (p.destaque_texto or p.destaque, p.destaque_suave),
            "roxo": (p.roxo, p.roxo_suave),
            "ciano": (p.ciano, p.ciano_suave),
        }.get(tom, (p.texto_sec, p.neutro_suave))

    def paint(self, pintor: QPainter, opcao: QStyleOptionViewItem, indice: QModelIndex) -> None:
        tom = indice.data(PAPEL_PILULA)
        if not tom:
            super().paint(pintor, opcao, indice)
            return
        opt = QStyleOptionViewItem(opcao)
        self.initStyleOption(opt, indice)
        texto = indice.data(PAPEL_PILULA_TEXTO) or opt.text
        opt.text = ""
        estilo = opt.widget.style() if opt.widget else QApplication.style()
        estilo.drawControl(QStyle.ControlElement.CE_ItemViewItem, opt, pintor, opt.widget)
        cor_texto, cor_fundo = self.cores(str(tom))
        fonte = QFont(opt.font)
        fonte.setWeight(QFont.Weight.DemiBold)
        fonte.setPointSizeF(max(7.0, fonte.pointSizeF() * 0.9))
        metricas = QFontMetrics(fonte)
        selo = indice.data(PAPEL_PILULA_SELO)
        fonte_selo = QFont(fonte)
        fonte_selo.setPointSizeF(max(6.5, fonte.pointSizeF() * 0.85))
        largura_selo = QFontMetrics(fonte_selo).horizontalAdvance(selo) + 10 if selo else 0
        disponivel = opt.rect.width() - 12 - (largura_selo + 4 if selo else 0)
        largura = min(metricas.horizontalAdvance(texto) + 22, disponivel)
        altura = min(metricas.height() + 6, opt.rect.height() - 6)
        x = opt.rect.left() + 8
        y = opt.rect.top() + (opt.rect.height() - altura) / 2
        caixa = QRectF(x, y, max(0, largura), altura)
        pintor.save()
        pintor.setRenderHint(QPainter.RenderHint.Antialiasing)
        pintor.setPen(Qt.PenStyle.NoPen)
        pintor.setBrush(QColor(cor_fundo))
        pintor.drawRoundedRect(caixa, altura / 2, altura / 2)
        pintor.setFont(fonte)
        pintor.setPen(QColor(cor_texto))
        cabe = metricas.horizontalAdvance(texto) + 22 <= disponivel
        visivel = texto if cabe else metricas.elidedText(texto, Qt.TextElideMode.ElideRight, int(caixa.width()) - 20)
        pintor.drawText(caixa.adjusted(11, 0, 0, 0), int(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft),
                        visivel)
        if selo:
            caixa_selo = QRectF(caixa.right() + 4, caixa.top() + 1, largura_selo, caixa.height() - 2)
            pintor.setPen(QPen(QColor(cor_texto), 1))
            pintor.setBrush(Qt.BrushStyle.NoBrush)
            pintor.drawRoundedRect(caixa_selo.adjusted(0.5, 0.5, -0.5, -0.5), 4, 4)
            pintor.setFont(fonte_selo)
            pintor.drawText(caixa_selo, int(Qt.AlignmentFlag.AlignCenter), selo)
        pintor.restore()

    def sizeHint(self, opcao: QStyleOptionViewItem, indice: QModelIndex):  # noqa: N802
        tamanho = super().sizeHint(opcao, indice)
        curto = indice.data(PAPEL_PILULA_TEXTO)
        if curto and indice.data(PAPEL_PILULA):
            opt = QStyleOptionViewItem(opcao)
            self.initStyleOption(opt, indice)
            fonte = QFont(opt.font)
            fonte.setWeight(QFont.Weight.DemiBold)
            fonte.setPointSizeF(max(7.0, fonte.pointSizeF() * 0.9))
            largura = QFontMetrics(fonte).horizontalAdvance(curto) + 22 + 16
            selo = indice.data(PAPEL_PILULA_SELO)
            if selo:
                fonte.setPointSizeF(max(6.5, fonte.pointSizeF() * 0.85))
                largura += QFontMetrics(fonte).horizontalAdvance(selo) + 14
            tamanho.setWidth(largura)
            return tamanho
        tamanho.setWidth(tamanho.width() + 16)
        return tamanho

