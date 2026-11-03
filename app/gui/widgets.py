"""Componentes da tela: indicador de status, cartão de contador, seletor de QoS, chips e painel de logs."""
from __future__ import annotations

from collections import deque
from datetime import datetime
from pathlib import Path

from PySide6.QtCore import QLocale, QTimer, Signal, Qt
from PySide6.QtGui import QColor, QFont, QGuiApplication, QTextCharFormat, QTextCursor
from PySide6.QtWidgets import (
    QButtonGroup,
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
    def __init__(self, titulo: str, dica: str, tipo: str, parent: QWidget | None = None):
        super().__init__(parent, margem=16, espaco=2)
        self._tipo = tipo
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
        self._valor = QLabel("0", self)
        self._valor.setObjectName("valorContador")
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

    `estado` (propriedade dinâmica usada no QSS): "neutro" | "ativo" | "ok" | "ocupado".
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
        lay.addWidget(self._ponto)
        lay.addWidget(self._prefixo)
        lay.addWidget(self._valor)
        self.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        self.estado = ""
        self.definir("", "", "neutro")

    def definir(self, prefixo: str, valor: str, estado: str = "neutro") -> None:
        if self._prefixo.text() != prefixo:
            self._prefixo.setText(prefixo)
        if self._valor.text() != valor:
            self._valor.setText(valor)
        self.setAccessibleDescription(self.texto())
        if estado != self.estado:
            self.estado = estado
            for w in (self, self._ponto, self._valor):
                w.setProperty("estado", estado)
            repolir(self, self._ponto, self._valor)

    def texto(self) -> str:
        return f"{self._prefixo.text()} {self._valor.text()}".strip()

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
        self.chip_ultima = ChipInfo("ultima", "Última gravação", self)
        self.chip_ultima.setToolTip("Dia e hora da última gravação bem-sucedida no SQL Server "
                                    "(o que foi para a contingência não conta).")
        faixa.addWidget(self.chip_proxima)
        faixa.addWidget(self.chip_ultima)
        faixa.addStretch(1)

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
