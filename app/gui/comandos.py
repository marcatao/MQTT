"""Diagnóstico e comandos do DigiRail na aba Clientes (CLAUDE.md, "Correções e comandos do DigiRail").

* `interpretar_diag` / `normalizar_rssi`: o objeto `diag` (JSON do sinal `diagnostico_dispositivo`) vira um
  `Diagnostico` com tipos limpos (lógica pura, sem Qt);
* `DelegadoWifi`: barras de sinal + "-54 dBm" na coluna Wi-Fi;
* `DelegadoAcao`: o botão da coluna de ação ("Zerar contadores", "Zerando…", "Zerado às 14:31", "Falhou"),
  desenhado pelo delegate para acompanhar a linha certa com ordenação e filtro (setIndexWidget ficaria preso à
  posição visual);
* `DialogoZerarContadores` e `confirmar_zerar`: escolha dos canais chd1–chd6 e a segunda confirmação.

A tela não publica nada: quem envia é o bridge (`zerar_contadores`, `pedir_diagnostico`).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable

from PySide6.QtCore import QEvent, QModelIndex, QRectF, Qt, Signal
from PySide6.QtGui import QColor, QFont, QFontMetrics, QPainter, QPen
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QVBoxLayout,
    QWidget,
)

from .estilo import CLARA, Paleta

CANAIS = (1, 2, 3, 4, 5, 6)  # chd1–chd6 do DigiRail
SEM_VALOR = "—"
TEXTO_CABO = "cabo"
AVISO_ZERAR = ("O contador volta a zero no aparelho e não pode ser desfeito. Só funciona nos canais com "
               "'reset por MQTT' habilitado no NXperience.")

PAPEL_WIFI = Qt.ItemDataRole.UserRole + 2   # int (dBm, negativo) ou None
PAPEL_ACAO = Qt.ItemDataRole.UserRole + 3   # (estado, texto) ou None; estado: ver DelegadoAcao.CLICAVEIS


# ---------------------------------------------------------------------- diagnóstico (lógica pura)
def normalizar_rssi(valor: Any) -> int | None:
    """RSSI do diag → dBm negativo (int) ou None.

    Suposição (decidida com o orquestrador em 04/11/2026): o manual (p. 21) diz que `curr_rssi` vem em "%" e
    `min/avg/max_rssi` em dBm, todos como TEXTO; mas o próprio exemplo traz `"curr_rssi": "55"` ao lado de
    `"avg_rssi": "-54"`, o que indica o mesmo RSSI em dBm com o sinal perdido. Por isso qualquer valor é tratado
    como dBm e vira `-abs(valor)`: "55" → -55, "-54" → -54, 55 → -55. Vazio, 0 ou fora de 1–120 → None (sem RSSI;
    a tela mostra "cabo").
    """
    if valor is None or isinstance(valor, bool):
        return None
    texto = str(valor).strip().lower().replace("dbm", "").replace("%", "").replace(",", ".").strip()
    if not texto:
        return None
    try:
        numero = abs(float(texto))
    except ValueError:
        return None
    if numero < 1 or numero > 120:
        return None
    return -int(round(numero))


def _texto(valor: Any) -> str | None:
    if valor is None:
        return None
    t = str(valor).strip()
    return t or None


def _inteiro(valor: Any) -> int | None:
    if valor is None or isinstance(valor, bool):
        return None
    try:
        return int(float(str(valor).strip()))
    except ValueError:
        return None


def _ipv4(valor: Any) -> str | None:
    """[192, 168, 0, 23] ou "192.168.0.23" → "192.168.0.23"; 0.0.0.0 e formatos estranhos → None."""
    if isinstance(valor, (list, tuple)):
        if len(valor) != 4:
            return None
        try:
            partes = [int(v) for v in valor]
        except (TypeError, ValueError):
            return None
    elif isinstance(valor, str):
        try:
            partes = [int(v) for v in valor.strip().split(".")]
        except ValueError:
            return None
        if len(partes) != 4:
            return None
    else:
        return None
    if not all(0 <= p <= 255 for p in partes) or not any(partes):
        return None
    return ".".join(str(p) for p in partes)


@dataclass
class Diagnostico:
    recebido_em: float                # hora do PC (epoch) em que o diag chegou
    sn: str | None = None
    firmware: str | None = None
    rssi: int | None = None           # curr_rssi em dBm (negativo)
    rssi_min: int | None = None
    rssi_med: int | None = None
    rssi_max: int | None = None
    fila: int | None = None           # mqtt_queue: leituras guardadas no aparelho esperando envio
    ip: str | None = None
    titulo: str | None = None
    local: str | None = None
    relogio: float | None = None      # curr_timestamp: relógio do aparelho (epoch UTC)

    @property
    def diferenca_relogio(self) -> float | None:
        """Segundos que o relógio do aparelho está à frente (+) ou atrás (-) do PC, na hora do diag."""
        return None if self.relogio is None else self.relogio - self.recebido_em


def interpretar_diag(dados: str | dict, recebido_em: float) -> Diagnostico:
    """JSON do objeto `diag` (aceita também a mensagem inteira com `reported.diag` ou `{"diag": …}`)."""
    if isinstance(dados, (str, bytes)):
        try:
            dados = json.loads(dados)
        except ValueError:
            dados = {}
    if not isinstance(dados, dict):
        dados = {}
    for chave in ("reported", "diag"):
        if isinstance(dados.get(chave), dict):
            dados = dados[chave]
    if isinstance(dados.get("diag"), dict):
        dados = dados["diag"]
    relogio = _inteiro(dados.get("curr_timestamp"))
    return Diagnostico(
        recebido_em=recebido_em,
        sn=_texto(dados.get("sn")),
        firmware=_texto(dados.get("fw_v")),
        rssi=normalizar_rssi(dados.get("curr_rssi")),
        rssi_min=normalizar_rssi(dados.get("min_rssi")),
        rssi_med=normalizar_rssi(dados.get("avg_rssi")),
        rssi_max=normalizar_rssi(dados.get("max_rssi")),
        fila=_inteiro(dados.get("mqtt_queue")),
        ip=_ipv4(dados.get("ipv4")),
        titulo=_texto(dados.get("title")),
        local=_texto(dados.get("location")),
        relogio=float(relogio) if relogio and relogio > 0 else None,
    )


def barras_wifi(dbm: int | None) -> int:
    """Quantas das 4 barras acendem: >= -60 dBm: 4; >= -67: 3; >= -75: 2; abaixo: 1; sem RSSI: 0."""
    if dbm is None:
        return 0
    if dbm >= -60:
        return 4
    if dbm >= -67:
        return 3
    if dbm >= -75:
        return 2
    return 1


QUALIDADE_WIFI = {4: "ótimo", 3: "bom", 2: "fraco", 1: "muito fraco"}


def texto_wifi(diag: Diagnostico | None) -> str:
    if diag is None:
        return SEM_VALOR
    if diag.rssi is None:
        return TEXTO_CABO
    return f"{diag.rssi} dBm"


def dica_wifi(diag: Diagnostico | None) -> str:
    if diag is None:
        return "Sem diagnóstico do aparelho ainda (botão Atualizar diagnóstico)."
    if diag.rssi is None:
        return "O diagnóstico não trouxe RSSI: o aparelho deve estar ligado por cabo."

    def dbm(v: int | None) -> str:
        return SEM_VALOR if v is None else f"{v} dBm"

    return (f"Sinal Wi-Fi {QUALIDADE_WIFI[barras_wifi(diag.rssi)]}: {diag.rssi} dBm\n"
            f"Mínimo: {dbm(diag.rssi_min)} · Média: {dbm(diag.rssi_med)} · Máximo: {dbm(diag.rssi_max)}")


def texto_duracao(segundos: float) -> str:
    s = int(round(abs(segundos)))
    if s < 60:
        return f"{s} s"
    if s < 3600:
        m, s = divmod(s, 60)
        return f"{m} min {s:02d} s" if s else f"{m} min"
    h, m = divmod(s // 60, 60)
    return f"{h} h {m:02d} min" if m else f"{h} h"


def texto_relogio(diag: Diagnostico) -> str | None:
    """'Relógio do aparelho: 3 min 12 s adiantado em relação ao PC' (até 5 s conta como acertado)."""
    dif = diag.diferenca_relogio
    if dif is None:
        return None
    if abs(dif) <= 5:
        return f"Relógio do aparelho: acertado com o PC (diferença de {texto_duracao(dif)})"
    sentido = "adiantado" if dif > 0 else "atrasado"
    return f"Relógio do aparelho: {texto_duracao(dif)} {sentido} em relação ao PC"


def texto_hora_diag(epoch: float | None, agora: float) -> str:
    """Hora do último diag: "14:31:05" se for hoje, senão "03/11 14:31"."""
    if not epoch:
        return SEM_VALOR
    quando = datetime.fromtimestamp(epoch)
    if quando.date() == datetime.fromtimestamp(agora).date():
        return quando.strftime("%H:%M:%S")
    return quando.strftime("%d/%m %H:%M")


def lista_canais(canais: Iterable[int]) -> str:
    """[2] → "chd2"; [1, 3] → "chd1 e chd3"; [1, 2, 3] → "chd1, chd2 e chd3"."""
    nomes = [f"chd{n}" for n in sorted(set(canais))]
    if len(nomes) <= 1:
        return "".join(nomes)
    return ", ".join(nomes[:-1]) + " e " + nomes[-1]


def numeros_canais(nomes: Iterable[Any]) -> list[int]:
    """["chd1", "chd3_value", 2] → [1, 3, 2] (só 1–6, sem repetir, na ordem recebida)."""
    saida: list[int] = []
    for nome in nomes or ():
        t = str(nome).strip().lower()
        if t.startswith("chd"):
            t = t[3:]
        digitos = ""
        for ch in t:
            if not ch.isdigit():
                break
            digitos += ch
        if digitos and int(digitos) in CANAIS and int(digitos) not in saida:
            saida.append(int(digitos))
    return saida


# ---------------------------------------------------------------------- delegates
class DelegadoWifi(QStyledItemDelegate):
    """Quatro barras (acesas conforme o RSSI) seguidas do texto da célula ("-54 dBm", "cabo" ou "—").

    A cor das barras acompanha a qualidade (verde, âmbar, vermelho), mas o texto em dBm está sempre ao lado:
    a informação nunca depende só da cor.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.paleta: Paleta = CLARA

    def definir_paleta(self, p: Paleta) -> None:
        self.paleta = p

    def cor_barras(self, acesas: int) -> str:
        p = self.paleta
        return {4: p.sucesso, 3: p.sucesso, 2: p.aviso, 1: p.erro}.get(acesas, p.borda_forte)

    def paint(self, pintor: QPainter, opcao: QStyleOptionViewItem, indice: QModelIndex) -> None:
        dbm = indice.data(PAPEL_WIFI)
        opt = QStyleOptionViewItem(opcao)
        self.initStyleOption(opt, indice)
        texto = opt.text
        if not texto or texto == SEM_VALOR or (dbm is None and texto != TEXTO_CABO):
            super().paint(pintor, opcao, indice)
            return
        opt.text = ""
        estilo = opt.widget.style() if opt.widget else QApplication.style()
        estilo.drawControl(QStyle.ControlElement.CE_ItemViewItem, opt, pintor, opt.widget)
        acesas = barras_wifi(dbm)
        r = opt.rect
        x0 = r.left() + 8
        base = r.top() + r.height() / 2 + 7
        pintor.save()
        pintor.setRenderHint(QPainter.RenderHint.Antialiasing)
        pintor.setPen(Qt.PenStyle.NoPen)
        if dbm is not None:
            for i in range(4):
                altura = 4 + i * 3.5
                pintor.setBrush(QColor(self.cor_barras(acesas) if i < acesas else self.paleta.borda_forte))
                pintor.drawRoundedRect(QRectF(x0 + i * 5, base - altura, 3, altura), 1, 1)
        else:  # por cabo: um traço discreto no lugar das barras
            pintor.setPen(QPen(QColor(self.paleta.texto_fraco), 1.6))
            pintor.drawLine(int(x0), int(base - 5), int(x0 + 18), int(base - 5))
        cor = opt.palette.color(opt.palette.ColorRole.Text)
        fg = indice.data(Qt.ItemDataRole.ForegroundRole)
        if fg is not None:
            cor = QColor(fg)
        pintor.setPen(cor)
        pintor.setFont(opt.font)
        caixa = r.adjusted(8 + 26, 0, -4, 0)
        pintor.drawText(caixa, int(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft),
                        QFontMetrics(opt.font).elidedText(texto, Qt.TextElideMode.ElideRight, caixa.width()))
        pintor.restore()

    def sizeHint(self, opcao: QStyleOptionViewItem, indice: QModelIndex):  # noqa: N802
        tamanho = super().sizeHint(opcao, indice)
        tamanho.setWidth(tamanho.width() + 30)
        return tamanho


class DelegadoAcao(QStyledItemDelegate):
    """Desenha o botão da coluna de ação a partir de `PAPEL_ACAO` = (estado, texto) e emite `acionado(índice)`
    quando o botão é clicado. Estados: "pronto", "ok", "falha" (clicáveis), "desabilitado", "enviando"."""

    CLICAVEIS = ("pronto", "ok", "falha")
    acionado = Signal(QModelIndex)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.paleta: Paleta = CLARA

    def definir_paleta(self, p: Paleta) -> None:
        self.paleta = p

    def _fonte(self, opcao: QStyleOptionViewItem) -> QFont:
        fonte = QFont(opcao.font)
        fonte.setWeight(QFont.Weight.DemiBold)
        return fonte

    def retangulo_botao(self, opcao: QStyleOptionViewItem, texto: str) -> QRectF:
        metricas = QFontMetrics(self._fonte(opcao))
        r = opcao.rect
        altura = min(26, r.height() - 6)
        largura = min(metricas.horizontalAdvance(texto) + 24, r.width() - 12)
        return QRectF(r.left() + 6, r.top() + (r.height() - altura) / 2, max(0, largura), altura)

    def cores(self, estado: str, sobre: bool) -> tuple[str, str, str]:
        """(fundo, borda, texto)."""
        p = self.paleta
        if estado == "ok":
            return (p.sucesso_suave, p.sucesso if sobre else p.sucesso_suave, p.sucesso)
        if estado == "falha":
            return (p.erro_suave, p.erro if sobre else p.erro_suave, p.erro)
        if estado in ("desabilitado", "enviando"):
            return (p.superficie_alt, p.borda, p.texto_fraco)
        return (p.neutro_suave if sobre else p.superficie, p.texto_fraco if sobre else p.borda_forte, p.texto)

    def paint(self, pintor: QPainter, opcao: QStyleOptionViewItem, indice: QModelIndex) -> None:
        acao = indice.data(PAPEL_ACAO)
        opt = QStyleOptionViewItem(opcao)
        self.initStyleOption(opt, indice)
        opt.text = ""
        estilo = opt.widget.style() if opt.widget else QApplication.style()
        estilo.drawControl(QStyle.ControlElement.CE_ItemViewItem, opt, pintor, opt.widget)
        if not acao:
            return
        estado, texto = acao
        sobre = bool(opt.state & QStyle.StateFlag.State_MouseOver) and estado in self.CLICAVEIS
        fundo, borda, cor_texto = self.cores(estado, sobre)
        caixa = self.retangulo_botao(opt, texto)
        fonte = self._fonte(opt)
        pintor.save()
        pintor.setRenderHint(QPainter.RenderHint.Antialiasing)
        pintor.setPen(QPen(QColor(borda), 1))
        pintor.setBrush(QColor(fundo))
        pintor.drawRoundedRect(caixa.adjusted(0.5, 0.5, -0.5, -0.5), 6, 6)
        pintor.setFont(fonte)
        pintor.setPen(QColor(cor_texto))
        visivel = QFontMetrics(fonte).elidedText(texto, Qt.TextElideMode.ElideRight, int(caixa.width()) - 16)
        pintor.drawText(caixa, int(Qt.AlignmentFlag.AlignCenter), visivel)
        pintor.restore()

    def editorEvent(self, evento: QEvent, modelo, opcao: QStyleOptionViewItem,  # noqa: N802
                    indice: QModelIndex) -> bool:
        if evento.type() != QEvent.Type.MouseButtonRelease or evento.button() != Qt.MouseButton.LeftButton:
            return False
        acao = indice.data(PAPEL_ACAO)
        if not acao or acao[0] not in self.CLICAVEIS:
            return False
        if self.retangulo_botao(opcao, acao[1]).contains(evento.position()):
            self.acionado.emit(QModelIndex(indice))
            return True
        return False

    def sizeHint(self, opcao: QStyleOptionViewItem, indice: QModelIndex):  # noqa: N802
        tamanho = super().sizeHint(opcao, indice)
        acao = indice.data(PAPEL_ACAO)
        if acao:
            tamanho.setWidth(QFontMetrics(self._fonte(opcao)).horizontalAdvance(acao[1]) + 40)
        return tamanho


# ---------------------------------------------------------------------- diálogo
def confirmar_zerar(parent: QWidget | None, device_id: str, canais: list[int]) -> bool:
    """Segunda confirmação (QMessageBox), com "Cancelar" como botão padrão."""
    caixa = QMessageBox(parent)
    caixa.setIcon(QMessageBox.Icon.Warning)
    caixa.setWindowTitle("Confirmar zeramento")
    plural = "o contador" if len(canais) == 1 else "os contadores"
    caixa.setText(f"Zerar {plural} {lista_canais(canais)} em {device_id}?")
    caixa.setInformativeText("O contador volta a zero no aparelho e não pode ser desfeito.")
    zerar = caixa.addButton("Zerar", QMessageBox.ButtonRole.DestructiveRole)
    cancelar = caixa.addButton("Cancelar", QMessageBox.ButtonRole.RejectRole)
    caixa.setDefaultButton(cancelar)
    caixa.setEscapeButton(cancelar)
    caixa.exec()
    return caixa.clickedButton() is zerar


class DialogoZerarContadores(QDialog):
    """Escolha dos canais a zerar. Os canais vistos nas leituras do aparelho vêm primeiro e destacados; nenhum vem
    marcado. "Zerar selecionados" só habilita com seleção e ainda pede a confirmação (`confirmar`)."""

    def __init__(self, device_id: str, canais_vistos: Iterable[int] = (), parent: QWidget | None = None,
                 confirmar: Callable[[QWidget | None, str, list[int]], bool] | None = None):
        super().__init__(parent)
        self.device_id = device_id
        self.canais_vistos = [n for n in numeros_canais(canais_vistos)]
        self._confirmar = confirmar or confirmar_zerar
        self.setObjectName("dialogoZerar")
        self.setWindowTitle(f"Zerar contadores — {device_id}")
        self.setModal(True)
        self.setMinimumWidth(460)

        corpo = QVBoxLayout(self)
        corpo.setContentsMargins(24, 24, 24, 24)
        corpo.setSpacing(16)

        titulo = QLabel(f"Zerar contadores — {device_id}", self)
        titulo.setObjectName("tituloDialogo")
        titulo.setWordWrap(True)
        descricao = QLabel("Marque os canais cujo contador deve voltar a zero.", self)
        descricao.setObjectName("descricaoDialogo")
        descricao.setWordWrap(True)
        cab = QVBoxLayout()
        cab.setSpacing(4)
        cab.addWidget(titulo)
        cab.addWidget(descricao)
        corpo.addLayout(cab)

        self.caixas: dict[int, QCheckBox] = {}
        outros = [n for n in CANAIS if n not in self.canais_vistos]
        grupo = QFrame(self)
        grupo.setObjectName("grupoCanais")
        lay_grupo = QVBoxLayout(grupo)
        lay_grupo.setContentsMargins(16, 12, 16, 12)
        lay_grupo.setSpacing(8)
        if self.canais_vistos:
            self._secao(lay_grupo, "CANAIS COM LEITURAS DESTE APARELHO", self.canais_vistos, visto=True)
            if outros:
                lay_grupo.addSpacing(4)
                self._secao(lay_grupo, "OUTROS CANAIS", outros, visto=False)
        else:
            self._secao(lay_grupo, "CANAIS", list(CANAIS), visto=False)
        corpo.addWidget(grupo)

        self.botao_todos = QPushButton("Marcar todos", self)
        self.botao_todos.setObjectName("botaoLink")
        self.botao_todos.setCursor(Qt.CursorShape.PointingHandCursor)
        self.botao_todos.setToolTip("Marcar (ou desmarcar) os seis canais de uma vez.")
        self.botao_todos.clicked.connect(self._alternar_todos)
        linha_todos = QHBoxLayout()
        linha_todos.setContentsMargins(0, 0, 0, 0)
        linha_todos.addWidget(self.botao_todos)
        linha_todos.addStretch(1)
        corpo.addLayout(linha_todos)

        aviso = QFrame(self)
        aviso.setObjectName("avisoZerar")
        lay_aviso = QHBoxLayout(aviso)
        lay_aviso.setContentsMargins(12, 10, 12, 10)
        lay_aviso.setSpacing(8)
        icone = QLabel("!", aviso)
        icone.setObjectName("avisoZerarIcone")
        icone.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignHCenter)
        icone.setFixedWidth(16)
        self.aviso = QLabel(AVISO_ZERAR, aviso)
        self.aviso.setObjectName("avisoZerarTexto")
        self.aviso.setWordWrap(True)
        lay_aviso.addWidget(icone)
        lay_aviso.addWidget(self.aviso, 1)
        corpo.addWidget(aviso)

        botoes = QHBoxLayout()
        botoes.setSpacing(8)
        botoes.addStretch(1)
        self.botao_cancelar = QPushButton("Cancelar", self)
        self.botao_cancelar.clicked.connect(self.reject)
        self.botao_zerar = QPushButton("Zerar selecionados", self)
        self.botao_zerar.setObjectName("botaoPerigo")
        self.botao_zerar.setCursor(Qt.CursorShape.PointingHandCursor)
        self.botao_zerar.clicked.connect(self._zerar)
        self.botao_cancelar.setDefault(True)  # Enter não zera por engano
        botoes.addWidget(self.botao_cancelar)
        botoes.addWidget(self.botao_zerar)
        corpo.addLayout(botoes)

        ordem = [self.caixas[n] for n in [*self.canais_vistos, *outros]] or [self.caixas[n] for n in CANAIS]
        ordem += [self.botao_todos, self.botao_cancelar, self.botao_zerar]
        for a, b in zip(ordem, ordem[1:]):
            QWidget.setTabOrder(a, b)
        ordem[0].setFocus()
        self._atualizar()

    def _secao(self, layout: QVBoxLayout, titulo: str, canais: list[int], visto: bool) -> None:
        rotulo = QLabel(titulo, self)
        rotulo.setObjectName("secaoCanais")
        layout.addWidget(rotulo)
        grade = QGridLayout()
        grade.setHorizontalSpacing(24)
        grade.setVerticalSpacing(8)
        for i, n in enumerate(canais):
            caixa = QCheckBox(f"chd{n}", self)
            caixa.setProperty("visto", visto)
            caixa.setToolTip("Este aparelho envia leituras deste canal." if visto
                             else "Nenhuma leitura deste canal chegou deste aparelho.")
            caixa.toggled.connect(self._atualizar)
            self.caixas[n] = caixa
            grade.addWidget(caixa, i // 3, i % 3)
        for col in range(3):
            grade.setColumnStretch(col, 1)
        layout.addLayout(grade)

    def canais_escolhidos(self) -> list[int]:
        return [n for n in CANAIS if self.caixas[n].isChecked()]

    def _alternar_todos(self) -> None:
        marcar = len(self.canais_escolhidos()) < len(CANAIS)
        for caixa in self.caixas.values():
            caixa.setChecked(marcar)

    def _atualizar(self, *_args) -> None:
        escolhidos = self.canais_escolhidos()
        self.botao_zerar.setEnabled(bool(escolhidos))
        self.botao_zerar.setToolTip("" if escolhidos else "Marque pelo menos um canal.")
        self.botao_todos.setText("Desmarcar todos" if len(escolhidos) == len(CANAIS) else "Marcar todos")

    def _zerar(self) -> None:
        escolhidos = self.canais_escolhidos()
        if not escolhidos:
            return
        if self._confirmar(self, self.device_id, escolhidos):
            self.accept()
