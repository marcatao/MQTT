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

from PySide6.QtCore import QEvent, QModelIndex, QPointF, QRectF, Qt, Signal
from PySide6.QtGui import QColor, QFont, QFontMetrics, QPainter, QPen, QPolygonF
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
    QToolTip,
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
PAPEL_TOPICOS = Qt.ItemDataRole.UserRole + 4  # botão "Tópicos" da linha: None (sem), "normal" ou "aviso"
PAPEL_REINICIO = Qt.ItemDataRole.UserRole + 5  # botão Reiniciar: (estado, texto) ou None; mesmos estados de PAPEL_ACAO
TEXTO_TOPICOS = "Tópicos"
DICA_TOPICOS = "Tópicos deste aparelho (os mesmos do NXperience)…"
DICA_TOPICOS_AVISO = ("Este aparelho publica em tópicos que o integrador não ouve.\n"
                      "Clique para ver e usar esses tópicos.")


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
    quando o botão é clicado. Estados: "pronto", "ok", "falha" (clicáveis), "desabilitado", "enviando".

    Ao lado, um botão compacto só com ícone (lista), "Tópicos" (`PAPEL_TOPICOS`: "normal" ou "aviso", âmbar quando
    o aparelho publica em tópicos que o integrador não ouve), com dica própria, que emite `topicos_acionado(índice)`.
    É só ícone para caber na largura da tabela em 1440 px; o mesmo comando está no menu de contexto ("Tópicos…"). O realce de passagem do mouse é por
    botão: `posicao_mouse` (coordenadas do viewport) é atualizada pelo painel.
    """

    CLICAVEIS = ("pronto", "ok", "falha")
    ESPACO = 6
    LARGURA_ICONE = 16  # ícone desenhado à esquerda do texto (só com `icone`)
    acionado = Signal(QModelIndex)
    topicos_acionado = Signal(QModelIndex)

    def __init__(self, parent=None, papel: int = PAPEL_ACAO, icone: str | None = None,
                 rotulo: Callable[[str], str] | None = None):
        """`papel`: de onde vem (estado, texto) do botão (PAPEL_ACAO ou PAPEL_REINICIO). `icone`: None ou
        "reiniciar" (seta circular antes do texto). `rotulo(texto)`: texto desenhado no botão, mais curto que o do
        modelo (o modelo, o CSV e os testes continuam com o texto completo; a dica explica)."""
        super().__init__(parent)
        self.paleta: Paleta = CLARA
        self.posicao_mouse = None  # QPointF no viewport, ou None
        self.papel = papel
        self.icone = icone
        self.rotulo = rotulo or (lambda texto: texto)

    def definir_paleta(self, p: Paleta) -> None:
        self.paleta = p

    def _fonte(self, opcao: QStyleOptionViewItem) -> QFont:
        fonte = QFont(opcao.font)
        fonte.setWeight(QFont.Weight.DemiBold)
        return fonte

    LARGURA_TOPICOS = 30

    def largura_topicos(self, opcao: QStyleOptionViewItem) -> int:
        return self.LARGURA_TOPICOS

    def largura_conteudo(self, opcao: QStyleOptionViewItem, texto: str) -> int:
        """Largura do texto curto (+ ícone) desenhado no botão."""
        largura = QFontMetrics(self._fonte(opcao)).horizontalAdvance(self.rotulo(texto))
        return largura + (self.LARGURA_ICONE + 4 if self.icone else 0)

    def retangulo_botao(self, opcao: QStyleOptionViewItem, texto: str, com_topicos: bool = False) -> QRectF:
        r = opcao.rect
        altura = min(26, r.height() - 6)
        reserva = self.largura_topicos(opcao) + self.ESPACO if com_topicos else 0
        folga = 16 if self.icone else 24  # com ícone o botão já tem respiro à esquerda
        largura = min(self.largura_conteudo(opcao, texto) + folga, r.width() - 12 - reserva)
        return QRectF(r.left() + 6, r.top() + (r.height() - altura) / 2, max(0, largura), altura)

    def retangulo_topicos(self, opcao: QStyleOptionViewItem, texto_acao: str) -> QRectF:
        principal = self.retangulo_botao(opcao, texto_acao, com_topicos=True)
        x = principal.right() + self.ESPACO
        largura = min(self.largura_topicos(opcao), max(0.0, opcao.rect.right() - 6 - x))
        return QRectF(x, principal.top(), largura, principal.height())

    def cores_topicos(self, estado: str, sobre: bool) -> tuple[str, str, str]:
        p = self.paleta
        if estado == "aviso":
            return (p.aviso_suave, p.aviso if sobre else p.aviso_suave, p.aviso)
        return (p.neutro_suave if sobre else p.superficie, p.texto_fraco if sobre else p.borda_forte, p.texto)

    def _sobre(self, opt: QStyleOptionViewItem, caixa: QRectF) -> bool:
        if not (opt.state & QStyle.StateFlag.State_MouseOver):
            return False
        return self.posicao_mouse is None or caixa.contains(self.posicao_mouse)

    def _desenhar_botao(self, pintor: QPainter, caixa: QRectF, fonte: QFont, texto: str,
                        cores: tuple[str, str, str]) -> None:
        fundo, borda, cor_texto = cores
        pintor.setPen(QPen(QColor(borda), 1))
        pintor.setBrush(QColor(fundo))
        pintor.drawRoundedRect(caixa.adjusted(0.5, 0.5, -0.5, -0.5), 6, 6)
        if texto == TEXTO_TOPICOS:  # ícone de lista: três marcadores e três linhas
            pintor.setPen(QPen(QColor(cor_texto), 1.6, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            cx, cy = caixa.center().x(), caixa.center().y()
            for dy in (-4.5, 0, 4.5):
                pintor.drawPoint(QPointF(cx - 6, cy + dy))
                pintor.drawLine(QPointF(cx - 2.5, cy + dy), QPointF(cx + 6.5, cy + dy))
            return
        texto = self.rotulo(texto)
        area = QRectF(caixa)
        if self.icone:
            largura_texto = QFontMetrics(fonte).horizontalAdvance(texto)
            conteudo = min(self.LARGURA_ICONE + 4 + largura_texto, caixa.width() - 8)
            x = caixa.left() + (caixa.width() - conteudo) / 2
            self._desenhar_icone(pintor, QRectF(x, caixa.center().y() - 7, 14, 14), QColor(cor_texto))
            area = QRectF(x + self.LARGURA_ICONE + 4, caixa.top(),
                          max(0.0, caixa.right() - 4 - (x + self.LARGURA_ICONE + 4)), caixa.height())
            alinhamento = Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft
            margem = 0
        else:
            alinhamento = Qt.AlignmentFlag.AlignCenter
            margem = 12
        pintor.setFont(fonte)
        pintor.setPen(QColor(cor_texto))
        visivel = QFontMetrics(fonte).elidedText(texto, Qt.TextElideMode.ElideRight, int(area.width()) - margem)
        pintor.drawText(area, int(alinhamento), visivel)

    def _desenhar_icone(self, pintor: QPainter, caixa: QRectF, cor: QColor) -> None:
        """Seta circular (reiniciar): arco de ~300° com a ponta da seta no fim."""
        pintor.save()
        pintor.setPen(QPen(cor, 1.6, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin))
        pintor.setBrush(Qt.BrushStyle.NoBrush)
        arco = caixa.adjusted(2, 2, -2, -2)
        pintor.drawArc(arco, 60 * 16, 290 * 16)  # começa em 60° e gira no sentido anti-horário
        # ponta da seta no início do arco (60°), apontando no sentido horário
        cx, cy, raio = arco.center().x(), arco.center().y(), arco.width() / 2
        px, py = cx + raio * 0.5, cy - raio * 0.866
        pintor.setBrush(cor)
        pintor.drawPolygon(QPolygonF([QPointF(px + 3.2, py - 0.6), QPointF(px - 1.4, py - 3.0),
                                      QPointF(px - 0.6, py + 2.4)]))
        pintor.restore()

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

    def _topicos(self, indice: QModelIndex):
        return indice.data(PAPEL_TOPICOS) if self.papel == PAPEL_ACAO else None

    def paint(self, pintor: QPainter, opcao: QStyleOptionViewItem, indice: QModelIndex) -> None:
        acao = indice.data(self.papel)
        opt = QStyleOptionViewItem(opcao)
        self.initStyleOption(opt, indice)
        opt.text = ""
        estilo = opt.widget.style() if opt.widget else QApplication.style()
        estilo.drawControl(QStyle.ControlElement.CE_ItemViewItem, opt, pintor, opt.widget)
        if not acao:
            return
        estado, texto = acao
        topicos = self._topicos(indice)
        caixa = self.retangulo_botao(opt, texto, com_topicos=bool(topicos))
        sobre = estado in self.CLICAVEIS and self._sobre(opt, caixa)
        fonte = self._fonte(opt)
        pintor.save()
        pintor.setRenderHint(QPainter.RenderHint.Antialiasing)
        self._desenhar_botao(pintor, caixa, fonte, texto, self.cores(estado, sobre))
        if topicos:
            caixa_t = self.retangulo_topicos(opt, texto)
            if caixa_t.width() > 16:
                self._desenhar_botao(pintor, caixa_t, fonte, TEXTO_TOPICOS,
                                     self.cores_topicos(topicos, self._sobre(opt, caixa_t)))
        pintor.restore()

    def botao_em(self, opcao: QStyleOptionViewItem, indice: QModelIndex, pos) -> str | None:
        """"acao", "topicos" ou None: qual botão da célula está em `pos` (coordenadas do viewport)."""
        acao = indice.data(self.papel)
        if not acao:
            return None
        topicos = self._topicos(indice)
        if topicos and self.retangulo_topicos(opcao, acao[1]).contains(pos):
            return "topicos"
        if acao[0] in self.CLICAVEIS and self.retangulo_botao(opcao, acao[1], bool(topicos)).contains(pos):
            return "acao"
        return None

    def editorEvent(self, evento: QEvent, modelo, opcao: QStyleOptionViewItem,  # noqa: N802
                    indice: QModelIndex) -> bool:
        if evento.type() != QEvent.Type.MouseButtonRelease or evento.button() != Qt.MouseButton.LeftButton:
            return False
        botao = self.botao_em(opcao, indice, evento.position())
        if botao == "acao":
            self.acionado.emit(QModelIndex(indice))
            return True
        if botao == "topicos":
            self.topicos_acionado.emit(QModelIndex(indice))
            return True
        return False

    def helpEvent(self, evento, vista, opcao: QStyleOptionViewItem, indice: QModelIndex) -> bool:  # noqa: N802
        """Dica própria do botão de tópicos (o resto da célula usa a dica do modelo)."""
        if evento.type() == QEvent.Type.ToolTip:
            topicos = self._topicos(indice)
            acao = indice.data(self.papel)
            if topicos and acao and self.retangulo_topicos(opcao, acao[1]).contains(QPointF(evento.pos())):
                QToolTip.showText(evento.globalPos(), DICA_TOPICOS_AVISO if topicos == "aviso" else DICA_TOPICOS,
                                  vista)
                return True
        return super().helpEvent(evento, vista, opcao, indice)

    def sizeHint(self, opcao: QStyleOptionViewItem, indice: QModelIndex):  # noqa: N802
        tamanho = super().sizeHint(opcao, indice)
        acao = indice.data(self.papel)
        if acao:
            largura = self.largura_conteudo(opcao, acao[1]) + (32 if self.icone else 40)
            if self._topicos(indice):
                largura += self.largura_topicos(opcao) + self.ESPACO
            tamanho.setWidth(largura)
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


AVISO_REINICIO_DESCONECTADO = ("O aparelho parece desconectado; o comando só chega se ele ainda estiver ouvindo o "
                               "broker.")


def texto_confirmar_reinicio(device_id: str, saida: int = 1, desconectado: bool = False) -> tuple[str, str]:
    """(pergunta, explicação) da confirmação do Reiniciar. Função pura (os testes conferem o texto)."""
    pergunta = f"Reiniciar {device_id}?"
    explicacao = (f"O integrador vai ligar a saída {saida} do aparelho, que aciona o relé e corta a alimentação por "
                  "alguns segundos. A contagem para durante o reinício.")
    if desconectado:
        explicacao += "\n\n" + AVISO_REINICIO_DESCONECTADO
    return pergunta, explicacao


def caixa_confirmar_reinicio(parent: QWidget | None, device_id: str, pergunta: str,
                             explicacao: str) -> tuple[QMessageBox, QPushButton]:
    """Monta a confirmação (sem abrir): QMessageBox com "Cancelar" como botão padrão (Enter e Esc cancelam)."""
    caixa = QMessageBox(parent)
    caixa.setIcon(QMessageBox.Icon.Warning)
    caixa.setWindowTitle(f"Reiniciar — {device_id}")
    caixa.setText(pergunta)
    caixa.setInformativeText(explicacao)
    reiniciar = caixa.addButton("Reiniciar", QMessageBox.ButtonRole.DestructiveRole)
    cancelar = caixa.addButton("Cancelar", QMessageBox.ButtonRole.RejectRole)
    caixa.setDefaultButton(cancelar)
    caixa.setEscapeButton(cancelar)
    return caixa, reiniciar


def confirmar_reinicio(parent: QWidget | None, device_id: str, pergunta: str, explicacao: str) -> bool:
    caixa, reiniciar = caixa_confirmar_reinicio(parent, device_id, pergunta, explicacao)
    try:
        caixa.exec()
        return caixa.clickedButton() is reiniciar
    finally:
        caixa.deleteLater()


def texto_etapa_curto(etapa: str) -> str:
    """Texto da etapa no botão (a dica mostra a etapa completa): "Aguardando o aparelho voltar…" →
    "Aguardando voltar…"; o resto passa igual (o botão corta com reticências se não couber)."""
    t = " ".join(str(etapa or "").split())
    baixo = t.casefold()
    if baixo.startswith("aguardando") and "voltar" in baixo:
        return "Aguardando voltar…"
    return t or "Reiniciando…"


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
