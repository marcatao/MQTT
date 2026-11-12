"""Aba "Apontamentos reais": acompanha o processo que gera a timeline das máquinas no portal OEE.

O core (app/core/apontamentos_oee.py) lê as contagens da IOT001 e grava `OEE.dbo.apontamentos_estado` em thread
própria; a regra completa está em docs/apontamentos_reais.md. Esta aba só mostra o resultado e conversa com o
bridge por três pontos do contrato (todos opcionais: um bridge sem eles deixa os controles desabilitados):

* `apontamentos_atualizados(str)`: JSON de cada ciclo concluído (ou falho), ver `interpretar_ciclo`;
* `definir_periodicidade_apontamentos(minutos)`: chamado quando o usuário muda a periodicidade e ao abrir a janela
  (o valor fica em QSettings, chave `apontamentos/minutos`, e vale mais que `[apontamentos] minutos`);
* `executar_apontamentos_agora()`: botão "Executar agora", que fica "Executando…" até o próximo ciclo chegar (ou
  até `TEMPO_LIMITE_EXECUTAR_MS`, rede de segurança).

O painel não chama o bridge diretamente: emite `periodicidade_alterada(float)` e `executar_pedido()`, que a janela
liga aos métodos do bridge. Partes: funções puras (`interpretar_ciclo`, `formatar_data`, `tom_legenda`), modelo Qt
(`ModeloApontamentos` + `FiltroApontamentos`) e o conteúdo da aba (`PainelApontamentos`).
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from PySide6.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QObject,
    QPointF,
    QSettings,
    QSortFilterProxyModel,
    QTimer,
    Qt,
    Signal,
)
from PySide6.QtGui import QColor, QFont, QFontMetrics, QIcon, QPainter, QPen, QPixmap, QPolygonF
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QSpinBox,
    QStackedWidget,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from . import tempo
from .estilo import CLARA, Paleta
from .widgets import PAPEL_PILULA, ChipInfo, DelegadoPilula, ResultadoTeste

# ---------------------------------------------------------------------- constantes
MINUTOS_PADRAO = 3
MINUTOS_MIN, MINUTOS_MAX = 1, 60
CHAVE_SETTINGS = "apontamentos/minutos"
TEMPO_LIMITE_EXECUTAR_MS = 120_000  # sem resposta em 2 min, o botão volta (o ciclo pode ter falhado em silêncio)
ESPERA_PERIODICIDADE_MS = 500       # junta as teclas ("1", "15") num pedido só ao bridge
SEM_VALOR = "—"
EXPLICACAO = ("Gera a timeline das máquinas no portal OEE a partir das contagens da IOT001: "
              "2 Produção Normal, 99 Microparada, 98 Macro parada.")
TEXTO_AGUARDANDO = "Aguardando a primeira execução…"

LEGENDAS = {"2": "Produção Normal", "99": "Microparada", "98": "Macro parada"}
TONS_LEGENDA = {"2": "sucesso", "99": "aviso", "98": "erro"}  # outro código: neutro (cinza-azulado)

COLUNAS = ("Máquina", "Dispositivo/canal", "Micro (s)", "Macro (s)", "Estado atual", "Desde", "Última produção",
           "Apontamentos hoje", "Última leitura")
(COL_MAQUINA, COL_CANAIS, COL_MICRO, COL_MACRO, COL_ESTADO, COL_DESDE, COL_PRODUCAO, COL_HOJE,
 COL_LEITURA) = range(len(COLUNAS))
COLUNAS_NUMERICAS = (COL_MICRO, COL_MACRO, COL_HOJE)
COLUNAS_DATA = {COL_DESDE: "desde", COL_PRODUCAO: "ultima_producao", COL_LEITURA: "ultima_leitura"}
ROTULOS_CURTOS = {COL_HOJE: "Apont. hoje"}
DICAS_COLUNAS = {
    COL_CANAIS: "Dispositivo IoT e canal de produção configurados na máquina no portal OEE.",
    COL_MICRO: "A partir deste tempo sem produção (segundos), o trecho vira 99 Microparada.",
    COL_MACRO: "A partir deste tempo sem produção (segundos), o trecho vira 98 Macro parada.",
    COL_ESTADO: "Legenda do apontamento em aberto agora (2 produzindo, 99 microparada, 98 macro parada "
                "ou o motivo apontado pelo operador).",
    COL_DESDE: "Início do estado atual.",
    COL_PRODUCAO: "Última vez que o contador do canal aumentou.",
    COL_HOJE: "Apontamentos da máquina hoje (hora de Brasília).",
    COL_LEITURA: "Última leitura do dispositivo gravada na IOT001.",
}
LARGURA_MAX = {COL_MAQUINA: 240, COL_CANAIS: 260, COL_ESTADO: 200}
PAPEL_ORDEM = Qt.ItemDataRole.UserRole + 20


# ---------------------------------------------------------------------- lógica pura
def _texto(valor: Any) -> str:
    return "" if valor is None else str(valor).strip()


def _inteiro(valor: Any) -> int | None:
    try:
        return int(valor)
    except (TypeError, ValueError):
        try:
            return int(float(valor))
        except (TypeError, ValueError):
            return None


def ler_iso(texto: Any) -> datetime | None:
    """ISO 8601 local ("AAAA-MM-DDTHH:MM:SS") → datetime; vazio ou inválido → None."""
    t = _texto(texto)
    if not t:
        return None
    try:
        return datetime.fromisoformat(t.replace("Z", "+00:00"))
    except ValueError:
        return None


def formatar_data(texto: Any) -> str:
    """ISO local → "dd/mm HH:MM:SS" (o texto original se não der para ler; "—" se vazio)."""
    d = ler_iso(texto)
    if d is None:
        return _texto(texto) or SEM_VALOR
    return d.strftime("%d/%m %H:%M:%S")


def formatar_data_completa(texto: Any) -> str | None:
    d = ler_iso(texto)
    return d.strftime("%d/%m/%Y %H:%M:%S") if d else None


def tom_legenda(codigo: Any) -> str:
    """Tom da pílula (DelegadoPilula) pela legenda: 2 verde, 99 âmbar, 98 vermelho, outro neutro."""
    return TONS_LEGENDA.get(_texto(codigo), "neutro")


@dataclass
class Maquina:
    maquina_id: int | None
    codigo: str
    nome: str
    canais: str
    micro_s: int | None
    macro_s: int | None
    legenda: str
    legenda_descricao: str
    desde: str
    ultima_producao: str
    apontamentos_hoje: int | None
    ultima_leitura: str
    aviso: str

    @property
    def nome_exibido(self) -> str:
        if self.codigo and self.nome and self.nome != self.codigo:
            return f"{self.codigo} – {self.nome}"
        return self.codigo or self.nome or (f"Máquina {self.maquina_id}" if self.maquina_id is not None else "?")

    @property
    def texto_estado(self) -> str:
        if not self.legenda:
            return SEM_VALOR
        descricao = self.legenda_descricao or LEGENDAS.get(self.legenda, "")
        return f"{self.legenda} · {descricao}" if descricao else self.legenda


@dataclass
class Ciclo:
    quando: str = ""
    proxima: str = ""
    minutos: float | None = None
    ok: bool = True
    mensagem: str = ""
    criados: int = 0
    alterados: int = 0
    excluidos: int = 0
    maquinas: list[Maquina] = field(default_factory=list)


def _maquina(d: dict) -> Maquina:
    return Maquina(
        maquina_id=_inteiro(d.get("maquina_id")),
        codigo=_texto(d.get("codigo")),
        nome=_texto(d.get("nome")),
        canais=_texto(d.get("canais")),
        micro_s=_inteiro(d.get("micro_s")),
        macro_s=_inteiro(d.get("macro_s")),
        legenda=_texto(d.get("legenda")),
        legenda_descricao=_texto(d.get("legenda_descricao")),
        desde=_texto(d.get("desde")),
        ultima_producao=_texto(d.get("ultima_producao")),
        apontamentos_hoje=_inteiro(d.get("apontamentos_hoje")),
        ultima_leitura=_texto(d.get("ultima_leitura")),
        aviso=_texto(d.get("aviso")),
    )


def interpretar_ciclo(texto_json: str) -> Ciclo:
    """JSON de `apontamentos_atualizados` → Ciclo. Campos ausentes ou com tipo errado viram valores neutros;
    JSON ilegível vira um ciclo com falha (a tela mostra o motivo em vez de quebrar)."""
    try:
        d = json.loads(texto_json)
    except (TypeError, ValueError):
        return Ciclo(ok=False, mensagem="Resposta do integrador ilegível (JSON inválido).")
    if not isinstance(d, dict):
        return Ciclo(ok=False, mensagem="Resposta do integrador ilegível (formato inesperado).")
    try:
        minutos = float(d["minutos"]) if d.get("minutos") is not None else None
    except (TypeError, ValueError):
        minutos = None
    maquinas = [_maquina(m) for m in d.get("maquinas") or [] if isinstance(m, dict)]
    return Ciclo(
        quando=_texto(d.get("quando")),
        proxima=_texto(d.get("proxima")),
        minutos=minutos,
        ok=bool(d.get("ok", True)),
        mensagem=_texto(d.get("mensagem")),
        criados=_inteiro(d.get("criados")) or 0,
        alterados=_inteiro(d.get("alterados")) or 0,
        excluidos=_inteiro(d.get("excluidos")) or 0,
        maquinas=maquinas,
    )


def _plural(n: int, singular: str, plural: str) -> str:
    return f"{n} {singular if n == 1 else plural}"


def texto_resumo(ciclo: Ciclo) -> str:
    return (f"Último ciclo: {_plural(ciclo.criados, 'criado', 'criados')} · "
            f"{_plural(ciclo.alterados, 'atualizado', 'atualizados')} · "
            f"{_plural(ciclo.excluidos, 'excluído', 'excluídos')}")


def icone_aviso(paleta: Paleta) -> QIcon:
    """Triângulo de aviso na cor de aviso do tema (desenhado à mão, não depende do glifo na fonte)."""
    icone = QIcon()
    for escala in (1, 2):
        lado = 16 * escala
        pix = QPixmap(lado, lado)
        pix.setDevicePixelRatio(escala)
        pix.fill(Qt.GlobalColor.transparent)
        pintor = QPainter(pix)
        pintor.setRenderHint(QPainter.RenderHint.Antialiasing)
        cor = QColor(paleta.aviso)
        pintor.setPen(QPen(cor, 1.2, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin))
        pintor.setBrush(cor)
        pintor.drawPolygon(QPolygonF([QPointF(8, 1.6), QPointF(15, 14.4), QPointF(1, 14.4)]))
        pintor.setPen(QPen(QColor(paleta.superficie), 1.8, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
        pintor.drawLine(QPointF(8, 5.8), QPointF(8, 9.6))
        pintor.drawPoint(QPointF(8, 12.2))
        pintor.end()
        icone.addPixmap(pix)
    return icone


# ---------------------------------------------------------------------- modelo Qt
class ModeloApontamentos(QAbstractTableModel):
    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        self.linhas: list[Maquina] = []
        self.paleta: Paleta = CLARA
        self._icone_aviso: QIcon | None = None

    def definir(self, maquinas: list[Maquina]) -> None:
        self.beginResetModel()
        self.linhas = list(maquinas)
        self.endResetModel()

    def definir_paleta(self, p: Paleta) -> None:
        self.paleta = p
        self._icone_aviso = None
        if self.linhas:
            self.dataChanged.emit(self.index(0, 0), self.index(len(self.linhas) - 1, len(COLUNAS) - 1))

    def icone_aviso(self) -> QIcon:
        if self._icone_aviso is None:
            self._icone_aviso = icone_aviso(self.paleta)
        return self._icone_aviso

    def texto(self, m: Maquina, col: int) -> str:
        if col == COL_MAQUINA:
            return m.nome_exibido
        if col == COL_CANAIS:
            return m.canais or SEM_VALOR
        if col == COL_MICRO:
            return SEM_VALOR if m.micro_s is None else str(m.micro_s)
        if col == COL_MACRO:
            return SEM_VALOR if m.macro_s is None else str(m.macro_s)
        if col == COL_ESTADO:
            return m.texto_estado
        if col == COL_HOJE:
            return SEM_VALOR if m.apontamentos_hoje is None else str(m.apontamentos_hoje)
        if col in COLUNAS_DATA:
            return formatar_data(getattr(m, COLUNAS_DATA[col]))
        return ""

    # ----- API do Qt -----
    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(self.linhas)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(COLUNAS)

    def headerData(self, secao: int, orientacao: Qt.Orientation, papel: int = Qt.ItemDataRole.DisplayRole):  # noqa: N802
        if orientacao != Qt.Orientation.Horizontal:
            return None
        if papel == Qt.ItemDataRole.DisplayRole:
            return ROTULOS_CURTOS.get(secao, COLUNAS[secao])
        if papel == Qt.ItemDataRole.ToolTipRole:
            return DICAS_COLUNAS.get(secao, COLUNAS[secao] if secao in ROTULOS_CURTOS else None)
        if papel == Qt.ItemDataRole.TextAlignmentRole and secao in COLUNAS_NUMERICAS:
            return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        return None

    def data(self, indice: QModelIndex, papel: int = Qt.ItemDataRole.DisplayRole):
        if not indice.isValid() or indice.row() >= len(self.linhas):
            return None
        m = self.linhas[indice.row()]
        col = indice.column()
        if papel == Qt.ItemDataRole.DisplayRole:
            return self.texto(m, col)
        if papel == PAPEL_ORDEM:
            return self._ordem(m, col)
        if papel == PAPEL_PILULA and col == COL_ESTADO:
            return tom_legenda(m.legenda) if m.legenda else None
        if papel == Qt.ItemDataRole.DecorationRole and col == COL_MAQUINA and m.aviso:
            return self.icone_aviso()
        if papel == Qt.ItemDataRole.ToolTipRole:
            return self._dica(m, col)
        if papel == Qt.ItemDataRole.ForegroundRole and self.texto(m, col) == SEM_VALOR:
            return QColor(self.paleta.texto_fraco)
        if papel == Qt.ItemDataRole.TextAlignmentRole and col in COLUNAS_NUMERICAS:
            return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        return None

    def _ordem(self, m: Maquina, col: int):
        if col in COLUNAS_NUMERICAS:
            valor = {COL_MICRO: m.micro_s, COL_MACRO: m.macro_s, COL_HOJE: m.apontamentos_hoje}[col]
            return -1 if valor is None else valor
        if col in COLUNAS_DATA:
            d = ler_iso(getattr(m, COLUNAS_DATA[col]))
            return d.timestamp() if d else 0.0
        if col == COL_ESTADO:
            # Agrupa por gravidade: macro, micro, outras legendas (motivos do operador), produção, sem estado.
            ordem = {"98": 0, "99": 1, "2": 3}.get(m.legenda, 2 if m.legenda else 4)
            return f"{ordem}|{m.texto_estado}".casefold()
        return self.texto(m, col).casefold()

    def _dica(self, m: Maquina, col: int) -> str | None:
        partes: list[str] = []
        if col == COL_MAQUINA:
            partes.append(m.nome_exibido)
            if m.maquina_id is not None:
                partes.append(f"Id no portal: {m.maquina_id}")
        elif col in COLUNAS_DATA:
            completa = formatar_data_completa(getattr(m, COLUNAS_DATA[col]))
            if completa:
                partes.append(completa)
        elif col == COL_ESTADO and m.legenda and m.legenda not in LEGENDAS:
            partes.append("Legenda apontada pelo operador; o integrador a mantém enquanto o trecho continuar parado.")
        elif col == COL_CANAIS and m.canais:
            partes.append(m.canais)
        if m.aviso:
            partes.append(f"Aviso: {m.aviso}")
        return "\n".join(partes) or None


class FiltroApontamentos(QSortFilterProxyModel):
    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        self.setSortRole(PAPEL_ORDEM)


# ---------------------------------------------------------------------- painel
class PainelApontamentos(QWidget):
    """Conteúdo da aba Apontamentos reais: explicação, Periodicidade, Executar agora, chips de última/próxima
    execução, resumo do último ciclo e a tabela de máquinas.

    Emite `periodicidade_alterada(minutos)` e `executar_pedido()`; a janela liga ao bridge. Sem bridge com esses
    métodos, a janela chama `definir_disponivel(False)` e os controles ficam desabilitados com uma dica.
    """

    periodicidade_alterada = Signal(float)
    executar_pedido = Signal()

    def __init__(self, parent: QWidget | None = None, relogio: Callable[[], float] | None = None,
                 settings: QSettings | None = None, minutos_padrao: float | None = None):
        super().__init__(parent)
        self.setObjectName("paginaAba")
        self._relogio = relogio or time.time
        self._settings = settings
        self._disponivel = True
        self._executando = False
        self.ciclo: Ciclo | None = None
        self._proxima_epoch: float | None = None

        padrao = MINUTOS_PADRAO if minutos_padrao is None else minutos_padrao
        self.minutos_padrao = self._limitar(padrao)
        minutos = self.minutos_padrao
        if settings is not None and settings.value(CHAVE_SETTINGS) is not None:
            minutos = self._limitar(settings.value(CHAVE_SETTINGS), minutos)

        self.modelo = ModeloApontamentos(self)
        self.filtro = FiltroApontamentos(self)
        self.filtro.setSourceModel(self.modelo)

        corpo = QVBoxLayout(self)
        corpo.setContentsMargins(0, 12, 0, 0)
        corpo.setSpacing(12)

        self.explicacao = QLabel(EXPLICACAO, self)
        self.explicacao.setObjectName("descricaoCartao")
        self.explicacao.setWordWrap(True)

        # Barra: Periodicidade [ 3 min ]  [Executar agora]   (chips à direita)
        barra = QHBoxLayout()
        barra.setSpacing(8)
        rotulo = QLabel("PERIODICIDADE", self)
        rotulo.setObjectName("rotuloCampo")
        self.campo_minutos = QSpinBox(self)
        self.campo_minutos.setRange(MINUTOS_MIN, MINUTOS_MAX)
        self.campo_minutos.setSuffix(" min")
        self.campo_minutos.setValue(minutos)
        self.campo_minutos.setFixedWidth(104)
        self.campo_minutos.setAccessibleName("Periodicidade em minutos")
        self.campo_minutos.setToolTip(f"De quantos em quantos minutos a timeline é recalculada "
                                      f"({MINUTOS_MIN} a {MINUTOS_MAX}; padrão {self.minutos_padrao}).")
        self.campo_minutos.lineEdit().setTextMargins(9, 0, 0, 0)
        rotulo.setBuddy(self.campo_minutos)
        self.botao_executar = QPushButton("Executar agora", self)
        self.botao_executar.setCursor(Qt.CursorShape.PointingHandCursor)
        self.botao_executar.setToolTip("Recalcular a timeline de todas as máquinas agora, sem esperar o próximo "
                                       "ciclo.")
        self.botao_executar.clicked.connect(self.executar_agora)
        self.chip_ultima = ChipInfo("ultimaExecucao", "Última execução", self)
        self.chip_proxima = ChipInfo("proximaExecucao", "Próxima execução", self)
        barra.addWidget(rotulo)
        barra.addWidget(self.campo_minutos)
        barra.addWidget(self.botao_executar)
        barra.addStretch(1)
        barra.addWidget(self.chip_ultima)
        barra.addWidget(self.chip_proxima)

        self.resumo = QLabel(self)
        self.resumo.setObjectName("descricaoCartao")
        self.resumo.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.resumo.hide()

        self.falha = ResultadoTeste(self)  # faixa vermelha com a mensagem do ciclo que falhou

        self.tabela = QTableView(self)
        self.tabela.setObjectName("tabelaDados")
        self.tabela.setAccessibleName("Máquinas com apontamento pelo IoT")
        fonte = self.tabela.font()
        fonte.setPointSizeF(9.0)
        self.tabela.setFont(fonte)
        self.tabela.setModel(self.filtro)
        self.tabela.setSortingEnabled(True)
        self.tabela.sortByColumn(COL_MAQUINA, Qt.SortOrder.AscendingOrder)
        self.tabela.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tabela.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.tabela.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.tabela.setAlternatingRowColors(True)
        self.tabela.setShowGrid(False)
        self.tabela.setWordWrap(False)
        self.tabela.setTextElideMode(Qt.TextElideMode.ElideRight)
        self.tabela.verticalHeader().hide()
        self.tabela.verticalHeader().setDefaultSectionSize(34)
        self.tabela.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        cab = self.tabela.horizontalHeader()
        cab.setHighlightSections(False)
        cab.setDefaultAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        cab.setMinimumSectionSize(56)
        cab.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        cab.setStretchLastSection(True)
        self.delegado = DelegadoPilula(self.tabela)
        self.tabela.setItemDelegateForColumn(COL_ESTADO, self.delegado)

        self.vazio = QLabel(self)
        self.vazio.setObjectName("estadoVazio")
        self.vazio.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.vazio.setWordWrap(True)
        self.pilha = QStackedWidget(self)
        self.pilha.addWidget(self.tabela)
        self.pilha.addWidget(self.vazio)

        corpo.addWidget(self.explicacao)
        corpo.addLayout(barra)
        corpo.addWidget(self.resumo)
        corpo.addWidget(self.falha)
        corpo.addWidget(self.pilha, 1)

        self._timer_periodicidade = QTimer(self)
        self._timer_periodicidade.setSingleShot(True)
        self._timer_periodicidade.setInterval(ESPERA_PERIODICIDADE_MS)
        self._timer_periodicidade.timeout.connect(self.aplicar_periodicidade)
        self.campo_minutos.valueChanged.connect(lambda _v: self._timer_periodicidade.start())
        self.campo_minutos.editingFinished.connect(self._aplicar_se_pendente)

        self._timer_limite = QTimer(self)  # rede de segurança do "Executando…"
        self._timer_limite.setSingleShot(True)
        self._timer_limite.setInterval(TEMPO_LIMITE_EXECUTAR_MS)
        self._timer_limite.timeout.connect(self._sem_resposta)

        self._aplicado: int | None = None
        self._atualizar_ultima()
        self._atualizar_proxima()
        self._atualizar_vazio()

    # ----- utilitários -----
    @staticmethod
    def _limitar(valor: Any, padrao: int = MINUTOS_PADRAO) -> int:
        try:
            n = int(round(float(valor)))
        except (TypeError, ValueError):
            return padrao
        return max(MINUTOS_MIN, min(MINUTOS_MAX, n))

    def minutos(self) -> int:
        return self.campo_minutos.value()

    # ----- periodicidade -----
    def _aplicar_se_pendente(self) -> None:
        if self._timer_periodicidade.isActive():
            self.aplicar_periodicidade()

    def aplicar_periodicidade(self, forcar: bool = False) -> None:
        """Salva em QSettings e avisa o bridge (via sinal). `forcar`: emite mesmo sem mudança (ao abrir)."""
        self._timer_periodicidade.stop()
        valor = self.campo_minutos.value()
        if self._settings is not None:
            self._settings.setValue(CHAVE_SETTINGS, valor)
            self._settings.sync()
        if forcar or valor != self._aplicado:
            self._aplicado = valor
            if self._disponivel:
                self.periodicidade_alterada.emit(float(valor))

    # ----- executar agora -----
    def executar_agora(self) -> bool:
        if self._executando or not self._disponivel:
            return False
        self._definir_executando(True)
        self.executar_pedido.emit()
        return True

    def _definir_executando(self, sim: bool) -> None:
        self._executando = sim
        if sim:
            self.botao_executar.setText("Executando…")
            self.botao_executar.setEnabled(False)
            self._timer_limite.start()
        else:
            self._timer_limite.stop()
            self.botao_executar.setText("Executar agora")
            self.botao_executar.setEnabled(self._disponivel)
        self._atualizar_proxima()

    def _sem_resposta(self) -> None:
        if self._executando:
            self._definir_executando(False)

    @property
    def executando(self) -> bool:
        return self._executando

    def definir_disponivel(self, disponivel: bool) -> None:
        """Bridge sem o processo de apontamentos: controles desabilitados com explicação."""
        self._disponivel = bool(disponivel)
        for w in (self.campo_minutos, self.botao_executar):
            w.setEnabled(self._disponivel and not (w is self.botao_executar and self._executando))
        if not self._disponivel:
            dica = "Esta versão do integrador não tem o processo de apontamentos."
            self.botao_executar.setToolTip(dica)
        self._atualizar_vazio()

    # ----- entrada do bridge -----
    def ao_ciclo(self, texto_json: str) -> None:
        """Ligado a `apontamentos_atualizados`. Ciclo com falha e sem máquinas mantém a tabela anterior."""
        ciclo = interpretar_ciclo(texto_json)
        self.ciclo = ciclo
        if ciclo.ok or ciclo.maquinas:
            self.modelo.definir(ciclo.maquinas)
            self._ajustar_colunas()
        # A periodicidade da tela manda (QSettings); `ciclo.minutos` é só informativo, não mexe no campo.
        proxima = ler_iso(ciclo.proxima)
        self._proxima_epoch = proxima.timestamp() if proxima else None
        if ciclo.ok:
            self.falha.ocultar()
            self.resumo.setText(texto_resumo(ciclo))
            self.resumo.show()
        else:
            self.falha.mostrar("falha", ciclo.mensagem or "O ciclo de apontamentos falhou.")
        if self._executando:
            self._definir_executando(False)
        self._atualizar_ultima()
        self._atualizar_proxima()
        self._atualizar_vazio()

    def tique(self) -> None:
        """A cada 1 s (timer da janela): contagem regressiva da próxima execução."""
        self._atualizar_proxima()

    def definir_paleta(self, p: Paleta) -> None:
        self.modelo.definir_paleta(p)
        self.delegado.definir_paleta(p)
        self.tabela.viewport().update()

    def parar(self) -> None:
        for t in (self._timer_periodicidade, self._timer_limite):
            t.stop()

    # ----- chips, resumo, estado vazio -----
    def _atualizar_ultima(self) -> None:
        c = self.ciclo
        if c is None:
            self.chip_ultima.definir("Última execução", SEM_VALOR, "neutro")
            self.chip_ultima.setToolTip("Nenhum ciclo concluído desde que o programa abriu.")
            return
        hora = ler_iso(c.quando)
        texto_hora = hora.strftime("%H:%M:%S") if hora else (c.quando or SEM_VALOR)
        if c.ok:
            self.chip_ultima.definir("Última execução", f"{texto_hora} · ok", "ok")
            dica = c.mensagem or texto_resumo(c)
        else:
            self.chip_ultima.definir("Última execução", f"{texto_hora} · falhou", "erro")
            dica = c.mensagem or "O ciclo de apontamentos falhou."
        completa = formatar_data_completa(c.quando)
        self.chip_ultima.setToolTip(f"{completa}\n{dica}" if completa else dica)

    def _atualizar_proxima(self) -> None:
        if self._executando:
            self.chip_proxima.definir("Próxima execução:", "executando…", "ocupado")
            return
        if self._proxima_epoch is None:
            self.chip_proxima.definir("Próxima execução", SEM_VALOR, "neutro")
            self.chip_proxima.setToolTip("Aparece depois do primeiro ciclo.")
            return
        restante = self._proxima_epoch - self._relogio()
        if restante <= 0:
            self.chip_proxima.definir("Próxima execução:", "em instantes", "ocupado")
        else:
            self.chip_proxima.definir("Próxima execução em", tempo.formatar_contagem(restante), "ativo")
        dica = f"Próximo ciclo às {datetime.fromtimestamp(self._proxima_epoch):%H:%M:%S}."
        if self.chip_proxima.toolTip() != dica:
            self.chip_proxima.setToolTip(dica)

    def _atualizar_vazio(self) -> None:
        if self.modelo.rowCount() > 0:
            self.pilha.setCurrentWidget(self.tabela)
            return
        c = self.ciclo
        if not self._disponivel:
            texto = "Esta versão do integrador não tem o processo de apontamentos reais."
        elif c is None or (not c.ok and not c.maquinas):
            texto = TEXTO_AGUARDANDO
            if c is not None and not c.ok:
                texto += "\n\nO último ciclo falhou: " + (c.mensagem or "motivo não informado.")
            else:
                texto += ("\nO ciclo roda sozinho quando o SQL Server está configurado com senha; "
                          "use Executar agora para não esperar.")
        else:
            texto = ("Nenhuma máquina com apontamento pelo IoT.\nNo portal OEE, a máquina precisa estar ativa, "
                     "com micro e macro parada preenchidos e um canal IoT de produção com dispositivo ativo.")
        self.vazio.setText(texto)
        self.pilha.setCurrentWidget(self.vazio)

    def _ajustar_colunas(self) -> None:
        cab = self.tabela.horizontalHeader()
        fonte = QFont(cab.font())
        fonte.setPointSizeF(8)
        fonte.setWeight(QFont.Weight.DemiBold)
        metricas = QFontMetrics(fonte)
        for col in range(len(COLUNAS)):
            texto_cab = metricas.horizontalAdvance(ROTULOS_CURTOS.get(col, COLUNAS[col])) + 20
            conteudo = self.tabela.sizeHintForColumn(col) if self.filtro.rowCount() else 0
            largura = max(texto_cab, conteudo)
            if col in LARGURA_MAX:
                largura = max(texto_cab, min(largura, LARGURA_MAX[col]))
            cab.resizeSection(col, largura)

    # ----- testes / exportação -----
    def linhas_visiveis(self) -> list[list[str]]:
        linhas = []
        for r in range(self.filtro.rowCount()):
            fonte = self.filtro.mapToSource(self.filtro.index(r, 0)).row()
            m = self.modelo.linhas[fonte]
            linhas.append([self.modelo.texto(m, col) for col in range(len(COLUNAS))])
        return linhas
