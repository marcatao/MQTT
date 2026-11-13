"""Aba "Apontamentos reais": acompanha o processo que gera a timeline das máquinas no portal OEE.

O core (app/core/apontamentos_oee.py) lê as contagens da IOT001 e grava `OEE.dbo.apontamentos_estado` em thread
própria; a regra completa está em docs/apontamentos_reais.md. Esta aba só mostra o resultado e conversa com o
bridge por pontos do contrato (todos opcionais: um bridge sem eles deixa os controles desabilitados):

* `apontamentos_atualizados(str)`: JSON de cada ciclo concluído (ou falho), ver `interpretar_ciclo`. Cada máquina
  traz também `diag` ("ok" | "sem resposta" | "sem MQTT" | null), `turno` ("dentro" | "fora" | "sem jornada") e
  `jornada` (texto, ex. "Seg–Sex 05:00–14:48", ou null);
* `definir_periodicidade_apontamentos(minutos)`: chamado quando o usuário muda a periodicidade e ao abrir a janela
  (o valor fica em QSettings, chave `apontamentos/minutos`, e vale mais que `[apontamentos] minutos`);
* `executar_apontamentos_agora()`: botão "Executar agora", que fica "Executando…" até o próximo ciclo chegar (ou
  até `TEMPO_LIMITE_EXECUTAR_MS`, rede de segurança);
* `recalcular_dia_apontamentos(maquina_id, "AAAA-MM-DD")` + `recalculo_concluido(maquina_id, dia, sucesso, msg)`:
  botão "Recalcular…" de cada linha. O diálogo (`DialogoRecalcular`) escolhe o dia (de hoje até
  `[apontamentos] dias_correcao` dias atrás) e avisa que os apontamentos MANUAIS a partir dele serão apagados; a linha
  fica "Recalculando…" até a resposta daquela máquina (ou `TEMPO_LIMITE_RECALCULO_S`).

O painel não chama o bridge diretamente: emite `periodicidade_alterada(float)`, `executar_pedido()` e
`recalculo_pedido(int, str)`, que a janela liga aos métodos do bridge. Partes: funções puras (`interpretar_ciclo`,
`formatar_data`, `tom_legenda`, `texto_aviso_recalculo`), modelo Qt (`ModeloApontamentos` + `FiltroApontamentos`),
o diálogo e o conteúdo da aba (`PainelApontamentos`).
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Callable

from PySide6.QtCore import (
    QAbstractTableModel,
    QDate,
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
    QDateEdit,
    QDialog,
    QFrame,
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
from .comandos import PAPEL_ACAO, DelegadoAcao
from .estilo import CLARA, Paleta
from .widgets import PAPEL_PILULA, ChipInfo, DelegadoPilula, ResultadoTeste

# ---------------------------------------------------------------------- constantes
MINUTOS_PADRAO = 3
MINUTOS_MIN, MINUTOS_MAX = 1, 60
CHAVE_SETTINGS = "apontamentos/minutos"
TEMPO_LIMITE_EXECUTAR_MS = 120_000  # sem resposta em 2 min, o botão volta (o ciclo pode ter falhado em silêncio)
TEMPO_LIMITE_RECALCULO_S = 120      # idem para o "Recalculando…" de cada máquina
ESPERA_PERIODICIDADE_MS = 500       # junta as teclas ("1", "15") num pedido só ao bridge
DIAS_CORRECAO_PADRAO = 7
SEM_VALOR = "—"
EXPLICACAO = ("Gera a timeline das máquinas no portal OEE a partir das contagens da IOT001: "
              "2 Produção Normal, 99 Microparada, 98 Macro parada, 97 Sem apontamento IOT e 96 Fora de turno.")
TEXTO_AGUARDANDO = "Aguardando a primeira execução…"
TEXTO_RECALCULAR = "Recalcular…"
TEXTO_RECALCULANDO = "Recalculando…"

LEGENDAS = {"2": "Produção Normal", "99": "Microparada", "98": "Macro parada", "97": "Sem apontamento IOT",
            "96": "Fora de turno"}
# 97 (aparelho sem leitura): cinza mais forte que o neutro dos motivos do operador (texto sobre borda_forte);
# 96 (fora do turno): índigo suave (tokens `indigo`/`indigo_suave` da paleta). Outro código: neutro (cinza-azulado).
TONS_LEGENDA = {"2": "sucesso", "99": "aviso", "98": "erro", "97": "sem_iot", "96": "fora_turno"}
# Ordem do Estado atual: paradas primeiro (98, 97, 96, 99), depois motivos do operador, produção e sem estado.
ORDEM_LEGENDA = {"98": 0, "97": 1, "96": 2, "99": 3, "2": 5}

TURNOS = {"dentro": "dentro", "fora": "fora", "sem jornada": "sem jornada"}
TONS_TURNO = {"dentro": "destaque", "fora": "fora_turno", "sem jornada": "neutro"}
TONS_DIAG = {"ok": "sucesso", "sem resposta": "erro", "sem MQTT": "neutro"}
DICAS_DIAG = {
    "ok": "O aparelho respondeu ao diagnóstico pedido antes deste ciclo.",
    "sem resposta": ("O aparelho não respondeu ao diagnóstico antes deste ciclo: a parada em aberto virou 97 Sem "
                     "apontamento IOT.\nConfira o tópico de confirmação na aba Clientes (Tópicos…)."),
    "sem MQTT": "O integrador não está conectado ao broker: não deu para perguntar ao aparelho neste ciclo.",
}

# Ordem das colunas = ordem na tela: o que se olha sempre à esquerda; o botão antes das colunas de configuração,
# para caber em 1440×900 sem rolar (Micro/Macro e Dispositivo/canal ficam no fim, com a sobra).
COLUNAS = ("Máquina", "Estado atual", "Desde", "Turno", "Diagnóstico", "Última produção", "Última leitura", "Ação",
           "Apontamentos hoje", "Micro/Macro (s)", "Dispositivo/canal")
(COL_MAQUINA, COL_ESTADO, COL_DESDE, COL_TURNO, COL_DIAG, COL_PRODUCAO, COL_LEITURA, COL_ACAO, COL_HOJE,
 COL_LIMITES, COL_CANAIS) = range(len(COLUNAS))
COLUNAS_NUMERICAS = (COL_HOJE,)
COLUNAS_DATA = {COL_DESDE: "desde", COL_PRODUCAO: "ultima_producao", COL_LEITURA: "ultima_leitura"}
COLUNAS_PILULA = (COL_ESTADO, COL_TURNO, COL_DIAG)
ROTULOS_CURTOS = {COL_DIAG: "Diag.", COL_PRODUCAO: "Últ. produção", COL_LEITURA: "Últ. leitura", COL_HOJE: "Hoje",
                  COL_ACAO: ""}
DICAS_COLUNAS = {
    COL_ESTADO: "Legenda do apontamento em aberto agora (2 produzindo, 99 microparada, 98 macro parada, 97 sem "
                "leitura do aparelho, 96 fora de turno ou o motivo apontado pelo operador).",
    COL_DESDE: "Início do estado atual.",
    COL_TURNO: "Se agora é horário de trabalho da máquina (calendário e jornada do portal). Fora do turno, "
               "paradas viram 96.",
    COL_DIAG: "Resposta do aparelho ao diagnóstico pedido antes do ciclo. Sem resposta, a parada em aberto vira 97.",
    COL_PRODUCAO: "Última vez que o contador do canal aumentou.",
    COL_LEITURA: "Última leitura do dispositivo gravada na IOT001.",
    COL_HOJE: "Apontamentos da máquina hoje (hora de Brasília).",
    COL_ACAO: "Refazer a timeline desta máquina a partir de um dia.",
    COL_LIMITES: "Micro parada / macro parada, em segundos: a partir desses tempos sem produção o trecho vira 99 "
                 "ou 98.",
    COL_CANAIS: "Dispositivo IoT e canal de produção configurados na máquina no portal OEE.",
}
LARGURA_MAX = {COL_MAQUINA: 170, COL_ESTADO: 170, COL_TURNO: 100, COL_DIAG: 104, COL_CANAIS: 240}
LARGURA_MIN = {COL_ACAO: 124}
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
    """Tom da pílula (DelegadoLegenda) pela legenda: 2 verde, 99 âmbar, 98 vermelho, 97 cinza, 96 índigo,
    outro neutro."""
    return TONS_LEGENDA.get(_texto(codigo), "neutro")


def texto_aviso_recalculo(dia: date) -> str:
    return (f"Os apontamentos MANUAIS desta máquina a partir de {dia:%d/%m/%Y} serão apagados e a timeline será "
            "refeita pelo IoT.")


class DelegadoLegenda(DelegadoPilula):
    """Pílulas da aba (Estado atual, Turno, Diagnóstico): os tons de sempre + ``sem_iot`` (97, cinza) e
    ``fora_turno`` (96, índigo), com cores próprias nos temas claro e escuro."""

    def cores(self, tom: str) -> tuple[str, str]:
        if tom == "sem_iot":
            return self.paleta.texto, self.paleta.borda_forte
        if tom == "fora_turno":
            return self.paleta.indigo, self.paleta.indigo_suave
        return super().cores(tom)


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
    diag: str = ""      # "ok" | "sem resposta" | "sem MQTT" | "" (null)
    turno: str = ""     # "dentro" | "fora" | "sem jornada" | ""
    jornada: str = ""   # "Seg–Sex 05:00–14:48" | ""

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

    @property
    def texto_limites(self) -> str:
        if self.micro_s is None and self.macro_s is None:
            return SEM_VALOR
        micro = SEM_VALOR if self.micro_s is None else str(self.micro_s)
        macro = SEM_VALOR if self.macro_s is None else str(self.macro_s)
        return f"{micro} / {macro}"


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
        diag=_texto(d.get("diag")),
        turno=_texto(d.get("turno")),
        jornada=_texto(d.get("jornada")),
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


@dataclass
class EstadoRecalculo:
    estado: str           # "enviando" | "ok" | "falha"
    quando: float         # epoch do pedido (enviando) ou da resposta
    dia: str              # "AAAA-MM-DD"
    mensagem: str = ""


def _dia_br(dia_iso: str) -> str:
    try:
        return date.fromisoformat(dia_iso).strftime("%d/%m/%Y")
    except ValueError:
        return dia_iso


# ---------------------------------------------------------------------- modelo Qt
class ModeloApontamentos(QAbstractTableModel):
    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        self.linhas: list[Maquina] = []
        self.paleta: Paleta = CLARA
        self.recalculo_disponivel = False                 # o bridge tem recalcular_dia_apontamentos
        self.recalculos: dict[int, EstadoRecalculo] = {}  # maquina_id → último Recalcular
        self._icone_aviso: QIcon | None = None

    def definir(self, maquinas: list[Maquina]) -> None:
        self.beginResetModel()
        self.linhas = list(maquinas)
        self.endResetModel()

    def atualizar_tudo(self) -> None:
        if self.linhas:
            self.dataChanged.emit(self.index(0, 0), self.index(len(self.linhas) - 1, len(COLUNAS) - 1))

    def atualizar_maquina(self, maquina_id: int) -> None:
        for r, m in enumerate(self.linhas):
            if m.maquina_id == maquina_id:
                self.dataChanged.emit(self.index(r, 0), self.index(r, len(COLUNAS) - 1))

    def definir_paleta(self, p: Paleta) -> None:
        self.paleta = p
        self._icone_aviso = None
        self.atualizar_tudo()

    def icone_aviso(self) -> QIcon:
        if self._icone_aviso is None:
            self._icone_aviso = icone_aviso(self.paleta)
        return self._icone_aviso

    def maquina(self, linha: int) -> Maquina:
        return self.linhas[linha]

    def acao(self, m: Maquina) -> tuple[str, str] | None:
        """(estado, texto) do botão "Recalcular…" (DelegadoAcao); None sem id da máquina."""
        if m.maquina_id is None:
            return None
        r = self.recalculos.get(m.maquina_id)
        if r is not None and r.estado == "enviando":
            return ("enviando", TEXTO_RECALCULANDO)
        if not self.recalculo_disponivel:
            return ("desabilitado", TEXTO_RECALCULAR)
        if r is not None and r.estado == "ok":
            return ("ok", "Refeito ✓")
        if r is not None and r.estado == "falha":
            return ("falha", "Falhou")
        return ("pronto", TEXTO_RECALCULAR)

    def texto(self, m: Maquina, col: int) -> str:
        if col == COL_MAQUINA:
            return m.nome_exibido
        if col == COL_CANAIS:
            return m.canais or SEM_VALOR
        if col == COL_LIMITES:
            return m.texto_limites
        if col == COL_ESTADO:
            return m.texto_estado
        if col == COL_TURNO:
            return TURNOS.get(m.turno, m.turno) or SEM_VALOR
        if col == COL_DIAG:
            return m.diag  # null = célula vazia
        if col == COL_HOJE:
            return SEM_VALOR if m.apontamentos_hoje is None else str(m.apontamentos_hoje)
        if col in COLUNAS_DATA:
            return formatar_data(getattr(m, COLUNAS_DATA[col]))
        if col == COL_ACAO:
            acao = self.acao(m)
            return acao[1] if acao else ""
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
            return "" if col == COL_ACAO else self.texto(m, col)  # o botão é desenhado pelo delegate
        if papel == PAPEL_ORDEM:
            return self._ordem(m, col)
        if papel == PAPEL_PILULA and col in COLUNAS_PILULA:
            return self._tom(m, col)
        if papel == PAPEL_ACAO and col == COL_ACAO:
            return self.acao(m)
        if papel == Qt.ItemDataRole.DecorationRole and col == COL_MAQUINA and m.aviso:
            return self.icone_aviso()
        if papel == Qt.ItemDataRole.ToolTipRole:
            return self._dica(m, col)
        if papel == Qt.ItemDataRole.ForegroundRole and self.texto(m, col) == SEM_VALOR:
            return QColor(self.paleta.texto_fraco)
        if papel == Qt.ItemDataRole.TextAlignmentRole and col in COLUNAS_NUMERICAS:
            return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        return None

    def _tom(self, m: Maquina, col: int) -> str | None:
        if col == COL_ESTADO:
            return tom_legenda(m.legenda) if m.legenda else None
        if col == COL_TURNO:
            return TONS_TURNO.get(m.turno, "neutro") if m.turno else None
        if col == COL_DIAG:
            return TONS_DIAG.get(m.diag, "neutro") if m.diag else None
        return None

    def _ordem(self, m: Maquina, col: int):
        if col == COL_HOJE:
            return -1 if m.apontamentos_hoje is None else m.apontamentos_hoje
        if col == COL_LIMITES:
            return (m.micro_s if m.micro_s is not None else -1) * 1_000_000 + (m.macro_s or 0)
        if col in COLUNAS_DATA:
            d = ler_iso(getattr(m, COLUNAS_DATA[col]))
            return d.timestamp() if d else 0.0
        if col == COL_ESTADO:
            # Paradas por gravidade (98, 97, 96, 99), motivos do operador, produção (2) e sem estado.
            ordem = ORDEM_LEGENDA.get(m.legenda, 4 if m.legenda else 6)
            return f"{ordem}|{m.texto_estado}".casefold()
        if col == COL_DIAG:  # problemas primeiro
            return {"sem resposta": 0, "sem MQTT": 1, "ok": 2}.get(m.diag, 3)
        if col == COL_TURNO:
            return {"fora": 0, "dentro": 1, "sem jornada": 2}.get(m.turno, 3)
        return self.texto(m, col).casefold()

    def _dica_acao(self, m: Maquina) -> str | None:
        if m.maquina_id is None:
            return None
        if not self.recalculo_disponivel:
            return "Esta versão do integrador não tem o recálculo por máquina."
        r = self.recalculos.get(m.maquina_id)
        if r is None:
            return "Refazer a timeline desta máquina a partir de um dia (apaga os apontamentos manuais do período)."
        desde = _dia_br(r.dia)
        if r.estado == "enviando":
            return f"Recalculando desde {desde}…"
        hora = datetime.fromtimestamp(r.quando).strftime("%H:%M:%S")
        situacao = "Recalculado" if r.estado == "ok" else "Falha ao recalcular"
        return f"{situacao} desde {desde} (às {hora}).\n{r.mensagem}".strip() + "\nClique para recalcular de novo."

    def _dica(self, m: Maquina, col: int) -> str | None:
        partes: list[str] = []
        if col == COL_ACAO:
            return self._dica_acao(m)
        if col == COL_MAQUINA:
            partes.append(m.nome_exibido)
            if m.maquina_id is not None:
                partes.append(f"Id no portal: {m.maquina_id}")
        elif col in COLUNAS_DATA:
            completa = formatar_data_completa(getattr(m, COLUNAS_DATA[col]))
            if completa:
                partes.append(completa)
        elif col == COL_ESTADO and m.legenda:
            partes.append(m.texto_estado)  # a pílula pode cortar a descrição
            if m.legenda not in LEGENDAS:
                partes.append("Legenda apontada pelo operador; o integrador a mantém enquanto o trecho continuar "
                              "parado.")
        elif col == COL_TURNO:
            if m.turno == "sem jornada":
                partes.append("Máquina sem horário de trabalho no portal: não há 96 Fora de turno.")
            elif m.jornada:
                partes.append(f"Jornada: {m.jornada}")
        elif col == COL_DIAG and m.diag:
            partes.append(DICAS_DIAG.get(m.diag, m.diag))
        elif col == COL_LIMITES:
            partes.append(f"Microparada a partir de {m.micro_s if m.micro_s is not None else SEM_VALOR} s; "
                          f"macro parada a partir de {m.macro_s if m.macro_s is not None else SEM_VALOR} s.")
        elif col == COL_CANAIS and m.canais:
            partes.append(m.canais)
        if m.aviso:
            partes.append(f"Aviso: {m.aviso}")
        return "\n".join(partes) or None


class FiltroApontamentos(QSortFilterProxyModel):
    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        self.setSortRole(PAPEL_ORDEM)


# ---------------------------------------------------------------------- diálogo Recalcular
class DialogoRecalcular(QDialog):
    """Escolha do dia (de hoje até `dias` dias atrás, padrão hoje) e aviso de que os apontamentos manuais a partir
    dele serão apagados. "Cancelar" é o botão padrão (Enter não recalcula por engano)."""

    def __init__(self, nome_maquina: str, hoje: date, dias: int = DIAS_CORRECAO_PADRAO,
                 parent: QWidget | None = None):
        super().__init__(parent)
        self.hoje = hoje
        self.dias = max(0, int(dias))
        self.setObjectName("dialogoRecalcular")
        self.setWindowTitle(f"Recalcular apontamentos — {nome_maquina}")
        self.setModal(True)
        self.setMinimumWidth(480)

        corpo = QVBoxLayout(self)
        corpo.setContentsMargins(24, 24, 24, 24)
        corpo.setSpacing(16)

        titulo = QLabel(f"Recalcular apontamentos — {nome_maquina}", self)
        titulo.setObjectName("tituloDialogo")
        titulo.setWordWrap(True)
        limite = "só hoje" if self.dias == 0 else f"até {self.dias} dia{'s' if self.dias != 1 else ''} atrás"
        descricao = QLabel("Refaz a timeline desta máquina pelas contagens do IoT, desde 00:00 do dia escolhido até "
                           f"agora ({limite}).", self)
        descricao.setObjectName("descricaoDialogo")
        descricao.setWordWrap(True)
        cab = QVBoxLayout()
        cab.setSpacing(4)
        cab.addWidget(titulo)
        cab.addWidget(descricao)
        corpo.addLayout(cab)

        rotulo = QLabel("A PARTIR DE", self)
        rotulo.setObjectName("rotuloCampo")
        self.campo_dia = QDateEdit(self)
        self.campo_dia.setCalendarPopup(True)
        self.campo_dia.setDisplayFormat("dd/MM/yyyy")
        self.campo_dia.setDateRange(QDate(self.minimo), QDate(hoje))
        self.campo_dia.setDate(QDate(hoje))
        self.campo_dia.setFixedWidth(160)
        self.campo_dia.setAccessibleName("Dia a partir do qual recalcular")
        self.campo_dia.setToolTip(f"De {self.minimo:%d/%m/%Y} a {hoje:%d/%m/%Y}.")
        rotulo.setBuddy(self.campo_dia)
        campo = QVBoxLayout()
        campo.setSpacing(6)
        campo.addWidget(rotulo)
        campo.addWidget(self.campo_dia)
        corpo.addLayout(campo)

        caixa_aviso = QFrame(self)
        caixa_aviso.setObjectName("avisoRecalcular")
        lay_aviso = QHBoxLayout(caixa_aviso)
        lay_aviso.setContentsMargins(12, 10, 12, 10)
        lay_aviso.setSpacing(8)
        icone = QLabel("!", caixa_aviso)
        icone.setObjectName("avisoRecalcularIcone")
        icone.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignHCenter)
        icone.setFixedWidth(16)
        self.aviso = QLabel(caixa_aviso)
        self.aviso.setObjectName("avisoRecalcularTexto")
        self.aviso.setWordWrap(True)
        lay_aviso.addWidget(icone)
        lay_aviso.addWidget(self.aviso, 1)
        corpo.addWidget(caixa_aviso)

        botoes = QHBoxLayout()
        botoes.setSpacing(8)
        botoes.addStretch(1)
        self.botao_cancelar = QPushButton("Cancelar", self)
        self.botao_cancelar.clicked.connect(self.reject)
        self.botao_recalcular = QPushButton("Recalcular", self)
        self.botao_recalcular.setObjectName("botaoPerigo")
        self.botao_recalcular.setCursor(Qt.CursorShape.PointingHandCursor)
        self.botao_recalcular.clicked.connect(self.accept)
        self.botao_cancelar.setDefault(True)
        botoes.addWidget(self.botao_cancelar)
        botoes.addWidget(self.botao_recalcular)
        corpo.addLayout(botoes)

        QWidget.setTabOrder(self.campo_dia, self.botao_cancelar)
        QWidget.setTabOrder(self.botao_cancelar, self.botao_recalcular)
        self.campo_dia.dateChanged.connect(self._atualizar_aviso)
        self._atualizar_aviso()
        self.campo_dia.setFocus()

    @property
    def minimo(self) -> date:
        return self.hoje - timedelta(days=self.dias)

    def dia(self) -> date:
        return self.campo_dia.date().toPython()

    def definir_dia(self, dia: date) -> None:
        self.campo_dia.setDate(QDate(dia))

    def _atualizar_aviso(self, *_args) -> None:
        self.aviso.setText(texto_aviso_recalculo(self.dia()))


# ---------------------------------------------------------------------- painel
class PainelApontamentos(QWidget):
    """Conteúdo da aba Apontamentos reais: explicação, Periodicidade, Executar agora, chips de última/próxima
    execução, resumo do último ciclo e a tabela de máquinas (com o botão Recalcular… em cada linha).

    Emite `periodicidade_alterada(minutos)`, `executar_pedido()` e `recalculo_pedido(maquina_id, "AAAA-MM-DD")`;
    a janela liga ao bridge. Sem bridge com esses métodos, a janela chama `definir_disponivel(False)` /
    `definir_recalculo_disponivel(False)` e os controles ficam desabilitados com uma dica.
    `executar_dialogo(dlg) -> int` é injetável para testar sem diálogo modal.
    """

    periodicidade_alterada = Signal(float)
    executar_pedido = Signal()
    recalculo_pedido = Signal(int, str)

    def __init__(self, parent: QWidget | None = None, relogio: Callable[[], float] | None = None,
                 settings: QSettings | None = None, minutos_padrao: float | None = None,
                 dias_correcao: int | None = None):
        super().__init__(parent)
        self.setObjectName("paginaAba")
        self._relogio = relogio or time.time
        self._settings = settings
        self._disponivel = True
        self._executando = False
        self.ciclo: Ciclo | None = None
        self._proxima_epoch: float | None = None
        self.dias_correcao = DIAS_CORRECAO_PADRAO if dias_correcao is None else max(0, int(dias_correcao))
        self.executar_dialogo: Callable[[QDialog], int] = lambda dlg: dlg.exec()
        self.ultimo_dialogo: DialogoRecalcular | None = None

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
        self.resultado_recalculo = ResultadoTeste(self)  # resultado do último Recalcular (verde ou vermelho)

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
        cab.setMinimumSectionSize(48)
        cab.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        cab.setStretchLastSection(True)
        self.delegado = DelegadoLegenda(self.tabela)
        for col in COLUNAS_PILULA:
            self.tabela.setItemDelegateForColumn(col, self.delegado)
        # Botão da linha desenhado pelo delegate (segue a linha certa com a ordenação), como o Zerar contadores.
        self.delegado_acao = DelegadoAcao(self.tabela)
        self.delegado_acao.acionado.connect(self._ao_acionar)
        self.tabela.setItemDelegateForColumn(COL_ACAO, self.delegado_acao)
        self.tabela.setMouseTracking(True)
        self.tabela.entered.connect(self._ao_passar)
        self.tabela.activated.connect(self._ao_ativar)  # Enter/duplo clique na célula do botão

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
        corpo.addWidget(self.resultado_recalculo)
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

    def hoje(self) -> date:
        return date.fromtimestamp(self._relogio())

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

    # ----- recalcular o dia de uma máquina -----
    def definir_recalculo_disponivel(self, disponivel: bool) -> None:
        self.modelo.recalculo_disponivel = bool(disponivel)
        self.modelo.atualizar_tudo()
        self._ajustar_colunas()

    def recalcular(self, maquina_id: int) -> bool:
        """Abre o diálogo da máquina; confirmado, emite `recalculo_pedido` e a linha fica "Recalculando…"."""
        m = next((x for x in self.modelo.linhas if x.maquina_id == maquina_id), None)
        if m is None or not self.modelo.recalculo_disponivel:
            return False
        atual = self.modelo.recalculos.get(maquina_id)
        if atual is not None and atual.estado == "enviando":
            return False
        dlg = DialogoRecalcular(m.nome_exibido, self.hoje(), self.dias_correcao, self.window())
        self.ultimo_dialogo = dlg
        try:
            if self.executar_dialogo(dlg) != QDialog.DialogCode.Accepted:
                return False
            dia = dlg.dia().isoformat()
        finally:
            dlg.deleteLater()
        self.modelo.recalculos[maquina_id] = EstadoRecalculo("enviando", self._relogio(), dia)
        self.modelo.atualizar_maquina(maquina_id)
        self.recalculo_pedido.emit(int(maquina_id), dia)
        return True

    def recalculando(self, maquina_id: int) -> bool:
        r = self.modelo.recalculos.get(maquina_id)
        return r is not None and r.estado == "enviando"

    def ao_recalculo_concluido(self, maquina_id: int, dia: str, sucesso: bool, mensagem: str) -> None:
        """Ligado a `recalculo_concluido`. Atualiza o botão da linha e a faixa de resultado."""
        maquina_id = int(maquina_id)
        anterior = self.modelo.recalculos.get(maquina_id)
        dia = dia or (anterior.dia if anterior else "")
        mensagem = (mensagem or "").strip() or ("Timeline refeita." if sucesso else "O recálculo falhou.")
        self.modelo.recalculos[maquina_id] = EstadoRecalculo("ok" if sucesso else "falha", self._relogio(), dia,
                                                             mensagem)
        self.modelo.atualizar_maquina(maquina_id)
        m = next((x for x in self.modelo.linhas if x.maquina_id == maquina_id), None)
        nome = m.nome_exibido if m else f"Máquina {maquina_id}"
        self.resultado_recalculo.mostrar("ok" if sucesso else "falha",
                                         f"{nome}, desde {_dia_br(dia)}: {mensagem}")

    def _ao_acionar(self, indice: QModelIndex) -> None:
        fonte = self.filtro.mapToSource(indice)
        if fonte.isValid():
            m = self.modelo.maquina(fonte.row())
            if m.maquina_id is not None:
                self.recalcular(m.maquina_id)

    def _ao_ativar(self, indice: QModelIndex) -> None:
        if indice.column() == COL_ACAO:
            acao = indice.data(PAPEL_ACAO)
            if acao and acao[0] in DelegadoAcao.CLICAVEIS:
                self._ao_acionar(indice)

    def _ao_passar(self, indice: QModelIndex) -> None:
        acao = indice.data(PAPEL_ACAO) if indice.column() == COL_ACAO else None
        if acao and acao[0] in DelegadoAcao.CLICAVEIS:
            self.tabela.viewport().setCursor(Qt.CursorShape.PointingHandCursor)
        else:
            self.tabela.viewport().unsetCursor()

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
        """A cada 1 s (timer da janela): contagem regressiva e tempo-limite dos recálculos sem resposta."""
        agora = self._relogio()
        for maquina_id, r in list(self.modelo.recalculos.items()):
            if r.estado == "enviando" and agora - r.quando > TEMPO_LIMITE_RECALCULO_S:
                self.ao_recalculo_concluido(maquina_id, r.dia, False,
                                            f"Sem resposta do integrador em {TEMPO_LIMITE_RECALCULO_S // 60} min; "
                                            "o resultado é desconhecido. Confira a aba Atividade.")
        self._atualizar_proxima()

    def definir_paleta(self, p: Paleta) -> None:
        self.modelo.definir_paleta(p)
        self.delegado.definir_paleta(p)
        self.delegado_acao.definir_paleta(p)
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
            largura = max(texto_cab, conteudo, LARGURA_MIN.get(col, 0))
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
