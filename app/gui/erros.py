"""Janela "Erros registrados": aberta pelo card ERROS (CLAUDE.md, "Erros detalhados").

Guarda os erros da sessão (sinal `erro_registrado`, um JSON por erro) e, ao abrir, acrescenta os do arquivo do
dia (`ler_erros_do_dia(pasta_logs)`, lido fora da thread da GUI) sem duplicar o que já está em memória: compara
por (quando, mensagem, tópico), contando repetições (dois erros iguais no mesmo segundo continuam sendo dois).

Partes: lógica pura (`RegistroErro`, `ListaErros`), modelo Qt (`ModeloErros` + `FiltroErros`) e a janela
(`JanelaErros`, QDialog não modal) com filtro por categoria, busca, painel de detalhe, cópia e exportação CSV.
"""
from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, Iterable

from PySide6.QtCore import QAbstractTableModel, QModelIndex, QObject, QSortFilterProxyModel, QTimer, QUrl, Qt
from PySide6.QtGui import QDesktopServices, QGuiApplication, QKeySequence, QShortcut
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QFileDialog,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QSplitter,
    QStackedWidget,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from . import tempo
from .estilo import Paleta, fonte_mono
from .widgets import PAPEL_PILULA, refiltrar, rodar_em_thread, ChipInfo, DelegadoPilula, avisar


def _ler_erros_do_dia_local(pasta_logs, data: date | None = None) -> list[dict]:
    """Reserva simples enquanto `app.core.erros` não existir: lê `<pasta_logs>/erros_AAAAMMDD.jsonl`."""
    from app.config import pasta_base

    pasta = Path(pasta_logs)
    if not pasta.is_absolute():
        pasta = pasta_base() / pasta
    caminho = pasta / f"erros_{(data or date.today()):%Y%m%d}.jsonl"
    try:
        bruto = caminho.read_bytes()
    except OSError:
        return []
    registros = []
    for linha in bruto.decode("utf-8", errors="replace").splitlines():
        try:
            obj = json.loads(linha)
        except ValueError:
            continue
        if isinstance(obj, dict):
            registros.append(obj)
    return registros


try:
    from app.core.erros import ler_erros_do_dia
except ImportError:  # módulo do core ainda não disponível
    ler_erros_do_dia = _ler_erros_do_dia_local

LIMITE_ERROS = 5000
INTERVALO_LOTE_MS = 200

CATEGORIAS = {  # valor → (rótulo na tabela, rótulo no filtro, tom da pílula)
    "payload": ("Payload", "Payload", "aviso"),
    "sql": ("SQL", "SQL Server", "erro"),
    "contingencia": ("Contingência", "Contingência", "destaque"),
    "mqtt": ("MQTT", "MQTT", "roxo"),
    "fila": ("Fila", "Fila", "neutro"),
    "comando": ("Comando", "Comandos ao dispositivo", "ciano"),
    "apontamentos": ("Apontamentos", "Apontamentos reais (OEE)", "erro"),
}
COLUNAS = ("Data/hora", "Categoria", "Mensagem")
COL_QUANDO, COL_CATEGORIA, COL_MENSAGEM = range(3)
PAPEL_ORDEM = Qt.ItemDataRole.UserRole


# ---------------------------------------------------------------------- lógica pura
@dataclass(frozen=True)
class RegistroErro:
    quando: str
    categoria: str
    mensagem: str
    topico: str | None = None
    detalhe: str | None = None
    payload: str | None = None

    @classmethod
    def de_dict(cls, d: dict) -> "RegistroErro":
        def txt(chave: str) -> str | None:
            v = d.get(chave)
            return None if v is None or v == "" else str(v)

        return cls(quando=txt("quando") or "", categoria=(txt("categoria") or "").lower(),
                   mensagem=txt("mensagem") or "", topico=txt("topico"), detalhe=txt("detalhe"),
                   payload=txt("payload"))

    @classmethod
    def de_json(cls, texto: str) -> "RegistroErro":
        try:
            obj = json.loads(texto)
        except (TypeError, ValueError):
            obj = None
        if not isinstance(obj, dict):
            return cls(quando="", categoria="", mensagem=str(texto))
        return cls.de_dict(obj)

    @property
    def chave(self) -> tuple[str, str, str | None]:
        return (self.quando, self.mensagem, self.topico)

    @property
    def rotulo_categoria(self) -> str:
        return CATEGORIAS.get(self.categoria, (self.categoria.capitalize() or "Outro",))[0]

    @property
    def data_hora(self) -> str:
        return tempo.formatar_data_hora(self.quando) if self.quando else "—"

    def payload_formatado(self) -> str:
        """Payload com JSON identado quando for JSON; senão como veio."""
        if not self.payload:
            return ""
        try:
            obj = json.loads(self.payload)
        except ValueError:
            return self.payload
        if isinstance(obj, (dict, list)):
            return json.dumps(obj, ensure_ascii=False, indent=2)
        return self.payload

    def texto_completo(self) -> str:
        partes = [f"Data/hora: {self.data_hora}", f"Categoria: {self.rotulo_categoria}",
                  f"Mensagem: {self.mensagem}", f"Tópico: {self.topico or '—'}", f"Detalhe: {self.detalhe or '—'}"]
        payload = self.payload_formatado()
        partes.append("Payload:" + (f"\n{payload}" if payload else " —"))
        return "\n".join(partes)


class ListaErros:
    """Erros em ordem cronológica, no máximo `limite` (os mais antigos saem primeiro)."""

    def __init__(self, limite: int = LIMITE_ERROS):
        self.limite = limite
        self.itens: list[RegistroErro] = []

    def __len__(self) -> int:
        return len(self.itens)

    def adicionar(self, novos: Iterable[RegistroErro]) -> int:
        """Acrescenta no fim (erros da sessão). Devolve quantos saíram do começo pelo limite."""
        self.itens.extend(novos)
        excesso = max(0, len(self.itens) - self.limite)
        if excesso:
            del self.itens[:excesso]
        return excesso

    def mesclar(self, registros: Iterable[RegistroErro]) -> int:
        """Junta os do arquivo do dia sem duplicar os da memória (por chave, contando repetições).
        Devolve quantos entraram."""
        restantes = Counter(r.chave for r in self.itens)
        entram = []
        for r in registros:
            if restantes[r.chave] > 0:
                restantes[r.chave] -= 1
            else:
                entram.append(r)
        if entram:
            # sort estável: dentro do mesmo segundo os do arquivo (mais antigos) ficam antes dos da memória
            self.itens = sorted(entram + self.itens, key=lambda r: r.quando)
            if len(self.itens) > self.limite:
                del self.itens[:len(self.itens) - self.limite]
        return len(entram)


# ---------------------------------------------------------------------- modelo Qt
class ModeloErros(QAbstractTableModel):
    def __init__(self, lista: ListaErros, parent: QObject | None = None):
        super().__init__(parent)
        self.lista = lista

    def acrescentar(self, novos: list[RegistroErro]) -> None:
        if not novos:
            return
        n = len(self.lista.itens)
        sobra = len(novos) + n - self.lista.limite
        if sobra > n:  # o lote sozinho passa do limite: recomeça
            self.beginResetModel()
            self.lista.adicionar(novos)
            self.endResetModel()
            return
        if sobra > 0:
            self.beginRemoveRows(QModelIndex(), 0, sobra - 1)
            del self.lista.itens[:sobra]
            self.endRemoveRows()
            n -= sobra
        self.beginInsertRows(QModelIndex(), n, n + len(novos) - 1)
        self.lista.adicionar(novos)
        self.endInsertRows()

    def mesclar(self, registros: list[RegistroErro]) -> int:
        self.beginResetModel()
        entraram = self.lista.mesclar(registros)
        self.endResetModel()
        return entraram

    def registro(self, linha: int) -> RegistroErro:
        return self.lista.itens[linha]

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(self.lista.itens)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(COLUNAS)

    def headerData(self, secao: int, orientacao: Qt.Orientation, papel: int = Qt.ItemDataRole.DisplayRole):  # noqa: N802
        if orientacao == Qt.Orientation.Horizontal and papel == Qt.ItemDataRole.DisplayRole:
            return COLUNAS[secao]
        return None

    def data(self, indice: QModelIndex, papel: int = Qt.ItemDataRole.DisplayRole):
        if not indice.isValid() or indice.row() >= len(self.lista.itens):
            return None
        r = self.lista.itens[indice.row()]
        col = indice.column()
        if papel == Qt.ItemDataRole.DisplayRole:
            return (r.data_hora, r.rotulo_categoria, r.mensagem)[col]
        if papel == PAPEL_ORDEM:
            # data/hora: o ISO ordena como texto; empate resolvido pela ordem de chegada
            return ((r.quando, indice.row()), r.rotulo_categoria.casefold(), r.mensagem.casefold())[col]
        if papel == PAPEL_PILULA and col == COL_CATEGORIA:
            return CATEGORIAS.get(r.categoria, ("", "", "neutro"))[2]
        if papel == Qt.ItemDataRole.ToolTipRole and col == COL_MENSAGEM:
            return r.mensagem
        return None


class FiltroErros(QSortFilterProxyModel):
    """Filtro por categoria ("" = todas) e busca de texto (mensagem, tópico, detalhe, payload, categoria)."""

    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        self.categoria = ""
        self.busca = ""
        self.setSortRole(PAPEL_ORDEM)
        self.setDynamicSortFilter(True)

    def definir(self, categoria: str | None = None, busca: str | None = None) -> None:
        if categoria is not None:
            self.categoria = categoria
        if busca is not None:
            self.busca = busca.strip().casefold()
        refiltrar(self)

    def filterAcceptsRow(self, linha: int, pai: QModelIndex) -> bool:  # noqa: N802
        r = self.sourceModel().registro(linha)
        if self.categoria and r.categoria != self.categoria:
            return False
        if not self.busca:
            return True
        campos = (r.mensagem, r.topico, r.detalhe, r.payload, r.rotulo_categoria, r.data_hora)
        return any(self.busca in c.casefold() for c in campos if c)

    def lessThan(self, a: QModelIndex, b: QModelIndex) -> bool:  # noqa: N802
        return a.data(PAPEL_ORDEM) < b.data(PAPEL_ORDEM)


# ---------------------------------------------------------------------- janela
class JanelaErros(QDialog):
    """Lista de erros da sessão + arquivo do dia. Não modal: a janela principal continua usável."""

    def __init__(self, pasta_logs: str | Path, parent: QWidget | None = None,
                 ler: Callable[..., list[dict]] | None = None, limite: int = LIMITE_ERROS):
        super().__init__(parent)
        self.setObjectName("janelaErros")
        self.setWindowTitle("Erros registrados")
        self.setModal(False)
        self.setWindowFlag(Qt.WindowType.WindowContextHelpButtonHint, False)
        self.setMinimumSize(760, 520)
        self.resize(980, 720)
        self.pasta_logs = pasta_logs
        self._ler = ler or ler_erros_do_dia
        self._pendentes: list[RegistroErro] = []
        self._carregando = False
        self.lista = ListaErros(limite)
        self.modelo = ModeloErros(self.lista, self)
        self.filtro = FiltroErros(self)
        self.filtro.setSourceModel(self.modelo)
        self._montar()

        self._timer = QTimer(self)
        self._timer.setInterval(INTERVALO_LOTE_MS)
        self._timer.timeout.connect(self.descarregar)
        self._timer.start()

    # ----- montagem -----
    def _montar(self) -> None:
        corpo = QVBoxLayout(self)
        corpo.setContentsMargins(24, 20, 24, 24)
        corpo.setSpacing(16)

        cab = QHBoxLayout()
        cab.setSpacing(8)
        titulo = QLabel("Erros registrados", self)
        titulo.setObjectName("titulo")
        self.chip_total = ChipInfo("total", "Total de erros", self)
        self.chip_total.setToolTip("Erros desta sessão e do arquivo de hoje em logs/ (no máximo 5.000).")
        cab.addWidget(titulo)
        cab.addSpacing(8)
        cab.addWidget(self.chip_total, 0, Qt.AlignmentFlag.AlignVCenter)
        cab.addStretch(1)
        self.botao_pasta = self._botao("Abrir pasta de logs", "Abre no Explorer a pasta com os arquivos de log "
                                                              "(erros_AAAAMMDD.jsonl).")
        cab.addWidget(self.botao_pasta)

        barra = QHBoxLayout()
        barra.setSpacing(8)
        self.combo_categoria = QComboBox(self)
        self.combo_categoria.setAccessibleName("Categoria")
        self.combo_categoria.setToolTip("Mostrar só uma categoria de erro.")
        self.combo_categoria.addItem("Todas as categorias", "")
        for valor, (_, rotulo, _) in CATEGORIAS.items():
            self.combo_categoria.addItem(rotulo, valor)
        self.combo_categoria.setMinimumWidth(180)
        self.campo_busca = QLineEdit(self)
        self.campo_busca.setPlaceholderText("Buscar na mensagem, tópico, detalhe ou payload")
        self.campo_busca.setClearButtonEnabled(True)
        self.campo_busca.setAccessibleName("Buscar")
        self.campo_busca.setToolTip("Filtra enquanto você digita (Ctrl+F).")
        self.botao_copiar = self._botao("Copiar detalhe", "Copia o erro selecionado, com o payload.")
        self.botao_exportar = self._botao("Exportar CSV…", "Salva os erros listados (com o filtro) em .csv.")
        barra.addWidget(self.combo_categoria)
        barra.addWidget(self.campo_busca, 1)
        barra.addSpacing(8)
        barra.addWidget(self.botao_copiar)
        barra.addWidget(self.botao_exportar)

        # tabela
        self.tabela = QTableView(self)
        self.tabela.setObjectName("tabelaDados")
        self.tabela.setAccessibleName("Lista de erros")
        self.tabela.setModel(self.filtro)
        self.tabela.setSortingEnabled(True)
        self.tabela.sortByColumn(COL_QUANDO, Qt.SortOrder.DescendingOrder)  # mais recente primeiro
        self.tabela.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tabela.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.tabela.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.tabela.setAlternatingRowColors(True)
        self.tabela.setShowGrid(False)
        self.tabela.setWordWrap(False)
        self.tabela.verticalHeader().hide()
        self.tabela.verticalHeader().setDefaultSectionSize(34)
        cab_t = self.tabela.horizontalHeader()
        cab_t.setHighlightSections(False)
        cab_t.setDefaultAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        cab_t.setSectionResizeMode(COL_QUANDO, QHeaderView.ResizeMode.ResizeToContents)
        cab_t.setSectionResizeMode(COL_CATEGORIA, QHeaderView.ResizeMode.Fixed)
        cab_t.resizeSection(COL_CATEGORIA, 140)
        cab_t.setSectionResizeMode(COL_MENSAGEM, QHeaderView.ResizeMode.Stretch)
        self.delegado = DelegadoPilula(self.tabela)
        self.tabela.setItemDelegateForColumn(COL_CATEGORIA, self.delegado)
        self.tabela.selectionModel().currentRowChanged.connect(lambda *_: self._mostrar_detalhe())

        self.vazio = QLabel(self)
        self.vazio.setObjectName("estadoVazio")
        self.vazio.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.vazio.setWordWrap(True)
        self.pilha = QStackedWidget(self)
        self.pilha.addWidget(self.tabela)
        self.pilha.addWidget(self.vazio)

        # detalhe
        self.detalhe = QFrame(self)
        self.detalhe.setObjectName("detalheErro")
        d = QVBoxLayout(self.detalhe)
        d.setContentsMargins(20, 16, 20, 16)
        d.setSpacing(8)
        self.detalhe_mensagem = QLabel(self.detalhe)
        self.detalhe_mensagem.setObjectName("detalheMensagem")
        self.detalhe_mensagem.setWordWrap(True)
        self.detalhe_mensagem.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.detalhe_campos = QWidget(self.detalhe)
        grade = QGridLayout(self.detalhe_campos)
        grade.setContentsMargins(0, 0, 0, 0)
        grade.setHorizontalSpacing(16)
        grade.setVerticalSpacing(6)
        self.detalhe_topico = self._valor()
        self.detalhe_texto = self._valor()
        self.payload = QPlainTextEdit(self.detalhe_campos)
        self.payload.setObjectName("payloadErro")
        self.payload.setReadOnly(True)
        self.payload.setFont(fonte_mono())
        self.payload.setAccessibleName("Payload")
        self.payload.setLineWrapMode(QPlainTextEdit.LineWrapMode.WidgetWidth)
        self.payload.setPlaceholderText("(sem payload)")
        for linha, (rotulo, valor) in enumerate((("Tópico", self.detalhe_topico), ("Detalhe", self.detalhe_texto),
                                                 ("Payload", self.payload))):
            r = QLabel(rotulo.upper(), self.detalhe_campos)
            r.setObjectName("rotuloCampo")
            r.setBuddy(valor)
            grade.addWidget(r, linha, 0, Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
            grade.addWidget(valor, linha, 1)
        grade.setColumnStretch(1, 1)
        grade.setRowStretch(2, 1)
        self.detalhe_vazio = QLabel("Selecione um erro para ver o tópico, o detalhe e o payload.", self.detalhe)
        self.detalhe_vazio.setObjectName("detalheVazio")
        d.addWidget(self.detalhe_mensagem)
        d.addWidget(self.detalhe_campos, 1)
        d.addWidget(self.detalhe_vazio)

        divisor = QSplitter(Qt.Orientation.Vertical, self)
        divisor.setChildrenCollapsible(False)
        divisor.addWidget(self.pilha)
        divisor.addWidget(self.detalhe)
        divisor.setStretchFactor(0, 3)
        divisor.setStretchFactor(1, 2)
        divisor.setSizes([290, 340])

        corpo.addLayout(cab)
        corpo.addLayout(barra)
        corpo.addWidget(divisor, 1)

        self.combo_categoria.currentIndexChanged.connect(
            lambda *_: self._filtrar(categoria=self.combo_categoria.currentData() or ""))
        self.campo_busca.textChanged.connect(lambda t: self._filtrar(busca=t))
        self.botao_copiar.clicked.connect(self.copiar_detalhe)
        self.botao_exportar.clicked.connect(self.exportar_dialogo)
        self.botao_pasta.clicked.connect(self.abrir_pasta_logs)
        for sinal in (self.filtro.rowsInserted, self.filtro.rowsRemoved, self.filtro.modelReset,
                      self.filtro.layoutChanged):
            sinal.connect(self._atualizar_resumo)
        QShortcut(QKeySequence.StandardKey.Find, self, activated=self._focar_busca)
        QShortcut(QKeySequence.StandardKey.Copy, self.tabela, activated=self.copiar_detalhe)
        for a, b in zip([self.combo_categoria, self.campo_busca, self.botao_copiar, self.botao_exportar,
                         self.botao_pasta, self.tabela, self.payload],
                        [self.campo_busca, self.botao_copiar, self.botao_exportar, self.botao_pasta, self.tabela,
                         self.payload]):
            QWidget.setTabOrder(a, b)
        self._atualizar_resumo()
        self._mostrar_detalhe()

    def _botao(self, texto: str, dica: str) -> QPushButton:
        b = QPushButton(texto, self)
        b.setToolTip(dica)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        return b

    def _valor(self) -> QLabel:
        v = QLabel(self.detalhe_campos)
        v.setObjectName("detalheValor")
        v.setWordWrap(True)
        v.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        return v

    # ----- API -----
    def adicionar_json(self, texto: str) -> None:
        """Ligado a `bridge.erro_registrado`. Enfileira; a tabela recebe em lote pelo QTimer."""
        self._pendentes.append(RegistroErro.de_json(texto))
        if len(self._pendentes) >= self.lista.limite:
            self.descarregar()

    def descarregar(self) -> None:
        if not self._pendentes:
            return
        novos, self._pendentes = self._pendentes, []
        self.modelo.acrescentar(novos)

    def abrir(self) -> None:
        """Mostra (ou traz para a frente) e recarrega o arquivo do dia em segundo plano."""
        self.descarregar()
        self.carregar_arquivo_do_dia()
        if self.isMinimized():
            self.showNormal()
        self.show()
        self.raise_()
        self.activateWindow()
        if self.filtro.rowCount() and not self.tabela.currentIndex().isValid():
            self.tabela.selectRow(0)
        self.tabela.setFocus()  # setas navegam na lista logo ao abrir

    def carregar_arquivo_do_dia(self) -> bool:
        if self._carregando:
            return False
        self._carregando = True
        self._atualizar_resumo()
        ler, pasta = self._ler, self.pasta_logs
        rodar_em_thread(lambda: ler(pasta), self._ao_carregado, "erros-do-dia")
        return True

    def registros_visiveis(self) -> list[RegistroErro]:
        return [self.modelo.registro(self.filtro.mapToSource(self.filtro.index(r, 0)).row())
                for r in range(self.filtro.rowCount())]

    def selecionado(self) -> RegistroErro | None:
        indice = self.tabela.currentIndex()
        if not indice.isValid():
            return None
        return self.modelo.registro(self.filtro.mapToSource(indice).row())

    def copiar_detalhe(self) -> None:
        r = self.selecionado()
        if r is None:
            avisar(self.botao_copiar, "Selecione um erro na lista.")
            return
        QGuiApplication.clipboard().setText(r.texto_completo())

    def exportar_csv(self, caminho: Path) -> Path:
        caminho = Path(caminho)
        with caminho.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["Data/hora", "Categoria", "Mensagem", "Tópico", "Detalhe", "Payload"])
            for r in self.registros_visiveis():
                w.writerow([r.data_hora, r.rotulo_categoria, r.mensagem, r.topico or "", r.detalhe or "",
                            r.payload or ""])
        return caminho

    def exportar_dialogo(self) -> None:
        nome = f"erros-mqtt-{date.today():%Y%m%d}.csv"
        caminho, _ = QFileDialog.getSaveFileName(self, "Exportar erros", nome, "CSV (*.csv);;Todos os arquivos (*)")
        if caminho:
            try:
                self.exportar_csv(Path(caminho))
            except OSError as exc:
                avisar(self.botao_exportar, f"Não foi possível salvar: {exc}")

    def pasta_resolvida(self) -> Path:
        from app.config import pasta_base

        p = Path(self.pasta_logs)
        return p if p.is_absolute() else pasta_base() / p

    def abrir_pasta_logs(self) -> None:
        pasta = self.pasta_resolvida()
        try:
            pasta.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        if not QDesktopServices.openUrl(QUrl.fromLocalFile(str(pasta))):
            avisar(self.botao_pasta, f"Não foi possível abrir {pasta}")

    def definir_paleta(self, p: Paleta) -> None:
        self.delegado.definir_paleta(p)
        self.tabela.viewport().update()

    def filtrar(self, categoria: str | None = None, busca: str | None = None) -> None:
        """Para testes e atalhos: ajusta os controles (que aplicam o filtro)."""
        if categoria is not None:
            i = self.combo_categoria.findData(categoria)
            self.combo_categoria.setCurrentIndex(max(0, i))
        if busca is not None:
            self.campo_busca.setText(busca)

    # ----- interno -----
    def _filtrar(self, categoria: str | None = None, busca: str | None = None) -> None:
        self.filtro.definir(categoria, busca)
        self._atualizar_resumo()
        if self.filtro.rowCount() and not self.tabela.currentIndex().isValid():
            self.tabela.selectRow(0)
        self._mostrar_detalhe()

    def _focar_busca(self) -> None:
        self.campo_busca.setFocus()
        self.campo_busca.selectAll()

    def _ao_carregado(self, registros: object, erro: str) -> None:
        self._carregando = False
        if erro:
            avisar(self.botao_pasta, f"Não foi possível ler o arquivo de erros de hoje: {erro}")
        else:
            self.descarregar()
            atual = self.selecionado()
            entraram = self.modelo.mesclar([RegistroErro.de_dict(d) for d in (registros or [])
                                            if isinstance(d, dict)])
            if entraram:
                self._reselecionar(atual)
        self._atualizar_resumo()

    def _reselecionar(self, registro: RegistroErro | None) -> None:
        if registro is None:
            if self.filtro.rowCount():
                self.tabela.selectRow(0)
            return
        for r in range(self.filtro.rowCount()):
            if self.modelo.registro(self.filtro.mapToSource(self.filtro.index(r, 0)).row()) is registro:
                self.tabela.selectRow(r)
                return

    def _atualizar_resumo(self, *_args) -> None:
        total, visiveis = len(self.lista), self.filtro.rowCount()
        if self._carregando and not total:
            self.chip_total.definir("", "carregando o arquivo de hoje…", "ocupado")
        elif visiveis == total:
            self.chip_total.definir("", f"{total} erro{'s' if total != 1 else ''}", "erro" if total else "neutro")
        else:
            self.chip_total.definir("Mostrando", f"{visiveis} de {total}", "ativo")
        if visiveis:
            self.pilha.setCurrentWidget(self.tabela)
        else:
            self.vazio.setText("Nenhum erro com esse filtro." if total
                               else "Nenhum erro registrado hoje.\nQuando o contador ERROS subir, os detalhes "
                                    "aparecem aqui.")
            self.pilha.setCurrentWidget(self.vazio)
        self.botao_exportar.setEnabled(bool(visiveis))
        self._mostrar_detalhe()

    def _mostrar_detalhe(self) -> None:
        r = self.selecionado()
        tem = r is not None
        self.botao_copiar.setEnabled(tem)
        self.detalhe_vazio.setVisible(not tem)
        self.detalhe_mensagem.setVisible(tem)
        self.detalhe_campos.setVisible(tem)
        if not tem:
            return
        self.detalhe_mensagem.setText(f"{r.data_hora} · {r.rotulo_categoria} — {r.mensagem}")
        self.detalhe_topico.setText(r.topico or "—")
        self.detalhe_texto.setText(r.detalhe or "—")
        self.payload.setPlainText(r.payload_formatado())

    @property
    def total(self) -> int:
        self.descarregar()
        return len(self.lista)
