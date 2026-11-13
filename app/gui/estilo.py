"""Tema visual: paletas clara/escura, folha de estilo (QSS) e fontes.

Todo o estilo da aplicação fica aqui. A janela só pede `aplicar(app, escuro)` e usa as cores devolvidas
para pintar o que o QSS não alcança (nível colorido na caixa de logs).
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from string import Template

from PySide6.QtCore import QTemporaryDir, Qt
from PySide6.QtGui import QColor, QFont, QFontDatabase, QGuiApplication, QPalette
from PySide6.QtWidgets import QApplication

FONTES_INTERFACE = ["Segoe UI Variable Text", "Segoe UI", "Inter", "Noto Sans", "DejaVu Sans"]
FONTES_MONO = ["Cascadia Mono", "Consolas", "JetBrains Mono", "DejaVu Sans Mono", "Liberation Mono"]


@dataclass(frozen=True)
class Paleta:
    fundo: str
    superficie: str
    superficie_alt: str
    borda: str
    borda_forte: str
    texto: str
    texto_sec: str
    texto_fraco: str
    destaque: str
    destaque_hover: str
    destaque_press: str
    destaque_suave: str
    texto_sobre_destaque: str
    sucesso: str
    sucesso_suave: str
    aviso: str
    aviso_suave: str
    erro: str
    erro_suave: str
    neutro_suave: str
    log_fundo: str
    selecao: str
    destaque_texto: str = ""  # texto na cor de destaque sobre destaque_suave (chips)
    # Console da aba Mosquitto: fundo de terminal (escuro nos dois temas, como o Prompt de Comando).
    # Contraste sobre terminal_fundo: texto 13:1, fraco 6:1, prompt/aviso/erro/ok acima de 7:1.
    terminal_fundo: str = "#0C0F14"
    terminal_texto: str = "#D7DCE3"
    terminal_fraco: str = "#8B95A5"
    terminal_prompt: str = "#FFFFFF"
    terminal_aviso: str = "#F5C04A"
    terminal_erro: str = "#FF8A80"
    terminal_ok: str = "#6EE7A0"
    terminal_selecao: str = "#2B4C80"
    terminal_borda: str = "#252C37"
    # Categoria "mqtt" na janela de erros (4ª cor de categoria, distinta de aviso/erro/destaque).
    roxo: str = "#6D28D9"
    roxo_suave: str = "#F0E9FD"
    # Categoria "comando" (comandos enviados ao dispositivo): 5ª cor de categoria. #0A6B7C sobre #E0F2F7: 5,6:1.
    ciano: str = "#0A6B7C"
    ciano_suave: str = "#E0F2F7"
    # Legenda 96 "Fora de turno" (aba Apontamentos reais): índigo suave, distinto do destaque azul e do roxo.
    # #3F4A9E sobre #E8EAF7: 6,4:1.
    indigo: str = "#3F4A9E"
    indigo_suave: str = "#E8EAF7"


# Contraste conferido para WCAG AA (texto normal >= 4,5:1 sobre a superfície correspondente).
CLARA = Paleta(
    fundo="#F2F4F7",
    superficie="#FFFFFF",
    superficie_alt="#F7F8FA",
    borda="#E1E5EB",
    borda_forte="#C9D0DA",
    texto="#18202C",
    texto_sec="#4F5A6B",
    texto_fraco="#6B7585",
    destaque="#2459D6",
    destaque_hover="#1D4CBD",
    destaque_press="#183F9E",
    destaque_suave="#E8EFFD",
    texto_sobre_destaque="#FFFFFF",
    sucesso="#127A3A",
    sucesso_suave="#E4F5EA",
    aviso="#9A5B00",
    aviso_suave="#FDF1DC",
    erro="#C62828",
    erro_suave="#FDE8E8",
    neutro_suave="#EDEFF3",
    log_fundo="#FBFCFD",
    selecao="#CFDDFB",
    destaque_texto="#1F4FC2",
)

ESCURA = Paleta(
    fundo="#0E1218",
    superficie="#161B23",
    superficie_alt="#1B212B",
    borda="#262E3A",
    borda_forte="#364152",
    texto="#E7EAF0",
    texto_sec="#A3ADBB",
    texto_fraco="#8892A1",
    destaque="#2F66E0",
    destaque_hover="#3B74F0",
    destaque_press="#2556C4",
    destaque_suave="#1A2740",
    texto_sobre_destaque="#FFFFFF",
    sucesso="#4ADE80",
    sucesso_suave="#12291C",
    aviso="#FBBF24",
    aviso_suave="#2D2410",
    erro="#F87171",
    erro_suave="#331A1C",
    neutro_suave="#202733",
    log_fundo="#11151C",
    selecao="#24406F",
    destaque_texto="#9DBBFF",
    roxo="#C4B5FD",
    roxo_suave="#261E3E",
    ciano="#67E8F9",
    ciano_suave="#0E2A31",
    indigo="#A5B4FC",       # sobre #1F2342: 7,6:1
    indigo_suave="#1F2342",
)

_QSS = Template(
    """
QMainWindow, QWidget#raiz { background: $fundo; }
QWidget { color: $texto; }
QToolTip {
    background: $superficie; color: $texto; border: 1px solid $borda_forte;
    padding: 6px 8px; border-radius: 6px;
}

/* ---------- Cabeçalho ---------- */
QLabel#titulo { font-size: 16pt; font-weight: 600; color: $texto; }
QLabel#subtitulo { color: $texto_sec; }
QLabel#detalheStatus { color: $texto_sec; }

QFrame#statusPill {
    border-radius: 15px; padding: 0 12px; min-height: 30px;
    background: $neutro_suave; border: 1px solid $borda;
}
QFrame#statusPill QLabel#statusTexto { font-weight: 600; color: $texto_sec; }
QFrame#statusPill[estado="conectado"] { background: $sucesso_suave; border-color: $sucesso_suave; }
QFrame#statusPill[estado="conectado"] QLabel#statusTexto { color: $sucesso; }
QFrame#statusPill[estado="conectando"] { background: $aviso_suave; border-color: $aviso_suave; }
QFrame#statusPill[estado="conectando"] QLabel#statusTexto { color: $aviso; }
QFrame#statusPill[estado="erro"] { background: $erro_suave; border-color: $erro_suave; }
QFrame#statusPill[estado="erro"] QLabel#statusTexto { color: $erro; }

QLabel#statusPonto { border-radius: 5px; background: $texto_fraco; }
QLabel#statusPonto[estado="conectado"] { background: $sucesso; }
QLabel#statusPonto[estado="conectando"] { background: $aviso; }
QLabel#statusPonto[estado="conectando"][pulso="true"] { background: transparent; border: 2px solid $aviso; }
QLabel#statusPonto[estado="erro"] { background: $erro; }

/* ---------- Cartões ---------- */
QFrame#cartao {
    background: $superficie; border: 1px solid $borda; border-radius: 12px;
}
QLabel#rotuloCampo {
    color: $texto_sec; font-size: 8pt; font-weight: 600; letter-spacing: 0.6px;
}
QLabel#tituloCartao { font-size: 11pt; font-weight: 600; }
QLabel#erroValidacao { color: $erro; }

/* ---------- Contadores ---------- */
QLabel#valorContador { font-size: 20pt; font-weight: 600; color: $texto; }
QLabel#valorContador[alerta="true"] { color: $erro; }
QLabel#dicaContador { color: $texto_fraco; }
QFrame#marcaContador { border-radius: 2px; background: $borda_forte; }
QFrame#marcaContador[tipo="recebidas"] { background: $destaque; }
QFrame#marcaContador[tipo="gravadas"] { background: $sucesso; }
QFrame#marcaContador[tipo="erros"] { background: $erro; }

/* ---------- Campos ---------- */
QLineEdit, QSpinBox {
    background: $superficie; color: $texto;
    border: 1px solid $borda_forte; border-radius: 8px;
    padding: 0 10px; min-height: 34px;
    selection-background-color: $selecao; selection-color: $texto;
}
QLineEdit:hover, QSpinBox:hover { border-color: $texto_fraco; }
QLineEdit:focus, QSpinBox:focus { border: 2px solid $destaque; padding: 0 9px; }
QLineEdit:disabled, QSpinBox:disabled {
    background: $superficie_alt; color: $texto_sec; border-color: $borda;
}
/* Spinbox sem setas visíveis (↑/↓ e roda do mouse continuam funcionando). O editor interno ocupa a caixa
   toda, então o recuo do texto fica nele. */
QSpinBox::up-button, QSpinBox::down-button { width: 0px; border: none; }
QSpinBox QLineEdit, QSpinBox QLineEdit:focus, QSpinBox QLineEdit:disabled, QSpinBox QLineEdit:hover {
    border: none; padding: 0 10px; min-height: 0; background: transparent;
}
QLineEdit[invalido="true"], QSpinBox[invalido="true"] { border: 2px solid $erro; padding: 0 9px; }

/* Seletor de QoS (controle segmentado) */
QPushButton#segmento {
    background: $superficie; color: $texto_sec; font-weight: 600;
    border: 1px solid $borda_forte; min-height: 34px; min-width: 34px; padding: 0;
    border-radius: 0;
}
QPushButton#segmento[posicao="primeiro"] { border-top-left-radius: 8px; border-bottom-left-radius: 8px; }
QPushButton#segmento[posicao="ultimo"] { border-top-right-radius: 8px; border-bottom-right-radius: 8px; }
QPushButton#segmento[posicao="meio"], QPushButton#segmento[posicao="ultimo"] { border-left: none; }
QPushButton#segmento:hover { background: $superficie_alt; color: $texto; }
QPushButton#segmento:checked {
    background: $destaque_suave; color: $destaque; border-color: $destaque;
}
QPushButton#segmento[posicao="meio"]:checked, QPushButton#segmento[posicao="ultimo"]:checked {
    border-left: 1px solid $destaque;
}
QPushButton#segmento:focus { outline: none; text-decoration: underline; }
QPushButton#segmento:disabled { background: $superficie_alt; color: $texto_fraco; border-color: $borda; }
QPushButton#segmento:checked:disabled { color: $texto_sec; background: $neutro_suave; }

/* ---------- Botões ---------- */
QPushButton {
    background: $superficie; color: $texto;
    border: 1px solid $borda_forte; border-radius: 8px;
    padding: 0 14px; min-height: 32px;
}
QPushButton:hover { background: $superficie_alt; border-color: $texto_fraco; }
QPushButton:pressed { background: $neutro_suave; }
QPushButton:focus { border: 2px solid $destaque; padding: 0 13px; }
QPushButton:disabled { color: $texto_fraco; border-color: $borda; background: $superficie_alt; }

QPushButton#botaoPrincipal {
    background: $destaque; color: $texto_sobre_destaque; border: none;
    font-weight: 600; min-height: 36px; padding: 0 22px;
}
QPushButton#botaoPrincipal:hover { background: $destaque_hover; }
QPushButton#botaoPrincipal:pressed { background: $destaque_press; }
QPushButton#botaoPrincipal:focus { border: 2px solid $texto; padding: 0 20px; }
QPushButton#botaoPrincipal[perigo="true"] {
    background: $superficie; color: $erro; border: 1px solid $erro;
}
QPushButton#botaoPrincipal[perigo="true"]:hover { background: $erro_suave; }
QPushButton#botaoPrincipal[perigo="true"]:focus { border: 2px solid $erro; padding: 0 21px; }
QPushButton#botaoPrincipal:disabled {
    background: $neutro_suave; color: $texto_sec; border: none;
}

QPushButton#botaoDiscreto {
    background: transparent; border: 1px solid transparent; color: $texto_sec;
    padding: 0 10px; min-height: 28px;
}
QPushButton#botaoDiscreto:hover { background: $neutro_suave; color: $texto; }
QPushButton#botaoDiscreto:focus { border: 1px solid $destaque; }
QPushButton#botaoDiscreto:disabled { color: $texto_fraco; background: transparent; border-color: transparent; }
/* Endereço Mosquitto: somente leitura com o Mosquitto rodando (não dá para trocar a pasta do processo vivo) */
QLineEdit#campoEndereco[readOnly="true"] { background: $superficie_alt; color: $texto_sec; border-color: $borda; }
QLineEdit#campoEndereco[readOnly="true"]:focus { border: 1px solid $borda_forte; padding: 0 10px; }

QPushButton#chipPausa {
    background: $aviso_suave; color: $aviso; border: none; border-radius: 13px;
    min-height: 26px; padding: 0 12px; font-weight: 600;
}
QPushButton#chipPausa:hover { background: $aviso; color: $superficie; }

/* ---------- Chips de tempo (Próxima / Última gravação) ---------- */
QFrame#chipInfo {
    background: $superficie_alt; border: 1px solid $borda; border-radius: 13px; min-height: 26px;
}
QFrame#chipInfo[estado="ativo"] { background: $destaque_suave; border-color: $destaque_suave; }
QFrame#chipInfo[estado="ocupado"] { background: $aviso_suave; border-color: $aviso_suave; }
QFrame#chipInfo[estado="ok"] { background: $sucesso_suave; border-color: $sucesso_suave; }
QLabel#chipPrefixo { color: $texto_sec; background: transparent; }
QLabel#chipValor { color: $texto; font-weight: 600; background: transparent; }
QLabel#chipValor[estado="ativo"] { color: $destaque_texto; }
QLabel#chipValor[estado="ocupado"] { color: $aviso; }
QLabel#chipValor[estado="ok"] { color: $sucesso; }
QLabel#chipPonto { border-radius: 4px; background: $texto_fraco; }
QLabel#chipPonto[estado="ativo"] { background: $destaque_texto; }
QLabel#chipPonto[estado="ocupado"] { background: $aviso; }
QLabel#chipPonto[estado="ok"] { background: $sucesso; }
QFrame#chipInfo[estado="aviso"] { background: $aviso_suave; border-color: $aviso_suave; }
QLabel#chipValor[estado="aviso"] { color: $aviso; }
QLabel#chipPonto[estado="aviso"] { background: $aviso; }
QFrame#chipInfo[estado="erro"] { background: $erro_suave; border-color: $erro_suave; }
QLabel#chipValor[estado="erro"] { color: $erro; }
QLabel#chipPonto[estado="erro"] { background: $erro; }

/* ---------- Abas do cartão de acompanhamento (Atividade / Mosquitto) ---------- */
QTabWidget#abasAcompanhamento::pane {
    border: none; border-top: 1px solid $borda; background: transparent; top: -1px;
}
QTabWidget#abasAcompanhamento::tab-bar { left: 0px; }
QTabBar#barraAbas { background: transparent; }
QTabBar#barraAbas::tab {
    background: transparent; color: $texto_sec; font-size: 11pt; font-weight: 600;
    border: none; border-bottom: 2px solid transparent;
    padding: 4px 4px 10px 4px; margin-right: 24px; min-width: 64px;
}
QTabBar#barraAbas::tab:hover:!selected { color: $texto; border-bottom: 2px solid $borda_forte; }
QTabBar#barraAbas::tab:selected { color: $texto; border-bottom: 2px solid $destaque; }
QTabBar#barraAbas:focus { outline: none; }
QWidget#paginaAba { background: transparent; }
/* O painel de atividade já está dentro do cartão: sem uma segunda borda. */
QTabWidget#abasAcompanhamento QFrame#cartao { background: transparent; border: none; }

/* ---------- Caixa de logs ---------- */
QPlainTextEdit#caixaLogs {
    background: $log_fundo; color: $texto;
    border: 1px solid $borda; border-radius: 8px; padding: 6px 4px;
    selection-background-color: $selecao; selection-color: $texto;
}

/* ---------- Console da aba Mosquitto (fundo de terminal) ---------- */
QPlainTextEdit#consoleMosquitto {
    background: $terminal_fundo; color: $terminal_texto;
    border: 1px solid $terminal_borda; border-radius: 8px; padding: 8px 6px;
    selection-background-color: $terminal_selecao; selection-color: $terminal_prompt;
}
QPlainTextEdit#consoleMosquitto QScrollBar::handle:vertical { background: #3A4453; }
QPlainTextEdit#consoleMosquitto QScrollBar::handle:vertical:hover { background: #56627A; }
QLabel#promptComando { color: $texto_sec; }

QScrollBar:vertical { background: transparent; width: 12px; margin: 4px 2px 4px 0; }
QScrollBar::handle:vertical { background: $borda_forte; border-radius: 4px; min-height: 32px; margin: 0 2px; }
QScrollBar::handle:vertical:hover { background: $texto_fraco; }
QScrollBar:horizontal { background: transparent; height: 12px; margin: 0 4px 2px 4px; }
QScrollBar::handle:horizontal { background: $borda_forte; border-radius: 4px; min-width: 32px; margin: 2px 0; }
QScrollBar::add-line, QScrollBar::sub-line { width: 0; height: 0; }
QScrollBar::add-page, QScrollBar::sub-page { background: transparent; }

/* ---------- Seções / banco de dados ---------- */
QScrollArea#colunaLateral, QWidget#colunaLateralConteudo { background: transparent; border: none; }
QLabel#descricaoCartao { color: $texto_sec; }
QCheckBox { spacing: 8px; color: $texto; }
QCheckBox:disabled { color: $texto_fraco; }
QCheckBox::indicator {
    width: 16px; height: 16px; border-radius: 4px;
    border: 1px solid $borda_forte; background: $superficie;
}
QCheckBox::indicator:hover { border-color: $destaque; }
QCheckBox::indicator:checked { background: $destaque; border-color: $destaque; image: url("$check"); }
QCheckBox::indicator:disabled { background: $superficie_alt; border-color: $borda; }
QCheckBox::indicator:checked:disabled { background: $borda_forte; border-color: $borda_forte; }
QCheckBox:focus { outline: none; }

QFrame#resultadoTeste { border-radius: 8px; background: $neutro_suave; }
QFrame#resultadoTeste[estado="ok"] { background: $sucesso_suave; }
QFrame#resultadoTeste[estado="falha"] { background: $erro_suave; }
QLabel#resultadoIcone {
    border-radius: 10px; font-weight: 700; color: $texto_sobre_destaque; background: $texto_fraco;
}
QLabel#resultadoIcone[estado="ok"] { background: $sucesso; color: $superficie; }
QLabel#resultadoIcone[estado="falha"] { background: $erro; color: $superficie; }
QLabel#resultadoTexto { color: $texto; }
QLabel#resultadoTexto[estado="ok"] { color: $sucesso; }
QLabel#resultadoTexto[estado="falha"] { color: $erro; }

/* ---------- Card ERROS clicável ---------- */
QFrame#cartao[clicavel="true"]:hover { background: $superficie_alt; border-color: $borda_forte; }
QFrame#cartao[clicavel="true"]:focus { border: 1px solid $destaque; }
QLabel#acaoContador { color: $texto_sec; font-size: 9pt; background: transparent; }
QFrame#cartao[clicavel="true"]:hover QLabel#acaoContador { color: $destaque_texto; }

/* ---------- Tabelas (aba Clientes, janela de erros) ---------- */
QTableView#tabelaDados {
    background: $log_fundo; alternate-background-color: $superficie_alt; color: $texto;
    border: 1px solid $borda; border-radius: 8px; gridline-color: transparent; outline: none;
    selection-background-color: $selecao; selection-color: $texto;
}
QTableView#tabelaDados::item { padding: 0 6px; border: none; }
QTableView#tabelaDados::item:selected { background: $selecao; }
QHeaderView { background: transparent; }
QHeaderView::section {
    background: $superficie_alt; color: $texto_sec; font-size: 8pt; font-weight: 600;
    border: none; border-bottom: 1px solid $borda; padding: 6px 6px;
}
QHeaderView::section:first { border-top-left-radius: 8px; }
QHeaderView::section:last { border-top-right-radius: 8px; }
QHeaderView::section:hover { color: $texto; }
QHeaderView::up-arrow, QHeaderView::down-arrow { width: 0; height: 0; }
QTableCornerButton::section { background: $superficie_alt; border: none; }
QLabel#estadoVazio {
    color: $texto_sec; background: $log_fundo; border: 1px dashed $borda_forte; border-radius: 8px; padding: 24px;
}

/* ---------- Janela de erros ---------- */
QDialog#janelaErros { background: $fundo; }
QFrame#detalheErro { background: $superficie; border: 1px solid $borda; border-radius: 12px; }
QLabel#detalheMensagem { font-weight: 600; color: $texto; }
QLabel#detalheValor { color: $texto; }
QLabel#detalheVazio { color: $texto_fraco; }
QPlainTextEdit#payloadErro {
    background: $log_fundo; color: $texto; border: 1px solid $borda; border-radius: 8px; padding: 6px 4px;
    selection-background-color: $selecao; selection-color: $texto;
}
QSplitter::handle { background: transparent; }
QSplitter::handle:vertical { height: 12px; }

QComboBox {
    background: $superficie; color: $texto; border: 1px solid $borda_forte; border-radius: 8px;
    padding: 0 10px; min-height: 32px;
}
QComboBox:hover { border-color: $texto_fraco; }
QComboBox:focus { border: 2px solid $destaque; padding: 0 9px; }
QComboBox::drop-down { border: none; width: 26px; }
QComboBox::down-arrow { image: url("$seta"); width: 10px; height: 10px; }
QComboBox QAbstractItemView {
    background: $superficie; color: $texto; border: 1px solid $borda_forte; outline: none; padding: 4px;
    selection-background-color: $destaque_suave; selection-color: $texto;
}

/* ---------- Diálogo Zerar contadores (aba Clientes) ---------- */
QDialog#dialogoZerar { background: $fundo; }
QLabel#tituloDialogo { font-size: 12pt; font-weight: 600; color: $texto; }
QLabel#descricaoDialogo { color: $texto_sec; }
QLabel#secaoCanais { color: $texto_sec; font-size: 8pt; font-weight: 600; }
QFrame#grupoCanais { background: $superficie; border: 1px solid $borda; border-radius: 8px; }
QCheckBox[visto="true"] { font-weight: 600; }
QFrame#avisoZerar { background: $aviso_suave; border: 1px solid $aviso_suave; border-radius: 8px; }
QLabel#avisoZerarTexto { color: $texto; background: transparent; }
QLabel#avisoZerarIcone { color: $aviso; background: transparent; font-weight: 700; font-size: 12pt; }
QPushButton#botaoLink {
    background: transparent; border: 1px solid transparent; color: $destaque_texto; padding: 0 6px; min-height: 24px;
}
QPushButton#botaoLink:hover { text-decoration: underline; }
QPushButton#botaoLink:focus { border: 1px solid $destaque; }
QPushButton#botaoLink:disabled { color: $texto_fraco; text-decoration: none; }
QPushButton#botaoPerigo {
    background: $erro; color: $superficie; border: 1px solid $erro; font-weight: 600; min-height: 34px;
    padding: 0 18px;
}
QPushButton#botaoPerigo:hover { border: 2px solid $texto; padding: 0 17px; }
QPushButton#botaoPerigo:focus { border: 2px solid $texto; padding: 0 17px; }
QPushButton#botaoPerigo:disabled { background: $neutro_suave; color: $texto_fraco; border-color: $neutro_suave; }

/* ---------- Diálogo Recalcular (aba Apontamentos reais) ---------- */
QDialog#dialogoRecalcular { background: $fundo; }
QFrame#avisoRecalcular { background: $erro_suave; border: 1px solid $erro; border-radius: 8px; }
QLabel#avisoRecalcularTexto { color: $texto; background: transparent; }
QLabel#avisoRecalcularIcone { color: $erro; background: transparent; font-weight: 700; font-size: 12pt; }
QDateEdit {
    background: $superficie; color: $texto; border: 1px solid $borda_forte; border-radius: 8px;
    padding: 0 10px; min-height: 34px;
    selection-background-color: $selecao; selection-color: $texto;
}
QDateEdit:hover { border-color: $texto_fraco; }
QDateEdit:focus { border: 2px solid $destaque; padding: 0 9px; }
QDateEdit::drop-down { border: none; width: 26px; }
QDateEdit::down-arrow { image: url("$seta"); width: 10px; height: 10px; }
QCalendarWidget QWidget { alternate-background-color: $superficie_alt; }
QCalendarWidget QToolButton { color: $texto; background: transparent; border: none; padding: 4px 8px; }
QCalendarWidget QAbstractItemView:enabled {
    background: $superficie; color: $texto; selection-background-color: $destaque; selection-color: $texto_sobre_destaque;
}
QCalendarWidget QAbstractItemView:disabled { color: $texto_fraco; }

/* ---------- Diálogo Tópicos (aba Clientes) ---------- */
QDialog#dialogoTopicos { background: $fundo; }
QFrame#grupoNaoOuvidos { background: $superficie; border: 1px solid $aviso; border-radius: 8px; }
QLabel#topicoNaoOuvido { color: $texto; background: transparent; }
QLabel#rotuloTopico { color: $texto_sec; font-weight: 600; }
QPushButton#botaoUsarComo { min-height: 28px; padding: 0 12px; }
QPushButton#botaoUsarComo:focus { padding: 0 11px; }
QLabel#indicadorTopico {
    border-radius: 11px; padding: 2px 10px; font-weight: 600; font-size: 9pt;
    background: $neutro_suave; color: $texto_sec;
}
QLabel#indicadorTopico[estado="ouvido"] { background: $sucesso_suave; color: $sucesso; }
QLabel#indicadorTopico[estado="nao_ouvido"] { background: $erro_suave; color: $erro; }
QLabel#indicadorTopico[estado="invalido"] { background: $aviso_suave; color: $aviso; }

QMenu { background: $superficie; border: 1px solid $borda_forte; border-radius: 8px; padding: 4px; }
QMenu::item { padding: 6px 20px; border-radius: 4px; }
QMenu::item:selected { background: $destaque_suave; color: $texto; }
QMenu::item:disabled { color: $texto_fraco; }
QMenu::separator { height: 1px; background: $borda; margin: 4px 8px; }
"""
)


def sistema_esta_escuro() -> bool:
    """True se o sistema (Windows 10/11, macOS, Linux com portal) está no modo escuro."""
    try:
        return QGuiApplication.styleHints().colorScheme() == Qt.ColorScheme.Dark
    except AttributeError:  # Qt < 6.5
        return False


def _paleta_qt(p: Paleta) -> QPalette:
    pal = QPalette()
    c = QColor
    for grupo in (QPalette.ColorGroup.Active, QPalette.ColorGroup.Inactive):
        pal.setColor(grupo, QPalette.ColorRole.Window, c(p.fundo))
        pal.setColor(grupo, QPalette.ColorRole.WindowText, c(p.texto))
        pal.setColor(grupo, QPalette.ColorRole.Base, c(p.superficie))
        pal.setColor(grupo, QPalette.ColorRole.AlternateBase, c(p.superficie_alt))
        pal.setColor(grupo, QPalette.ColorRole.Text, c(p.texto))
        pal.setColor(grupo, QPalette.ColorRole.Button, c(p.superficie))
        pal.setColor(grupo, QPalette.ColorRole.ButtonText, c(p.texto))
        pal.setColor(grupo, QPalette.ColorRole.Highlight, c(p.destaque))
        pal.setColor(grupo, QPalette.ColorRole.HighlightedText, c(p.texto_sobre_destaque))
        pal.setColor(grupo, QPalette.ColorRole.ToolTipBase, c(p.superficie))
        pal.setColor(grupo, QPalette.ColorRole.ToolTipText, c(p.texto))
        pal.setColor(grupo, QPalette.ColorRole.PlaceholderText, c(p.texto_fraco))
        pal.setColor(grupo, QPalette.ColorRole.Link, c(p.destaque))
    for papel in (QPalette.ColorRole.WindowText, QPalette.ColorRole.Text, QPalette.ColorRole.ButtonText):
        pal.setColor(QPalette.ColorGroup.Disabled, papel, c(p.texto_fraco))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Base, c(p.superficie_alt))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Window, c(p.fundo))
    return pal


def fonte_interface() -> QFont:
    fonte = QFont()
    fonte.setFamilies(FONTES_INTERFACE)
    fonte.setPointSizeF(10)
    fonte.setHintingPreference(QFont.HintingPreference.PreferNoHinting)
    return fonte


def fonte_mono() -> QFont:
    fonte = QFontDatabase.systemFont(QFontDatabase.SystemFont.FixedFont)
    fonte.setFamilies(FONTES_MONO + [fonte.family()])
    fonte.setPointSizeF(9.5)
    fonte.setStyleHint(QFont.StyleHint.Monospace)
    return fonte


_CHECK_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><path d="M3.5 8.5l3 3 6-7" fill="none" '
    'stroke="{cor}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>'
)
_SETA_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10"><path d="M2 3.5l3 3 3-3" fill="none" '
    'stroke="{cor}" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>'
)
_pasta_temp: QTemporaryDir | None = None


def _arquivo_svg(nome: str, modelo: str, cor: str) -> str:
    """O QSS só aceita imagem por caminho: grava o SVG numa pasta temporária do processo."""
    global _pasta_temp
    if _pasta_temp is None:
        _pasta_temp = QTemporaryDir()
    caminho = Path(_pasta_temp.path()) / f"{nome}-{cor.lstrip('#')}.svg"
    if not caminho.exists():
        caminho.write_text(modelo.format(cor=cor), encoding="utf-8")
    return caminho.as_posix()


def _arquivo_check(cor: str) -> str:
    return _arquivo_svg("check", _CHECK_SVG, cor)


def aplicar(app: QApplication, escuro: bool) -> Paleta:
    """Aplica paleta e QSS na aplicação inteira e devolve a paleta usada."""
    p = ESCURA if escuro else CLARA
    qss = _QSS.substitute(asdict(p), check=_arquivo_check(p.texto_sobre_destaque),
                          seta=_arquivo_svg("seta", _SETA_SVG, p.texto_sec))
    if app.styleSheet() == qss:
        return p  # mesmo tema: reaplicar o QSS repoliria todos os widgets de todas as janelas à toa
    app.setPalette(_paleta_qt(p))
    app.setStyleSheet(qss)
    return p
