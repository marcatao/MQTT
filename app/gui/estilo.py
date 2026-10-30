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

QPushButton#chipPausa {
    background: $aviso_suave; color: $aviso; border: none; border-radius: 13px;
    min-height: 26px; padding: 0 12px; font-weight: 600;
}
QPushButton#chipPausa:hover { background: $aviso; color: $superficie; }

/* ---------- Caixa de logs ---------- */
QPlainTextEdit#caixaLogs {
    background: $log_fundo; color: $texto;
    border: 1px solid $borda; border-radius: 8px; padding: 6px 4px;
    selection-background-color: $selecao; selection-color: $texto;
}

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

QMenu { background: $superficie; border: 1px solid $borda_forte; border-radius: 8px; padding: 4px; }
QMenu::item { padding: 6px 20px; border-radius: 4px; }
QMenu::item:selected { background: $destaque_suave; color: $texto; }
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
_pasta_temp: QTemporaryDir | None = None


def _arquivo_check(cor: str) -> str:
    """O QSS só aceita imagem por caminho: grava o SVG do “visto” numa pasta temporária do processo."""
    global _pasta_temp
    if _pasta_temp is None:
        _pasta_temp = QTemporaryDir()
    caminho = Path(_pasta_temp.path()) / f"check-{cor.lstrip('#')}.svg"
    if not caminho.exists():
        caminho.write_text(_CHECK_SVG.format(cor=cor), encoding="utf-8")
    return caminho.as_posix()


def aplicar(app: QApplication, escuro: bool) -> Paleta:
    """Aplica paleta e QSS na aplicação inteira e devolve a paleta usada."""
    p = ESCURA if escuro else CLARA
    app.setPalette(_paleta_qt(p))
    app.setStyleSheet(_QSS.substitute(asdict(p), check=_arquivo_check(p.texto_sobre_destaque)))
    return p
