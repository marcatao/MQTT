"""Recursos visuais gerados por código (sem arquivos binários no repositório).

O ícone do app é um SVG: sinal de rádio (MQTT) à esquerda e um cilindro de banco de dados à direita.
Para gerar o .ico usado pelo PyInstaller:

    python -m app.gui.recursos build/icone.ico
"""
from __future__ import annotations

import sys
from pathlib import Path

from PySide6.QtCore import QByteArray, QRectF, Qt
from PySide6.QtGui import QIcon, QImage, QPainter, QPixmap
from PySide6.QtSvg import QSvgRenderer

ICONE_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
  <defs>
    <linearGradient id="fundo" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#2F6BEA"/>
      <stop offset="1" stop-color="#0E8FB0"/>
    </linearGradient>
  </defs>
  <rect x="2" y="2" width="60" height="60" rx="15" fill="url(#fundo)"/>
  <g fill="none" stroke="#FFFFFF" stroke-width="4.2" stroke-linecap="round">
    <path d="M13 31 A10 10 0 0 1 23 41"/>
    <path d="M13 22 A19 19 0 0 1 32 41"/>
  </g>
  <circle cx="14.5" cy="39.5" r="3.6" fill="#FFFFFF"/>
  <path d="M37 19 V43 A9.5 4.2 0 0 0 56 43 V19 Z" fill="#FFFFFF"/>
  <g fill="none" stroke="#1F5FD0" stroke-width="2" stroke-linecap="round">
    <path d="M37 27 A9.5 4.2 0 0 0 56 27"/>
    <path d="M37 35 A9.5 4.2 0 0 0 56 35"/>
  </g>
  <ellipse cx="46.5" cy="19" rx="9.5" ry="4.2" fill="#CFE0FF"/>
</svg>"""

TAMANHOS = (16, 20, 24, 32, 40, 48, 64, 128, 256)


def _renderizar(tamanho: int) -> QImage:
    imagem = QImage(tamanho, tamanho, QImage.Format.Format_ARGB32_Premultiplied)
    imagem.fill(Qt.GlobalColor.transparent)
    pintor = QPainter(imagem)
    pintor.setRenderHint(QPainter.RenderHint.Antialiasing)
    QSvgRenderer(QByteArray(ICONE_SVG.encode())).render(pintor, QRectF(0, 0, tamanho, tamanho))
    pintor.end()
    return imagem


def icone_app() -> QIcon:
    icone = QIcon()
    for tamanho in TAMANHOS:
        icone.addPixmap(QPixmap.fromImage(_renderizar(tamanho)))
    return icone


def pixmap_app(tamanho: int, escala: float = 1.0) -> QPixmap:
    """Ícone nítido em telas de DPI alto."""
    pix = QPixmap.fromImage(_renderizar(round(tamanho * escala)))
    pix.setDevicePixelRatio(escala)
    return pix


_OLHO = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="{cor}"
 stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
 <path d="M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/>{risco}</svg>"""


def icone_olho(oculto: bool, cor: str) -> QIcon:
    """Ícone de mostrar/ocultar senha, na cor do tema."""
    risco = '<path d="M3 3l18 18"/>' if oculto else ""
    svg = _OLHO.format(cor=cor, risco=risco).encode()
    icone = QIcon()
    for tamanho in (16, 20, 32, 40):
        img = QImage(tamanho, tamanho, QImage.Format.Format_ARGB32_Premultiplied)
        img.fill(Qt.GlobalColor.transparent)
        p = QPainter(img)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        QSvgRenderer(QByteArray(svg)).render(p, QRectF(0, 0, tamanho, tamanho))
        p.end()
        icone.addPixmap(QPixmap.fromImage(img))
    return icone


def salvar_ico(destino: Path) -> Path:
    """Grava um .ico (256 px; o Windows reduz para os demais tamanhos) e um .png ao lado."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    imagem = _renderizar(256)
    if not imagem.save(str(destino), "ICO"):
        raise RuntimeError(f"Não foi possível gravar {destino} (plugin de imagem ICO ausente?)")
    imagem.save(str(destino.with_suffix(".png")), "PNG")
    return destino


if __name__ == "__main__":
    from PySide6.QtGui import QGuiApplication

    _app = QGuiApplication(sys.argv[:1])
    caminho = salvar_ico(Path(sys.argv[1] if len(sys.argv) > 1 else "build/icone.ico"))
    print(f"Ícone gravado em {caminho}")
