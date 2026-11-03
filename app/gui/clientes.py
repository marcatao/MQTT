"""Aba Clientes: quem está enviando dados ao broker, com nome, IP, MAC, último envio, tópico, status e mensagens.

O MQTT não traz IP nem MAC, então a lista junta três fontes (CLAUDE.md, "Aba Clientes"):

* a saída do `mosquitto -v` que a aba Mosquitto já captura (`ConsoleMosquitto.saida`): IP, client id, PUBLISH,
  SUBSCRIBE e desconexões (`interpretar_linha`, testável sem Qt);
* o sinal `dispositivo_visto(device_id, tópico, iso)` do bridge: o nome (device_id) é ligado ao client id que
  publicou por último no MESMO tópico; sem Mosquitto na aba, a linha fica só com nome, tópico e último envio;
* a tabela ARP do Windows (`arp -a`), consultada fora da thread da GUI a cada 60 s e quando surge um IP novo
  (`interpretar_arp`, `normalizar_mac`; o executor é injetável para testar em Linux).

Partes: lógica pura (`RegistroClientes`), modelo Qt (`ModeloClientes` + `FiltroClientes`), consulta ARP
(`ConsultaArp`) e o conteúdo da aba (`PainelClientes`).
"""
from __future__ import annotations

import csv
import ipaddress
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from PySide6.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QObject,
    QSortFilterProxyModel,
    QTimer,
    Qt,
    Signal,
)
from PySide6.QtGui import QColor, QFont, QFontMetrics
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QStackedWidget,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from .estilo import CLARA, Paleta
from .widgets import LOCALE_BR, PAPEL_PILULA, refiltrar, rodar_em_thread, ChipInfo, DelegadoPilula, avisar

# ---------------------------------------------------------------------- constantes
LIMITE_ATRASO_S = 120     # sem envio há mais de 2 min: âmbar + "atrasado"
LIMITE_PARADO_S = 600     # há mais de 10 min: vermelho + "sem envio"
INTERVALO_ARP_MS = 60_000
ESPERA_IP_NOVO_MS = 800   # junta vários IPs novos numa consulta só
MAX_CLIENTES = 500        # cada mosquitto_pub cria um client id novo: limita a lista
PREFIXOS_PROPRIOS = ("mqtt-sqlserver-",)          # o próprio app (app/core/mqtt_client.py)
PREFIXOS_SUB = ("mosqsub", "mosquitto_sub")       # mosquitto_sub antigo; o 2.x usa "mosq-…" (assinante sem PUBLISH)
SEM_VALOR = "—"
ESTE_COMPUTADOR = "este computador"

COLUNAS = ("Nome", "IP", "MAC", "Último envio", "Há quanto tempo", "Tópico", "Status", "Mensagens")
(COL_NOME, COL_IP, COL_MAC, COL_ULTIMO, COL_HA, COL_TOPICO, COL_STATUS, COL_MENSAGENS) = range(len(COLUNAS))
PAPEL_ORDEM = Qt.ItemDataRole.UserRole


# ---------------------------------------------------------------------- endereços (funções puras)
def normalizar_ip(texto: str) -> str:
    """'::ffff:192.168.0.10' → '192.168.0.10'; '[::1]' → '::1'; remove zona ('fe80::1%eth0'). Inválido volta igual."""
    t = str(texto or "").strip().strip("[]")
    sem_zona = t.split("%", 1)[0]
    try:
        ip = ipaddress.ip_address(sem_zona)
    except ValueError:
        return t
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return str(ip.ipv4_mapped)
    return str(ip)


def _e_ip(texto: str) -> bool:
    try:
        ipaddress.ip_address(texto.strip("[]").split("%", 1)[0])
        return True
    except ValueError:
        return False


def separar_endereco(texto: str) -> tuple[str, int | None]:
    """'192.168.0.5:51544' → ('192.168.0.5', 51544); '[::1]:5000' → ('::1', 5000);
    '::ffff:192.168.0.10:51544' → ('192.168.0.10', 51544); '192.168.0.5' → ('192.168.0.5', None)."""
    t = str(texto or "").strip()
    m = re.fullmatch(r"\[([^\]]+)\](?::(\d+))?", t)
    if m:
        return normalizar_ip(m.group(1)), int(m.group(2)) if m.group(2) else None
    if t.count(":") == 1:
        host, porta = t.split(":")
        return normalizar_ip(host), int(porta) if porta.isdigit() else None
    if t.count(":") > 1:
        host, _, porta = t.rpartition(":")
        if porta.isdigit() and _e_ip(host):
            return normalizar_ip(host), int(porta)
    return normalizar_ip(t), None


def ip_local(ip: str | None, locais: Iterable[str] = ()) -> bool:
    if not ip:
        return False
    try:
        if ipaddress.ip_address(ip).is_loopback:
            return True
    except ValueError:
        return False
    return ip in set(locais)


def normalizar_mac(texto: str) -> str | None:
    """'aa-bb-cc-dd-ee-ff', 'aa:bb:…', 'aabb.ccdd.eeff' → 'AA:BB:CC:DD:EE:FF'. Outro formato → None."""
    hexa = re.sub(r"[^0-9A-Fa-f]", "", str(texto or ""))
    if len(hexa) != 12 or not re.fullmatch(r"[0-9A-Fa-f:\-.\s]+", str(texto).strip()):
        return None
    hexa = hexa.upper()
    return ":".join(hexa[i:i + 2] for i in range(0, 12, 2))


_ARP_IP = re.compile(r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3})(?![\d.])")
_ARP_MAC = re.compile(r"(?<![0-9A-Fa-f])([0-9A-Fa-f]{2}(?:[-:][0-9A-Fa-f]{2}){5})(?![0-9A-Fa-f])")


def interpretar_arp(texto: str) -> dict[str, str]:
    """Saída do `arp -a` (Windows: '  192.168.100.50   aa-bb-cc-dd-ee-ff   dinâmico'; também o formato do Linux/macOS
    '? (192.168.0.1) at aa:bb:cc:dd:ee:ff …') → {ip: 'AA:BB:CC:DD:EE:FF'}. Ignora broadcast e multicast."""
    tabela: dict[str, str] = {}
    for linha in str(texto or "").splitlines():
        m_ip, m_mac = _ARP_IP.search(linha), _ARP_MAC.search(linha)
        if not m_ip or not m_mac:
            continue
        mac = normalizar_mac(m_mac.group(1))
        if mac is None or int(mac[:2], 16) & 1:  # bit de grupo: broadcast (FF:…) e multicast (01:00:5E:…)
            continue
        tabela[normalizar_ip(m_ip.group(1))] = mac
    return tabela


# ---------------------------------------------------------------------- saída do mosquitto -v (função pura)
@dataclass(frozen=True)
class EventoMosquitto:
    """tipo: "conexao" | "conectado" | "publish" | "subscribe" | "desconectado"."""
    tipo: str
    epoch: float | None = None
    client_id: str | None = None
    ip: str | None = None
    porta: int | None = None
    topico: str | None = None
    usuario: str | None = None
    tamanho: int | None = None


_PREFIXO = re.compile(r"^\s*(\d{9,11}(?:\.\d+)?):\s?(.*)$")
_NOVA_CONEXAO = re.compile(r"^New connection from (\S+) on port (\d+)\.?\s*$")
_CONECTADO = re.compile(r"^New client connected from (\S+) as (.+?) \((p\d[^)]*)\)\.?\s*$")
_USUARIO = re.compile(r"\bu'(.*)'")
_PUBLISH = re.compile(r"^Received PUBLISH from (.+?) \(d\d+, q\d, r\d, m\d+, '(.*)',.*?\((\d+) bytes\)\)\s*$")
_SUBSCRIBE = re.compile(r"^Received SUBSCRIBE from (.+?)\s*$")
_DESCONECTADO = re.compile(r"^Client (.+?) (?:disconnected\b|closed its connection\b|has exceeded timeout\b)")
_ERRO_SOCKET = re.compile(r"^Socket error on client (.+?), disconnecting\.?\s*$")


def interpretar_linha(linha: str) -> EventoMosquitto | None:
    """Uma linha do `mosquitto -v` (Mosquitto 2.x, com ou sem o prefixo de epoch "1730650014: ").

    Linhas que não interessam (ou que não casam) devolvem None.
    """
    texto = str(linha or "").rstrip("\r\n")
    epoch: float | None = None
    m = _PREFIXO.match(texto)
    if m:
        epoch, texto = float(m.group(1)), m.group(2)
    texto = texto.strip()
    if m := _PUBLISH.match(texto):
        return EventoMosquitto("publish", epoch, client_id=m.group(1), topico=m.group(2), tamanho=int(m.group(3)))
    if m := _CONECTADO.match(texto):
        ip, porta = separar_endereco(m.group(1))
        u = _USUARIO.search(m.group(3))
        return EventoMosquitto("conectado", epoch, client_id=m.group(2), ip=ip, porta=porta,
                               usuario=u.group(1) if u else None)
    if m := _NOVA_CONEXAO.match(texto):
        ip, porta = separar_endereco(m.group(1))
        return EventoMosquitto("conexao", epoch, ip=ip, porta=porta)
    if m := _SUBSCRIBE.match(texto):
        return EventoMosquitto("subscribe", epoch, client_id=m.group(1))
    if m := (_DESCONECTADO.match(texto) or _ERRO_SOCKET.match(texto)):
        return EventoMosquitto("desconectado", epoch, client_id=m.group(1))
    return None


# ---------------------------------------------------------------------- registro (lógica pura)
@dataclass
class Cliente:
    chave: str                       # client id, ou "dispositivo:<device_id>" quando só veio do pipeline
    client_id: str | None = None
    nome: str | None = None          # device_id do payload
    ip: str | None = None
    porta: int | None = None
    usuario: str | None = None
    topico: str | None = None
    ultimo_envio: float | None = None
    mensagens: int | None = 0        # None: sem Mosquitto não dá para contar (o sinal é limitado a 1 a cada 5 s)
    conectado: bool | None = None    # None: desconhecido (sem Mosquitto)
    assinante: bool = False
    atividade: float = 0.0           # último evento qualquer (para descartar os mais antigos)

    @property
    def nome_exibido(self) -> str:
        return self.nome or self.client_id or SEM_VALOR

    @property
    def status(self) -> str:
        if self.client_id is None or self.conectado is None:
            return SEM_VALOR
        if not self.conectado:
            return "Desconectado"
        if self.assinante and not self.mensagens:
            return "Assinante"
        return "Conectado"

    @property
    def oculto_por_padrao(self) -> bool:
        """O próprio app, mosquitto_sub e quem só assina: escondidos sem "Mostrar assinantes"."""
        cid = self.client_id or ""
        if cid.startswith(PREFIXOS_PROPRIOS) or cid.lower().startswith(PREFIXOS_SUB):
            return True
        return self.assinante and not self.mensagens and not self.nome


# Resultado de uma mudança: nada, só dados das linhas, ou linhas entrando/saindo (o modelo reinicia).
NADA, DADOS, ESTRUTURA = 0, 1, 2


class RegistroClientes:
    """Junta os eventos do Mosquitto e do pipeline numa linha por cliente. Sem Qt."""

    def __init__(self, maximo: int = MAX_CLIENTES):
        self.maximo = maximo
        self.linhas: list[Cliente] = []
        self._por_chave: dict[str, Cliente] = {}
        self._publicador: dict[str, str] = {}  # tópico → client id que publicou nele por último
        self._ips_novos: list[str] = []
        self._ips_vistos: set[str] = set()

    # ----- consulta -----
    def cliente(self, chave: str) -> Cliente | None:
        return self._por_chave.get(chave)

    def por_nome(self, nome: str) -> Cliente | None:
        return next((c for c in self.linhas if c.nome == nome), None)

    def retirar_ips_novos(self) -> list[str]:
        novos, self._ips_novos = self._ips_novos, []
        return novos

    # ----- entrada: texto do mosquitto -v -----
    def processar_texto(self, texto: str, agora: float) -> int:
        mudanca = NADA
        for linha in str(texto or "").split("\n"):
            evento = interpretar_linha(linha)
            if evento is not None:
                mudanca = max(mudanca, self.aplicar(evento, agora))
        return mudanca

    def aplicar(self, ev: EventoMosquitto, agora: float) -> int:
        quando = ev.epoch if ev.epoch is not None else agora
        if ev.tipo == "conexao":
            self._ver_ip(ev.ip)
            return NADA
        if not ev.client_id:
            return NADA
        c, mudanca = self._obter(ev.client_id)
        c.atividade = max(c.atividade, quando)
        if ev.tipo == "conectado":
            c.ip, c.porta, c.conectado = ev.ip, ev.porta, True
            c.usuario = ev.usuario or c.usuario
            self._ver_ip(ev.ip)
        elif ev.tipo == "publish":
            c.conectado = True
            c.mensagens = (c.mensagens or 0) + 1
            c.ultimo_envio = max(c.ultimo_envio or 0.0, quando)
            c.topico = ev.topico
            if ev.topico is not None:
                self._publicador[ev.topico] = c.chave
                mudanca = max(mudanca, self._absorver_por_topico(c, ev.topico))
        elif ev.tipo == "subscribe":
            c.conectado = True
            c.assinante = True
        elif ev.tipo == "desconectado":
            c.conectado = False
        return max(mudanca, DADOS)

    # ----- entrada: sinal dispositivo_visto do bridge -----
    def dispositivo_visto(self, device_id: str, topico: str, quando: float) -> int:
        device_id = str(device_id or "").strip()
        if not device_id:
            return NADA
        chave_cliente = self._publicador.get(topico)
        c = self._por_chave.get(chave_cliente) if chave_cliente else None
        mudanca = NADA
        if c is not None:
            c.nome = device_id
            c.ultimo_envio = max(c.ultimo_envio or 0.0, quando)
            c.atividade = max(c.atividade, quando)
            avulso = self._por_chave.get(_chave_dispositivo(device_id))
            if avulso is not None:  # a linha só-pipeline vira parte do cliente do Mosquitto
                self._remover(avulso)
                mudanca = ESTRUTURA
            return max(mudanca, DADOS)
        c, mudanca = self._obter(_chave_dispositivo(device_id))
        c.nome = device_id
        c.topico = topico or c.topico
        c.mensagens = None
        c.ultimo_envio = max(c.ultimo_envio or 0.0, quando)
        c.atividade = max(c.atividade, quando)
        return max(mudanca, DADOS)

    # ----- interno -----
    def _obter(self, chave: str) -> tuple[Cliente, int]:
        c = self._por_chave.get(chave)
        if c is not None:
            return c, NADA
        if len(self.linhas) >= self.maximo:
            self._descartar_mais_antigo()
        c = Cliente(chave=chave, client_id=None if chave.startswith("dispositivo:") else chave)
        self.linhas.append(c)
        self._por_chave[chave] = c
        return c, ESTRUTURA

    def _absorver_por_topico(self, c: Cliente, topico: str) -> int:
        """Linha só-pipeline com o mesmo tópico: o nome passa para o cliente que publicou e a linha some."""
        avulso = next((a for a in self.linhas if a.client_id is None and a.topico == topico), None)
        if avulso is None:
            return NADA
        c.nome = c.nome or avulso.nome
        c.ultimo_envio = max(c.ultimo_envio or 0.0, avulso.ultimo_envio or 0.0)
        self._remover(avulso)
        return ESTRUTURA

    def _remover(self, c: Cliente) -> None:
        self.linhas.remove(c)
        self._por_chave.pop(c.chave, None)
        for topico in [t for t, k in self._publicador.items() if k == c.chave]:
            del self._publicador[topico]

    def _descartar_mais_antigo(self) -> None:
        candidatos = [c for c in self.linhas if c.conectado is not True] or self.linhas
        self._remover(min(candidatos, key=lambda c: c.atividade))

    def _ver_ip(self, ip: str | None) -> None:
        if ip and ip not in self._ips_vistos:
            self._ips_vistos.add(ip)
            self._ips_novos.append(ip)


def _chave_dispositivo(device_id: str) -> str:
    return f"dispositivo:{device_id}"


# ---------------------------------------------------------------------- textos de tempo
def texto_ha(idade: float | None) -> str:
    """Segundos desde o último envio → "há 8 s", "há 3 min", "há 2 h", "há 3 dias"."""
    if idade is None:
        return SEM_VALOR
    s = max(0, int(idade))
    if s < 1:
        return "agora"
    if s < 60:
        return f"há {s} s"
    if s < 3600:
        return f"há {s // 60} min"
    if s < 86400:
        h, m = divmod(s // 60, 60)
        return f"há {h} h {m:02d} min" if m else f"há {h} h"
    dias = s // 86400
    return f"há {dias} dia{'s' if dias > 1 else ''}"


def nivel_inatividade(idade: float | None) -> str:
    """"" (em dia), "atrasado" (> 2 min) ou "parado" (> 10 min)."""
    if idade is None:
        return ""
    if idade > LIMITE_PARADO_S:
        return "parado"
    if idade > LIMITE_ATRASO_S:
        return "atrasado"
    return ""


SUFIXO_INATIVIDADE = {"atrasado": " · atrasado", "parado": " · sem envio"}
DICA_INATIVIDADE = {
    "atrasado": "Sem envio há mais de 2 minutos.",
    "parado": "Sem envio há mais de 10 minutos: confira o dispositivo e a rede.",
}


def formatar_epoch(epoch: float | None) -> str:
    if not epoch:
        return SEM_VALOR
    return datetime.fromtimestamp(epoch).strftime("%d/%m/%Y %H:%M:%S")


def epoch_de_iso(iso: str, padrao: float) -> float:
    try:
        return datetime.fromisoformat(str(iso).strip()).timestamp()
    except (TypeError, ValueError):
        return padrao


# ---------------------------------------------------------------------- modelo Qt
class ModeloClientes(QAbstractTableModel):
    def __init__(self, registro: RegistroClientes, relogio: Callable[[], float],
                 locais: Callable[[], set[str]] | None = None, parent: QObject | None = None):
        super().__init__(parent)
        self.registro = registro
        self._relogio = relogio
        self._locais = locais or (lambda: set())
        self.macs: dict[str, str] = {}
        self.paleta: Paleta = CLARA

    # ----- atualização -----
    def aplicar(self, mudanca: int) -> None:
        if mudanca == ESTRUTURA:
            self.beginResetModel()
            self.endResetModel()
        elif mudanca == DADOS:
            self.atualizar_tudo()

    def atualizar_tudo(self) -> None:
        if self.registro.linhas:
            self.dataChanged.emit(self.index(0, 0), self.index(len(self.registro.linhas) - 1, len(COLUNAS) - 1))

    def definir_macs(self, macs: dict[str, str]) -> None:
        self.macs = dict(macs)
        if self.registro.linhas:
            fim = len(self.registro.linhas) - 1
            self.dataChanged.emit(self.index(0, COL_MAC), self.index(fim, COL_MAC))

    def definir_paleta(self, p: Paleta) -> None:
        self.paleta = p
        self.atualizar_tudo()

    def cliente(self, linha: int) -> Cliente:
        return self.registro.linhas[linha]

    # ----- textos -----
    def idade(self, c: Cliente) -> float | None:
        return None if not c.ultimo_envio else max(0.0, self._relogio() - c.ultimo_envio)

    def texto_mac(self, c: Cliente) -> str:
        if not c.ip:
            return SEM_VALOR
        if ip_local(c.ip, self._locais()):
            return ESTE_COMPUTADOR
        return self.macs.get(c.ip, SEM_VALOR)

    def texto(self, c: Cliente, coluna: int) -> str:
        if coluna == COL_NOME:
            return c.nome_exibido
        if coluna == COL_IP:
            return c.ip or SEM_VALOR
        if coluna == COL_MAC:
            return self.texto_mac(c)
        if coluna == COL_ULTIMO:
            return formatar_epoch(c.ultimo_envio)
        if coluna == COL_HA:
            idade = self.idade(c)
            return texto_ha(idade) + SUFIXO_INATIVIDADE.get(nivel_inatividade(idade), "")
        if coluna == COL_TOPICO:
            return c.topico or SEM_VALOR
        if coluna == COL_STATUS:
            return c.status
        if coluna == COL_MENSAGENS:
            return SEM_VALOR if c.mensagens is None else LOCALE_BR.toString(int(c.mensagens))
        return ""

    # ----- API do Qt -----
    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(self.registro.linhas)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(COLUNAS)

    def headerData(self, secao: int, orientacao: Qt.Orientation, papel: int = Qt.ItemDataRole.DisplayRole):  # noqa: N802
        if orientacao == Qt.Orientation.Horizontal:
            if papel == Qt.ItemDataRole.DisplayRole:
                return COLUNAS[secao]
            if papel == Qt.ItemDataRole.TextAlignmentRole and secao == COL_MENSAGENS:
                return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        return None

    def data(self, indice: QModelIndex, papel: int = Qt.ItemDataRole.DisplayRole):
        if not indice.isValid() or indice.row() >= len(self.registro.linhas):
            return None
        c = self.registro.linhas[indice.row()]
        col = indice.column()
        if papel == Qt.ItemDataRole.DisplayRole:
            return self.texto(c, col)
        if papel == PAPEL_ORDEM:
            return self._ordem(c, col)
        if papel == Qt.ItemDataRole.ForegroundRole:
            return self._cor(c, col)
        if papel == Qt.ItemDataRole.ToolTipRole:
            return self._dica(c, col)
        if papel == PAPEL_PILULA and col == COL_STATUS:
            return {"Conectado": "sucesso", "Assinante": "destaque", "Desconectado": "neutro"}.get(c.status)
        if papel == Qt.ItemDataRole.TextAlignmentRole and col == COL_MENSAGENS:
            return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        return None

    def _ordem(self, c: Cliente, col: int):
        if col == COL_IP:
            try:
                ip = ipaddress.ip_address(c.ip or "")
                return float(int(ip)) + (0 if ip.version == 4 else 2.0 ** 33)
            except ValueError:
                return float("inf")
        if col in (COL_ULTIMO, COL_HA):
            # "Há quanto tempo" crescente = mais recente primeiro
            envio = c.ultimo_envio or 0.0
            return envio if col == COL_ULTIMO else -envio
        if col == COL_MENSAGENS:
            return -1 if c.mensagens is None else c.mensagens
        return self.texto(c, col).casefold()

    def _cor(self, c: Cliente, col: int) -> QColor | None:
        nivel = nivel_inatividade(self.idade(c))
        if nivel == "parado":
            return QColor(self.paleta.erro)
        if nivel == "atrasado":
            return QColor(self.paleta.aviso)
        if col in (COL_MAC, COL_ULTIMO, COL_HA, COL_TOPICO) and self.texto(c, col) in (SEM_VALOR, ESTE_COMPUTADOR):
            return QColor(self.paleta.texto_fraco)
        if c.conectado is False and col != COL_STATUS:
            return QColor(self.paleta.texto_sec)
        return None

    def _dica(self, c: Cliente, col: int) -> str | None:
        if col == COL_NOME:
            partes = [f"Client id: {c.client_id}" if c.client_id else "Sem client id (Mosquitto não está na aba)"]
            if c.nome:
                partes.insert(0, f"Dispositivo: {c.nome}")
            if c.usuario:
                partes.append(f"Usuário MQTT: {c.usuario}")
            return "\n".join(partes)
        if col == COL_IP and c.ip:
            return f"{c.ip}:{c.porta}" if c.porta else c.ip
        if col == COL_MAC:
            texto = self.texto_mac(c)
            if texto == SEM_VALOR and c.ip:
                return "Fora da tabela ARP: outra rede ou ainda não consultado (botão Atualizar MAC)."
            return None
        if col == COL_HA:
            return DICA_INATIVIDADE.get(nivel_inatividade(self.idade(c)))
        if col == COL_STATUS and c.client_id and c.assinante and c.mensagens:
            return "Publica e também assina tópicos."
        if col == COL_TOPICO and c.topico:
            return c.topico
        return None


class FiltroClientes(QSortFilterProxyModel):
    """Esconde o próprio app, mosquitto_sub e quem só assina, a menos que "Mostrar assinantes" esteja marcado."""

    def __init__(self, parent: QObject | None = None):
        super().__init__(parent)
        self.mostrar_assinantes = False
        self.setSortRole(PAPEL_ORDEM)
        self.setDynamicSortFilter(True)

    def definir_mostrar_assinantes(self, mostrar: bool) -> None:
        self.mostrar_assinantes = bool(mostrar)
        refiltrar(self)

    def filterAcceptsRow(self, linha: int, pai: QModelIndex) -> bool:  # noqa: N802
        if self.mostrar_assinantes:
            return True
        modelo = self.sourceModel()
        return not modelo.cliente(linha).oculto_por_padrao


# ---------------------------------------------------------------------- ARP (fora da thread da GUI)
def executor_arp_padrao() -> str:
    """`arp -a` no Windows (sem janela de console). Fora do Windows não consulta (devolve "")."""
    if sys.platform != "win32":
        return ""
    from .mosquitto import decodificar

    r = subprocess.run(["arp", "-a"], capture_output=True, stdin=subprocess.DEVNULL, timeout=15,
                       creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    return decodificar(r.stdout or b"")


class ConsultaArp(QObject):
    """Roda o executor (padrão: `arp -a`) numa thread de trabalho e devolve {ip: mac} em `concluida`.

    Uma consulta por vez; pedidos durante uma consulta viram uma nova consulta logo depois. O resultado chega à
    thread da GUI pelo sinal (conexão enfileirada), como os sinais do bridge.
    """

    concluida = Signal(dict)
    ocupada_alterada = Signal(bool)

    def __init__(self, executor: Callable[[], str] | None = None, parent: QObject | None = None):
        super().__init__(parent)
        self._executor = executor or executor_arp_padrao
        self._rodando = False
        self._pendente = False
        self.ultimo_erro = ""

    @property
    def ocupada(self) -> bool:
        return self._rodando

    def consultar(self) -> bool:
        if self._rodando:
            self._pendente = True
            return False
        self._rodando = True
        self._pendente = False
        self.ocupada_alterada.emit(True)
        rodar_em_thread(self._executor, self._ao_resultado, "arp")
        return True

    def _ao_resultado(self, texto: object, erro: str) -> None:
        texto = texto or ""
        self._rodando = False
        self.ultimo_erro = erro
        self.ocupada_alterada.emit(False)
        if not erro:
            self.concluida.emit(interpretar_arp(str(texto)))
        if self._pendente:
            self.consultar()


def enderecos_locais() -> set[str]:
    """IPs deste computador (para mostrar "este computador" no lugar do MAC)."""
    try:
        from PySide6.QtNetwork import QNetworkInterface

        return {normalizar_ip(a.toString()) for a in QNetworkInterface.allAddresses()}
    except Exception:  # noqa: BLE001
        return set()


# ---------------------------------------------------------------------- tela
class PainelClientes(QWidget):
    """Conteúdo da aba Clientes: barra (chip, Mostrar assinantes, Atualizar MAC, Exportar CSV) e a tabela."""

    def __init__(self, parent: QWidget | None = None, relogio: Callable[[], float] | None = None,
                 executor_arp: Callable[[], str] | None = None, locais: set[str] | None = None):
        super().__init__(parent)
        self.setObjectName("paginaAba")
        self._relogio = relogio or time.time
        self._locais = set(locais) if locais is not None else enderecos_locais()
        self.registro = RegistroClientes()
        self.modelo = ModeloClientes(self.registro, self._relogio, lambda: self._locais, self)
        self.filtro = FiltroClientes(self)
        self.filtro.setSourceModel(self.modelo)
        self.arp = ConsultaArp(executor_arp, self)
        self.arp.concluida.connect(self._ao_arp)
        self.arp.ocupada_alterada.connect(self._ao_arp_ocupada)

        corpo = QVBoxLayout(self)
        corpo.setContentsMargins(0, 12, 0, 0)
        corpo.setSpacing(12)

        barra = QHBoxLayout()
        barra.setSpacing(8)
        self.chip_enviando = ChipInfo("enviando", "Dispositivos enviando", self)
        self.chip_enviando.setToolTip("Clientes que publicaram nos últimos 2 minutos.")
        self.opcao_assinantes = QCheckBox("Mostrar assinantes", self)
        self.opcao_assinantes.setToolTip("Mostra também quem só assina tópicos: este programa e o mosquitto_sub.")
        self.opcao_assinantes.toggled.connect(self._ao_mostrar_assinantes)
        self.botao_mac = self._botao("Atualizar MAC", "Consultar agora a tabela ARP do Windows (arp -a).")
        self.botao_exportar = self._botao("Exportar CSV…", "Salvar a lista em um arquivo .csv (abre no Excel).")
        self.botao_mac.clicked.connect(self.atualizar_mac)
        self.botao_exportar.clicked.connect(self.exportar_dialogo)
        barra.addWidget(self.chip_enviando)
        barra.addSpacing(8)
        barra.addWidget(self.opcao_assinantes)
        barra.addStretch(1)
        barra.addWidget(self.botao_mac)
        barra.addWidget(self.botao_exportar)

        self.tabela = QTableView(self)
        self.tabela.setObjectName("tabelaDados")
        self.tabela.setAccessibleName("Lista de clientes")
        fonte = self.tabela.font()
        fonte.setPointSizeF(9.0)  # tabela densa: 8 colunas
        self.tabela.setFont(fonte)
        self.tabela.setModel(self.filtro)
        self.tabela.setSortingEnabled(True)
        self.tabela.sortByColumn(COL_NOME, Qt.SortOrder.AscendingOrder)
        self.tabela.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tabela.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.tabela.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.tabela.setAlternatingRowColors(True)
        self.tabela.setShowGrid(False)
        self.tabela.setWordWrap(False)
        self.tabela.setTextElideMode(Qt.TextElideMode.ElideMiddle)  # tópicos longos: começo e fim visíveis
        self.tabela.verticalHeader().hide()
        self.tabela.verticalHeader().setDefaultSectionSize(34)
        self.tabela.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        cab = self.tabela.horizontalHeader()
        cab.setHighlightSections(False)
        cab.setDefaultAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        cab.setMinimumSectionSize(56)
        # Larguras pelo conteúdo (com teto) quando entram linhas ou MACs; o usuário pode ajustar; a última coluna
        # ocupa a sobra. Em janela estreita aparece a barra de rolagem horizontal (nada é espremido).
        cab.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        cab.setStretchLastSection(True)
        self.delegado = DelegadoPilula(self.tabela)
        self.tabela.setItemDelegateForColumn(COL_STATUS, self.delegado)

        self.vazio = QLabel(self)
        self.vazio.setObjectName("estadoVazio")
        self.vazio.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.vazio.setWordWrap(True)
        self.pilha = QStackedWidget(self)
        self.pilha.addWidget(self.tabela)
        self.pilha.addWidget(self.vazio)

        corpo.addLayout(barra)
        corpo.addWidget(self.pilha, 1)

        self._timer_arp = QTimer(self)
        self._timer_arp.setInterval(INTERVALO_ARP_MS)
        self._timer_arp.timeout.connect(self.atualizar_mac)
        self._timer_arp.start()
        self._timer_ip_novo = QTimer(self)
        self._timer_ip_novo.setSingleShot(True)
        self._timer_ip_novo.setInterval(ESPERA_IP_NOVO_MS)
        self._timer_ip_novo.timeout.connect(self.atualizar_mac)
        for sinal in (self.filtro.rowsInserted, self.filtro.rowsRemoved, self.filtro.modelReset,
                      self.filtro.layoutChanged):
            sinal.connect(self._atualizar_vazio)
        for sinal in (self.filtro.rowsInserted, self.filtro.modelReset):
            sinal.connect(self._ajustar_colunas)
        self._ajustar_colunas()
        self._atualizar_vazio()
        self._atualizar_chip()

    def _botao(self, texto: str, dica: str) -> QPushButton:
        b = QPushButton(texto, self)
        b.setObjectName("botaoDiscreto")
        b.setToolTip(dica)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        return b

    LARGURA_MAX = {COL_NOME: 180, COL_IP: 140, COL_MAC: 160, COL_ULTIMO: 160, COL_HA: 160, COL_TOPICO: 150,
                   COL_STATUS: 100}
    LARGURA_MIN = {COL_MAC: 124}  # o MAC nunca aparece cortado

    def _ajustar_colunas(self, *_args) -> None:
        """Ajusta ao conteúdo (cabeçalho incluso), com teto por coluna para caber na janela."""
        cab = self.tabela.horizontalHeader()
        fonte = QFont(cab.font())
        fonte.setPointSizeF(8)  # a do QSS do cabeçalho
        fonte.setWeight(QFont.Weight.DemiBold)
        metricas = QFontMetrics(fonte)
        for col in range(len(COLUNAS)):
            texto_cab = metricas.horizontalAdvance(COLUNAS[col]) + 16
            conteudo = self.tabela.sizeHintForColumn(col) if self.filtro.rowCount() else 0
            largura = max(texto_cab, conteudo, self.LARGURA_MIN.get(col, 0))
            if col in self.LARGURA_MAX:
                largura = max(texto_cab, min(largura, self.LARGURA_MAX[col]))
            cab.resizeSection(col, largura)

    # ----- entradas -----
    def ao_saida_mosquitto(self, tipo: str, texto: str) -> None:
        """Ligado a `ConsoleMosquitto.saida`. Só a saída dos processos interessa (não os avisos do próprio app)."""
        if tipo != "saida":
            return
        self._aplicar(self.registro.processar_texto(texto, self._relogio()))

    def ao_dispositivo_visto(self, device_id: str, topico: str, iso: str) -> None:
        agora = self._relogio()
        self._aplicar(self.registro.dispositivo_visto(device_id, topico, epoch_de_iso(iso, agora)))

    def tique(self) -> None:
        """A cada 1 s (timer da janela): "Há quanto tempo", cores de inatividade e o chip."""
        self.modelo.atualizar_tudo()
        self._atualizar_chip()

    def definir_paleta(self, p: Paleta) -> None:
        self.modelo.definir_paleta(p)
        self.delegado.definir_paleta(p)

    def atualizar_mac(self) -> None:
        self.arp.consultar()

    def parar(self) -> None:
        self._timer_arp.stop()
        self._timer_ip_novo.stop()

    # ----- interno -----
    def _aplicar(self, mudanca: int) -> None:
        if mudanca == NADA:
            return
        self.modelo.aplicar(mudanca)
        novos = [ip for ip in self.registro.retirar_ips_novos() if not ip_local(ip, self._locais)]
        if novos:
            self._timer_ip_novo.start()
        self._atualizar_chip()

    def _ao_arp(self, macs: dict) -> None:
        self.modelo.definir_macs(macs)
        self._ajustar_colunas()

    def _ao_arp_ocupada(self, ocupada: bool) -> None:
        self.botao_mac.setEnabled(not ocupada)
        self.botao_mac.setText("Consultando…" if ocupada else "Atualizar MAC")

    def _ao_mostrar_assinantes(self, mostrar: bool) -> None:
        self.filtro.definir_mostrar_assinantes(mostrar)
        self._atualizar_vazio()

    def enviando(self) -> int:
        agora = self._relogio()
        return sum(1 for c in self.registro.linhas
                   if c.ultimo_envio and agora - c.ultimo_envio <= LIMITE_ATRASO_S and not c.oculto_por_padrao)

    def _atualizar_chip(self) -> None:
        n = self.enviando()
        texto = f"{n} dispositivo{'s' if n != 1 else ''} enviando"
        self.chip_enviando.definir("", texto, "ok" if n else "neutro")

    def _atualizar_vazio(self, *_args) -> None:
        if self.filtro.rowCount() > 0:
            self.pilha.setCurrentWidget(self.tabela)
            return
        if self.registro.linhas:
            self.vazio.setText("Só há assinantes conectados (este programa, mosquitto_sub).\n"
                               "Marque Mostrar assinantes para vê-los.")
        else:
            self.vazio.setText("Nenhum cliente ainda.\nOs dispositivos aparecem aqui quando publicam no broker; "
                               "IP e MAC vêm da aba Mosquitto.")
        self.pilha.setCurrentWidget(self.vazio)

    # ----- exportação -----
    def linhas_visiveis(self) -> list[list[str]]:
        linhas = []
        for r in range(self.filtro.rowCount()):
            fonte = self.filtro.mapToSource(self.filtro.index(r, 0)).row()
            c = self.modelo.cliente(fonte)
            valores = [self.modelo.texto(c, col) for col in range(len(COLUNAS))]
            valores[COL_HA] = texto_ha(self.modelo.idade(c))
            linhas.append(valores + [c.client_id or ""])
        return linhas

    def exportar_csv(self, caminho: Path) -> Path:
        caminho = Path(caminho)
        with caminho.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow([*COLUNAS, "Client id"])
            w.writerows(self.linhas_visiveis())
        return caminho

    def exportar_dialogo(self) -> None:
        nome = f"clientes-mqtt-{datetime.fromtimestamp(self._relogio()):%Y%m%d-%H%M%S}.csv"
        caminho, _ = QFileDialog.getSaveFileName(self, "Exportar clientes", nome, "CSV (*.csv);;Todos os arquivos (*)")
        if caminho:
            try:
                self.exportar_csv(Path(caminho))
            except OSError as exc:
                avisar(self.botao_exportar, f"Não foi possível salvar: {exc}")
