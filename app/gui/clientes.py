"""Aba Clientes: quem está enviando dados ao broker, com nome, IP, MAC, último envio, tópico, status e mensagens.

O MQTT não traz IP nem MAC, então a lista junta três fontes (CLAUDE.md, "Aba Clientes"):

* a saída do `mosquitto -v` que a aba Mosquitto já captura (`ConsoleMosquitto.saida`): IP, client id, PUBLISH,
  SUBSCRIBE e desconexões (`interpretar_linha`, testável sem Qt). O PUBLISH conta mensagens, define tópico e status
  e alimenta os tópicos não ouvidos, mas não mexe no Último envio;
* o sinal `dispositivo_visto(device_id, tópico, iso)` do bridge: o nome (device_id) é ligado ao client id que
  publicou por último no MESMO tópico; sem Mosquitto na aba, a linha fica só com nome, tópico e último envio.
  O core só o emite para leituras de contagem (`channels`), e ele é a ÚNICA fonte do Último envio, do "Há quanto
  tempo", da cor de atrasado e do chip "N dispositivos enviando" (diagnóstico, respostas e eventos não contam);
* a tabela ARP do Windows (`arp -a`), consultada fora da thread da GUI a cada 60 s e quando surge um IP novo
  (`interpretar_arp`, `normalizar_mac`; o executor é injetável para testar em Linux);
* os sinais do DigiRail (`diagnostico_dispositivo`, `canais_vistos`, `resposta_comando`): nº de série, firmware,
  Wi-Fi, fila no aparelho, hora do diagnóstico ("sem resposta" quando o pedido falhou), IP (quando o Mosquitto não
  deu) e o botão Zerar contadores (diálogo e delegates em app/gui/comandos.py). O painel não publica nada: emite
  `zerar_pedido` e `diagnostico_pedido`, que a janela liga a `bridge.zerar_contadores` e `bridge.pedir_diagnostico`;
* tópicos por dispositivo (diálogo "Tópicos…" em app/gui/topicos.py): a janela entrega o bridge em
  `definir_api_topicos`. Cada tópico publicado (linha PUBLISH do mosquitto -v) é conferido com
  `topico_coberto`; o que o integrador não ouve vira um AVISO na Atividade (1 vez por tópico por sessão), um ⚠ na
  coluna Nome e um item no diálogo, com "Usar como…".

Partes: lógica pura (`RegistroClientes`), modelo Qt (`ModeloClientes` + `FiltroClientes`), consulta ARP
(`ConsultaArp`) e o conteúdo da aba (`PainelClientes`).
"""
from __future__ import annotations

import csv
import ipaddress
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from PySide6.QtCore import (
    QAbstractTableModel,
    QEvent,
    QPoint,
    QPointF,
    QSettings,
    QModelIndex,
    QObject,
    QSortFilterProxyModel,
    QTimer,
    Qt,
    Signal,
)
from PySide6.QtGui import QColor, QFont, QFontMetrics, QIcon, QPainter, QPen, QPixmap, QPolygonF
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMenu,
    QPushButton,
    QStackedWidget,
    QStyleOptionViewItem,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from .comandos import (
    PAPEL_ACAO,
    PAPEL_TOPICOS,
    PAPEL_WIFI,
    DelegadoAcao,
    DelegadoWifi,
    Diagnostico,
    DialogoZerarContadores,
    dica_wifi,
    interpretar_diag,
    lista_canais,
    numeros_canais,
    texto_hora_diag,
    texto_relogio,
    texto_wifi,
)
from .estilo import CLARA, Paleta
from .topicos import DialogoTopicos
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

# Ordem lógica (modelo, CSV, `linhas_visiveis`): as 8 colunas de sempre e depois as do DigiRail.
COLUNAS = ("Nome", "IP", "MAC", "Último envio", "Há quanto tempo", "Tópico", "Status", "Mensagens",
           "Nº série", "Firmware", "Wi-Fi", "Fila no aparelho", "Diagnóstico", "Ação")
(COL_NOME, COL_IP, COL_MAC, COL_ULTIMO, COL_HA, COL_TOPICO, COL_STATUS, COL_MENSAGENS,
 COL_SERIE, COL_FIRMWARE, COL_WIFI, COL_FILA, COL_DIAG, COL_ACAO) = range(len(COLUNAS))
# Ordem na tela: o que se olha sempre (nome, status, envio, sinal, fila) à esquerda; endereços e identificação no
# meio; tópico, contagem e a ação no fim. Colunas menos usadas podem ser escondidas (clique direito no cabeçalho).
ORDEM_VISUAL = (COL_NOME, COL_STATUS, COL_HA, COL_ULTIMO, COL_WIFI, COL_FILA, COL_IP, COL_MAC, COL_SERIE,
                COL_FIRMWARE, COL_DIAG, COL_TOPICO, COL_MENSAGENS, COL_ACAO)
DICA_ULTIMO_ENVIO = "Última leitura de contagem recebida (diagnóstico e eventos não contam)."
DICAS_COLUNAS = {
    COL_ULTIMO: DICA_ULTIMO_ENVIO,
    COL_HA: "Tempo desde a última leitura de contagem recebida (diagnóstico e eventos não contam).",
    COL_WIFI: "Sinal Wi-Fi (RSSI) informado pelo diagnóstico do aparelho.",
    COL_FILA: "Leituras guardadas no aparelho esperando envio por MQTT (mqtt_queue do diagnóstico).",
    COL_SERIE: "Número de série, pelo diagnóstico do aparelho.",
    COL_FIRMWARE: "Versão do firmware, pelo diagnóstico do aparelho.",
    COL_DIAG: "Hora do último diagnóstico recebido do aparelho.",
    COL_MENSAGENS: "Mensagens publicadas, contadas pela saída do mosquitto -v.",
    COL_ACAO: "Comandos ao aparelho (precisa estar conectado ao broker).",
}
COLUNAS_FIXAS = (COL_NOME, COL_ACAO)       # não podem ser escondidas
# Cabeçalho na tela mais curto que o nome da coluna (CSV, menu Colunas e QSettings usam o nome completo).
ROTULOS_CURTOS = {COL_FILA: "Fila"}
# Em 1440 px a tabela tem ~940 px (a coluna lateral fica com o resto): por padrão escondem-se as colunas cuja
# informação também aparece numa dica — Último envio (dica de "Há quanto tempo"), MAC (dica do IP), Tópico (dica do
# nome), Firmware (dica do nº de série) — e Mensagens. O menu "Colunas" (ou clique direito no cabeçalho) as mostra.
OCULTAS_PADRAO = (COL_ULTIMO, COL_MAC, COL_TOPICO, COL_MENSAGENS, COL_FIRMWARE)
# Exportação CSV: as 8 colunas originais na ordem de sempre, o client id e depois as do diagnóstico.
COLUNAS_CSV = tuple(range(COL_MENSAGENS + 1))
COLUNAS_CSV_DIAG = (COL_SERIE, COL_FIRMWARE, COL_WIFI, COL_FILA, COL_DIAG)
PAPEL_ORDEM = Qt.ItemDataRole.UserRole
CHAVE_COLUNAS_OCULTAS = "clientes/colunas_ocultas"
TEMPO_SEM_RESPOSTA_S = 30  # o core responde em até 15 s; sem nada em 30 s a tela desiste
TEXTO_ZERAR = "Zerar contadores"
TEXTO_SEM_RESPOSTA = "sem resposta"
MAX_TOPICOS_POR_CLIENTE = 50   # tópicos distintos lembrados por cliente (para reavaliar depois de salvar)


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
    publicados: list[str] = field(default_factory=list)   # tópicos distintos em que publicou (mosquitto -v)
    nao_ouvidos: list[str] = field(default_factory=list)  # desses, os que o integrador não ouve

    @property
    def proprio(self) -> bool:
        """O próprio integrador (publica os comandos; não precisa ouvir o que ele mesmo publica)."""
        return (self.client_id or "").startswith(PREFIXOS_PROPRIOS)

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
        self.diagnosticos: dict[str, Diagnostico] = {}  # device_id → último diag
        self.canais: dict[str, list[int]] = {}           # device_id → canais vistos nas leituras (1–6)
        # Tópicos não ouvidos. `coberto(tópico)` → True/False, ou None quando não dá para saber agora (integrador
        # desconectado): nesse caso nada muda e o tópico é conferido de novo em `reavaliar`.
        self.coberto: Callable[[str], bool | None] | None = None
        self._cobertura: dict[str, bool] = {}           # cache até o próximo `reavaliar`
        self._avisados: set[str] = set()                # tópicos já avisados nesta sessão
        self._avisos: list[tuple[str, str]] = []        # (nome exibido do cliente, tópico) a avisar

    # ----- consulta -----
    def cliente(self, chave: str) -> Cliente | None:
        return self._por_chave.get(chave)

    def por_nome(self, nome: str) -> Cliente | None:
        return next((c for c in self.linhas if c.nome == nome), None)

    def retirar_ips_novos(self) -> list[str]:
        novos, self._ips_novos = self._ips_novos, []
        return novos

    def retirar_avisos(self) -> list[tuple[str, str]]:
        avisos, self._avisos = self._avisos, []
        return avisos

    def nao_ouvidos_do_dispositivo(self, device_id: str) -> list[str]:
        saida: list[str] = []
        for c in self.linhas:
            if c.nome == device_id:
                saida += [t for t in c.nao_ouvidos if t not in saida]
        return saida

    # ----- tópicos não ouvidos -----
    def _e_coberto(self, topico: str) -> bool | None:
        if self.coberto is None:
            return None
        if topico in self._cobertura:
            return self._cobertura[topico]
        resultado = self.coberto(topico)
        if resultado is not None:
            self._cobertura[topico] = bool(resultado)
        return resultado

    def _conferir(self, c: Cliente, topico: str) -> bool:
        """Confere um tópico publicado por `c`. Devolve True se a lista de não ouvidos mudou."""
        if c.proprio:
            return False
        coberto = self._e_coberto(topico)
        if coberto is None:
            return False
        if coberto:
            if topico in c.nao_ouvidos:
                c.nao_ouvidos.remove(topico)
                return True
            return False
        if topico in c.nao_ouvidos:
            return False
        c.nao_ouvidos.append(topico)
        if topico not in self._avisados:
            self._avisados.add(topico)
            self._avisos.append((c.nome_exibido, topico))
        return True

    def reavaliar(self) -> int:
        """Confere de novo todos os tópicos publicados (depois de salvar/restaurar tópicos ou ao conectar)."""
        self._cobertura.clear()
        mudou = False
        for c in self.linhas:
            for topico in list(c.publicados):
                mudou = self._conferir(c, topico) or mudou
        return DADOS if mudou else NADA

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
            # O PUBLISH não mexe no último envio: pode ser diagnóstico, resposta de comando ou evento. O último envio
            # vem só de `dispositivo_visto` (leituras de contagem), decidido com o usuário em 11/11/2026.
            c.topico = ev.topico
            if ev.topico is not None:
                self._publicador[ev.topico] = c.chave
                mudanca = max(mudanca, self._absorver_por_topico(c, ev.topico))
                if ev.topico not in c.publicados:
                    if len(c.publicados) >= MAX_TOPICOS_POR_CLIENTE:
                        antigo = c.publicados.pop(0)
                        if antigo in c.nao_ouvidos:
                            c.nao_ouvidos.remove(antigo)
                    c.publicados.append(ev.topico)
                    self._conferir(c, ev.topico)
                elif ev.topico not in self._cobertura and ev.topico not in c.nao_ouvidos:
                    self._conferir(c, ev.topico)  # visto antes de conectar: confere agora
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

    # ----- entrada: sinais do DigiRail -----
    def registrar_diagnostico(self, device_id: str, diag: Diagnostico) -> int:
        """Guarda o diag do device_id. Se o dispositivo ainda não tem linha, cria uma (só-pipeline)."""
        device_id = str(device_id or "").strip()
        if not device_id:
            return NADA
        self.diagnosticos[device_id] = diag
        if diag.ip:
            self._ver_ip(diag.ip)
        if self.por_nome(device_id) is not None:
            return DADOS
        c, mudanca = self._obter(_chave_dispositivo(device_id))
        c.nome = device_id
        c.mensagens = None
        c.atividade = max(c.atividade, diag.recebido_em)
        return max(mudanca, DADOS)

    def registrar_canais(self, device_id: str, canais: list[int]) -> None:
        device_id = str(device_id or "").strip()
        if device_id:
            self.canais[device_id] = list(canais)

    def diagnostico(self, c: Cliente) -> Diagnostico | None:
        return self.diagnosticos.get(c.nome) if c.nome else None

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


@dataclass
class EstadoComando:
    """Último "Zerar contadores" de um dispositivo: "enviando" | "ok" | "falha"."""
    estado: str
    quando: float
    canais: list[int]
    mensagem: str = ""


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
        self.conectado = False                        # integrador conectado ao broker (comandos possíveis)
        self.comandos: dict[str, EstadoComando] = {}  # device_id → último Zerar contadores
        self.diag_pedido: dict[str, float] = {}       # device_id → quando o diag foi pedido (sem resposta ainda)
        self.diag_falha: dict[str, tuple[float, str]] = {}  # device_id → (quando, mensagem) do último diag sem resposta
        self.topicos_editaveis = False                # há API de tópicos (botão "Tópicos" e menu)
        self._icone_aviso: QIcon | None = None

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
        self._icone_aviso = None
        self.atualizar_tudo()

    def icone_aviso(self) -> QIcon:
        """Triângulo de aviso (⚠) na cor de aviso do tema, desenhado à mão (não depende da fonte ter o glifo)."""
        if self._icone_aviso is None:
            icone = QIcon()
            for escala in (1, 2):
                lado = 16 * escala
                pix = QPixmap(lado, lado)
                pix.setDevicePixelRatio(escala)
                pix.fill(Qt.GlobalColor.transparent)
                pintor = QPainter(pix)
                pintor.setRenderHint(QPainter.RenderHint.Antialiasing)
                cor = QColor(self.paleta.aviso)
                pintor.setPen(QPen(cor, 1.2, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap,
                                   Qt.PenJoinStyle.RoundJoin))
                pintor.setBrush(cor)
                pintor.drawPolygon(QPolygonF([QPointF(8, 1.6), QPointF(15, 14.4), QPointF(1, 14.4)]))
                pintor.setPen(QPen(QColor(self.paleta.superficie), 1.8, Qt.PenStyle.SolidLine,
                                   Qt.PenCapStyle.RoundCap))
                pintor.drawLine(QPointF(8, 5.8), QPointF(8, 9.6))
                pintor.drawPoint(QPointF(8, 12.2))
                pintor.end()
                icone.addPixmap(pix)
            self._icone_aviso = icone
        return self._icone_aviso

    def atualizar_dispositivo(self, device_id: str) -> None:
        for linha, c in enumerate(self.registro.linhas):
            if c.nome == device_id:
                self.dataChanged.emit(self.index(linha, 0), self.index(linha, len(COLUNAS) - 1))

    def cliente(self, linha: int) -> Cliente:
        return self.registro.linhas[linha]

    # ----- textos -----
    def idade(self, c: Cliente) -> float | None:
        return None if not c.ultimo_envio else max(0.0, self._relogio() - c.ultimo_envio)

    def ip(self, c: Cliente) -> str | None:
        """IP do Mosquitto; sem ele, o `ipv4` do diagnóstico do aparelho."""
        if c.ip:
            return c.ip
        diag = self.registro.diagnostico(c)
        return diag.ip if diag else None

    def texto_mac(self, c: Cliente) -> str:
        ip = self.ip(c)
        if not ip:
            return SEM_VALOR
        if ip_local(ip, self._locais()):
            return ESTE_COMPUTADOR
        return self.macs.get(ip, SEM_VALOR)

    def acao(self, c: Cliente) -> tuple[str, str] | None:
        """(estado, texto) do botão da linha; None sem device_id (não há a quem mandar o comando)."""
        if not c.nome:
            return None
        cmd = self.comandos.get(c.nome)
        if cmd is not None and cmd.estado == "enviando":
            return ("enviando", "Zerando…")
        if not self.conectado:
            return ("desabilitado", TEXTO_ZERAR)
        if cmd is not None and cmd.estado == "ok":
            return ("ok", f"Zerado às {datetime.fromtimestamp(cmd.quando):%H:%M}")
        if cmd is not None and cmd.estado == "falha":
            return ("falha", "Falhou")
        return ("pronto", TEXTO_ZERAR)

    def _texto_diag(self, c: Cliente, coluna: int) -> str:
        diag = self.registro.diagnostico(c)
        if coluna == COL_DIAG:
            if c.nome and c.nome in self.diag_pedido:
                return "pedido…"
            if c.nome and c.nome in self.diag_falha:
                return TEXTO_SEM_RESPOSTA
            return texto_hora_diag(diag.recebido_em if diag else None, self._relogio())
        if diag is None:
            return SEM_VALOR
        if coluna == COL_SERIE:
            return diag.sn or SEM_VALOR
        if coluna == COL_FIRMWARE:
            return diag.firmware or SEM_VALOR
        if coluna == COL_WIFI:
            return texto_wifi(diag)
        if coluna == COL_FILA:
            return SEM_VALOR if diag.fila is None else LOCALE_BR.toString(int(diag.fila))
        return ""

    def texto(self, c: Cliente, coluna: int) -> str:
        if coluna == COL_NOME:
            return c.nome_exibido
        if coluna == COL_IP:
            return self.ip(c) or SEM_VALOR
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
        if coluna in (COL_SERIE, COL_FIRMWARE, COL_WIFI, COL_FILA, COL_DIAG):
            return self._texto_diag(c, coluna)
        if coluna == COL_ACAO:
            acao = self.acao(c)
            return acao[1] if acao else ""
        return ""

    # ----- API do Qt -----
    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(self.registro.linhas)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return 0 if parent.isValid() else len(COLUNAS)

    def headerData(self, secao: int, orientacao: Qt.Orientation, papel: int = Qt.ItemDataRole.DisplayRole):  # noqa: N802
        if orientacao == Qt.Orientation.Horizontal:
            if papel == Qt.ItemDataRole.DisplayRole:
                return ROTULOS_CURTOS.get(secao, COLUNAS[secao])
            if papel == Qt.ItemDataRole.ToolTipRole:
                return DICAS_COLUNAS.get(secao)
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
        if papel == PAPEL_PILULA and col == COL_FILA:
            diag = self.registro.diagnostico(c)
            return "aviso" if diag and diag.fila else None  # fila > 0: leituras atrasadas no aparelho
        if papel == PAPEL_WIFI and col == COL_WIFI:
            diag = self.registro.diagnostico(c)
            return diag.rssi if diag else None
        if papel == PAPEL_ACAO and col == COL_ACAO:
            return self.acao(c)
        if papel == PAPEL_TOPICOS and col == COL_ACAO:
            if not (self.topicos_editaveis and c.nome):
                return None
            return "aviso" if c.nao_ouvidos else "normal"
        if papel == Qt.ItemDataRole.DecorationRole and col == COL_NOME and c.nao_ouvidos:
            return self.icone_aviso()
        if papel == Qt.ItemDataRole.TextAlignmentRole and col == COL_MENSAGENS:
            return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        return None

    def _ordem(self, c: Cliente, col: int):
        if col == COL_IP:
            try:
                ip = ipaddress.ip_address(self.ip(c) or "")
                return float(int(ip)) + (0 if ip.version == 4 else 2.0 ** 33)
            except ValueError:
                return float("inf")
        if col in (COL_ULTIMO, COL_HA):
            # "Há quanto tempo" crescente = mais recente primeiro
            envio = c.ultimo_envio or 0.0
            return envio if col == COL_ULTIMO else -envio
        if col == COL_MENSAGENS:
            return -1 if c.mensagens is None else c.mensagens
        diag = self.registro.diagnostico(c)
        if col == COL_WIFI:  # melhor sinal primeiro em ordem decrescente; "cabo" acima de "—"
            return -1000.0 if diag is None else (-999.0 if diag.rssi is None else float(diag.rssi))
        if col == COL_FILA:
            return -1 if diag is None or diag.fila is None else diag.fila
        if col == COL_DIAG:
            return diag.recebido_em if diag else 0.0
        if col == COL_ACAO:
            return f"{self.texto(c, col)}|{c.nome_exibido}".casefold()
        return self.texto(c, col).casefold()

    def _cor(self, c: Cliente, col: int) -> QColor | None:
        if col == COL_DIAG and self.texto(c, col) == TEXTO_SEM_RESPOSTA:
            return QColor(self.paleta.aviso)
        nivel = nivel_inatividade(self.idade(c))
        if nivel == "parado":
            return QColor(self.paleta.erro)
        if nivel == "atrasado":
            return QColor(self.paleta.aviso)
        if col in (COL_MAC, COL_ULTIMO, COL_HA, COL_TOPICO, COL_IP, COL_SERIE, COL_FIRMWARE, COL_WIFI, COL_FILA,
                   COL_DIAG) and self.texto(c, col) in (SEM_VALOR, ESTE_COMPUTADOR, "cabo", "pedido…"):
            return QColor(self.paleta.texto_fraco)
        if c.conectado is False and col != COL_STATUS:
            return QColor(self.paleta.texto_sec)
        return None

    def _dica(self, c: Cliente, col: int) -> str | None:
        diag = self.registro.diagnostico(c)
        if col == COL_NOME:
            partes = [f"Client id: {c.client_id}" if c.client_id else "Sem client id (Mosquitto não está na aba)"]
            if c.nome:
                partes.insert(0, f"Dispositivo: {c.nome}")
            if c.usuario:
                partes.append(f"Usuário MQTT: {c.usuario}")
            if c.topico:
                partes.append(f"Tópico: {c.topico}")
            if c.nao_ouvidos:
                partes.append("")
                partes.append("⚠ Publica em tópicos que o integrador não ouve:")
                partes += [f"  • {t}" for t in c.nao_ouvidos]
                if c.nome:
                    partes.append("Clique com o botão direito → Tópicos… para usar esses tópicos.")
            if diag is not None:
                if diag.titulo:
                    partes.append(f"Título no NXperience: {diag.titulo}")
                if diag.local:
                    partes.append(f"Localização: {diag.local}")
                relogio = texto_relogio(diag)
                if relogio:
                    partes.append(relogio)
            return "\n".join(partes)
        if col == COL_IP and self.ip(c):
            ip = f"{c.ip}:{c.porta}" if c.ip and c.porta else self.ip(c)
            if not c.ip:
                ip += " (informado pelo diagnóstico do aparelho)"
            mac = self.texto_mac(c)
            return f"{ip}\nMAC: {mac}" if mac != SEM_VALOR else ip
        if col in (COL_SERIE, COL_FIRMWARE) and diag is not None:
            return f"Nº de série: {diag.sn or SEM_VALOR}\nFirmware: {diag.firmware or SEM_VALOR}"
        if col == COL_WIFI:
            return dica_wifi(diag)
        if col == COL_FILA and diag is not None and diag.fila is not None:
            if diag.fila:
                return (f"{LOCALE_BR.toString(int(diag.fila))} leitura(s) guardada(s) no aparelho esperando envio. "
                        "Elas chegam atrasadas quando a rede voltar.")
            return "Nenhuma leitura pendente no aparelho."
        if col == COL_DIAG:
            if c.nome and c.nome in self.diag_pedido:
                return "Diagnóstico pedido; aguardando a resposta do aparelho."
            if c.nome and c.nome in self.diag_falha:
                quando, mensagem = self.diag_falha[c.nome]
                partes = [f"Diagnóstico sem resposta às {datetime.fromtimestamp(quando):%H:%M:%S}."]
                if mensagem:
                    partes.append(mensagem)
                if diag is not None:
                    partes.append(f"Último diagnóstico recebido em {formatar_epoch(diag.recebido_em)}.")
                partes.append("Confira o tópico de confirmação de comando em Tópicos… (clique direito).")
                return "\n".join(partes)
            if diag is not None:
                return f"Último diagnóstico recebido em {formatar_epoch(diag.recebido_em)}."
            return "Nenhum diagnóstico recebido ainda (botão Atualizar diagnóstico)." if c.nome else None
        if col == COL_ACAO:
            return self._dica_acao(c)
        if col == COL_MAC:
            texto = self.texto_mac(c)
            if texto == SEM_VALOR and c.ip:
                return "Fora da tabela ARP: outra rede ou ainda não consultado (botão Atualizar MAC)."
            return None
        if col == COL_HA:
            dica = DICA_INATIVIDADE.get(nivel_inatividade(self.idade(c)))
            envio = (f"Última leitura de contagem: {formatar_epoch(c.ultimo_envio)}" if c.ultimo_envio
                     else "Nenhuma leitura de contagem recebida ainda.")
            return "\n".join(t for t in (envio, dica) if t) or None
        if col == COL_ULTIMO:
            return DICA_ULTIMO_ENVIO
        if col == COL_STATUS and c.client_id and c.assinante and c.mensagens:
            return "Publica e também assina tópicos."
        if col == COL_TOPICO and c.topico:
            return c.topico
        return None


    def _dica_acao(self, c: Cliente) -> str | None:
        acao = self.acao(c)
        if acao is None:
            return None
        cmd = self.comandos.get(c.nome)
        estado = acao[0]
        if estado == "enviando":
            return f"Enviando o comando para zerar {lista_canais(cmd.canais)}; aguardando a resposta do aparelho."
        if estado == "desabilitado":
            return "Conecte ao broker para enviar comandos ao aparelho."
        if estado == "ok":
            return (f"Contadores {lista_canais(cmd.canais)} zerados às {datetime.fromtimestamp(cmd.quando):%H:%M:%S}."
                    f"{' ' + cmd.mensagem if cmd.mensagem else ''}\nClique para zerar de novo.")
        if estado == "falha":
            return (f"Falhou ao zerar {lista_canais(cmd.canais)}: {cmd.mensagem or 'sem detalhe.'}\n"
                    "Clique para tentar de novo.")
        return "Escolher os canais (chd1 a chd6) e zerar os contadores no aparelho."


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
    """Conteúdo da aba Clientes: barra (chip, Mostrar assinantes, Atualizar diagnóstico, Atualizar MAC, Colunas,
    Exportar CSV) e a tabela.

    Comandos: `zerar_pedido(device_id, [canais])` e `diagnostico_pedido(device_id)` (a janela liga ao bridge);
    `registrar_log(nivel, mensagem)` vai para a caixa da aba Atividade. `executar_dialogo(dlg) -> int` e
    `confirmar(parent, device_id, canais) -> bool` são injetáveis para testar sem diálogos modais.
    """

    zerar_pedido = Signal(str, list)
    diagnostico_pedido = Signal(str)
    registrar_log = Signal(str, str)

    def __init__(self, parent: QWidget | None = None, relogio: Callable[[], float] | None = None,
                 executor_arp: Callable[[], str] | None = None, locais: set[str] | None = None,
                 settings: QSettings | None = None):
        super().__init__(parent)
        self.setObjectName("paginaAba")
        self._relogio = relogio or time.time
        self._locais = set(locais) if locais is not None else enderecos_locais()
        self._settings = settings
        self.executar_dialogo: Callable[[QDialog], int] = lambda dlg: dlg.exec()
        self.confirmar: Callable[[QWidget | None, str, list[int]], bool] | None = None
        self.ultimo_dialogo: DialogoZerarContadores | None = None
        self.ultimo_dialogo_topicos: DialogoTopicos | None = None
        self.confirmar_restaurar: Callable[[QWidget | None, str], bool] | None = None
        self.api_topicos = None  # o bridge (topicos_do_dispositivo, salvar_…, restaurar_…, topico_coberto)
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
        self.chip_enviando.setToolTip("Dispositivos que mandaram leitura de contagem nos últimos 2 minutos "
                                      "(diagnóstico e eventos não contam).")
        self.opcao_assinantes = QCheckBox("Mostrar assinantes", self)
        self.opcao_assinantes.setToolTip("Mostra também quem só assina tópicos: este programa e o mosquitto_sub.")
        self.opcao_assinantes.toggled.connect(self._ao_mostrar_assinantes)
        self.botao_diag = self._botao("Atualizar diagnóstico", "")
        self.botao_mac = self._botao("Atualizar MAC", "Consultar agora a tabela ARP do Windows (arp -a).")
        self.botao_colunas = self._botao("Colunas", "Escolher as colunas visíveis (também no clique direito no "
                                                    "cabeçalho da tabela).")
        self.botao_exportar = self._botao("Exportar CSV…", "Salvar a lista em um arquivo .csv (abre no Excel).")
        self.botao_diag.clicked.connect(self.atualizar_diagnosticos)
        self.botao_mac.clicked.connect(self.atualizar_mac)
        self.botao_exportar.clicked.connect(self.exportar_dialogo)
        self.menu_colunas = QMenu(self)
        self.menu_colunas.aboutToShow.connect(self._montar_menu_colunas)
        self.botao_colunas.setMenu(self.menu_colunas)
        barra.addWidget(self.chip_enviando)
        barra.addSpacing(8)
        barra.addWidget(self.opcao_assinantes)
        barra.addStretch(1)
        barra.addWidget(self.botao_diag)
        barra.addWidget(self.botao_mac)
        barra.addWidget(self.botao_colunas)
        barra.addWidget(self.botao_exportar)

        self.tabela = QTableView(self)
        self.tabela.setObjectName("tabelaDados")
        self.tabela.setAccessibleName("Lista de clientes")
        fonte = self.tabela.font()
        fonte.setPointSizeF(9.0)  # tabela densa: até 14 colunas
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
        for posicao, col in enumerate(ORDEM_VISUAL):
            cab.moveSection(cab.visualIndex(col), posicao)
        self.delegado = DelegadoPilula(self.tabela)
        self.tabela.setItemDelegateForColumn(COL_STATUS, self.delegado)
        self.tabela.setItemDelegateForColumn(COL_FILA, self.delegado)
        self.delegado_wifi = DelegadoWifi(self.tabela)
        self.tabela.setItemDelegateForColumn(COL_WIFI, self.delegado_wifi)
        # Botão da linha desenhado pelo delegate: lê o estado do índice, então segue a linha certa com ordenação e
        # filtro (setIndexWidget ficaria preso à posição visual).
        self.delegado_acao = DelegadoAcao(self.tabela)
        self.delegado_acao.acionado.connect(self._ao_acionar)
        self.delegado_acao.topicos_acionado.connect(self._ao_acionar_topicos)
        self.tabela.setItemDelegateForColumn(COL_ACAO, self.delegado_acao)
        self.tabela.setMouseTracking(True)  # hover do botão
        self.tabela.viewport().installEventFilter(self)  # realce de cada botão da célula de ação
        self.tabela.entered.connect(self._ao_passar)
        self.tabela.activated.connect(self._ao_ativar)  # Enter/duplo clique na célula de ação
        self.tabela.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tabela.customContextMenuRequested.connect(self._menu_linha)
        cab.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        cab.customContextMenuRequested.connect(self._menu_cabecalho)
        self._restaurar_colunas()

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
        self._atualizar_botao_diag()

    def _botao(self, texto: str, dica: str) -> QPushButton:
        b = QPushButton(texto, self)
        b.setObjectName("botaoDiscreto")
        b.setToolTip(dica)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        return b

    LARGURA_MAX = {COL_NOME: 170, COL_IP: 120, COL_MAC: 140, COL_ULTIMO: 150, COL_HA: 150, COL_TOPICO: 170,
                   COL_STATUS: 110, COL_SERIE: 100, COL_FIRMWARE: 90, COL_WIFI: 100, COL_FILA: 120, COL_DIAG: 100}
    LARGURA_MIN = {COL_MAC: 124, COL_ACAO: 144}  # o MAC e o botão nunca aparecem cortados (+ "Tópicos")

    def _ajustar_colunas(self, *_args) -> None:
        """Ajusta ao conteúdo (cabeçalho incluso), com teto por coluna para caber na janela."""
        cab = self.tabela.horizontalHeader()
        fonte = QFont(cab.font())
        fonte.setPointSizeF(8)  # a do QSS do cabeçalho
        fonte.setWeight(QFont.Weight.DemiBold)
        metricas = QFontMetrics(fonte)
        for col in range(len(COLUNAS)):
            if self.tabela.isColumnHidden(col):
                continue
            texto_cab = metricas.horizontalAdvance(ROTULOS_CURTOS.get(col, COLUNAS[col])) + 16
            conteudo = self.tabela.sizeHintForColumn(col) if self.filtro.rowCount() else 0
            minimo = self.LARGURA_MIN.get(col, 0)
            if col == COL_ACAO and self.modelo.topicos_editaveis:
                minimo += DelegadoAcao.LARGURA_TOPICOS + DelegadoAcao.ESPACO
            largura = max(texto_cab, conteudo, minimo)
            if col in self.LARGURA_MAX:
                largura = max(texto_cab, min(largura, self.LARGURA_MAX[col]))
            cab.resizeSection(col, largura)

    # ----- entradas -----
    def ao_saida_mosquitto(self, tipo: str, texto: str) -> None:
        """Ligado a `ConsoleMosquitto.saida`. Só a saída dos processos interessa (não os avisos do próprio app)."""
        if tipo != "saida":
            return
        self._aplicar(self.registro.processar_texto(texto, self._relogio()))
        self._emitir_avisos_topicos()

    def ao_dispositivo_visto(self, device_id: str, topico: str, iso: str) -> None:
        agora = self._relogio()
        self._aplicar(self.registro.dispositivo_visto(device_id, topico, epoch_de_iso(iso, agora)))

    def tique(self) -> None:
        """A cada 1 s (timer da janela): "Há quanto tempo", cores de inatividade, o chip e os comandos sem
        resposta há mais de `TEMPO_SEM_RESPOSTA_S` (o core responde em até 15 s; isto é só uma rede de proteção)."""
        agora = self._relogio()
        for device, cmd in list(self.modelo.comandos.items()):
            if cmd.estado == "enviando" and agora - cmd.quando > TEMPO_SEM_RESPOSTA_S:
                # Nenhum resposta_comando chegou: o core não registrou nada, então a tela avisa na Atividade.
                self._resultado_zerar(device, False, f"Sem resposta do integrador em {TEMPO_SEM_RESPOSTA_S} s.")
                alvo = f"{lista_canais(cmd.canais)} em {device}" if cmd.canais else device
                self.registrar_log.emit("AVISO", f"Zerar contadores {alvo}: sem resposta do integrador em "
                                                 f"{TEMPO_SEM_RESPOSTA_S} s; o resultado é desconhecido.")
        for device, quando in list(self.modelo.diag_pedido.items()):
            if agora - quando > TEMPO_SEM_RESPOSTA_S:
                del self.modelo.diag_pedido[device]
        self.modelo.atualizar_tudo()
        self._atualizar_chip()

    def definir_paleta(self, p: Paleta) -> None:
        self.modelo.definir_paleta(p)
        self.delegado.definir_paleta(p)
        self.delegado_wifi.definir_paleta(p)
        self.delegado_acao.definir_paleta(p)
        self.tabela.viewport().update()

    # ----- DigiRail: diagnóstico, canais e comandos -----
    def definir_conectado(self, conectado: bool) -> None:
        """Comandos só com o integrador conectado ao broker (a janela chama a cada mudança de estado)."""
        antes = self.modelo.conectado
        self.modelo.conectado = bool(conectado)
        if self.modelo.conectado and not antes:
            self.reavaliar_topicos()  # o que chegou antes de conectar é conferido agora
        self.modelo.atualizar_tudo()
        self._atualizar_botao_diag()

    def ao_diagnostico(self, device_id: str, texto_json: str) -> None:
        diag = interpretar_diag(texto_json, self._relogio())
        self.modelo.diag_pedido.pop(device_id, None)
        self.modelo.diag_falha.pop(device_id, None)  # respondeu: some o "sem resposta"
        self._aplicar(self.registro.registrar_diagnostico(device_id, diag))
        self._ajustar_colunas()

    def ao_canais_vistos(self, device_id: str, texto_json: str) -> None:
        try:
            nomes = json.loads(texto_json)
        except (TypeError, ValueError):
            nomes = []
        self.registro.registrar_canais(device_id, numeros_canais(nomes if isinstance(nomes, list) else []))

    def ao_resposta_comando(self, device_id: str, comando: str, sucesso: bool, mensagem: str) -> None:
        if comando == "reset_counters":
            self._resultado_zerar(device_id, sucesso, mensagem)
        elif comando == "diag":
            self.modelo.diag_pedido.pop(device_id, None)
            if not sucesso:
                self.modelo.diag_falha[device_id] = (self._relogio(), str(mensagem or ""))
                self.registrar_log.emit("AVISO", f"Diagnóstico de {device_id} falhou: {mensagem}")
            self.modelo.atualizar_dispositivo(device_id)

    def dispositivos_listados(self) -> list[str]:
        """device_ids das linhas visíveis (na ordem da tabela)."""
        nomes = []
        for r in range(self.filtro.rowCount()):
            c = self.modelo.cliente(self.filtro.mapToSource(self.filtro.index(r, 0)).row())
            if c.nome and c.nome not in nomes:
                nomes.append(c.nome)
        return nomes

    def atualizar_diagnosticos(self) -> None:
        dispositivos = self.dispositivos_listados()
        if not self.modelo.conectado or not dispositivos:
            return
        for device in dispositivos:
            self._pedir_diagnostico(device)
        n = len(dispositivos)
        self.registrar_log.emit("INFO", f"Pedindo diagnóstico a {n} dispositivo{'s' if n != 1 else ''}.")

    def pedir_diagnostico(self, device_id: str) -> None:
        if self.modelo.conectado and device_id:
            self._pedir_diagnostico(device_id)
            self.registrar_log.emit("INFO", f"Pedindo diagnóstico a {device_id}.")

    def _pedir_diagnostico(self, device_id: str) -> None:
        self.modelo.diag_pedido[device_id] = self._relogio()
        self.modelo.atualizar_dispositivo(device_id)
        self.diagnostico_pedido.emit(device_id)

    def zerar(self, device_id: str) -> bool:
        """Abre o diálogo de canais; confirmado, envia e põe a linha em "Zerando…". Devolve True se enviou."""
        acao = next((self.modelo.acao(c) for c in self.registro.linhas if c.nome == device_id), None)
        if not acao or acao[0] not in DelegadoAcao.CLICAVEIS:
            return False
        dlg = DialogoZerarContadores(device_id, self.registro.canais.get(device_id, []), self,
                                     confirmar=self.confirmar)
        self.ultimo_dialogo = dlg
        try:
            if self.executar_dialogo(dlg) != QDialog.DialogCode.Accepted:
                return False
            canais = dlg.canais_escolhidos()
        finally:
            dlg.deleteLater()
        if not canais or not self.modelo.conectado:
            return False
        self.modelo.comandos[device_id] = EstadoComando("enviando", self._relogio(), canais)
        self.modelo.atualizar_dispositivo(device_id)
        self.zerar_pedido.emit(device_id, list(canais))  # o log do envio e do resultado vem do core
        return True

    def _resultado_zerar(self, device_id: str, sucesso: bool, mensagem: str) -> None:
        anterior = self.modelo.comandos.get(device_id)
        canais = anterior.canais if anterior else []
        self.modelo.comandos[device_id] = EstadoComando("ok" if sucesso else "falha", self._relogio(), canais,
                                                        str(mensagem or ""))
        # Só a linha da tabela muda: o log do resultado na Atividade vem do core (evita linha duplicada).
        self.modelo.atualizar_dispositivo(device_id)

    # ----- tópicos por dispositivo -----
    def definir_api_topicos(self, api) -> None:
        """A janela entrega o bridge (ou None). Sem ele, não há botão "Tópicos" nem conferência de tópicos."""
        necessarios = ("topicos_do_dispositivo", "salvar_topicos_dispositivo", "restaurar_topicos_padrao",
                       "topico_coberto")
        self.api_topicos = api if api is not None and all(hasattr(api, n) for n in necessarios) else None
        self.modelo.topicos_editaveis = self.api_topicos is not None
        self.registro.coberto = self._coberto if self.api_topicos is not None else None
        self.reavaliar_topicos()
        self._ajustar_colunas()

    def _coberto(self, topico: str) -> bool | None:
        """Pergunta ao bridge. Desconectado não há assinatura para comparar: None (confere ao conectar).
        Se o bridge falhar, considera ouvido (melhor não avisar do que avisar errado)."""
        if self.api_topicos is None or not self.modelo.conectado:
            return None
        try:
            return bool(self.api_topicos.topico_coberto(topico))
        except Exception:  # noqa: BLE001
            return True

    def reavaliar_topicos(self) -> None:
        mudanca = self.registro.reavaliar()
        if mudanca:
            self.modelo.aplicar(mudanca)
        self._emitir_avisos_topicos()

    def _emitir_avisos_topicos(self) -> None:
        for nome, topico in self.registro.retirar_avisos():
            self.registrar_log.emit("AVISO", f"{nome} publica em '{topico}', que o integrador não está ouvindo. "
                                             "Abra Clientes → Tópicos… para usar esse tópico.")

    def abrir_topicos(self, device_id: str) -> bool:
        """Abre o diálogo "Tópicos — <device_id>". Devolve True se salvou ou restaurou o padrão."""
        if self.api_topicos is None or not device_id:
            return False
        try:
            dlg = DialogoTopicos(device_id, self.api_topicos, self.registro.nao_ouvidos_do_dispositivo(device_id),
                                 self, confirmar=self.confirmar_restaurar)
        except Exception as exc:  # noqa: BLE001
            self.registrar_log.emit("ERRO", f"Não foi possível abrir os tópicos de {device_id}: {exc}")
            return False
        self.ultimo_dialogo_topicos = dlg
        try:
            if self.executar_dialogo(dlg) != QDialog.DialogCode.Accepted:
                return False
        finally:
            dlg.deleteLater()
        # o log de "salvos"/"restaurados" vem do core, com as assinaturas aplicadas
        self.reavaliar_topicos()  # o que passou a ser ouvido sai da lista e o ⚠ some
        self.modelo.atualizar_dispositivo(device_id)
        return True

    def _ao_acionar_topicos(self, indice: QModelIndex) -> None:
        c = self._cliente_do_proxy(indice)
        if c is not None and c.nome:
            self.abrir_topicos(c.nome)

    def eventFilter(self, objeto, evento) -> bool:  # noqa: N802
        """Movimento do mouse na tabela: realce só no botão sob o cursor e mãozinha sobre os botões."""
        if objeto is self.tabela.viewport() and evento.type() in (QEvent.Type.MouseMove, QEvent.Type.Leave):
            pos = evento.position() if evento.type() == QEvent.Type.MouseMove else None
            self.delegado_acao.posicao_mouse = pos
            indice = self.tabela.indexAt(pos.toPoint()) if pos is not None else QModelIndex()
            if indice.isValid() and indice.column() == COL_ACAO:
                opcao = QStyleOptionViewItem()
                opcao.initFrom(self.tabela)
                opcao.rect = self.tabela.visualRect(indice)
                opcao.font = self.tabela.font()
                if self.delegado_acao.botao_em(opcao, indice, pos):
                    self.tabela.viewport().setCursor(Qt.CursorShape.PointingHandCursor)
                else:
                    self.tabela.viewport().unsetCursor()
                self.tabela.viewport().update(self.tabela.visualRect(indice))
        return super().eventFilter(objeto, evento)

    def _cliente_do_proxy(self, indice: QModelIndex) -> Cliente | None:
        if not indice.isValid():
            return None
        fonte = self.filtro.mapToSource(indice)
        return self.modelo.cliente(fonte.row()) if fonte.isValid() else None

    def _ao_acionar(self, indice: QModelIndex) -> None:
        c = self._cliente_do_proxy(indice)
        if c is not None and c.nome:
            self.zerar(c.nome)

    def _ao_ativar(self, indice: QModelIndex) -> None:
        if indice.column() == COL_ACAO:
            self._ao_acionar(indice)

    def _ao_passar(self, indice: QModelIndex) -> None:
        if indice.column() != COL_ACAO:  # na coluna de ação quem decide é o eventFilter (depende do botão)
            self.tabela.viewport().unsetCursor()

    def menu_da_linha(self, indice: QModelIndex) -> QMenu | None:
        """Menu de contexto de uma linha (índice do proxy). Exposto para testes."""
        c = self._cliente_do_proxy(indice)
        if c is None:
            return None
        menu = QMenu(self)
        conectado = self.modelo.conectado
        acao = self.modelo.acao(c)
        zerar = menu.addAction("Zerar contadores…")
        zerar.setEnabled(bool(acao) and acao[0] in DelegadoAcao.CLICAVEIS)
        zerar.triggered.connect(lambda: self.zerar(c.nome))
        diag = menu.addAction("Atualizar diagnóstico deste dispositivo")
        diag.setEnabled(bool(c.nome) and conectado)
        diag.triggered.connect(lambda: self.pedir_diagnostico(c.nome))
        topicos = menu.addAction("Tópicos…")
        topicos.setEnabled(bool(c.nome) and self.api_topicos is not None)
        if c.nao_ouvidos:
            topicos.setIcon(self.modelo.icone_aviso())
        topicos.setToolTip("Ver e corrigir os tópicos deste aparelho (como no NXperience)." if c.nome
                           else "Este cliente ainda não tem nome (device_id).")
        topicos.triggered.connect(lambda: self.abrir_topicos(c.nome))
        if not conectado:
            for a in (zerar, diag):
                a.setToolTip("Conecte ao broker para enviar comandos.")
        menu.setToolTipsVisible(True)
        menu.addSeparator()
        ip = self.modelo.ip(c)
        copiar_ip = menu.addAction("Copiar IP")
        copiar_ip.setEnabled(bool(ip))
        copiar_ip.triggered.connect(lambda: QApplication.clipboard().setText(ip or ""))
        return menu

    def _menu_linha(self, pos: QPoint) -> None:
        menu = self.menu_da_linha(self.tabela.indexAt(pos))
        if menu is not None:
            menu.exec(self.tabela.viewport().mapToGlobal(pos))
            menu.deleteLater()

    def _atualizar_botao_diag(self) -> None:
        if not self.modelo.conectado:
            self.botao_diag.setEnabled(False)
            self.botao_diag.setToolTip("Conecte ao broker para pedir o diagnóstico aos aparelhos.")
        elif not self.dispositivos_listados():
            self.botao_diag.setEnabled(False)
            self.botao_diag.setToolTip("Nenhum dispositivo com nome (device_id) na lista ainda.")
        else:
            self.botao_diag.setEnabled(True)
            self.botao_diag.setToolTip("Pedir agora o diagnóstico (série, firmware, Wi-Fi, fila e IP) a todos os "
                                       "dispositivos listados.")

    # ----- colunas visíveis -----
    def colunas_ocultas(self) -> list[int]:
        return [c for c in range(len(COLUNAS)) if self.tabela.isColumnHidden(c)]

    def definir_coluna_visivel(self, coluna: int, visivel: bool) -> None:
        if coluna in COLUNAS_FIXAS:
            return
        self.tabela.setColumnHidden(coluna, not visivel)
        if visivel:
            self._ajustar_colunas()
        if self._settings is not None:
            self._settings.setValue(CHAVE_COLUNAS_OCULTAS, [COLUNAS[c] for c in self.colunas_ocultas()])

    def _restaurar_colunas(self) -> None:
        ocultas = set(OCULTAS_PADRAO)
        if self._settings is not None and self._settings.contains(CHAVE_COLUNAS_OCULTAS):
            valor = self._settings.value(CHAVE_COLUNAS_OCULTAS) or []
            nomes = [valor] if isinstance(valor, str) else list(valor)
            ocultas = {COLUNAS.index(n) for n in nomes if n in COLUNAS} - set(COLUNAS_FIXAS)
        for col in range(len(COLUNAS)):
            self.tabela.setColumnHidden(col, col in ocultas)

    def _menu_cabecalho(self, pos: QPoint) -> None:
        menu = self._montar_menu_colunas(QMenu(self))
        menu.exec(self.tabela.horizontalHeader().mapToGlobal(pos))
        menu.deleteLater()

    def _montar_menu_colunas(self, menu: QMenu | None = None) -> QMenu:
        menu = self.menu_colunas if menu is None else menu
        menu.clear()
        for col in ORDEM_VISUAL:
            acao = menu.addAction(COLUNAS[col])
            acao.setCheckable(True)
            acao.setChecked(not self.tabela.isColumnHidden(col))
            acao.setEnabled(col not in COLUNAS_FIXAS)
            acao.toggled.connect(lambda marcado, c=col: self.definir_coluna_visivel(c, marcado))
        return menu

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
        self._atualizar_botao_diag()

    def _ao_arp(self, macs: dict) -> None:
        self.modelo.definir_macs(macs)
        self._ajustar_colunas()

    def _ao_arp_ocupada(self, ocupada: bool) -> None:
        self.botao_mac.setEnabled(not ocupada)
        self.botao_mac.setText("Consultando…" if ocupada else "Atualizar MAC")

    def _ao_mostrar_assinantes(self, mostrar: bool) -> None:
        self.filtro.definir_mostrar_assinantes(mostrar)
        self._atualizar_vazio()
        self._atualizar_botao_diag()

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
        """Colunas originais na ordem de sempre, client id e depois as do diagnóstico (todas, mesmo ocultas)."""
        caminho = Path(caminho)
        with caminho.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow([*(COLUNAS[c] for c in COLUNAS_CSV), "Client id", *(COLUNAS[c] for c in COLUNAS_CSV_DIAG)])
            for valores in self.linhas_visiveis():
                w.writerow([*(valores[c] for c in COLUNAS_CSV), valores[-1], *(valores[c] for c in COLUNAS_CSV_DIAG)])
        return caminho

    def exportar_dialogo(self) -> None:
        nome = f"clientes-mqtt-{datetime.fromtimestamp(self._relogio()):%Y%m%d-%H%M%S}.csv"
        caminho, _ = QFileDialog.getSaveFileName(self, "Exportar clientes", nome, "CSV (*.csv);;Todos os arquivos (*)")
        if caminho:
            try:
                self.exportar_csv(Path(caminho))
            except OSError as exc:
                avisar(self.botao_exportar, f"Não foi possível salvar: {exc}")
