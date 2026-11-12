"""Janela principal: seções "Broker MQTT" e "Banco de dados", status, contadores e caixa de logs.

A janela não sabe nada de MQTT nem de SQL: recebe um objeto que segue o contrato de `app/bridge.py`
(Signals `status_alterado`, `log`, `contadores`, `teste_sql_concluido`, `proxima_gravacao`, `ultima_gravacao_ok`,
`ultima_leitura`; métodos `definir_sql`, `testar_sql`, `conectar`, `desconectar`, `encerrar`).

Operação contínua (CLAUDE.md): no topo da caixa ATIVIDADE, a contagem regressiva do vigia ("Reinício automático em
59:12 se nada for gravado", recomeça a cada gravação com sucesso) e a última gravação; o chip "Próxima gravação" só
aparece com amostragem ligada ([gravacao] intervalo_gravacao_segundos > 0). Limpeza da caixa à meia-noite (hora
local) e o vigia, que reinicia o programa se faltar gravação (ou leitura, sem SQL) por `[vigia] minutos`.

O cartão da direita tem quatro abas: "Atividade" (a caixa de logs), "Mosquitto" (console do broker local, processo
filho do app; ver app/gui/mosquitto.py), "Clientes" (quem envia dados, com IP e MAC; ver app/gui/clientes.py) e
"Apontamentos reais" (timeline das máquinas no portal OEE gerada pelo core; ver app/gui/apontamentos.py, sinal
opcional `apontamentos_atualizados` e métodos `definir_periodicidade_apontamentos`/`executar_apontamentos_agora`).
Os processos filhos param ao fechar a janela e antes do reinício do vigia.

O card ERROS é clicável e abre a janela "Erros registrados" (app/gui/erros.py), alimentada por
`erro_registrado` e pelo arquivo do dia em logs/. Os sinais novos (`dispositivo_visto`, `erro_registrado`) são
opcionais: um bridge sem eles continua funcionando.
"""
from __future__ import annotations

import dataclasses
import re
import time
from datetime import date, datetime
from typing import Any, Callable

from PySide6.QtCore import QSettings, QTimer, Qt
from PySide6.QtGui import QCloseEvent, QColor, QGuiApplication, QIcon, QKeySequence, QPainter, QPixmap, QShortcut
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpinBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from app.config import Config, MosquittoConfig, SqlServerConfig

from . import estilo, tempo
from . import vigia as vigia_mod
from .apontamentos import PainelApontamentos
from .clientes import PainelClientes
from .credenciais import CofreSenhas
from .erros import JanelaErros
from .mosquitto import ConsoleMosquitto, PainelMosquitto
from .recursos import icone_app, icone_olho, pixmap_app
from .widgets import (
    Cartao,
    CartaoContador,
    IndicadorStatus,
    PainelLogs,
    ResultadoTeste,
    SeletorQoS,
    SeletorSegmentado,
    repolir,
)

TITULO = "Integrador Alumbra MQTT + SQL + Mosquitto"
# Identificadores internos NÃO mudam com o título (senão o usuário perde a senha e os valores salvos).
ORGANIZACAO = "Alumbra"
APLICACAO = "MQTT SQL Server"

_HOST = re.compile(r"^[A-Za-z0-9._\-:\[\]]+$")
ESPERA_CANCELAR_MS = 3000
TEMPO_ENCERRAR_S = 8.0  # a última gravação pode levar até timeout_conexao (5 s)
ESPERA_DEFINIR_SQL_MS = 600
TIQUE_MS = 1000  # contagem regressiva, virada do dia e vigia
AVISO_LEMBRAR_SENHA = "Para o reinício automático continuar gravando no banco, marque Lembrar senha."
AVISO_SEM_SENHA = ("Senha do SQL Server não salva: digite a senha e marque Lembrar senha para gravar no banco "
                   "automaticamente. Até lá as leituras vão para a contingência.")
MSG_AUTO_ABERTURA = "Conexão automática ao abrir o programa"
MSG_AUTO_REINICIO = "Conexão automática após reinício automático"

CRIPTOGRAFIA = [
    ("Optional", "Opcional", "Criptografa se o servidor oferecer (Encrypt=Optional)."),
    ("Mandatory", "Obrigatório", "Exige criptografia TLS (Encrypt=Mandatory)."),
    ("Strict", "Estrito", "TDS 8.0 com TLS estrito (Encrypt=Strict, SQL Server 2022+)."),
]
AUTENTICACAO = [
    (False, "SQL Server", "Usuário e senha do próprio SQL Server."),
    (True, "Windows", "Usa o usuário do Windows que está rodando o app (sem senha)."),
]


def tamanho_inicial() -> tuple[int, int]:
    """1440×900 (a aba Clientes tem 8 colunas), limitado à área útil da tela; nunca menor que o mínimo."""
    largura, altura = 1440, 900
    tela = QGuiApplication.primaryScreen()
    if tela is not None:
        area = tela.availableGeometry()
        largura = min(largura, area.width() - 40)
        altura = min(altura, area.height() - 40)
    return max(980, largura), max(640, altura)


def configuracoes_padrao() -> QSettings:
    return QSettings(ORGANIZACAO, APLICACAO)


def validar_broker(texto: str) -> str | None:
    if not texto:
        return "Informe o endereço do broker (ex.: 192.168.0.10)."
    if not _HOST.match(texto):
        return "Broker inválido: use só o nome ou IP, sem espaços nem “mqtt://”."
    return None


def validar_topico(texto: str) -> str | None:
    if not texto:
        return "Informe o tópico (ex.: fabrica/+/sensores ou NOVUS/#)."
    niveis = texto.split("/")
    for i, nivel in enumerate(niveis):
        if "#" in nivel and (nivel != "#" or i != len(niveis) - 1):
            return "Tópico inválido: “#” só pode aparecer sozinho no último nível (ex.: NOVUS/#)."
        if "+" in nivel and nivel != "+":
            return "Tópico inválido: “+” deve ocupar um nível inteiro (ex.: fabrica/+/sensores)."
    if "\x00" in texto or len(texto.encode()) > 65535:
        return "Tópico inválido."
    return None


def _int(valor: Any, padrao: int) -> int:
    try:
        return int(valor)
    except (TypeError, ValueError):
        return padrao


def _bool(valor: Any, padrao: bool) -> bool:
    if valor is None:
        return padrao
    if isinstance(valor, str):
        return valor.strip().lower() in ("1", "true", "sim", "yes")
    return bool(valor)


class BotaoPrincipal(QPushButton):
    def __init__(self, texto: str, parent: QWidget | None = None):
        super().__init__(texto, parent)
        self.setObjectName("botaoPrincipal")
        self.setCursor(Qt.CursorShape.PointingHandCursor)


def _rotulo(texto: str, buddy: QWidget, parent: QWidget) -> QLabel:
    rot = QLabel(texto.upper(), parent)
    rot.setObjectName("rotuloCampo")
    rot.setBuddy(buddy)
    return rot


def _cabecalho_cartao(cartao: Cartao, titulo: str, descricao: str) -> None:
    t = QLabel(titulo, cartao)
    t.setObjectName("tituloCartao")
    t.setToolTip(descricao)
    cartao.corpo.addWidget(t)
    cartao.corpo.addSpacing(6)


class JanelaPrincipal(QMainWindow):
    def __init__(
        self,
        bridge,
        config: Config,
        settings: QSettings | None = None,
        tema: str | None = None,
        cofre: CofreSenhas | None = None,
        parent: QWidget | None = None,
        relogio: Callable[[], float] | None = None,
        iniciar_processo: Callable[[vigia_mod.Comando], bool] | None = None,
        sair: Callable[[], None] | None = None,
        console_mosquitto: ConsoleMosquitto | None = None,
        executor_arp: Callable[[], str] | None = None,
        enderecos_locais: set[str] | None = None,
        ler_erros: Callable[..., list] | None = None,
    ):
        """`tema`: None segue o sistema; "claro" ou "escuro" força um deles.
        `cofre`: onde guardar a senha do SQL (padrão: keyring); testes passam um falso.
        `relogio` (epoch em s), `iniciar_processo` e `sair`: injetáveis para testar o vigia e o reinício.
        `console_mosquitto`: processo da aba Mosquitto (padrão: criado com `config.mosquitto`). A janela NÃO o
        inicia sozinha: quem abre o programa chama `iniciar_mosquitto_ao_abrir()` (app/main.py).
        `executor_arp` (devolve o texto do `arp -a`), `enderecos_locais` e `ler_erros` (lê o arquivo de erros do
        dia): injetáveis para testar a aba Clientes e a janela de erros fora do Windows."""
        super().__init__(parent)
        self.bridge = bridge
        self.config = config
        self.settings = settings if settings is not None else configuracoes_padrao()
        self.cofre = cofre if cofre is not None else CofreSenhas()
        self._tema_forcado = tema
        self._paleta = estilo.CLARA
        self._estado = "desconectado"
        self._comandos_disponiveis = False
        self._encerrado = False
        self._testando = False
        self._senha_no_cofre: tuple[str, str] | None = None  # (usuario, senha) já gravados no cofre
        self._relogio = relogio or time.time
        self._iniciar_processo = iniciar_processo or vigia_mod.iniciar_processo
        self._sair = sair or vigia_mod.sair_do_app
        self._proxima: float | None = None       # epoch do próximo ciclo de gravação
        self._ultima_gravacao: str | None = None  # ISO local da última gravação ok
        self._dia_atividade: date = date.fromtimestamp(self._relogio())
        minutos = getattr(getattr(config, "vigia", None), "minutos", 60)
        intervalo = getattr(getattr(config, "gravacao", None), "intervalo_gravacao_segundos", 0)
        try:
            self._com_amostragem = float(intervalo or 0) > 0
        except (TypeError, ValueError):
            self._com_amostragem = False
        self.vigia = vigia_mod.Vigia(minutos, self._reiniciar, sql_configurado=self._sql_configurado,
                                     relogio=self._relogio)
        if console_mosquitto is None:
            console_mosquitto = ConsoleMosquitto(getattr(config, "mosquitto", None) or MosquittoConfig(), self)
        elif console_mosquitto.parent() is None:
            console_mosquitto.setParent(self)
        self.mosquitto = console_mosquitto
        self._executor_arp = executor_arp
        self._enderecos_locais = enderecos_locais
        pasta_logs = getattr(getattr(config, "gravacao", None), "pasta_logs", "logs") or "logs"
        self.janela_erros = JanelaErros(pasta_logs, self, ler=ler_erros)

        self.setWindowTitle(TITULO)
        self.setWindowIcon(icone_app())
        self.setMinimumSize(980, 640)
        self.resize(*tamanho_inicial())

        self._montar()
        self._carregar_valores()
        self._ligar_bridge()
        self._aplicar_tema()
        if tema is None:
            try:
                QGuiApplication.styleHints().colorSchemeChanged.connect(lambda *_: self._aplicar_tema())
            except AttributeError:
                pass
        self._atualizar_autenticacao()
        self._aplicar_estado("desconectado", "")
        self._atualizar_tempo()
        self._agendar_meia_noite()
        self._timer_tique.start()
        self.bridge.definir_sql(self.sql_config())

    # ================================================================== montagem
    def _montar(self) -> None:
        raiz = QWidget(self)
        raiz.setObjectName("raiz")
        self.setCentralWidget(raiz)
        corpo = QVBoxLayout(raiz)
        corpo.setContentsMargins(24, 20, 24, 24)
        corpo.setSpacing(16)
        corpo.addLayout(self._montar_cabecalho(raiz))

        colunas = QHBoxLayout()
        colunas.setSpacing(16)

        # Coluna esquerda (configuração), com rolagem se a tela for baixa.
        lateral = QScrollArea(raiz)
        lateral.setObjectName("colunaLateral")
        lateral.setWidgetResizable(True)
        lateral.setFrameShape(QScrollArea.Shape.NoFrame)
        lateral.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        lateral.setFixedWidth(400)
        conteudo = QWidget()
        conteudo.setObjectName("colunaLateralConteudo")
        col = QVBoxLayout(conteudo)
        col.setContentsMargins(0, 0, 4, 0)
        col.setSpacing(16)
        col.addWidget(self._montar_mqtt(conteudo))
        col.addWidget(self._montar_sql(conteudo))
        col.addStretch(1)
        lateral.setWidget(conteudo)
        self._lateral = lateral

        # Coluna direita (acompanhamento).
        direita = QVBoxLayout()
        direita.setSpacing(16)
        linha = QHBoxLayout()
        linha.setSpacing(16)
        self.contador_recebidas = CartaoContador("Recebidas", "mensagens MQTT", "recebidas", raiz)
        self.contador_gravadas = CartaoContador("Gravadas", "registros no SQL Server", "gravadas", raiz)
        self.contador_erros = CartaoContador("Erros", "leitura ou gravação", "erros", raiz, clicavel=True,
                                             dica_clique="Clique para ver os erros", texto_acao="Ver lista ›")
        self.contador_erros.clicado.connect(self.abrir_erros)
        for c in (self.contador_recebidas, self.contador_gravadas, self.contador_erros):
            c.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            linha.addWidget(c)
        direita.addLayout(linha)
        direita.addWidget(self._montar_abas(raiz), 1)

        colunas.addWidget(lateral)
        colunas.addLayout(direita, 1)
        corpo.addLayout(colunas, 1)

        self._montar_teclado()

        self._timer_cancelar = QTimer(self)
        self._timer_cancelar.setSingleShot(True)
        self._timer_cancelar.setInterval(ESPERA_CANCELAR_MS)
        self._timer_cancelar.timeout.connect(self._permitir_cancelar)

        self._timer_sql = QTimer(self)
        self._timer_sql.setSingleShot(True)
        self._timer_sql.setInterval(ESPERA_DEFINIR_SQL_MS)
        self._timer_sql.timeout.connect(self._enviar_sql)

        self._timer_teste = QTimer(self)  # rede de segurança se a resposta do teste nunca chegar
        self._timer_teste.setSingleShot(True)
        self._timer_teste.timeout.connect(self._teste_sem_resposta)

        self._timer_tique = QTimer(self)
        self._timer_tique.setTimerType(Qt.TimerType.PreciseTimer)
        self._timer_tique.setInterval(TIQUE_MS)
        self._timer_tique.timeout.connect(self._tique)

        # Disparo único recalculado a cada dia (PreciseTimer: o CoarseTimer pode adiantar até 5%).
        self._timer_meia_noite = QTimer(self)
        self._timer_meia_noite.setSingleShot(True)
        self._timer_meia_noite.setTimerType(Qt.TimerType.PreciseTimer)
        self._timer_meia_noite.timeout.connect(self._ao_meia_noite)

    def _montar_abas(self, raiz: QWidget) -> Cartao:
        """Cartão com as abas "Atividade" (a caixa de logs de sempre) e "Mosquitto" (console do broker local)."""
        cartao = Cartao(raiz, margem=16, espaco=0)
        cartao.corpo.setContentsMargins(16, 10, 16, 16)
        self.abas = QTabWidget(cartao)
        self.abas.setObjectName("abasAcompanhamento")
        self.abas.tabBar().setObjectName("barraAbas")
        self.abas.tabBar().setDrawBase(False)
        self.abas.tabBar().setExpanding(False)
        self.abas.tabBar().setCursor(Qt.CursorShape.PointingHandCursor)
        self.abas.setDocumentMode(False)

        # Aba Atividade: o mesmo painel de antes (chips, botões, caixa); o nome agora fica na própria aba.
        self.logs = PainelLogs(parent=self.abas)
        self.logs.corpo.setContentsMargins(0, 12, 0, 0)
        self.logs.titulo.hide()
        self.logs.chip_proxima.setVisible(self._com_amostragem)
        self.abas.addTab(self.logs, "Atividade")
        self.abas.setTabToolTip(0, "Eventos da conexão, das mensagens e das gravações no SQL Server.")

        self.painel_mosquitto = PainelMosquitto(self.mosquitto, self.abas, settings=self.settings)
        self.abas.addTab(self.painel_mosquitto, "Mosquitto")
        self.abas.setTabToolTip(1, f"Broker Mosquitto local: {self.mosquitto.prompt()}")
        self.mosquitto.estado_alterado.connect(lambda estado, _t: self._icone_aba_mosquitto(estado))
        self.mosquitto.pasta_alterada.connect(
            lambda _p: self.abas.setTabToolTip(1, f"Broker Mosquitto local: {self.mosquitto.prompt()}"))
        self._icone_aba_mosquitto(self.mosquitto.estado)

        self.painel_clientes = PainelClientes(self.abas, relogio=self._relogio, executor_arp=self._executor_arp,
                                              locais=self._enderecos_locais, settings=self.settings)
        self.abas.addTab(self.painel_clientes, "Clientes")
        self.abas.setTabToolTip(2, "Dispositivos que enviam dados ao broker: nome, IP, MAC e último envio.")
        self.mosquitto.saida.connect(self.painel_clientes.ao_saida_mosquitto)

        minutos_apont = getattr(getattr(self.config, "apontamentos", None), "minutos", None)
        self.painel_apontamentos = PainelApontamentos(self.abas, relogio=self._relogio, settings=self.settings,
                                                      minutos_padrao=minutos_apont)
        self.abas.addTab(self.painel_apontamentos, "Apontamentos reais")
        self.abas.setTabToolTip(3, "Timeline das máquinas no portal OEE (produção, microparada e macro parada) "
                                   "gerada a partir das contagens da IOT001.")

        cartao.corpo.addWidget(self.abas, 1)
        return cartao

    def abrir_erros(self) -> None:
        """Card ERROS: abre (ou traz para a frente) a janela "Erros registrados"."""
        self.janela_erros.abrir()

    def _icone_aba_mosquitto(self, estado: str) -> None:
        """Ponto colorido ao lado do nome da aba (o texto do estado fica no chip dentro dela)."""
        p = self._paleta
        cor = {"rodando": p.sucesso, "erro": p.erro, "iniciando": p.aviso, "parando": p.aviso}.get(estado)
        if cor is None:
            self.abas.setTabIcon(1, QIcon())
            return
        dpr = self.devicePixelRatioF()
        pix = QPixmap(int(10 * dpr), int(10 * dpr))
        pix.setDevicePixelRatio(dpr)
        pix.fill(Qt.GlobalColor.transparent)
        pintor = QPainter(pix)
        pintor.setRenderHint(QPainter.RenderHint.Antialiasing)
        pintor.setPen(Qt.PenStyle.NoPen)
        pintor.setBrush(QColor(cor))
        pintor.drawEllipse(1, 1, 8, 8)
        pintor.end()
        self.abas.setTabIcon(1, QIcon(pix))

    def iniciar_mosquitto_ao_abrir(self) -> bool:
        """Ao abrir o programa (antes da conexão automática): inicia o Mosquitto se
        `[mosquitto] iniciar_automaticamente` estiver ligado. O cliente MQTT reconecta sozinho, sem esperar."""
        if self._encerrado:
            return False
        return self.mosquitto.iniciar_ao_abrir()

    def _montar_cabecalho(self, raiz: QWidget) -> QHBoxLayout:
        cab = QHBoxLayout()
        cab.setSpacing(12)
        self._logo = QLabel(raiz)
        self._logo.setFixedSize(40, 40)
        self._logo.setPixmap(pixmap_app(40, self.devicePixelRatioF()))
        titulos = QVBoxLayout()
        titulos.setSpacing(0)
        titulo = QLabel(TITULO, raiz)
        titulo.setObjectName("titulo")
        sub = QLabel("Recebe as leituras dos dispositivos via MQTT e grava no SQL Server, "
                     "e mostra o broker Mosquitto local.", raiz)
        sub.setObjectName("subtitulo")
        sub.setWordWrap(True)  # janela estreita (980 px): quebra em duas linhas em vez de cortar ou empurrar o status
        sub.setMinimumWidth(240)
        sub.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        titulos.addWidget(titulo)
        titulos.addWidget(sub)
        self.status = IndicadorStatus(raiz)
        self.detalhe_status = QLabel(raiz)
        self.detalhe_status.setObjectName("detalheStatus")
        self.detalhe_status.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.detalhe_status.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        lado = QVBoxLayout()
        lado.setSpacing(4)
        lado.addWidget(self.status, 0, Qt.AlignmentFlag.AlignRight)
        lado.addWidget(self.detalhe_status, 0, Qt.AlignmentFlag.AlignRight)
        cab.addWidget(self._logo, 0, Qt.AlignmentFlag.AlignTop)
        cab.addLayout(titulos, 1)
        cab.addLayout(lado)
        return cab

    def _montar_mqtt(self, parent: QWidget) -> Cartao:
        c = Cartao(parent, margem=20, espaco=6)
        c.corpo.setContentsMargins(20, 16, 20, 18)
        _cabecalho_cartao(c, "Broker MQTT", "De onde chegam as leituras dos dispositivos.")
        g = QGridLayout()
        g.setHorizontalSpacing(12)
        g.setVerticalSpacing(6)

        self.campo_broker = QLineEdit(c)
        self.campo_broker.setPlaceholderText("ex.: 192.168.0.10")
        self.campo_broker.setToolTip("Nome ou IP do broker MQTT (Mosquitto).")
        self.campo_broker.setAccessibleName("Broker")

        self.campo_porta = QSpinBox(c)
        self.campo_porta.setRange(1, 65535)
        self.campo_porta.setGroupSeparatorShown(False)
        self.campo_porta.setToolTip("Porta TCP do broker (padrão 1883; 8883 com TLS).")
        self.campo_porta.setAccessibleName("Porta")
        self.campo_porta.setFixedWidth(116)
        self.campo_porta.lineEdit().setTextMargins(9, 0, 0, 0)

        self.campo_topico = QLineEdit(c)
        self.campo_topico.setPlaceholderText("ex.: fabrica/+/sensores")
        self.campo_topico.setToolTip(
            "Tópico a assinar. Curingas: “+” vale um nível, “#” vale todos os níveis abaixo (só no fim)."
        )
        self.campo_topico.setAccessibleName("Tópico")

        self.seletor_qos = SeletorQoS(c)
        self.seletor_qos.setFixedWidth(116)

        self.botao_conectar = BotaoPrincipal("Conectar", c)
        self.botao_conectar.clicked.connect(self._alternar)

        g.addWidget(_rotulo("Broker", self.campo_broker, c), 0, 0)
        g.addWidget(_rotulo("Porta", self.campo_porta, c), 0, 1)
        g.addWidget(self.campo_broker, 1, 0)
        g.addWidget(self.campo_porta, 1, 1)
        g.addItem(_espaco(6), 2, 0)
        g.addWidget(_rotulo("Tópico", self.campo_topico, c), 3, 0)
        g.addWidget(_rotulo("QoS", self.seletor_qos, c), 3, 1)
        g.addWidget(self.campo_topico, 4, 0)
        g.addWidget(self.seletor_qos, 4, 1)
        g.addItem(_espaco(10), 5, 0)
        g.addWidget(self.botao_conectar, 6, 0, 1, 2)
        g.setColumnStretch(0, 1)
        c.corpo.addLayout(g)

        self.erro_validacao = QLabel(c)
        self.erro_validacao.setObjectName("erroValidacao")
        self.erro_validacao.setWordWrap(True)
        self.erro_validacao.hide()
        c.corpo.addWidget(self.erro_validacao)

        for campo in (self.campo_broker, self.campo_topico):
            campo.returnPressed.connect(self._enter)
            campo.textEdited.connect(self._limpar_validacao)
        self.campo_porta.lineEdit().returnPressed.connect(self._enter)
        return c

    def _montar_sql(self, parent: QWidget) -> Cartao:
        c = Cartao(parent, margem=20, espaco=6)
        c.corpo.setContentsMargins(20, 16, 20, 18)
        _cabecalho_cartao(c, "Banco de dados", "SQL Server onde as leituras são gravadas.")
        g = QGridLayout()
        g.setHorizontalSpacing(12)
        g.setVerticalSpacing(6)

        self.campo_servidor = QLineEdit(c)
        self.campo_servidor.setPlaceholderText("ex.: SERVIDOR\\INSTANCIA")
        self.campo_servidor.setToolTip("Servidor e instância (ex.: TERRA\\SOLO) ou “host,porta”.")
        self.campo_servidor.setAccessibleName("Servidor")

        self.campo_banco = QLineEdit(c)
        self.campo_banco.setPlaceholderText("ex.: MQTT")
        self.campo_banco.setToolTip("Nome do banco de dados.")
        self.campo_banco.setAccessibleName("Banco")

        self.seletor_autenticacao = SeletorSegmentado(AUTENTICACAO, False, c, largura_min=80, nome="Autenticação")
        self.seletor_autenticacao.alterado.connect(lambda *_: self._atualizar_autenticacao())

        self.campo_usuario = QLineEdit(c)
        self.campo_usuario.setPlaceholderText("login do SQL")
        self.campo_usuario.setToolTip("Login do SQL Server.")
        self.campo_usuario.setAccessibleName("Usuário")

        self.campo_senha = QLineEdit(c)
        self.campo_senha.setEchoMode(QLineEdit.EchoMode.Password)
        self.campo_senha.setPlaceholderText("senha")
        self.campo_senha.setToolTip("Senha do login. Não é gravada em arquivo; veja “Lembrar senha”.")
        self.campo_senha.setAccessibleName("Senha")
        self._acao_olho = self.campo_senha.addAction(icone_olho(False, "#5B6576"), QLineEdit.ActionPosition.TrailingPosition)
        self._acao_olho.setToolTip("Mostrar senha")
        self._acao_olho.triggered.connect(self._alternar_senha_visivel)

        self.opcao_lembrar = QCheckBox("Lembrar senha neste computador", c)
        self.opcao_lembrar.setToolTip("Guarda a senha no Gerenciador de Credenciais do Windows.")

        self.seletor_cripto = SeletorSegmentado(CRIPTOGRAFIA, "Optional", c, largura_min=80, nome="Criptografar")
        self.opcao_confiar = QCheckBox("Confiar no certificado do servidor", c)
        self.opcao_confiar.setToolTip(
            "Aceita certificado autoassinado (TrustServerCertificate=yes). Use só em rede interna confiável."
        )

        self.botao_testar = QPushButton("Testar conexão", c)
        self.botao_testar.setCursor(Qt.CursorShape.PointingHandCursor)
        self.botao_testar.setToolTip("Abre e fecha uma conexão com o SQL Server usando os dados acima.")
        self.botao_testar.clicked.connect(self._testar_sql)
        self.resultado_sql = ResultadoTeste(c)

        r = 0
        g.addWidget(_rotulo("Servidor", self.campo_servidor, c), r, 0)
        g.addWidget(_rotulo("Banco", self.campo_banco, c), r, 1)
        g.addWidget(self.campo_servidor, r + 1, 0)
        g.addWidget(self.campo_banco, r + 1, 1)
        g.addItem(_espaco(6), r + 2, 0)
        r = 3
        g.addWidget(_rotulo("Autenticação", self.seletor_autenticacao, c), r, 0, 1, 2)
        g.addWidget(self.seletor_autenticacao, r + 1, 0, 1, 2)
        g.addItem(_espaco(6), r + 2, 0)
        r = 6
        g.addWidget(_rotulo("Usuário", self.campo_usuario, c), r, 0)
        g.addWidget(_rotulo("Senha", self.campo_senha, c), r, 1)
        g.addWidget(self.campo_usuario, r + 1, 0)
        g.addWidget(self.campo_senha, r + 1, 1)
        g.addWidget(self.opcao_lembrar, r + 2, 0, 1, 2)
        g.addItem(_espaco(6), r + 3, 0)
        r = 10
        g.addWidget(_rotulo("Criptografar", self.seletor_cripto, c), r, 0, 1, 2)
        g.addWidget(self.seletor_cripto, r + 1, 0, 1, 2)
        g.addWidget(self.opcao_confiar, r + 2, 0, 1, 2)
        g.addItem(_espaco(10), r + 3, 0)
        g.addWidget(self.botao_testar, r + 4, 0, 1, 2)
        g.setColumnStretch(0, 1)
        g.setColumnStretch(1, 1)
        c.corpo.addLayout(g)
        c.corpo.addWidget(self.resultado_sql)

        for campo in (self.campo_servidor, self.campo_banco, self.campo_usuario, self.campo_senha):
            campo.textEdited.connect(self._sql_alterado)
        for opcao in (self.opcao_lembrar, self.opcao_confiar):
            opcao.toggled.connect(self._sql_alterado)
        self.seletor_autenticacao.alterado.connect(self._sql_alterado)
        self.seletor_cripto.alterado.connect(self._sql_alterado)
        return c

    def _montar_teclado(self) -> None:
        ordem = [self.campo_broker, self.campo_porta, self.campo_topico, *self.seletor_qos.botoes,
                 self.botao_conectar, self.campo_servidor, self.campo_banco, *self.seletor_autenticacao.botoes,
                 self.campo_usuario, self.campo_senha, self.opcao_lembrar, *self.seletor_cripto.botoes,
                 self.opcao_confiar, self.botao_testar, self.abas.tabBar(), self.logs.botao_limpar,
                 self.logs.botao_copiar, self.logs.botao_salvar, self.logs.caixa,
                 self.painel_mosquitto.botao_iniciar, self.painel_mosquitto.botao_parar,
                 self.painel_mosquitto.botao_limpar, self.painel_mosquitto.campo_endereco,
                 self.painel_mosquitto.botao_procurar, self.painel_mosquitto.botao_restaurar,
                 self.painel_mosquitto.caixa,
                 self.painel_mosquitto.campo_comando, self.painel_mosquitto.botao_executar,
                 self.painel_clientes.opcao_assinantes, self.painel_clientes.botao_diag,
                 self.painel_clientes.botao_mac, self.painel_clientes.botao_colunas,
                 self.painel_clientes.botao_exportar, self.painel_clientes.tabela]
        for a, b in zip(ordem, ordem[1:]):
            QWidget.setTabOrder(a, b)
        QShortcut(QKeySequence("Ctrl+L"), self, activated=self.logs.limpar)
        QShortcut(QKeySequence.StandardKey.Save, self, activated=self.logs.salvar_dialogo)

    # ================================================================== valores
    def _carregar_valores(self) -> None:
        m = self.config.mqtt
        s = self.settings
        self.campo_broker.setText(str(s.value("mqtt/broker", m.broker) or m.broker))
        self.campo_porta.setValue(_int(s.value("mqtt/porta", m.porta), m.porta))
        self.campo_topico.setText(str(s.value("mqtt/topico", m.topico) or m.topico))
        self.seletor_qos.definir(_int(s.value("mqtt/qos", m.qos), m.qos))

        q = self.config.sqlserver
        self.campo_servidor.setText(str(s.value("sql/servidor", q.servidor) or ""))
        self.campo_banco.setText(str(s.value("sql/banco", q.banco) or ""))
        self.seletor_autenticacao.definir(_bool(s.value("sql/autenticacao_windows"), q.autenticacao_windows))
        self.campo_usuario.setText(str(s.value("sql/usuario", q.usuario) or ""))
        self.seletor_cripto.definir(str(s.value("sql/criptografar", q.criptografar) or q.criptografar))
        self.opcao_confiar.setChecked(_bool(s.value("sql/trust_server_certificate"), q.trust_server_certificate))
        lembrar = _bool(s.value("sql/lembrar_senha"), False)
        self._lembrava = lembrar
        self.opcao_lembrar.setChecked(lembrar)
        # Senha: config.toml local (se o usuário pôs lá) ou cofre do sistema. Nunca QSettings.
        senha = q.senha or ""
        usuario = self.campo_usuario.text().strip()
        if lembrar and usuario:
            salva, erro = self.cofre.ler(usuario)
            if erro:
                self._log_local("AVISO", f"Não foi possível ler a senha salva ({erro}). Digite a senha.")
            elif salva is not None:
                senha = salva
                self._senha_no_cofre = (usuario, salva)
        self.campo_senha.setText(senha)

        geo = s.value("janela/geometria")
        if geo is not None:
            self.restoreGeometry(geo)

    def _salvar_valores(self) -> None:
        s = self.settings
        s.setValue("mqtt/broker", self.campo_broker.text().strip())
        s.setValue("mqtt/porta", self.campo_porta.value())
        s.setValue("mqtt/topico", self.campo_topico.text().strip())
        s.setValue("mqtt/qos", self.seletor_qos.valor())
        s.setValue("sql/servidor", self.campo_servidor.text().strip())
        s.setValue("sql/banco", self.campo_banco.text().strip())
        s.setValue("sql/autenticacao_windows", bool(self.seletor_autenticacao.valor()))
        s.setValue("sql/usuario", self.campo_usuario.text().strip())
        s.setValue("sql/criptografar", self.seletor_cripto.valor())
        s.setValue("sql/trust_server_certificate", self.opcao_confiar.isChecked())
        s.setValue("sql/lembrar_senha", self.opcao_lembrar.isChecked())
        s.remove("sql/senha")  # garantia: a senha nunca fica no QSettings
        s.setValue("janela/geometria", self.saveGeometry())
        s.sync()
        self._sincronizar_cofre()

    def _sincronizar_cofre(self) -> None:
        usuario = self.campo_usuario.text().strip()
        senha = self.campo_senha.text()
        windows = bool(self.seletor_autenticacao.valor())
        anterior = self._senha_no_cofre
        if self.opcao_lembrar.isChecked() and not windows and usuario and senha:
            if anterior == (usuario, senha):
                return
            if anterior and anterior[0] != usuario:
                self.cofre.apagar(anterior[0])
            erro = self.cofre.salvar(usuario, senha)
            if erro:
                self._log_local("AVISO", f"Não foi possível guardar a senha no cofre do sistema ({erro}).")
                self._senha_no_cofre = None
            else:
                self._senha_no_cofre = (usuario, senha)
        elif not self.opcao_lembrar.isChecked() and (anterior or self._lembrava):
            alvos = {u for u in (usuario, anterior[0] if anterior else "") if u}
            for alvo in alvos:
                erro = self.cofre.apagar(alvo)
                if erro:
                    self._log_local("AVISO", f"Não foi possível apagar a senha do cofre do sistema ({erro}).")
            self._senha_no_cofre = None
        self._lembrava = self.opcao_lembrar.isChecked()

    def sql_config(self) -> SqlServerConfig:
        """SqlServerConfig com os campos da tela (tabela, driver e timeout vêm do config)."""
        windows = bool(self.seletor_autenticacao.valor())
        return dataclasses.replace(
            self.config.sqlserver,
            servidor=self.campo_servidor.text().strip(),
            banco=self.campo_banco.text().strip(),
            autenticacao_windows=windows,
            usuario="" if windows else self.campo_usuario.text().strip(),
            senha="" if windows else self.campo_senha.text(),
            criptografar=str(self.seletor_cripto.valor()),
            trust_server_certificate=self.opcao_confiar.isChecked(),
        )

    def _ligar_bridge(self) -> None:
        self.bridge.status_alterado.connect(self._ao_status)
        self.bridge.log.connect(self.logs.adicionar)
        self.bridge.contadores.connect(self._ao_contadores)
        self.bridge.teste_sql_concluido.connect(self._ao_teste_sql)
        self.bridge.proxima_gravacao.connect(self._ao_proxima_gravacao)
        self.bridge.ultima_gravacao_ok.connect(self._ao_ultima_gravacao)
        self.bridge.ultima_leitura.connect(self._ao_ultima_leitura)
        # Sinais da aba Clientes e da janela de erros: opcionais (um bridge antigo não os tem).
        if hasattr(self.bridge, "dispositivo_visto"):
            self.bridge.dispositivo_visto.connect(self.painel_clientes.ao_dispositivo_visto)
        if hasattr(self.bridge, "erro_registrado"):
            self.bridge.erro_registrado.connect(self.janela_erros.adicionar_json)
        # DigiRail: diagnóstico, canais e comandos (opcionais: um bridge sem eles deixa os botões desabilitados).
        painel = self.painel_clientes
        if hasattr(self.bridge, "diagnostico_dispositivo"):
            self.bridge.diagnostico_dispositivo.connect(painel.ao_diagnostico)
        if hasattr(self.bridge, "canais_vistos"):
            self.bridge.canais_vistos.connect(painel.ao_canais_vistos)
        if hasattr(self.bridge, "resposta_comando"):
            self.bridge.resposta_comando.connect(painel.ao_resposta_comando)
        self._comandos_disponiveis = all(hasattr(self.bridge, n) for n in (
            "zerar_contadores", "pedir_diagnostico", "resposta_comando"))
        if self._comandos_disponiveis:
            painel.zerar_pedido.connect(lambda device, canais: self.bridge.zerar_contadores(device, list(canais)))
            painel.diagnostico_pedido.connect(self.bridge.pedir_diagnostico)
        painel.registrar_log.connect(self._log_local)
        # Tópicos por dispositivo (v0.1.11): o painel só usa os métodos do contrato; bridge antigo → sem botão.
        painel.definir_api_topicos(self.bridge)
        self._ligar_apontamentos()

    def _ligar_apontamentos(self) -> None:
        """Aba Apontamentos reais: sinal e métodos opcionais. Ao abrir, reaplica a periodicidade salva."""
        aba = self.painel_apontamentos
        if hasattr(self.bridge, "apontamentos_atualizados"):
            self.bridge.apontamentos_atualizados.connect(aba.ao_ciclo)
        tem_periodo = hasattr(self.bridge, "definir_periodicidade_apontamentos")
        tem_executar = hasattr(self.bridge, "executar_apontamentos_agora")
        if tem_periodo:
            aba.periodicidade_alterada.connect(self.bridge.definir_periodicidade_apontamentos)
        if tem_executar:
            aba.executar_pedido.connect(self.bridge.executar_apontamentos_agora)
        aba.definir_disponivel(tem_periodo and tem_executar)
        if tem_periodo:
            aba.aplicar_periodicidade(forcar=True)

    def _log_local(self, nivel: str, mensagem: str) -> None:
        self.logs.adicionar(datetime.fromtimestamp(self._relogio()).strftime("%H:%M:%S"), nivel, mensagem)

    # ================================================================== tema
    def _aplicar_tema(self) -> None:
        if self._tema_forcado is not None:
            escuro = self._tema_forcado == "escuro"
        else:
            escuro = estilo.sistema_esta_escuro()
        paleta = estilo.aplicar(QApplication.instance(), escuro)
        self._paleta = paleta
        self.logs.definir_paleta(paleta)
        self.painel_mosquitto.caixa.definir_paleta(paleta)
        self.painel_clientes.definir_paleta(paleta)
        self.painel_apontamentos.definir_paleta(paleta)
        self.janela_erros.definir_paleta(paleta)
        self._icone_aba_mosquitto(self.mosquitto.estado)
        self._atualizar_icone_olho()
        self.tema_escuro = escuro

    # ================================================================== MQTT
    def _enter(self) -> None:
        if self._estado in ("desconectado", "erro"):
            self._alternar()

    def _alternar(self) -> None:
        if self._estado in ("desconectado", "erro"):
            self._conectar()
        else:
            self.vigia.parar()  # o usuário não quer mais estar conectado
            self._atualizar_tempo()
            self.botao_conectar.setEnabled(False)
            self.botao_conectar.setText("Desconectando…")
            self.bridge.desconectar()

    def auto_conectar(self, reinicio: bool = False) -> None:
        """Conexão automática ao abrir (`[inicio] conectar_automaticamente`) ou após o reinício do vigia
        (`--auto-conectar`): envia o SQL, testa o banco (como o botão Testar conexão) e conecta no broker como o
        clique em Conectar, inclusive iniciando o vigia. Chamada pelo app/main.py depois de a janela aparecer."""
        if self._encerrado or self._estado not in ("desconectado", "erro"):
            return
        self._log_local("INFO", MSG_AUTO_REINICIO if reinicio else MSG_AUTO_ABERTURA)
        self._enviar_sql()
        cfg = self.sql_config()
        sem_senha = cfg.habilitado and not cfg.autenticacao_windows and not cfg.senha
        if sem_senha:
            self._log_local("AVISO", AVISO_SEM_SENHA)
        elif cfg.habilitado:
            self._testar_sql()
        if not self._conectar(avisar_senha=not sem_senha):
            self._log_local("AVISO", f"Conexão automática não iniciada: {self.erro_validacao.text()} "
                                     "Corrija e clique em Conectar.")

    def _conectar(self, avisar_senha: bool = True) -> bool:
        """Valida os campos e pede a conexão ao bridge. Devolve False se a validação falhar."""
        broker = self.campo_broker.text().strip()
        topico = self.campo_topico.text().strip()
        self.campo_broker.setText(broker)
        self.campo_topico.setText(topico)
        for campo, erro in ((self.campo_broker, validar_broker(broker)), (self.campo_topico, validar_topico(topico))):
            if erro:
                self._mostrar_validacao(campo, erro)
                return False
        self._limpar_validacao()
        porta = self.campo_porta.value()
        qos = self.seletor_qos.valor()
        self._salvar_valores()
        self._enviar_sql()
        self._aplicar_estado("conectando", f"{broker}:{porta}")
        self.vigia.iniciar()
        self._atualizar_tempo()
        if avisar_senha and self.vigia.ligado and self._precisa_lembrar_senha():
            self._log_local("AVISO", AVISO_LEMBRAR_SENHA)
        self.bridge.conectar(broker, porta, topico, qos)
        return True

    def _precisa_lembrar_senha(self) -> bool:
        """SQL com login e senha, sem a senha salva: depois de um reinício automático o banco ficaria sem senha."""
        cfg = self.sql_config()
        return (cfg.habilitado and not cfg.autenticacao_windows and not self.opcao_lembrar.isChecked()
                and not self.config.sqlserver.senha)

    def _mostrar_validacao(self, campo: QWidget, mensagem: str) -> None:
        self._limpar_validacao()
        campo.setProperty("invalido", True)
        repolir(campo)
        self.erro_validacao.setText(mensagem)
        self.erro_validacao.show()
        campo.setFocus()

    def _limpar_validacao(self, *_args) -> None:
        for campo in (self.campo_broker, self.campo_topico):
            if campo.property("invalido"):
                campo.setProperty("invalido", False)
                repolir(campo)
        self.erro_validacao.hide()

    def _permitir_cancelar(self) -> None:
        if self._estado == "conectando":
            self.botao_conectar.setText("Cancelar")
            self.botao_conectar.setToolTip("Desistir da tentativa de conexão.")
            self.botao_conectar.setEnabled(True)

    # ================================================================== SQL
    def _atualizar_autenticacao(self) -> None:
        sql_auth = not bool(self.seletor_autenticacao.valor())
        for w in (self.campo_usuario, self.campo_senha, self.opcao_lembrar):
            w.setEnabled(sql_auth)

    def _atualizar_icone_olho(self) -> None:
        oculto = self.campo_senha.echoMode() == QLineEdit.EchoMode.Normal
        self._acao_olho.setIcon(icone_olho(oculto, self._paleta.texto_sec))

    def _alternar_senha_visivel(self) -> None:
        visivel = self.campo_senha.echoMode() == QLineEdit.EchoMode.Password
        self.campo_senha.setEchoMode(QLineEdit.EchoMode.Normal if visivel else QLineEdit.EchoMode.Password)
        self._acao_olho.setToolTip("Ocultar senha" if visivel else "Mostrar senha")
        self._atualizar_icone_olho()

    def _sql_alterado(self, *_args) -> None:
        if not self._testando:
            self.resultado_sql.ocultar()
        self._timer_sql.start()

    def _enviar_sql(self) -> None:
        self._timer_sql.stop()
        self.bridge.definir_sql(self.sql_config())

    def _validar_sql(self, cfg: SqlServerConfig) -> str | None:
        if not cfg.servidor:
            self.campo_servidor.setFocus()
            return "Informe o servidor (ex.: TERRA\\SOLO)."
        if not cfg.banco:
            self.campo_banco.setFocus()
            return "Informe o banco de dados."
        if not cfg.autenticacao_windows and not cfg.usuario:
            self.campo_usuario.setFocus()
            return "Informe o usuário do SQL Server (ou escolha autenticação do Windows)."
        return None

    def _testar_sql(self) -> None:
        if self._testando:
            return
        cfg = self.sql_config()
        erro = self._validar_sql(cfg)
        if erro:
            self.resultado_sql.mostrar("falha", erro)
            return
        self._salvar_valores()
        self._enviar_sql()
        self._testando = True
        self.botao_testar.setEnabled(False)
        self.botao_testar.setText("Testando…")
        self.resultado_sql.mostrar("testando", f"Conectando a {cfg.servidor} / {cfg.banco}…")
        self._log_local("INFO", f"Testando conexão com o SQL Server {cfg.servidor} (banco {cfg.banco})…")
        self._timer_teste.start((max(1, int(cfg.timeout_conexao)) + 30) * 1000)
        self.bridge.testar_sql(cfg)

    def _ao_teste_sql(self, sucesso: bool, mensagem: str) -> None:
        self._timer_teste.stop()
        self._testando = False
        self.botao_testar.setEnabled(True)
        self.botao_testar.setText("Testar conexão")
        mensagem = mensagem.strip() or ("Conexão bem-sucedida." if sucesso else "Falha na conexão.")
        self.resultado_sql.mostrar("ok" if sucesso else "falha", mensagem)
        QTimer.singleShot(0, lambda: self._lateral.ensureWidgetVisible(self.resultado_sql, 0, 16))
        for i, linha in enumerate(l for l in mensagem.splitlines() if l.strip()):
            self._log_local("INFO" if sucesso else "ERRO", ("SQL Server: " if i == 0 else "") + linha)

    def _teste_sem_resposta(self) -> None:
        if self._testando:
            self._ao_teste_sql(False, "O teste não respondeu a tempo. Verifique o servidor e tente de novo.")

    # ================================================================== sinais do bridge
    def _ao_status(self, estado: str, detalhe: str) -> None:
        self._aplicar_estado(estado, detalhe)

    def _ao_contadores(self, recebidas: int, gravadas: int, erros: int) -> None:
        self.contador_recebidas.definir(recebidas)
        self.contador_gravadas.definir(gravadas)
        self.contador_erros.definir(erros)

    # ================================================================== tempo: contagem, última gravação, dia
    def _ao_proxima_gravacao(self, epoch: float) -> None:
        try:
            self._proxima = float(epoch) if epoch and epoch > 0 else None
        except (TypeError, ValueError):
            self._proxima = None
        self._atualizar_tempo()

    def _ao_ultima_gravacao(self, iso: str) -> None:
        self._ultima_gravacao = iso or None
        epoch = vigia_mod.epoch_de_iso(iso, self._relogio())
        self.vigia.registrar_gravacao(epoch)
        if (self.vigia.ultima_leitura or 0.0) < epoch:  # gravou = chegou leitura: o contador recomeça inteiro
            self.vigia.registrar_leitura(epoch)
        self._atualizar_tempo()

    def _ao_ultima_leitura(self, iso: str) -> None:
        self.vigia.registrar_leitura(vigia_mod.epoch_de_iso(iso, self._relogio()))
        self._atualizar_tempo()

    def _atualizar_tempo(self) -> None:
        sql = self._sql_configurado()
        prefixo, valor, sufixo, estado = tempo.contagem_reinicio(self.vigia.restante(), self.vigia.ligado, sql)
        chip = self.logs.chip_reinicio
        chip.definir(prefixo, valor, estado, sufixo)
        limite = vigia_mod.minutos_txt(self.vigia.minutos)
        if not self.vigia.ligado:
            dica = "Reinício automático desligado ([vigia] minutos = 0 no config.toml)."
        elif sql:
            dica = (f"Se passar {limite} sem nenhuma gravação com sucesso no SQL Server, o programa se reinicia "
                    "e reconecta sozinho. A contagem recomeça a cada gravação.")
        else:
            dica = (f"Sem SQL Server configurado: se passar {limite} sem nenhuma leitura recebida, o programa se "
                    "reinicia e reconecta sozinho. A contagem recomeça a cada leitura.")
        if chip.toolTip() != dica:
            chip.setToolTip(dica)
        if self._com_amostragem:
            prefixo, valor = tempo.contagem_proxima(self._proxima, self._relogio())
            estado = "neutro" if valor == tempo.SEM_VALOR else ("ocupado" if valor == tempo.GRAVANDO else "ativo")
            self.logs.chip_proxima.definir(prefixo, valor, estado)
        prefixo, valor = tempo.texto_ultima(self._ultima_gravacao)
        self.logs.chip_ultima.definir(prefixo, valor, "ok" if self._ultima_gravacao else "neutro")
        self.logs.ajustar_chips()

    def _tique(self) -> None:
        self._conferir_dia()
        self._atualizar_tempo()
        self.painel_clientes.tique()
        self.painel_apontamentos.tique()
        self.vigia.verificar()

    def _agendar_meia_noite(self) -> None:
        self._timer_meia_noite.start(tempo.ms_ate_meia_noite(self._relogio()))

    def _ao_meia_noite(self) -> None:
        self._conferir_dia()
        self._agendar_meia_noite()

    def _conferir_dia(self) -> None:
        """Virou o dia (hora local)? Limpa a caixa ATIVIDADE. Também cobre o PC que dormiu na virada."""
        hoje = date.fromtimestamp(self._relogio())
        if hoje != self._dia_atividade:
            self._dia_atividade = hoje
            self.limpar_atividade_do_dia()

    def limpar_atividade_do_dia(self) -> None:
        """Limpa a caixa (o arquivo em logs/ não muda) e abre o dia com "Atividade de dd/mm/aaaa"."""
        self.logs.limpar()
        self._log_local("INFO", tempo.linha_do_dia(self._relogio()))
        self.logs.descarregar()

    # ================================================================== vigia
    def _sql_configurado(self) -> bool:
        return self.sql_config().habilitado

    def _reiniciar(self, motivo: str) -> None:
        if self._encerrado:
            return
        self._encerrado = True
        for t in (self._timer_cancelar, self._timer_sql, self._timer_teste, self._timer_tique,
                  self._timer_meia_noite):
            t.stop()
        self.detalhe_status.setText("Reiniciando…")
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            ok = vigia_mod.reiniciar_programa(
                motivo,
                registrar=self._registrar_agora,
                salvar=self._salvar_valores,
                encerrar=self._encerrar_tudo,
                iniciar=self._iniciar_processo,
                sair=self._sair,
            )
        finally:
            QApplication.restoreOverrideCursor()
        if not ok:
            self._aplicar_estado("erro", "Reinício automático falhou")

    def _encerrar_tudo(self, timeout: float = TEMPO_ENCERRAR_S) -> None:
        """Pipeline (grava o último lote e desconecta) e depois os processos filhos da aba Mosquitto
        (o broker local pode ser o mesmo em que o pipeline está conectado)."""
        try:
            self.bridge.encerrar(timeout=timeout)
        finally:
            self.mosquitto.encerrar()

    def _registrar_agora(self, nivel: str, mensagem: str) -> None:
        self._log_local(nivel, mensagem)
        self.logs.descarregar()
        QApplication.processEvents()

    def _aplicar_estado(self, estado: str, detalhe: str) -> None:
        if estado not in IndicadorStatus.TEXTOS:
            estado = "erro"
        anterior = self._estado
        self._estado = estado
        if estado == "desconectado" and self._proxima is not None:
            self._proxima = None
            self._atualizar_tempo()
        self.status.definir(estado)
        self.detalhe_status.setText(self.detalhe_status.fontMetrics().elidedText(
            detalhe, Qt.TextElideMode.ElideMiddle, 360))
        self.detalhe_status.setToolTip(detalhe)

        self.painel_clientes.definir_conectado(estado == "conectado" and self._comandos_disponiveis)
        livre = estado in ("desconectado", "erro")
        for w in (self.campo_broker, self.campo_porta, self.campo_topico, self.seletor_qos):
            w.setEnabled(livre)

        b = self.botao_conectar
        if estado == "conectado":
            b.setText("Desconectar")
            b.setEnabled(True)
            b.setProperty("perigo", True)
            b.setToolTip("Encerrar a conexão com o broker.")
        elif estado == "conectando":
            b.setProperty("perigo", True)
            if anterior != "conectando":
                b.setText("Conectando…")
                b.setToolTip("")
                b.setEnabled(False)
                self._timer_cancelar.start()
        else:
            b.setText("Conectar")
            b.setEnabled(True)
            b.setProperty("perigo", False)
            b.setToolTip("Conectar ao broker e assinar o tópico (Enter).")
        if estado != "conectando":
            self._timer_cancelar.stop()
        repolir(b)

    # ================================================================== fechamento
    def closeEvent(self, evento: QCloseEvent) -> None:
        if not self._encerrado:
            self._encerrado = True
            self.vigia.parar()
            for t in (self._timer_cancelar, self._timer_sql, self._timer_teste, self._timer_tique,
                      self._timer_meia_noite):
                t.stop()
            self.painel_clientes.parar()
            self.painel_apontamentos.parar()
            self.janela_erros.close()
            self._salvar_valores()
            self.detalhe_status.setText("Encerrando…")
            QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
            QApplication.processEvents()
            try:
                self._encerrar_tudo(timeout=TEMPO_ENCERRAR_S)
            finally:
                QApplication.restoreOverrideCursor()
        super().closeEvent(evento)

    @property
    def estado(self) -> str:
        return self._estado


def _espaco(altura: int):
    from PySide6.QtWidgets import QSpacerItem

    return QSpacerItem(0, altura, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Fixed)
