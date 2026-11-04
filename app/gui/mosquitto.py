"""Aba Mosquitto: o broker local rodando como processo filho do app, com a saída ao vivo num console.

Duas partes:

* `ConsoleMosquitto` (QObject, sem widgets): inicia/para o `mosquitto -v` e um "comando extra" (ex.:
  `mosquitto_sub -h localhost -t "#" -v`) com QProcess e avisa por sinais (`saida`, `estado_alterado`,
  `comando_alterado`). A fábrica de processo e o interpretador do comando extra são injetáveis para os testes.
* `PainelMosquitto` (QWidget): chip de estado, botões Iniciar/Parar/Limpar, o campo "Endereço Mosquitto" (pasta do
  mosquitto.exe, com botão "…" e "Restaurar padrão"), o console e o campo "Comando".

Tudo roda na thread da GUI: o QProcess é assíncrono (sinais `readyRead`/`finished`), nada aqui bloqueia, exceto
`encerrar()`, que é chamado só ao fechar o programa ou no reinício do vigia (no máximo ~3 s).

Config (`[mosquitto]` no config.toml, `MosquittoConfig` em app/config.py): `pasta`, `comando` (1º token = programa,
".exe" acrescentado se faltar; o resto são argumentos) e `iniciar_automaticamente`. O prompt mostrado é sempre
``<pasta>><comando>``, como no Prompt de Comando do Windows.

Endereço Mosquitto: `[mosquitto] pasta` é só o padrão. A pasta escolhida no campo da aba fica em QSettings
(`mosquitto/pasta`, chave `CHAVE_PASTA`), vale mais que o config.toml e é lida ao abrir (inclusive no reinício do
vigia, antes do início automático). Só é salva por ação do usuário: Iniciar com sucesso, Enter no campo ou o seletor.
"""
from __future__ import annotations

import locale
import logging
import os
import shlex
import subprocess
import sys
import time
from typing import Callable

from PySide6.QtCore import QEvent, QObject, QProcess, QSettings, QTimer, Qt, Signal
from PySide6.QtGui import QKeySequence
from PySide6.QtWidgets import (
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from app.config import MosquittoConfig

from .estilo import fonte_mono
from .widgets import CaixaConsole, ChipInfo, repolir

log = logging.getLogger("app.mosquitto")

ESPERA_TERMINAR_MS = 3000  # terminate → espera até 3 s → kill
CHAVE_PASTA = "mosquitto/pasta"   # QSettings: pasta escolhida no campo "Endereço Mosquitto"
DICA_PARE = "Pare o Mosquitto para alterar o endereço"
DICA_SERVICO = ("Se o Mosquitto já roda como serviço do Windows, pare o serviço ou desligue "
                "[mosquitto] iniciar_automaticamente")

FabricaProcesso = Callable[[QObject], QProcess]


# ---------------------------------------------------------------------- utilitários (testáveis sem Qt)
def separar_comando(comando: str) -> tuple[str, list[str]]:
    """'mosquitto -v -c "a b.conf"' → ('mosquitto.exe', ['-v', '-c', 'a b.conf']).

    Sem a semântica POSIX de barra invertida (caminhos do Windows passam intactos)."""
    partes = [p[1:-1] if len(p) >= 2 and p[0] == p[-1] == '"' else p
              for p in shlex.split(comando.strip(), posix=False)] if comando.strip() else []
    if not partes:
        return "", []
    programa, args = partes[0], partes[1:]
    if not os.path.splitext(programa)[1]:
        programa += ".exe"
    return programa, args


def caminho_programa(pasta: str, programa: str) -> str:
    if os.path.isabs(programa) or not pasta:
        return programa
    sep = "\\" if ("\\" in pasta and "/" not in pasta) else os.sep
    return pasta.rstrip("\\/") + sep + programa


def normalizar_pasta(texto: str) -> str:
    """Texto do campo → pasta: tira espaços e aspas; se for o caminho de um arquivo .exe (ex.: o próprio
    mosquitto.exe), devolve a pasta dele. Barras finais saem (menos na raiz, como C:\\)."""
    t = str(texto or "").strip().strip('"').strip()
    if not t:
        return ""
    if t.lower().endswith(".exe"):
        corte = max(t.rfind("\\"), t.rfind("/"))
        t = t[:corte] if corte > 0 else t
        if t.endswith(":"):
            t += "\\"
    if len(t) > 1 and t[-1] in "\\/" and not t.endswith(":\\") and not t.endswith(":/"):
        t = t.rstrip("\\/") or t[0]
    return t


def codificacao_local() -> str:
    """Codificação de console do Windows (OEM, ex.: cp850) ou a do sistema em outros SOs."""
    if sys.platform == "win32":
        try:
            import ctypes

            return f"cp{ctypes.windll.kernel32.GetOEMCP()}"
        except Exception:  # noqa: BLE001
            pass
    try:
        return locale.getencoding()
    except AttributeError:  # Python < 3.11
        return locale.getpreferredencoding(False)


def decodificar(dados: bytes, alternativa: str | None = None) -> str:
    """UTF-8; se não for UTF-8 válido, a codificação local do Windows; em último caso UTF-8 com substituição."""
    try:
        return dados.decode("utf-8")
    except UnicodeDecodeError:
        pass
    try:
        return dados.decode(alternativa or codificacao_local(), errors="replace")
    except LookupError:
        return dados.decode("utf-8", errors="replace")


def interpretador_padrao() -> tuple[str, list[str]]:
    """Como rodar o comando extra: `cmd.exe /c <comando>` no Windows; `/bin/sh -c` fora dele (testes)."""
    if sys.platform == "win32":
        return os.environ.get("COMSPEC") or "cmd.exe", ["/c"]
    return "/bin/sh", ["-c"]


class _Leitor:
    """Junta os pedaços lidos do processo em linhas inteiras (um caractere UTF-8 pode vir partido).

    Decodifica LINHA A LINHA. Tópicos com caracteres fora do ASCII (o `Teste_Alumbra` publica em
    `NOÏUS/Teste_Alumbra/ack/command`) precisam chegar íntegros à aba Clientes, que compara o texto do log com os
    tópicos assinados e oferece "Usar como…" com esse texto.

    Por que costuma dar certo: o MQTT exige tópicos em UTF-8 (o Mosquitto desconecta quem manda outra coisa) e o
    `mosquitto -v` escreve o tópico com os bytes que recebeu; pelo QProcess não há console no meio convertendo a
    página de código, então chegam aqui os mesmos bytes UTF-8 do tópico e `decodificar` devolve o texto exato.

    Risco: se uma linha não for UTF-8 válido (mensagem do Windows/console em cp850 ou cp1252, ou a saída de outro
    programa), ela é decodificada pela codificação do console. Antes isso era feito no pedaço inteiro lido e
    estragava também as linhas vizinhas (o "Ï" UTF-8 virava "├Å" em cp850); por linha, só a linha estranha é
    afetada. Mesmo assim, o texto mostrado no log pode não bater byte a byte com o tópico real em casos raros
    (linha com UTF-8 e cp850 misturados, caracteres invisíveis ou parecidos, como Ï × I). Como o usuário confere:
    no diálogo Tópicos…, a dica do tópico listado mostra os caracteres fora do ASCII com o código (ex.: "Ï (U+00CF)");
    depois de salvar, o indicador fica "ouvido" e o ⚠ some quando o aparelho publicar de novo nesse tópico. Também
    dá para testar na aba Mosquitto com `mosquitto_sub -t "<tópico>" -v`.
    """

    def __init__(self, alternativa: str | None = None) -> None:
        self._resto = b""
        self._alternativa = alternativa

    def _decodificar(self, dados: bytes) -> str:
        return "\n".join(decodificar(linha, self._alternativa) for linha in dados.split(b"\n")).replace("\r", "")

    def ler(self, dados: bytes) -> str | None:
        dados = self._resto + dados
        corte = dados.rfind(b"\n")
        if corte < 0:
            self._resto = dados
            return None
        self._resto = dados[corte + 1:]
        return self._decodificar(dados[:corte])

    def final(self) -> str | None:
        resto, self._resto = self._resto, b""
        return self._decodificar(resto) if resto else None


# ---------------------------------------------------------------------- lógica
class ConsoleMosquitto(QObject):
    """Processo do Mosquitto + um comando extra por vez, sem widgets.

    Sinais:
      saida(tipo, texto): tipo "prompt" | "saida" | "info" | "ok" | "aviso" | "erro"; texto pode ter várias linhas.
      estado_alterado(estado, texto): estado "parado" | "iniciando" | "rodando" | "parando" | "erro";
          texto pronto para o chip: "Rodando (PID 1234)", "Parado", "Erro: <motivo>"…
      comando_alterado(rodando): o comando extra começou (True) ou terminou (False).
      pasta_alterada(pasta): a pasta usada (campo "Endereço Mosquitto") mudou.
    """

    saida = Signal(str, str)
    estado_alterado = Signal(str, str)
    comando_alterado = Signal(bool)
    pasta_alterada = Signal(str)

    def __init__(self, config: MosquittoConfig, parent: QObject | None = None,
                 fabrica_processo: FabricaProcesso | None = None,
                 interpretador: tuple[str, list[str]] | None = None):
        super().__init__(parent)
        self.config = config
        self._pasta: str | None = None  # None = a de config.pasta
        self._fabrica = fabrica_processo or (lambda pai: QProcess(pai))
        self._interpretador = interpretador or interpretador_padrao()
        self._proc: QProcess | None = None
        self._extra: QProcess | None = None
        self._leitor = _Leitor()
        self._leitor_extra = _Leitor()
        self._parando = False
        self._parando_extra = False
        self._encerrado = False
        self.estado = "parado"
        self.texto_estado = "Parado"
        self.codigo_saida: int | None = None

    # ----- propriedades -----
    @property
    def pasta_padrao(self) -> str:
        return str(self.config.pasta or "")

    @property
    def pasta(self) -> str:
        return self.pasta_padrao if self._pasta is None else self._pasta

    def definir_pasta(self, pasta: str | None) -> None:
        """Troca a pasta usada no próximo Iniciar (None = volta ao `[mosquitto] pasta`). Não mexe no que roda."""
        anterior = self.pasta
        self._pasta = None if pasta is None else str(pasta)
        if self.pasta != anterior:
            self.pasta_alterada.emit(self.pasta)

    def validar_pasta(self, pasta: str | None = None) -> str | None:
        """Mensagem de erro se a pasta não tem o programa do comando (mosquitto.exe); None se está tudo certo."""
        pasta = self.pasta if pasta is None else pasta
        nome = separar_comando(self.comando)[0] or "mosquitto.exe"
        if not pasta:
            return f"Informe a pasta onde está o {nome}."
        if not os.path.isdir(pasta):
            return f"Pasta não encontrada: {pasta}"
        if not os.path.isfile(caminho_programa(pasta, nome)):
            return f"{nome} não encontrado em {pasta}"
        return None

    @property
    def comando(self) -> str:
        return str(self.config.comando or "").strip()

    def prompt(self, comando: str | None = None) -> str:
        return f"{self.pasta}>{self.comando if comando is None else comando}"

    @property
    def programa(self) -> str:
        return caminho_programa(self.pasta, separar_comando(self.comando)[0])

    @property
    def rodando(self) -> bool:
        return self._proc is not None and self._proc.state() != QProcess.ProcessState.NotRunning

    @property
    def comando_rodando(self) -> bool:
        return self._extra is not None and self._extra.state() != QProcess.ProcessState.NotRunning

    @property
    def pid(self) -> int | None:
        return int(self._proc.processId()) or None if self.rodando else None

    # ----- Mosquitto -----
    def iniciar_ao_abrir(self) -> bool:
        """Chamado ao abrir o programa: inicia se `[mosquitto] iniciar_automaticamente` estiver ligado."""
        if not getattr(self.config, "iniciar_automaticamente", False):
            log.info("Mosquitto: início automático desligado ([mosquitto] iniciar_automaticamente = false).")
            return False
        return self.iniciar()

    def iniciar(self) -> bool:
        if self._encerrado or self.rodando:
            return False
        nome, args = separar_comando(self.comando)
        self.saida.emit("prompt", self.prompt())
        if not nome:
            self._erro("comando vazio", "Comando vazio — confira [mosquitto] comando no config.toml.")
            return False
        programa = caminho_programa(self.pasta, nome)
        if not os.path.isfile(programa):
            self._erro(f"{nome} não encontrado",
                       f"{nome} não encontrado em {self.pasta} — confira o Endereço Mosquitto (botão …).")
            return False
        proc = self._fabrica(self)
        proc.setProcessChannelMode(QProcess.ProcessChannelMode.MergedChannels)  # mosquitto -v escreve no stderr
        proc.setWorkingDirectory(self.pasta)
        proc.setProgram(programa)
        proc.setArguments(args)
        proc.readyReadStandardOutput.connect(lambda p=proc: self._ler(p, self._leitor))
        proc.started.connect(lambda p=proc: self._iniciado(p))
        proc.finished.connect(lambda codigo, status, p=proc: self._terminou(p, codigo, status))
        proc.errorOccurred.connect(lambda erro, p=proc: self._falhou(p, erro))
        self._proc = proc
        self._parando = False
        self._leitor = _Leitor()
        self.codigo_saida = None
        self._definir_estado("iniciando", "Iniciando…")
        log.info("Mosquitto: iniciando %s %s (pasta %s)", programa, " ".join(args), self.pasta)
        proc.start()
        return True

    def parar(self) -> None:
        """Pede para o Mosquitto parar sem bloquear: terminate e, se em 3 s não saiu, kill."""
        proc = self._proc
        if proc is None or not self.rodando:
            return
        self._parando = True
        self._definir_estado("parando", "Parando…")
        proc.terminate()
        QTimer.singleShot(ESPERA_TERMINAR_MS, self, lambda p=proc: self._matar_se_vivo(p))

    def _iniciado(self, proc: QProcess) -> None:
        if proc is not self._proc:
            return
        pid = int(proc.processId())
        log.info("Mosquitto: rodando (PID %s).", pid)
        self._definir_estado("rodando", f"Rodando (PID {pid})")

    def _terminou(self, proc: QProcess, codigo: int, status: QProcess.ExitStatus) -> None:
        self._ler(proc, self._leitor)
        resto = self._leitor.final()
        if resto and not self._encerrado:
            self.saida.emit("saida", resto)
        if proc is not self._proc:
            return
        self._proc = None
        proc.deleteLater()
        self.codigo_saida = int(codigo)
        if self._encerrado:
            return
        if self._parando:
            self._parando = False
            log.info("Mosquitto: parado (código %s).", codigo)
            self.saida.emit("info", f"Mosquitto parado (código de saída {codigo}).")
            self._definir_estado("parado", "Parado")
            return
        travou = status == QProcess.ExitStatus.CrashExit
        motivo = "encerrou inesperadamente" if travou else f"encerrou com código {codigo}"
        log.warning("Mosquitto: %s. %s.", motivo, DICA_SERVICO)
        self.saida.emit("erro", f"O Mosquitto {motivo}"
                                f"{'' if not travou else f' (código {codigo})'}. Ele não será reiniciado sozinho.")
        self.saida.emit("aviso", f"Dica: a porta pode estar em uso. {DICA_SERVICO}.")
        self._definir_estado("erro", f"Erro: {motivo}")

    def _falhou(self, proc: QProcess, erro: QProcess.ProcessError) -> None:
        # Só a falha ao iniciar importa aqui: nos outros casos o `finished` chega em seguida.
        if erro != QProcess.ProcessError.FailedToStart or proc is not self._proc:
            return
        self._proc = None
        proc.deleteLater()
        if self._encerrado:
            return
        self._erro("não iniciou", f"Não foi possível iniciar {self.programa}: {proc.errorString()}")

    def _erro(self, motivo: str, mensagem: str) -> None:
        log.error("Mosquitto: %s", mensagem)
        self.saida.emit("erro", mensagem)
        self._definir_estado("erro", f"Erro: {motivo}")

    def _definir_estado(self, estado: str, texto: str) -> None:
        self.estado = estado
        self.texto_estado = texto
        self.estado_alterado.emit(estado, texto)

    # ----- comando extra -----
    def executar(self, comando: str) -> bool:
        """Roda `cmd.exe /c <comando>` na pasta do Mosquitto, com a saída no mesmo console. Um por vez."""
        comando = comando.strip()
        if not comando or self._encerrado:
            return False
        if self.comando_rodando:
            self.saida.emit("aviso", "Já há um comando rodando. Pare-o (botão Parar ou Ctrl+C) antes de outro.")
            return False
        self.saida.emit("prompt", self.prompt(comando))
        if not os.path.isdir(self.pasta):
            self.saida.emit("erro", f"Pasta não encontrada: {self.pasta} — confira o Endereço Mosquitto.")
            return False
        programa, prefixo = self._interpretador
        proc = self._fabrica(self)
        proc.setProcessChannelMode(QProcess.ProcessChannelMode.MergedChannels)
        proc.setWorkingDirectory(self.pasta)
        proc.setProgram(programa)
        if sys.platform == "win32" and hasattr(proc, "setNativeArguments"):
            # o cmd.exe entende a linha como o usuário digitou (aspas inclusive); sem o escape do QProcess
            proc.setNativeArguments(" ".join([*prefixo, comando]))
        else:
            proc.setArguments([*prefixo, comando])
        proc.readyReadStandardOutput.connect(lambda p=proc: self._ler(p, self._leitor_extra))
        proc.started.connect(lambda p=proc: p.closeWriteChannel())  # sem entrada: quem pedir lê EOF
        proc.finished.connect(lambda codigo, status, p=proc: self._extra_terminou(p, codigo, status))
        proc.errorOccurred.connect(lambda erro, p=proc: self._extra_falhou(p, erro))
        self._extra = proc
        self._parando_extra = False
        self._leitor_extra = _Leitor()
        log.info("Mosquitto: comando extra: %s", comando)
        proc.start()
        self.comando_alterado.emit(True)
        return True

    def parar_comando(self) -> None:
        proc = self._extra
        if proc is None or not self.comando_rodando:
            return
        self._parando_extra = True
        self.saida.emit("info", "^C")
        proc.terminate()
        QTimer.singleShot(ESPERA_TERMINAR_MS, self, lambda p=proc: self._matar_se_vivo(p, arvore=True))

    def _extra_terminou(self, proc: QProcess, codigo: int, status: QProcess.ExitStatus) -> None:
        self._ler(proc, self._leitor_extra)
        resto = self._leitor_extra.final()
        if resto and not self._encerrado:
            self.saida.emit("saida", resto)
        if proc is not self._extra:
            return
        self._extra = None
        proc.deleteLater()
        if self._encerrado:
            return
        if self._parando_extra:
            self.saida.emit("info", "Comando interrompido.")
        else:
            self.saida.emit("info" if codigo == 0 else "aviso", f"Comando terminou (código de saída {codigo}).")
        self.comando_alterado.emit(False)

    def _extra_falhou(self, proc: QProcess, erro: QProcess.ProcessError) -> None:
        if erro != QProcess.ProcessError.FailedToStart or proc is not self._extra:
            return
        self._extra = None
        proc.deleteLater()
        if self._encerrado:
            return
        self.saida.emit("erro", f"Não foi possível rodar o comando: {proc.errorString()}")
        self.comando_alterado.emit(False)

    # ----- leitura e encerramento -----
    def _ler(self, proc: QProcess, leitor: _Leitor) -> None:
        try:
            dados = bytes(proc.readAllStandardOutput().data())
        except RuntimeError:  # processo já destruído
            return
        if not dados:
            return
        texto = leitor.ler(dados)
        if texto is not None and not self._encerrado:
            self.saida.emit("saida", texto)

    @staticmethod
    def _matar_se_vivo(proc: QProcess, arvore: bool = False) -> None:
        try:
            vivo = proc.state() != QProcess.ProcessState.NotRunning
        except RuntimeError:  # já apagado (deleteLater)
            return
        if vivo:
            _matar(proc, arvore)

    def encerrar(self, timeout: float = ESPERA_TERMINAR_MS / 1000) -> None:
        """Ao fechar o programa ou no reinício do vigia: terminate em todos, espera até `timeout` s, depois kill.

        Bloqueia no máximo ~timeout s. Depois disso o console não emite mais nada."""
        self._encerrado = True
        vivos = [(p, p is self._extra) for p in (self._proc, self._extra)
                 if p is not None and p.state() != QProcess.ProcessState.NotRunning]
        if not vivos:
            return
        log.info("Mosquitto: encerrando %d processo(s) filho(s).", len(vivos))
        for proc, _ in vivos:
            proc.terminate()
        limite = time.monotonic() + max(0.0, timeout)
        for proc, _ in vivos:
            restante = int(max(0.0, limite - time.monotonic()) * 1000)
            proc.waitForFinished(max(1, restante))
        for proc, arvore in vivos:
            if proc.state() != QProcess.ProcessState.NotRunning:
                log.warning("Mosquitto: PID %s não saiu em %.0f s; forçando (kill).", proc.processId(), timeout)
                _matar(proc, arvore)
                proc.waitForFinished(1000)


def _matar(proc: QProcess, arvore: bool) -> None:
    """kill; no Windows o comando extra roda dentro do cmd.exe, então mata a árvore (cmd + filho)."""
    if arvore and sys.platform == "win32":
        pid = int(proc.processId())
        if pid:
            try:
                subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, timeout=3,
                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            except (OSError, subprocess.SubprocessError):
                pass
    proc.kill()


# ---------------------------------------------------------------------- tela
def escolher_pasta(parent: QWidget | None, inicial: str) -> str:
    """Diálogo "Procurar" do Endereço Mosquitto. Devolve a pasta (ou o caminho do mosquitto.exe) ou "" se cancelou.

    Usa o diálogo do Qt (não o nativo) para mostrar também os arquivos: assim o usuário pode clicar no próprio
    mosquitto.exe ou só entrar na pasta e clicar em "Escolher". O diálogo nativo de pasta do Windows esconde os
    arquivos, e o de arquivo não aceita pasta."""
    dialogo = _DialogoPasta(parent, inicial)
    if dialogo.exec() != _DialogoPasta.DialogCode.Accepted:
        return ""
    return dialogo.escolhido


class _DialogoPasta(QFileDialog):
    """QFileDialog em modo pasta que também aceita um arquivo selecionado (o mosquitto.exe)."""

    def __init__(self, parent: QWidget | None, inicial: str):
        super().__init__(parent, "Endereço Mosquitto — escolha a pasta ou o mosquitto.exe",
                         inicial if inicial and os.path.isdir(inicial) else "")
        self.setOption(QFileDialog.Option.DontUseNativeDialog, True)
        self.setFileMode(QFileDialog.FileMode.Directory)
        self.setOption(QFileDialog.Option.ShowDirsOnly, False)
        self.setNameFilter("mosquitto.exe (mosquitto.exe);;Programas (*.exe);;Todos os arquivos (*)")
        self.setLabelText(QFileDialog.DialogLabel.Accept, "Escolher")
        self.setLabelText(QFileDialog.DialogLabel.Reject, "Cancelar")
        self.escolhido = ""

    def accept(self) -> None:  # noqa: D401 - API do Qt
        selecionados = self.selectedFiles()
        alvo = selecionados[0] if selecionados else self.directory().absolutePath()
        if os.path.isfile(alvo):  # clicou no mosquitto.exe: aceita (a pasta é tirada depois)
            self.escolhido = alvo
            self.done(QFileDialog.DialogCode.Accepted)
            return
        self.escolhido = alvo
        super().accept()


class PainelMosquitto(QWidget):
    """Conteúdo da aba Mosquitto: barra (estado, Iniciar, Parar, Limpar), Endereço Mosquitto, console e Comando.

    `settings` (QSettings da janela): de onde vem e para onde vai a pasta escolhida (`CHAVE_PASTA`). Sem ele, o
    campo funciona mas nada é salvo. `escolher` (injetável nos testes): diálogo de "…"."""

    def __init__(self, console: ConsoleMosquitto, parent: QWidget | None = None,
                 settings: QSettings | None = None,
                 escolher: Callable[[QWidget | None, str], str] | None = None):
        super().__init__(parent)
        self.setObjectName("paginaAba")
        self.console = console
        self.settings = settings
        self._escolher = escolher
        salva = str(settings.value(CHAVE_PASTA, "") or "").strip() if settings is not None else ""
        if salva:
            console.definir_pasta(salva)  # QSettings vale mais que o config.toml (também no reinício do vigia)
        corpo = QVBoxLayout(self)
        corpo.setContentsMargins(0, 12, 0, 0)
        corpo.setSpacing(12)

        barra = QHBoxLayout()
        barra.setSpacing(8)
        self.chip_estado = ChipInfo("mosquitto", "Estado do Mosquitto", self)
        self.chip_estado.setToolTip(f"Processo {console.prompt()}")
        self.chip_pausa = QPushButton("Rolagem pausada · ir para o fim ↓", self)
        self.chip_pausa.setObjectName("chipPausa")
        self.chip_pausa.setCursor(Qt.CursorShape.PointingHandCursor)
        self.chip_pausa.setToolTip("Novas linhas continuam chegando. Clique para voltar ao fim.")
        self.chip_pausa.hide()
        self.botao_iniciar = self._botao("Iniciar", "")
        self.botao_parar = self._botao("Parar", "Encerrar o Mosquitto")
        self.botao_limpar = self._botao("Limpar", "Apagar as linhas do console (o processo continua)")
        barra.addWidget(self.chip_estado)
        barra.addSpacing(8)
        barra.addWidget(self.chip_pausa)
        barra.addStretch(1)
        barra.addWidget(self.botao_iniciar)
        barra.addWidget(self.botao_parar)
        barra.addWidget(self.botao_limpar)

        # Endereço Mosquitto: pasta do mosquitto.exe, editável só com o processo parado.
        endereco = QHBoxLayout()
        endereco.setSpacing(8)
        self.rotulo_endereco = QLabel("ENDEREÇO MOSQUITTO", self)
        self.rotulo_endereco.setObjectName("rotuloCampo")
        self.campo_endereco = QLineEdit(self)
        self.campo_endereco.setObjectName("campoEndereco")
        self.campo_endereco.setFont(fonte_mono())
        self.campo_endereco.setPlaceholderText(r"ex.: C:\Program Files\mosquitto")
        self.campo_endereco.setAccessibleName("Endereço Mosquitto")
        self.campo_endereco.setClearButtonEnabled(False)
        self.rotulo_endereco.setBuddy(self.campo_endereco)
        self.botao_procurar = QPushButton("…", self)
        self.botao_procurar.setObjectName("botaoProcurar")
        self.botao_procurar.setAccessibleName("Procurar pasta do Mosquitto")
        self.botao_procurar.setCursor(Qt.CursorShape.PointingHandCursor)
        self.botao_procurar.setFixedWidth(40)
        self.botao_restaurar = QPushButton("Restaurar padrão", self)
        self.botao_restaurar.setObjectName("botaoLink")
        self.botao_restaurar.setCursor(Qt.CursorShape.PointingHandCursor)
        self.erro_endereco = QLabel(self)
        self.erro_endereco.setObjectName("erroValidacao")
        self.erro_endereco.setWordWrap(True)
        self.erro_endereco.hide()
        endereco.addWidget(self.campo_endereco, 1)
        endereco.addWidget(self.botao_procurar)
        endereco.addWidget(self.botao_restaurar)
        bloco_endereco = QVBoxLayout()  # rótulo em cima (como os campos da coluna esquerda): o caminho cabe inteiro
        bloco_endereco.setSpacing(4)
        bloco_endereco.addWidget(self.rotulo_endereco)
        bloco_endereco.addLayout(endereco)

        self.caixa = CaixaConsole(parent=self)
        self.caixa.seguindo_alterado.connect(lambda seguir: self.chip_pausa.setVisible(not seguir))
        self.chip_pausa.clicked.connect(self.caixa.ir_para_fim)

        linha = QHBoxLayout()
        linha.setSpacing(8)
        self.rotulo_comando = QLabel("COMANDO", self)
        self.rotulo_comando.setObjectName("rotuloCampo")
        self.campo_comando = QLineEdit(self)
        self.campo_comando.setObjectName("campoComando")
        self.campo_comando.setFont(fonte_mono())
        self.campo_comando.setPlaceholderText('ex.: mosquitto_sub -h localhost -t "#" -v')

        self.campo_comando.setAccessibleName("Comando")
        self.rotulo_comando.setBuddy(self.campo_comando)
        self.botao_executar = QPushButton("Executar", self)
        self.botao_executar.setCursor(Qt.CursorShape.PointingHandCursor)
        self.botao_executar.setMinimumWidth(96)
        linha.addWidget(self.rotulo_comando)
        linha.addWidget(self.campo_comando, 1)
        linha.addWidget(self.botao_executar)

        corpo.addLayout(barra)
        corpo.addLayout(bloco_endereco)
        bloco_endereco.addWidget(self.erro_endereco)
        corpo.addWidget(self.caixa, 1)
        corpo.addLayout(linha)

        self.botao_iniciar.clicked.connect(self.iniciar)
        self.botao_procurar.clicked.connect(self.procurar)
        self.botao_restaurar.clicked.connect(self.restaurar_padrao)
        self.campo_endereco.returnPressed.connect(self.confirmar_endereco)
        self.campo_endereco.textEdited.connect(self._endereco_editado)
        console.pasta_alterada.connect(self._ao_pasta)
        self.botao_parar.clicked.connect(console.parar)
        self.botao_limpar.clicked.connect(self.caixa.limpar)
        self.botao_executar.clicked.connect(self._executar_ou_parar)
        self.campo_comando.returnPressed.connect(self._executar)
        self.campo_comando.installEventFilter(self)
        console.saida.connect(self.caixa.adicionar)
        console.estado_alterado.connect(self._ao_estado)
        console.comando_alterado.connect(self._ao_comando)
        self.campo_endereco.setText(console.pasta)
        self._ao_pasta(console.pasta)
        self._ao_estado(console.estado, console.texto_estado)
        self._ao_comando(console.comando_rodando)

    def _botao(self, texto: str, dica: str) -> QPushButton:
        b = QPushButton(texto, self)
        b.setObjectName("botaoDiscreto")
        b.setToolTip(dica)
        b.setCursor(Qt.CursorShape.PointingHandCursor)
        return b

    def _ao_estado(self, estado: str, texto: str) -> None:
        visual = {"rodando": "ok", "erro": "erro", "iniciando": "ocupado", "parando": "ocupado"}.get(estado, "neutro")
        self.chip_estado.definir("Mosquitto", texto, visual)
        ativo = estado in ("iniciando", "rodando", "parando")
        self.botao_iniciar.setEnabled(not ativo)
        self.botao_parar.setEnabled(estado in ("iniciando", "rodando"))
        # Endereço: só com o Mosquitto parado (o processo que roda não muda de pasta).
        self.campo_endereco.setReadOnly(ativo)
        self.botao_procurar.setEnabled(not ativo)
        self.botao_restaurar.setEnabled(not ativo)
        if ativo:
            self.campo_endereco.setText(self.console.pasta)  # mostra a pasta em uso, não um rascunho
            self._limpar_erro_endereco()
            self.campo_endereco.setToolTip(DICA_PARE)
            self.botao_procurar.setToolTip(DICA_PARE)
            self.botao_restaurar.setToolTip(DICA_PARE)
        else:
            self.campo_endereco.setToolTip(
                "Pasta onde está o mosquitto.exe. Enter confirma; o botão … procura a pasta (ou o próprio "
                "mosquitto.exe). Fica salvo e vale mais que o config.toml.")
            self.botao_procurar.setToolTip("Procurar a pasta do Mosquitto (ou o mosquitto.exe)")
            self.botao_restaurar.setToolTip(
                f"Voltar ao padrão do config.toml: {self.console.pasta_padrao or '(vazio)'}")
        repolir(self.campo_endereco)

    # ----- Endereço Mosquitto -----
    def _ao_pasta(self, pasta: str) -> None:
        prompt = self.console.prompt()
        self.chip_estado.setToolTip(f"Processo {prompt}")
        self.botao_iniciar.setToolTip(f"Rodar {self.console.comando} em {pasta}")
        self.caixa.setPlaceholderText(f"O Mosquitto não está rodando. Clique em Iniciar para executar {prompt}")
        self.campo_comando.setToolTip(
            f"Roda outro comando em {pasta} (cmd.exe /c), com a saída neste console. "
            "Enter executa; Ctrl+C ou Parar encerra.")

    def _endereco_editado(self, _texto: str = "") -> None:
        self._limpar_erro_endereco()

    def _limpar_erro_endereco(self) -> None:
        self.erro_endereco.hide()
        if self.campo_endereco.property("invalido"):
            self.campo_endereco.setProperty("invalido", False)
            repolir(self.campo_endereco)

    def _mostrar_erro_endereco(self, mensagem: str) -> None:
        self.campo_endereco.setProperty("invalido", True)
        repolir(self.campo_endereco)
        self.erro_endereco.setText(mensagem)
        self.erro_endereco.show()

    def _salvar_pasta(self, pasta: str) -> None:
        if self.settings is not None:
            self.settings.setValue(CHAVE_PASTA, pasta)
            self.settings.sync()

    def _aplicar_endereco(self, texto: str, no_console: bool) -> str | None:
        """Normaliza (exe → pasta), valida e, se ok, passa ao console. Devolve a pasta ou None se inválida."""
        pasta = normalizar_pasta(texto)
        self.campo_endereco.setText(pasta)
        erro = self.console.validar_pasta(pasta)
        if erro:
            mensagem = f"{erro}. Use o botão … para escolher a pasta do mosquitto.exe."
            self._mostrar_erro_endereco(mensagem)
            if no_console:
                self.caixa.adicionar("prompt", f"{pasta}>{self.console.comando}")
                self.caixa.adicionar("erro", mensagem)
                self.caixa.ir_para_fim()
            log.warning("Mosquitto: endereço inválido: %s", erro)
            return None
        self._limpar_erro_endereco()
        self.console.definir_pasta(pasta)
        return pasta

    def confirmar_endereco(self) -> bool:
        """Enter no campo: valida e salva (sem iniciar)."""
        if self.console.rodando or self.campo_endereco.isReadOnly():
            return False
        pasta = self._aplicar_endereco(self.campo_endereco.text(), no_console=False)
        if pasta is None:
            return False
        self._salvar_pasta(pasta)
        return True

    def procurar(self) -> bool:
        """Botão …: escolhe a pasta (ou o mosquitto.exe, e usa a pasta dele), valida e salva."""
        if self.console.rodando or self.campo_endereco.isReadOnly():
            return False
        escolher = self._escolher or escolher_pasta
        caminho = escolher(self, normalizar_pasta(self.campo_endereco.text()) or self.console.pasta)
        if not caminho:
            return False
        pasta = self._aplicar_endereco(caminho, no_console=False)
        if pasta is None:
            return False
        self._salvar_pasta(pasta)
        return True

    def restaurar_padrao(self) -> None:
        """Volta ao `[mosquitto] pasta` do config.toml e apaga o valor salvo."""
        if self.console.rodando or self.campo_endereco.isReadOnly():
            return
        self.console.definir_pasta(None)
        self.campo_endereco.setText(self.console.pasta)
        self._limpar_erro_endereco()
        if self.settings is not None:
            self.settings.remove(CHAVE_PASTA)
            self.settings.sync()

    def iniciar(self) -> bool:
        """Botão Iniciar: usa a pasta do campo; pasta sem mosquitto.exe não inicia (erro no console e no campo)."""
        if self.console.rodando:
            return False
        pasta = self._aplicar_endereco(self.campo_endereco.text(), no_console=True)
        if pasta is None:
            return False
        ok = self.console.iniciar()
        if ok:
            self._salvar_pasta(pasta)
            self.caixa.ir_para_fim()
        return ok

    def _ao_comando(self, rodando: bool) -> None:
        self.botao_executar.setText("Parar" if rodando else "Executar")
        self.botao_executar.setToolTip("Encerrar o comando em execução (Ctrl+C)" if rodando
                                       else "Executar o comando (Enter)")

    def _executar(self) -> None:
        if self.console.executar(self.campo_comando.text()):
            self.caixa.ir_para_fim()

    def _executar_ou_parar(self) -> None:
        if self.console.comando_rodando:
            self.console.parar_comando()
        else:
            self._executar()

    def eventFilter(self, obj, evento) -> bool:  # noqa: N802 - API do Qt
        # Ctrl+C no campo: com texto selecionado copia (normal); sem seleção e com comando rodando, interrompe.
        if (obj is self.campo_comando and evento.type() == QEvent.Type.KeyPress
                and evento.matches(QKeySequence.StandardKey.Copy)
                and not self.campo_comando.hasSelectedText() and self.console.comando_rodando):
            self.console.parar_comando()
            return True
        return super().eventFilter(obj, evento)
