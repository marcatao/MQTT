"""Vigia (watchdog) e reinício automático do programa.

A lógica não depende de timers nem do relógio real: quem usa injeta `relogio` (epoch em segundos) e chama
`verificar()` periodicamente; o reinício recebe as ações por parâmetro. Assim os testes rodam com relógio falso,
sem esperar minutos de verdade.

Regra (CLAUDE.md, "Operação contínua"): enquanto o usuário quer estar conectado, reinicia se
    agora − max(início da conexão, última leitura) > N minutos, ou
    agora − max(início da conexão, última gravação ok) > N minutos (só com o SQL configurado).
"""
from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

ARG_AUTO = "--auto-conectar"
TEMPO_ENCERRAR_REINICIO_S = 8.0

log = logging.getLogger("app.vigia")


def epoch_de_iso(texto: str, padrao: float | None = None) -> float | None:
    """'AAAA-MM-DDTHH:MM:SS' (hora local) → epoch. Texto inválido devolve `padrao`."""
    try:
        return datetime.fromisoformat(str(texto).strip()).timestamp()
    except (TypeError, ValueError):
        return padrao


def minutos_txt(minutos: float) -> str:
    """60 → "1 h"; 90 → "1 h 30 min"; 10 → "10 min"; 0,5 → "0.5 min"."""
    if minutos >= 60 and float(minutos).is_integer():
        h, m = divmod(int(minutos), 60)
        return f"{h} h" + (f" {m} min" if m else "")
    return f"{minutos:g} min"


_minutos_txt = minutos_txt  # nome antigo


class Vigia:
    """Decide quando reiniciar. Não reinicia nada: chama `ao_disparar(motivo)` uma única vez."""

    def __init__(self, minutos: float, ao_disparar: Callable[[str], None],
                 sql_configurado: Callable[[], bool] = lambda: False,
                 relogio: Callable[[], float] = time.time):
        try:
            self.minutos = max(0.0, float(minutos))
        except (TypeError, ValueError):
            self.minutos = 0.0
        self._ao_disparar = ao_disparar
        self._sql_configurado = sql_configurado
        self._relogio = relogio
        self.inicio: float | None = None
        self.ultima_leitura: float | None = None
        self.ultima_gravacao: float | None = None
        self.disparou = False

    @property
    def ligado(self) -> bool:
        return self.minutos > 0

    @property
    def ativo(self) -> bool:
        """Vigiando agora: ligado na config e o usuário quer estar conectado."""
        return self.ligado and self.inicio is not None and not self.disparou

    def iniciar(self) -> None:
        """Usuário clicou Conectar (ou --auto-conectar): a contagem começa agora."""
        self.inicio = self._relogio()
        self.disparou = False

    def parar(self) -> None:
        """Usuário clicou Desconectar (ou fechou a janela)."""
        self.inicio = None

    def registrar_leitura(self, epoch: float | None = None) -> None:
        self.ultima_leitura = self._relogio() if epoch is None else float(epoch)

    def registrar_gravacao(self, epoch: float | None = None) -> None:
        self.ultima_gravacao = self._relogio() if epoch is None else float(epoch)

    @property
    def sql_configurado(self) -> bool:
        return bool(self._sql_configurado())

    def restante(self) -> float | None:
        """Segundos até o reinício automático (o menor dos prazos que valem agora); None se não está vigiando.

        Mesma regra de `motivo()`: o prazo da leitura sempre vale; o da gravação só com o SQL configurado."""
        if not self.ativo:
            return None
        limite = self.minutos * 60.0
        prazo = max(self.inicio, self.ultima_leitura or 0.0) + limite
        if self._sql_configurado():
            prazo = min(prazo, max(self.inicio, self.ultima_gravacao or 0.0) + limite)
        return prazo - self._relogio()

    def motivo(self) -> str | None:
        """Motivo para reiniciar agora, ou None se está tudo bem."""
        if not self.ativo:
            return None
        agora = self._relogio()
        limite = self.minutos * 60.0
        base_leitura = max(self.inicio, self.ultima_leitura or 0.0)
        if agora - base_leitura > limite:
            return f"Sem leitura há {_minutos_txt(self.minutos)} — reiniciando o programa"
        if self._sql_configurado():
            base_gravacao = max(self.inicio, self.ultima_gravacao or 0.0)
            if agora - base_gravacao > limite:
                return f"Sem gravação no SQL Server há {_minutos_txt(self.minutos)} — reiniciando o programa"
        return None

    def verificar(self) -> str | None:
        """Chamado periodicamente (QTimer). Dispara no máximo uma vez por conexão."""
        m = self.motivo()
        if m:
            self.disparou = True
            self._ao_disparar(m)
        return m


# ---------------------------------------------------------------------- reinício
@dataclass
class Comando:
    programa: str
    argumentos: list[str] = field(default_factory=list)
    pasta: str = ""


def comando_reinicio(frozen: bool | None = None, executavel: str | None = None,
                     argv: list[str] | None = None, pasta: str | None = None) -> Comando:
    """Como abrir uma nova instância já reconectando.

    Empacotado (PyInstaller): o próprio .exe com os mesmos argumentos + --auto-conectar.
    Desenvolvimento: `python -m app.main --auto-conectar`, no mesmo diretório de trabalho.
    """
    frozen = bool(getattr(sys, "frozen", False)) if frozen is None else frozen
    executavel = executavel or sys.executable
    argv = list(sys.argv if argv is None else argv)
    extras = [a for a in argv[1:] if a != ARG_AUTO]
    pasta = pasta if pasta is not None else os.getcwd()
    if frozen:
        return Comando(executavel, [*extras, ARG_AUTO], pasta)
    return Comando(executavel, ["-m", "app.main", *extras, ARG_AUTO], pasta)


def iniciar_processo(cmd: Comando) -> bool:
    """Abre o processo desacoplado deste (sobrevive ao fim do atual)."""
    from PySide6.QtCore import QProcess

    resultado = QProcess.startDetached(cmd.programa, cmd.argumentos, cmd.pasta)
    return bool(resultado[0] if isinstance(resultado, tuple) else resultado)


def sair_do_app() -> None:
    from PySide6.QtWidgets import QApplication

    QApplication.quit()


def reiniciar_programa(
    motivo: str,
    *,
    registrar: Callable[[str, str], None],
    salvar: Callable[[], None],
    encerrar: Callable[..., None],
    iniciar: Callable[[Comando], bool] = iniciar_processo,
    sair: Callable[[], None] = sair_do_app,
    comando: Comando | None = None,
) -> bool:
    """Registra o motivo (tela + arquivo), salva os campos, encerra o pipeline, abre a nova instância e sai.

    `registrar(nivel, mensagem)` escreve na caixa de atividade; o log em arquivo é feito aqui.
    Se não conseguir abrir a nova instância, registra o erro e NÃO sai (melhor a janela aberta que nada).
    """
    registrar("ERRO", motivo)
    log.error("Vigia: %s", motivo)
    try:
        salvar()
    except Exception:  # noqa: BLE001 - salvar os campos não pode impedir o reinício
        log.exception("Vigia: falha ao salvar os campos antes de reiniciar")
    try:
        encerrar(timeout=TEMPO_ENCERRAR_REINICIO_S)
    except Exception:  # noqa: BLE001
        log.exception("Vigia: falha ao encerrar o pipeline antes de reiniciar")
    cmd = comando or comando_reinicio()
    log.info("Vigia: abrindo nova instância: %s %s (pasta %s)", cmd.programa, " ".join(cmd.argumentos), cmd.pasta)
    try:
        ok = iniciar(cmd)
    except Exception:  # noqa: BLE001
        log.exception("Vigia: falha ao abrir a nova instância")
        ok = False
    if not ok:
        msg = "Não foi possível reiniciar o programa sozinho. Feche e abra o programa de novo."
        registrar("ERRO", msg)
        log.error("Vigia: %s", msg)
        return False
    sair()
    return True
