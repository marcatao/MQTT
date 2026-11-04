"""Tópicos MQTT por dispositivo (NOVUS DigiRail IoT), guardados em ``dispositivos.json`` na pasta do programa.

Motivo: o NXperience de alguns aparelhos grava um tópico diferente do padrão ``NOVUS/{id}/...`` (caso real:
``NOÏUS/Teste_Alumbra/ack/command``, o V trocado por Ï, sem como corrigir no aparelho). Cada dispositivo tem 5
tópicos **exatos** (sem curingas):

- ``events``: dados e eventos (o aparelho publica);
- ``config``: configuração (o aparelho recebe);
- ``ack_config``: confirmação de configuração (o aparelho publica);
- ``command``: comandos (o integrador publica ``diag``/``reset_counters``);
- ``ack_command``: confirmação de comando (o aparelho publica a resposta).

Sem nada salvo, valem os padrões dos modelos de ``[dispositivos]`` (``{id}`` = device_id). O arquivo tem o formato
``{"Teste_Alumbra": {"events": "...", "config": "...", "ack_config": "...", "command": "...", "ack_command": "..."}}``,
em UTF-8 (``ensure_ascii=False``: ``Ï`` fica legível no arquivo) e é gravado de forma atômica (temporário +
``os.replace``). O texto de cada tópico é preservado exatamente; só os espaços das pontas são tirados.

Arquivo corrompido (JSON inválido ou formato inesperado) não derruba o app: vira um AVISO, uma cópia é guardada em
``<arquivo>.corrompido`` e valem os padrões até o próximo salvamento. Entradas inválidas são ignoradas com AVISO.

Sem Qt e sem rede. Thread-safe (uma trava protege o dicionário e o índice tópico → dispositivo).
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Callable

from app.config import DispositivosConfig
from app.core import digirail

CHAVES: tuple[str, ...] = ("events", "config", "ack_config", "command", "ack_command")
ACKS: tuple[str, ...] = ("ack_command", "ack_config")
PUBLICADOS_PELO_APARELHO: tuple[str, ...] = ("events", "ack_command", "ack_config")   # os que o integrador ouve

NOMES = {
    "events": "dados/eventos",
    "config": "configuração",
    "ack_config": "confirmação de configuração",
    "command": "comando",
    "ack_command": "confirmação de comando",
}

MAX_BYTES = 65535

Logar = Callable[[str, str], None]


def validar_topico(topico: object, nome: str = "tópico") -> str:
    """Tópico **exato** para PUBLISH/assinatura de um dispositivo. Devolve o texto sem espaços nas pontas.

    ValueError (mensagem em português) se vazio, com ``+``/``#`` (curingas não valem aqui), com caractere nulo ou
    com mais de 65535 bytes em UTF-8. Qualquer outro caractere (``Ï``, espaço interno...) é preservado.
    """
    if not isinstance(topico, str):
        raise ValueError(f"Tópico de {nome} inválido: precisa ser texto.")
    t = topico.strip()
    if not t:
        raise ValueError(f"Informe o tópico de {nome}.")
    if "\x00" in t:
        raise ValueError(f"Tópico de {nome} inválido: contém caractere nulo.")
    if "+" in t or "#" in t:
        raise ValueError(f"Tópico de {nome} inválido: {t!r} tem '+' ou '#'. Use o tópico exato do aparelho, "
                         "sem curingas.")
    if len(t.encode("utf-8")) > MAX_BYTES:
        raise ValueError(f"Tópico de {nome} longo demais (máximo {MAX_BYTES} bytes em UTF-8).")
    return t


def padroes(device_id: str, cfg: DispositivosConfig | None = None) -> dict[str, str]:
    """Os 5 tópicos padrão do device, pelos modelos de ``[dispositivos]``."""
    cfg = cfg or DispositivosConfig()
    padrao = DispositivosConfig()
    modelos = {
        "events": getattr(cfg, "topico_dados", None) or padrao.topico_dados,
        "config": getattr(cfg, "topico_config", None) or padrao.topico_config,
        "ack_config": getattr(cfg, "topico_ack_config", None) or padrao.topico_ack_config,
        "command": getattr(cfg, "topico_comando", None) or padrao.topico_comando,
        "ack_command": getattr(cfg, "topico_resposta", None) or padrao.topico_resposta,
    }
    return {chave: digirail.topico_do_modelo(str(modelo), device_id) for chave, modelo in modelos.items()}


def gravar_atomico(caminho: Path, dados: dict) -> None:
    """JSON UTF-8 legível (``ensure_ascii=False``) num temporário da mesma pasta e ``os.replace`` no destino."""
    caminho = Path(caminho)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    texto = json.dumps(dados, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    fd, tmp = tempfile.mkstemp(prefix=caminho.name + ".", suffix=".tmp", dir=str(caminho.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(texto)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, caminho)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class TopicosDispositivos:
    """Tópicos salvos por device + padrões. ``carregar`` é chamado no construtor (nunca levanta exceção)."""

    def __init__(self, caminho: Path, cfg: DispositivosConfig | None = None, logar: Logar | None = None) -> None:
        self.caminho = Path(caminho)
        self.cfg = cfg or DispositivosConfig()
        self._logar = logar or (lambda nivel, msg: None)
        self._trava = threading.RLock()
        self._salvos: dict[str, dict[str, str]] = {}
        self._indice: dict[str, tuple[str, str]] = {}   # tópico exato → (device_id, chave)
        self.avisos: list[str] = []                      # da última carga (o pipeline repete no log da tela)
        self.carregar()

    # -- leitura ----------------------------------------------------------------------------------------

    def carregar(self) -> None:
        avisos: list[str] = []
        salvos: dict[str, dict[str, str]] = {}
        if self.caminho.exists():
            try:
                bruto = json.loads(self.caminho.read_text(encoding="utf-8-sig"))
                if not isinstance(bruto, dict):
                    raise ValueError("o conteúdo não é um objeto JSON {dispositivo: {tópicos}}")
            except (OSError, UnicodeDecodeError, ValueError) as exc:
                copia = self.caminho.with_name(self.caminho.name + ".corrompido")
                try:
                    shutil.copyfile(self.caminho, copia)
                    guardado = f" Cópia guardada em {copia.name}."
                except OSError:
                    guardado = ""
                avisos.append(f"Arquivo de tópicos {self.caminho.name} ilegível ({exc}); usando os tópicos "
                              f"padrão de todos os dispositivos.{guardado}")
                bruto = {}
            for device, topicos in bruto.items():
                try:
                    dev = digirail.validar_device_id(device)
                    if not isinstance(topicos, dict):
                        raise ValueError("os tópicos precisam ser um objeto")
                    salvos[dev] = self._completar(dev, topicos)
                except ValueError as exc:
                    avisos.append(f"{self.caminho.name}: tópicos de {device!r} ignorados ({exc}); valem os "
                                  "padrões.")
        with self._trava:
            self._salvos = salvos
            self._reindexar()
            self.avisos = avisos
        for aviso in avisos:
            self._logar("AVISO", aviso)

    def _completar(self, device: str, topicos: dict) -> dict[str, str]:
        """Valida os informados e completa os que faltam com o padrão."""
        base = padroes(device, self.cfg)
        final: dict[str, str] = {}
        for chave in CHAVES:
            valor = topicos.get(chave)
            final[chave] = base[chave] if valor is None else validar_topico(valor, NOMES[chave])
        return final

    def _reindexar(self) -> None:
        indice: dict[str, tuple[str, str]] = {}
        for device in sorted(self._salvos):
            for chave in CHAVES:
                indice.setdefault(self._salvos[device][chave], (device, chave))
        self._indice = indice

    def efetivos(self, device_id: str) -> dict[str, str]:
        """Os 5 tópicos em uso pelo device: os salvos ou, sem nada salvo, os padrões."""
        with self._trava:
            salvos = self._salvos.get(device_id)
            if salvos is not None:
                return dict(salvos)
        return padroes(device_id, self.cfg)

    def salvos(self, device_id: str) -> dict[str, str] | None:
        with self._trava:
            s = self._salvos.get(device_id)
            return dict(s) if s is not None else None

    def dispositivos_salvos(self) -> list[str]:
        with self._trava:
            return sorted(self._salvos)

    def todos_salvos(self) -> dict[str, dict[str, str]]:
        with self._trava:
            return {d: dict(t) for d, t in self._salvos.items()}

    def por_topico(self, topico: str) -> tuple[str, str] | None:
        """(device_id, chave) de um tópico salvo (comparação exata); None se não for de nenhum device salvo."""
        with self._trava:
            return self._indice.get(topico)

    # -- escrita ----------------------------------------------------------------------------------------

    def salvar(self, device_id: str, topicos: dict[str, str]) -> dict[str, str]:
        """Valida, grava o arquivo inteiro (atômico) e devolve os 5 tópicos salvos. ValueError se inválido."""
        device = digirail.validar_device_id(device_id)
        if not isinstance(topicos, dict):
            raise ValueError("Tópicos inválidos: informe um dicionário {chave: tópico}.")
        desconhecidas = [k for k in topicos if k not in CHAVES]
        if desconhecidas:
            raise ValueError(f"Chave(s) de tópico desconhecida(s): {', '.join(map(str, desconhecidas))} "
                             f"(use {', '.join(CHAVES)}).")
        final = self._completar(device, topicos)
        with self._trava:
            novos = {d: dict(t) for d, t in self._salvos.items()}
            novos[device] = final
            gravar_atomico(self.caminho, novos)
            self._salvos = novos
            self._reindexar()
        return dict(final)

    def restaurar(self, device_id: str) -> bool:
        """Apaga os tópicos salvos do device (volta aos padrões). Devolve True se havia algo salvo."""
        device = digirail.validar_device_id(device_id)
        with self._trava:
            if device not in self._salvos:
                return False
            novos = {d: dict(t) for d, t in self._salvos.items() if d != device}
            gravar_atomico(self.caminho, novos)
            self._salvos = novos
            self._reindexar()
        return True
