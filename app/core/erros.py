"""Registro detalhado de erros (janela "Erros" da GUI) e o arquivo do dia ``<pasta_logs>/erros_AAAAMMDD.jsonl``.

Regra: cada +1 no contador de erros do pipeline gera exatamente um registro, e nenhum registro existe sem o +1.

Formato de cada registro (um objeto JSON por linha, UTF-8)::

    {"quando": "AAAA-MM-DDTHH:MM:SS" (hora local),
     "categoria": "payload" | "sql" | "contingencia" | "mqtt" | "fila",
     "mensagem": str, "topico": str | null, "detalhe": str | null,
     "payload": str | null (no máximo 2000 caracteres)}

Senhas nunca entram: ``mensagem`` e ``detalhe`` passam por ``sql_conexao.mascarar`` (``PWD=***``).
Sem dependência de Qt.
"""
from __future__ import annotations

import base64
import json
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Callable

from app.config import pasta_base
from app.core.sql_conexao import mascarar

CATEGORIAS = ("payload", "sql", "contingencia", "mqtt", "fila")
CHAVES = ("quando", "categoria", "mensagem", "topico", "detalhe", "payload")
MAX_PAYLOAD = 2000

Logar = Callable[[str, str], None]


def resolver_pasta(pasta_logs: str | Path, base: Path | None = None) -> Path:
    """Pasta absoluta: relativa à ``base`` (padrão ``pasta_base()``), como a contingência."""
    p = Path(pasta_logs)
    if not p.is_absolute():
        p = (base if base is not None else pasta_base()) / p
    return p


def arquivo_do_dia(pasta_logs: str | Path, data: date | None = None, base: Path | None = None) -> Path:
    data = data or date.today()
    return resolver_pasta(pasta_logs, base) / f"erros_{data:%Y%m%d}.jsonl"


def texto_payload(payload: bytes | bytearray | str | None, limite: int = MAX_PAYLOAD) -> str | None:
    """Payload legível e truncado em ``limite`` caracteres. Binário (não UTF-8) vira ``base64:...``."""
    if payload is None:
        return None
    if isinstance(payload, (bytes, bytearray)):
        try:
            texto = bytes(payload).decode("utf-8")
        except UnicodeDecodeError:
            texto = "base64:" + base64.b64encode(bytes(payload)).decode("ascii")
    else:
        texto = str(payload)
    if not texto:
        return None
    return texto if len(texto) <= limite else texto[:limite]


def montar_registro(quando: str, categoria: str, mensagem: str, topico: str | None = None,
                    detalhe: str | None = None, payload: bytes | str | None = None) -> dict:
    return {
        "quando": quando,
        "categoria": categoria if categoria in CATEGORIAS else "payload",
        "mensagem": mascarar(str(mensagem)),
        "topico": topico or None,
        "detalhe": mascarar(str(detalhe)) if detalhe else None,
        "payload": texto_payload(payload),
    }


def para_json(registro: dict) -> str:
    return json.dumps(registro, ensure_ascii=False)


class GravadorErros:
    """Acrescenta registros ao arquivo do dia. Erro de E/S não derruba quem chama: um AVISO e segue.

    O aviso sai uma vez só; volta a valer depois que uma gravação der certo de novo.
    """

    def __init__(self, pasta: Path, logar: Logar | None = None) -> None:
        self.pasta = Path(pasta)
        self._logar = logar
        self._trava = threading.Lock()
        self._avisou = False

    def gravar(self, registro_json: str, data: date) -> bool:
        caminho = self.pasta / f"erros_{data:%Y%m%d}.jsonl"
        try:
            with self._trava:
                self.pasta.mkdir(parents=True, exist_ok=True)
                with caminho.open("a", encoding="utf-8", newline="\n") as f:
                    f.write(registro_json + "\n")
        except OSError as exc:
            if not self._avisou:
                self._avisou = True
                if self._logar is not None:
                    try:
                        self._logar("AVISO", f"Não foi possível gravar o registro de erros em {caminho}: {exc}. "
                                             "Os erros continuam na tela e no log.")
                    except Exception:  # noqa: BLE001
                        pass
            return False
        self._avisou = False
        return True


def ler_erros_do_dia(pasta_logs: str | Path, data: date | datetime | None = None) -> list[dict]:
    """Registros de ``<pasta_logs>/erros_AAAAMMDD.jsonl`` do dia (hoje, hora local, se ``data`` for None).

    ``pasta_logs`` relativa é resolvida a partir de ``pasta_base()`` (como na gravação). Arquivo ausente → ``[]``.
    Linhas vazias, corrompidas ou que não sejam objeto JSON são ignoradas; chaves que faltarem vêm como ``None``.
    Ordem: a do arquivo (mais antigo primeiro).
    """
    if isinstance(data, datetime):
        data = data.date()
    caminho = arquivo_do_dia(pasta_logs, data)
    try:
        bruto = caminho.read_bytes()
    except OSError:
        return []
    registros: list[dict] = []
    for linha in bruto.decode("utf-8", errors="replace").splitlines():
        linha = linha.strip()
        if not linha:
            continue
        try:
            obj = json.loads(linha)
        except ValueError:
            continue
        if not isinstance(obj, dict):
            continue
        registros.append({chave: obj.get(chave) for chave in CHAVES})
    return registros
