"""Textos de tempo da caixa ATIVIDADE e cálculo da próxima meia-noite (funções puras, testáveis)."""
from __future__ import annotations

import math
from datetime import datetime, timedelta

SEM_VALOR = "—"
GRAVANDO = "gravando…"
AINDA_NAO = "ainda não houve"


def formatar_contagem(segundos: float) -> str:
    """Segundos restantes → "m:ss" (ou "h:mm:ss" a partir de 1 hora). Arredonda para cima: 0,2 s vira 0:01."""
    total = max(0, math.ceil(segundos - 1e-6))
    h, resto = divmod(total, 3600)
    m, s = divmod(resto, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def contagem_proxima(proxima_epoch: float | None, agora: float) -> tuple[str, str]:
    """(prefixo, valor) do chip "Próxima gravação".

    Sem valor (desconectado ou sem ciclo) → ("Próxima gravação", "—");
    no futuro → ("Próxima gravação em", "0:42"); chegou a hora → ("Próxima gravação:", "gravando…").
    """
    if proxima_epoch is None:
        return "Próxima gravação", SEM_VALOR
    restante = proxima_epoch - agora
    if restante <= 0:
        return "Próxima gravação:", GRAVANDO
    return "Próxima gravação em", formatar_contagem(restante)


def formatar_data_hora(iso: str) -> str:
    """ISO 8601 local "AAAA-MM-DDTHH:MM:SS" → "dd/mm/aaaa HH:MM:SS". Texto inválido volta como veio."""
    try:
        dt = datetime.fromisoformat(str(iso).strip())
    except (TypeError, ValueError):
        return str(iso).strip() or SEM_VALOR
    return dt.strftime("%d/%m/%Y %H:%M:%S")


def texto_ultima(iso: str | None) -> tuple[str, str]:
    """(prefixo, valor) do chip "Última gravação"."""
    if not iso:
        return "Última gravação:", AINDA_NAO
    return "Última gravação:", formatar_data_hora(iso)


def proxima_meia_noite(agora: datetime) -> datetime:
    """Próxima 00:00:00 (hora local, datetime ingênuo) estritamente depois de `agora`."""
    amanha = (agora + timedelta(days=1)).date()
    return datetime(amanha.year, amanha.month, amanha.day)


def ms_ate_meia_noite(agora_epoch: float) -> int:
    """Milissegundos até a próxima meia-noite local (recalculado a cada disparo: sem desvio acumulado).

    Pelo menos 1 s, para nunca disparar em laço se o relógio do sistema andar para trás.
    """
    agora = datetime.fromtimestamp(agora_epoch)
    alvo = proxima_meia_noite(agora).timestamp()
    return max(1000, int(math.ceil((alvo - agora_epoch) * 1000)) + 50)


def linha_do_dia(agora_epoch: float) -> str:
    return f"Atividade de {datetime.fromtimestamp(agora_epoch):%d/%m/%Y}"
