"""Aviso ao portal OEE depois de gravar os apontamentos reais (repositório ``marcatao/OEE``, Laravel).

O integrador grava ``apontamentos_estado`` direto no banco, e as regras que no portal rodam ao salvar um apontamento
(SS automática da legenda com "Abre SS") não rodam com um INSERT direto. Por isso, depois de cada gravação com
sucesso, o integrador chama ``POST {portal_url}/api/iot/apontamentos/processar`` com os ids dos apontamentos CRIADOS
ou com a LEGENDA TROCADA, e o portal aplica a mesma regra das telas. O portal é idempotente (mandar de novo não abre
outra SS). Situação da máquina e status das OPs o integrador já atualiza sozinho.
Peças (22/11/2026): na mesma chamada vão ``producao`` (apontamentos de produção com ``ciclos`` novos: o portal
calcula as peças = ciclos × cavidades ativas do molde da OP) e ``ops`` (OPs afetadas: o portal soma a quantidade real).

Configuração em ``[oee]``: ``portal_url`` (ex.: ``https://www.alumbra.com.br/oee``) e ``portal_token`` (o mesmo
``IOT_TOKEN`` do ``.env`` do portal; ou a variável de ambiente ``OEE_PORTAL_TOKEN``). Sem os dois, o aviso fica
desligado (1 INFO). Falha de rede não perde nada: os ids ficam pendentes e vão de novo no próximo ciclo.
Só biblioteca padrão (urllib), sem dependência nova.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Callable

Logar = Callable[[str, str], None]
Enviar = Callable[[str, bytes, dict, float], tuple[int, bytes]]

MAX_POR_CHAMADA = 100       # o portal aceita até 100 por chamada (config oee.iot.max_apontamentos)
MAX_PENDENTES = 20000       # limite da fila em memória (portal fora do ar por muito tempo)
ROTA = "/api/iot/apontamentos/processar"


def _enviar_http(url: str, corpo: bytes, cabecalhos: dict, timeout: float) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=corpo, headers=cabecalhos, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (URL da configuração)
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read() or b""


class NotificadorPortal:
    """Fila de ids a avisar ao portal. Usado só pela thread dos apontamentos (sem trava)."""

    def __init__(self, url: str | None, token: str | None, logar: Logar, enviar: Enviar = _enviar_http,
                 timeout: float = 15.0) -> None:
        self._url = (url or "").strip().rstrip("/")
        self._token = (token or os.environ.get("OEE_PORTAL_TOKEN") or "").strip()
        self._logar = logar
        self._enviar = enviar
        self._timeout = timeout
        self._pendentes: list[int] = []         # SS automática (criados / legenda trocada)
        self._producao: list[int] = []          # peças (ciclos novos)
        self._ops: list[int] = []               # quantidade real das OPs
        self._excluir: list[int] = []           # do IoT com SS/OS que o banco não deixou excluir (o portal exclui)
        self._info_desligado = False
        self._falhando = False

    @property
    def ligado(self) -> bool:
        return bool(self._url and self._token)

    @property
    def pendentes(self) -> list[int]:
        return list(self._pendentes)

    def avisar(self, ids: list[int], producao: list[int] | None = None, ops: list[int] | None = None,
               excluir: list[int] | None = None) -> None:
        """Acrescenta os ids às filas e envia tudo o que estiver pendente. Nunca levanta exceção."""
        if not self.ligado:
            if (ids or producao or ops or excluir) and not self._info_desligado:
                self._info_desligado = True
                self._logar("INFO", "Apontamentos reais: aviso ao portal OEE desligado ([oee] portal_url e portal_token "
                                    "vazios); a SS automática das legendas com \"Abre SS\" não abre para o IoT.")
            return
        for fila, novos in ((self._pendentes, ids), (self._producao, producao or []), (self._ops, ops or []),
                            (self._excluir, excluir or [])):
            vistos = set(fila)
            for i in novos:
                if int(i) not in vistos:
                    fila.append(int(i))
                    vistos.add(int(i))
            if len(fila) > MAX_PENDENTES:
                descartados = len(fila) - MAX_PENDENTES
                del fila[:descartados]
                self._logar("AVISO", f"Apontamentos reais: {descartados} aviso(s) antigos ao portal descartados (fila cheia).")
        while self._pendentes or self._producao or self._ops or self._excluir:
            lote = (self._pendentes[:MAX_POR_CHAMADA], self._producao[:MAX_POR_CHAMADA], self._ops[:MAX_POR_CHAMADA])
            excluir_lote = self._excluir[:MAX_POR_CHAMADA]
            if not (self._enviar_lote(*lote, excluir=excluir_lote) if excluir_lote else self._enviar_lote(*lote)):
                return
            del self._pendentes[:len(lote[0])]
            del self._producao[:len(lote[1])]
            del self._ops[:len(lote[2])]
            del self._excluir[:len(excluir_lote)]

    def _enviar_lote(self, lote: list[int], producao: list[int] | None = None, ops: list[int] | None = None,
                     excluir: list[int] | None = None) -> bool:
        dados_envio: dict = {"apontamentos": lote}
        if excluir:
            dados_envio["excluir"] = excluir
        if producao:
            dados_envio["producao"] = producao
        if ops:
            dados_envio["ops"] = ops
        corpo = json.dumps(dados_envio).encode()
        cab = {"Content-Type": "application/json", "Accept": "application/json",
               "Authorization": f"Bearer {self._token}", "User-Agent": "Integrador-MQTT-Alumbra"}
        try:
            status, resposta = self._enviar(self._url + ROTA, corpo, cab, self._timeout)
        except Exception as exc:  # noqa: BLE001 (rede: tenta de novo no próximo ciclo)
            return self._falhou(f"sem conexão com o portal ({exc})")
        if status == 422 and not lote:
            # portal antigo (antes das peças) exige "apontamentos": descarta só esta remessa de peças
            return True
        if status != 200:
            motivo = {401: "token recusado (confira [oee] portal_token = IOT_TOKEN do portal)",
                      503: "rota desligada no portal (IOT_TOKEN vazio no .env do portal)",
                      429: "limite de chamadas do portal"}.get(status, f"resposta HTTP {status}")
            return self._falhou(motivo)
        if self._falhando:
            self._falhando = False
            self._logar("INFO", "Apontamentos reais: aviso ao portal OEE voltou a funcionar.")
        try:
            dados = json.loads(resposta or b"{}")
        except ValueError:
            dados = {}
        for item in dados.get("excluidos") or []:
            chamados = "; ".join(item.get("chamados") or [])
            self._logar("INFO", f"Apontamentos reais: o portal excluiu o apontamento {item.get('id')} (trecho corrigido)"
                                + (f": {chamados}." if chamados else "."))
        for item in dados.get("apontamentos") or []:
            if item.get("ss"):
                self._logar("INFO", f"Apontamentos reais: o portal abriu a SS {item['ss']} (apontamento {item.get('id')}).")
        return True

    def _falhou(self, motivo: str) -> bool:
        if not self._falhando:
            self._falhando = True
            self._logar("AVISO", f"Apontamentos reais: não deu para avisar o portal OEE: {motivo}. "
                                 f"{len(self._pendentes)} apontamento(s) ficam para o próximo ciclo.")
        return False
