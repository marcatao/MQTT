"""Cadastro dos dispositivos no portal OEE (``dispositivos_iot``), no mesmo SQL Server do integrador.

O portal OEE (Laravel, repositório ``marcatao/OEE``) tem a tabela ``dbo.dispositivos_iot`` (migração
``2026_11_06_000002_criar_dispositivos_iot.php`` + coluna ``unidade``)::

    id bigint identity PK, codigo nvarchar(40) UNIQUE, nome nvarchar(120) NOT NULL, tipo nvarchar(20) NOT NULL,
    protocolo nvarchar(20) NOT NULL, endereco nvarchar(200) NULL, porta int NULL, topico nvarchar(250) NULL,
    maquina_id bigint NULL FK, ultimo_sinal_em datetime NULL, observacao nvarchar(500) NULL, ativo bit NOT NULL,
    criado_em datetime NULL, criado_por_id bigint NULL, atualizado_em datetime NULL, atualizado_por_id bigint NULL,
    unidade nvarchar(20)        -- datas em UTC

O integrador **não cria nem altera** essa tabela; só insere e atualiza linhas, com o login de ``[sqlserver]`` e o
nome em 3 partes ``[banco].[dbo].[dispositivos_iot]`` (``[oee]`` no config.toml).

O que é gravado
---------------
- **Diagnóstico** (``diagnostico``): upsert por ``codigo = device_id`` numa transação: ``UPDATE ... WITH (UPDLOCK,
  SERIALIZABLE) WHERE codigo = ?`` e, se nenhuma linha mudou, ``INSERT`` (sem MERGE).
  - novo: codigo, nome (= device_id), tipo (``[oee] tipo``), protocolo ``'mqtt'``, endereco (IP do diag ou NULL),
    topico (tópico de dados do device), ultimo_sinal_em, observacao (resumo do diag, até 500), ativo 1, unidade
    (``[oee] unidade``), criado_em e atualizado_em. maquina_id, porta e ``*_por_id`` ficam NULL (não entram no
    INSERT).
  - existente: só endereco (se o diag trouxe IP), topico, ultimo_sinal_em, observacao e atualizado_em. **Nunca**
    maquina_id, nome, tipo, protocolo, porta, ativo, unidade nem criado_* (são do portal).
- **Leituras** (``sinal``): ``UPDATE`` só de ultimo_sinal_em e atualizado_em, se o registro existir; no máx. 1 vez
  por minuto por device (o limite é aplicado antes de enfileirar).

Só as colunas que existem na tabela são usadas (ex.: sem ``unidade``, não grava unidade).

Detecção (na 1ª operação e a cada conexão nova): ``DB_ID(banco)``, ``OBJECT_ID('[banco].[dbo].[tabela]', 'U')``,
permissões INSERT e UPDATE (``HAS_PERMS_BY_NAME`` executado no contexto do banco do OEE, via
``[banco].sys.sp_executesql``) e as colunas (``[banco].INFORMATION_SCHEMA.COLUMNS``). Se faltar banco, tabela,
permissão ou coluna obrigatória: um INFO ("Portal OEE não encontrado...") e a etapa fica desligada, em silêncio, até
a próxima conexão MQTT (``nova_sessao``) ou troca da configuração do SQL Server.

Execução: thread e fila próprias, com conexão própria (nunca atrasa a gravação na IOT001). Falhas viram AVISO no
log, no máx. 1 por device a cada 10 min, e **não** contam no contador de erros. As datas vão como ``datetime``
ingênuo em UTC, truncado no milissegundo (a coluna é ``datetime``, não ``datetime2``).
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable

from app.config import OeeConfig, SqlServerConfig
from app.core import sql_conexao
from app.core.sink_sqlserver import classificar_erro

logger = logging.getLogger("app.core.oee")

Logar = Callable[[str, str], None]
Relogio = Callable[[], float]

PROTOCOLO = "mqtt"
INTERVALO_SINAL = 60.0          # s: no máx. 1 UPDATE de ultimo_sinal_em por device por minuto (leituras)
INTERVALO_AVISO = 600.0         # s: no máx. 1 AVISO de falha por device a cada 10 min
TAM = {"codigo": 40, "nome": 120, "tipo": 20, "protocolo": 20, "endereco": 200, "topico": 250,
       "observacao": 500, "unidade": 20}
OBRIGATORIAS = ("codigo", "nome", "tipo", "protocolo", "ativo")   # NOT NULL sem padrão útil para o integrador
SUFIXO_OBS = " (atualizado pelo Integrador MQTT)"

_FIM = object()


def _sem_log(nivel: str, msg: str) -> None:
    pass


# ------------------------------------------------------------------------------------------ funções puras


def ip_do_diag(diag: Any) -> str | None:
    """``ipv4`` do diag do DigiRail: ``[172, 16, 30, 140]`` → ``"172.16.30.140"``; texto ``"a.b.c.d"`` também vale."""
    if not isinstance(diag, dict):
        return None
    ip = diag.get("ipv4")
    if isinstance(ip, str):
        ip = ip.strip().split(".")
    if not isinstance(ip, (list, tuple)) or len(ip) != 4:
        return None
    partes = []
    for p in ip:
        if isinstance(p, bool):
            return None
        try:
            n = int(str(p).strip())
        except (TypeError, ValueError):
            return None
        if not 0 <= n <= 255:
            return None
        partes.append(str(n))
    ip_txt = ".".join(partes)
    return None if ip_txt == "0.0.0.0" else ip_txt


def _tem(diag: dict, chave: str) -> bool:
    return diag.get(chave) not in (None, "")


def observacao_diag(diag: Any, quando_local: datetime) -> str:
    """Resumo do diag para ``observacao`` (até 500 caracteres)::

        Diag 11/11/2026 14:31 · Nº série 21004417 · Firmware 1.23 · Wi-Fi -55 dBm · Fila 0 · IP 172.16.30.140
        (atualizado pelo Integrador MQTT)
    """
    diag = diag if isinstance(diag, dict) else {}
    partes = [f"Diag {quando_local:%d/%m/%Y %H:%M}"]
    if _tem(diag, "sn"):
        partes.append(f"Nº série {diag['sn']}")
    if _tem(diag, "fw_v"):
        partes.append(f"Firmware {diag['fw_v']}")
    if _tem(diag, "avg_rssi"):
        partes.append(f"Wi-Fi {diag['avg_rssi']} dBm")
    elif _tem(diag, "curr_rssi"):
        partes.append(f"Wi-Fi {diag['curr_rssi']}%")
    if _tem(diag, "mqtt_queue"):
        partes.append(f"Fila {diag['mqtt_queue']}")
    ip = ip_do_diag(diag)
    if ip:
        partes.append(f"IP {ip}")
    texto = " · ".join(partes)
    limite = TAM["observacao"]
    if len(texto) + len(SUFIXO_OBS) <= limite:
        return texto + SUFIXO_OBS
    return texto[:limite]


def utc_sql(t: float) -> datetime:
    """Epoch → ``datetime`` UTC ingênuo, truncado no milissegundo (coluna ``datetime`` do SQL Server)."""
    dt = datetime.fromtimestamp(t, timezone.utc).replace(tzinfo=None)
    return dt.replace(microsecond=dt.microsecond // 1000 * 1000)


def _corta(valor: str | None, coluna: str) -> str | None:
    return None if valor is None else str(valor)[:TAM[coluna]]


def montar_update_diag(alvo: str, colunas: set[str], com_endereco: bool) -> tuple[str, list[str]]:
    """UPDATE do upsert (com UPDLOCK/SERIALIZABLE). Devolve (sql, campos na ordem dos ``?`` do SET)."""
    campos = [c for c in ("endereco", "topico", "ultimo_sinal_em", "observacao", "atualizado_em")
              if c in colunas and (c != "endereco" or com_endereco)]
    sets = ", ".join(f"[{c}] = ?" for c in campos) or "[codigo] = [codigo]"
    return f"UPDATE {alvo} WITH (UPDLOCK, SERIALIZABLE) SET {sets} WHERE [codigo] = ?", campos


def montar_insert(alvo: str, colunas: set[str]) -> tuple[str, list[str]]:
    """INSERT de device novo, só com as colunas que existem. Devolve (sql, campos na ordem dos ``?``)."""
    campos = [c for c in ("codigo", "nome", "tipo", "protocolo", "endereco", "topico", "ultimo_sinal_em",
                          "observacao", "ativo", "unidade", "criado_em", "atualizado_em") if c in colunas]
    nomes = ", ".join(f"[{c}]" for c in campos)
    return f"INSERT INTO {alvo} ({nomes}) VALUES ({', '.join('?' for _ in campos)})", campos


def montar_update_sinal(alvo: str, colunas: set[str]) -> tuple[str, list[str]] | None:
    campos = [c for c in ("ultimo_sinal_em", "atualizado_em") if c in colunas]
    if "ultimo_sinal_em" not in campos:
        return None
    return f"UPDATE {alvo} SET {', '.join(f'[{c}] = ?' for c in campos)} WHERE [codigo] = ?", campos


# ---------------------------------------------------------------------------------------------- detecção


SQL_PERMISSOES = ("EXEC {banco}.sys.sp_executesql N'SELECT HAS_PERMS_BY_NAME(@obj, N''OBJECT'', N''INSERT''), "
                  "HAS_PERMS_BY_NAME(@obj, N''OBJECT'', N''UPDATE'')', N'@obj nvarchar(400)', @obj = ?")


def detectar(cursor, oee: OeeConfig) -> tuple[set[str] | None, str]:
    """(colunas existentes, "") se o cadastro pode ser feito; (None, motivo) se não.

    Tudo por parâmetro, exceto o nome do banco (validado e citado) no prefixo de 3 partes.
    """
    banco = sql_conexao.validar_banco(oee.banco)
    schema, tabela = sql_conexao.validar_tabela(oee.tabela)
    alvo = f"[{banco}].[{schema}].[{tabela}]"
    cursor.execute("SELECT DB_ID(?)", banco)
    if cursor.fetchone()[0] is None:
        return None, "banco inexistente"
    cursor.execute("SELECT OBJECT_ID(?, 'U')", alvo)
    if cursor.fetchone()[0] is None:
        return None, "tabela inexistente ou sem acesso ao banco"
    cursor.execute(SQL_PERMISSOES.format(banco=f"[{banco}]"), f"[{schema}].[{tabela}]")
    insert, update = cursor.fetchone()[:2]
    if insert != 1 or update != 1:
        faltam = [n for n, v in (("INSERT", insert), ("UPDATE", update)) if v != 1]
        return None, f"sem permissão de {' e '.join(faltam)}"
    colunas = sql_conexao.colunas_da_tabela(cursor, schema, tabela, banco=banco)
    faltam = [c for c in OBRIGATORIAS if c not in colunas]
    if faltam:
        return None, f"colunas ausentes: {', '.join(faltam)}"
    return colunas, ""


# ------------------------------------------------------------------------------------------------ worker


class CadastroOee:
    """Fila + thread próprias para o cadastro no portal OEE. Nunca levanta exceção para quem chama.

    ``conectar``, ``relogio`` e ``iniciar_thread=False`` (com ``executar_pendentes()``) existem para os testes.
    """

    def __init__(self, sql: SqlServerConfig, oee: OeeConfig | None, logar: Logar = _sem_log,
                 conectar: Callable[[SqlServerConfig], object] | None = None, relogio: Relogio = time.time,
                 iniciar_thread: bool = True) -> None:
        self._sql = replace(sql) if sql is not None else None
        self._oee = replace(oee) if oee is not None else OeeConfig()
        self._logar = logar
        self._conectar = conectar
        self._relogio = relogio
        self._iniciar_thread = iniciar_thread
        self._fila: queue.Queue = queue.Queue(maxsize=1000)
        self._thread: threading.Thread | None = None
        self._trava = threading.Lock()
        self._parado = False
        self._sinal_t: dict[str, float] = {}       # device → último sinal enfileirado (thread de quem chama)
        # estado da thread do worker
        self._conexao = None
        self._colunas: set[str] | None = None      # None = ainda não detectado nesta conexão
        self._desligado = False                    # detecção negativa: só volta na próxima sessão/config
        self._avisos_info: set[tuple] = set()      # INFO "não encontrado" já registrado (por servidor/banco/tabela)
        self._aviso_t: dict[str, float] = {}       # device → último AVISO de falha
        self._inseridos: set[str] = set()          # INFO "cadastrado" já registrado
        self._atualizados: set[str] = set()        # INFO "atualizado" já registrado nesta sessão

    # -- API (qualquer thread) ---------------------------------------------------------------------------

    def diagnostico(self, device_id: str, diag: Any, topico_dados: str | None) -> None:
        """Enfileira o upsert de um diagnóstico recebido."""
        if device_id and self._ativo_cfg():
            self._enfileirar(("diag", str(device_id), diag if isinstance(diag, dict) else {}, topico_dados,
                              self._relogio()))

    def sinal(self, device_id: str) -> None:
        """Leitura de contagem recebida: renova ``ultimo_sinal_em`` (no máx. 1 vez por minuto por device)."""
        if not device_id or not self._ativo_cfg():
            return
        agora = self._relogio()
        with self._trava:
            ultimo = self._sinal_t.get(device_id)
            if ultimo is not None and 0 <= agora - ultimo < INTERVALO_SINAL:
                return
            self._sinal_t[device_id] = agora
        self._enfileirar(("sinal", str(device_id), agora))

    def nova_sessao(self) -> None:
        """Nova conexão MQTT: refaz a detecção (na próxima operação) e volta a registrar o 1º "atualizado"."""
        self._enfileirar(("sessao",), forcar=True)

    def definir_sql(self, sql: SqlServerConfig) -> None:
        self._enfileirar(("sql", replace(sql)), forcar=True)

    def parar(self, timeout: float = 1.0) -> None:
        """Para a thread (o que estiver na fila é descartado: o próximo diagnóstico refaz o cadastro)."""
        self._parado = True
        thread = self._thread
        if thread is not None:
            try:
                self._fila.put_nowait(_FIM)
            except queue.Full:
                pass
            thread.join(timeout)
        else:
            self._fechar_conexao()

    def executar_pendentes(self) -> None:
        """Processa a fila na thread de quem chama (testes, ou ``iniciar_thread=False``)."""
        while True:
            try:
                item = self._fila.get_nowait()
            except queue.Empty:
                return
            if item is _FIM:
                self._fechar_conexao()
                return
            self._executar(item)

    # -- internos ---------------------------------------------------------------------------------------

    def _ativo_cfg(self) -> bool:
        if self._parado or not getattr(self._oee, "habilitado", False):
            return False
        sql = self._sql
        if sql is None or not sql.habilitado:
            return False
        # sem senha salva (autenticação do SQL): o integrador está em contingência; não adianta tentar
        return bool(sql.autenticacao_windows or sql.senha)

    def _enfileirar(self, item: tuple, forcar: bool = False) -> None:
        if self._parado:
            return
        try:
            self._fila.put_nowait(item)
        except queue.Full:
            if not forcar:
                logger.debug("Fila do cadastro OEE cheia; %s descartado", item[0])
                return
        if self._iniciar_thread:
            with self._trava:
                if self._thread is None or not self._thread.is_alive():
                    self._thread = threading.Thread(target=self._loop, name="oee-cadastro", daemon=True)
                    self._thread.start()

    def _loop(self) -> None:
        while True:
            item = self._fila.get()
            if item is _FIM:
                self._fechar_conexao()
                return
            self._executar(item)

    def _executar(self, item: tuple) -> None:
        tipo = item[0]
        if tipo == "sessao":
            self._fechar_conexao()
            self._desligado = False
            self._atualizados = set()
            return
        if tipo == "sql":
            self._fechar_conexao()
            self._sql = item[1]
            self._desligado = False
            return
        if not self._ativo_cfg() or self._desligado:
            return
        device = item[1]
        try:
            if not self._preparar():
                return
            if tipo == "diag":
                self._upsert(device, item[2], item[3], item[4])
            elif tipo == "sinal":
                self._renovar_sinal(device, item[2])
        except Exception as exc:  # noqa: BLE001 - nunca derruba a thread nem conta erro
            self._rollback()
            if _erro_de_conexao(exc):
                self._fechar_conexao()   # a próxima operação reconecta e detecta de novo
            self._avisar_falha(device, exc)

    def _alvo(self) -> str:
        return sql_conexao.tabela_em_banco(self._oee.banco, self._oee.tabela)

    def _descricao(self) -> str:
        try:
            schema, tabela = sql_conexao.validar_tabela(self._oee.tabela)
            return f"{sql_conexao.validar_banco(self._oee.banco)}.{schema}.{tabela}"
        except ValueError:
            return f"{self._oee.banco}.{self._oee.tabela}"

    def _preparar(self) -> bool:
        """Conecta e detecta (1ª vez em cada conexão). False = desligado (já logado)."""
        if self._conexao is None:
            self._conexao = (self._conectar or sql_conexao.conectar)(self._sql)
            self._colunas = None
        if self._colunas is not None:
            return True
        cursor = self._conexao.cursor()
        try:
            try:
                colunas, motivo = detectar(cursor, self._oee)
            except ValueError as exc:  # nome de banco/tabela inválido na configuração
                colunas, motivo = None, str(exc)
            except Exception as exc:  # noqa: BLE001
                if _erro_de_conexao(exc):
                    raise
                colunas, motivo = None, sql_conexao.traduzir_erro(exc).splitlines()[0]
        finally:
            _fechar(cursor)
        try:
            self._conexao.commit()   # encerra a transação implícita das consultas
        except Exception:  # noqa: BLE001
            pass
        if colunas is None:
            self._desligado = True
            self._fechar_conexao()
            chave = (self._sql.servidor, self._oee.banco, self._oee.tabela)
            if chave not in self._avisos_info:
                self._avisos_info.add(chave)
                self._logar("INFO", f"Portal OEE não encontrado neste SQL Server (banco {self._descricao()}): "
                                    "cadastro de dispositivos desligado.")
            logger.info("Cadastro OEE desligado: %s", motivo)
            return False
        self._colunas = colunas
        return True

    def _upsert(self, device: str, diag: dict, topico: str | None, t: float) -> None:
        alvo, colunas = self._alvo(), self._colunas or set()
        agora = utc_sql(t)
        ip = ip_do_diag(diag)
        obs = observacao_diag(diag, datetime.fromtimestamp(t))
        codigo = _corta(device, "codigo")
        valores = {
            "codigo": codigo, "nome": _corta(device, "nome"), "tipo": _corta(self._oee.tipo, "tipo"),
            "protocolo": PROTOCOLO, "endereco": _corta(ip, "endereco"), "topico": _corta(topico, "topico"),
            "ultimo_sinal_em": agora, "observacao": obs, "ativo": 1, "unidade": _corta(self._oee.unidade, "unidade"),
            "criado_em": agora, "atualizado_em": agora,
        }
        sql_upd, campos_upd = montar_update_diag(alvo, colunas, ip is not None)
        cursor = self._conexao.cursor()
        try:
            cursor.execute(sql_upd, *[valores[c] for c in campos_upd], codigo)
            inserido = False
            if (cursor.rowcount or 0) <= 0:
                sql_ins, campos_ins = montar_insert(alvo, colunas)
                cursor.execute(sql_ins, *[valores[c] for c in campos_ins])
                inserido = True
            self._conexao.commit()
        except Exception:
            self._rollback()
            raise
        finally:
            _fechar(cursor)
        if inserido and device not in self._inseridos:
            self._inseridos.add(device)
            self._atualizados.add(device)
            self._logar("INFO", f"Dispositivo {device} cadastrado no portal OEE")
        elif not inserido and device not in self._atualizados:
            self._atualizados.add(device)
            self._logar("INFO", f"Dispositivo {device} atualizado no portal OEE")

    def _renovar_sinal(self, device: str, t: float) -> None:
        montado = montar_update_sinal(self._alvo(), self._colunas or set())
        if montado is None:
            return
        sql_upd, campos = montado
        agora = utc_sql(t)
        cursor = self._conexao.cursor()
        try:
            cursor.execute(sql_upd, *[agora for _ in campos], _corta(device, "codigo"))
            self._conexao.commit()
        except Exception:
            self._rollback()
            raise
        finally:
            _fechar(cursor)

    def _avisar_falha(self, device: str, exc: Exception) -> None:
        agora = self._relogio()
        ultimo = self._aviso_t.get(device)
        if ultimo is not None and 0 <= agora - ultimo < INTERVALO_AVISO:
            logger.info("Falha no cadastro OEE de %s (aviso suprimido): %s", device, exc)
            return
        self._aviso_t[device] = agora
        resumo = sql_conexao.traduzir_erro(exc).replace("\n", " ")
        self._logar("AVISO", f"Não foi possível atualizar o dispositivo {device} no portal OEE "
                             f"({self._descricao()}): {resumo} A gravação das leituras não é afetada.")

    def _rollback(self) -> None:
        if self._conexao is not None:
            try:
                self._conexao.rollback()
            except Exception:  # noqa: BLE001
                pass

    def _fechar_conexao(self) -> None:
        conexao, self._conexao = self._conexao, None
        self._colunas = None
        if conexao is not None:
            try:
                conexao.close()
            except Exception:  # noqa: BLE001
                pass


def _fechar(cursor) -> None:
    try:
        cursor.close()
    except Exception:  # noqa: BLE001
        pass


def _erro_de_conexao(exc: BaseException) -> bool:
    return classificar_erro(exc)[0] == "conexao"
