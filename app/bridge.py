"""Adaptador core ↔ GUI: os eventos do ``Pipeline`` viram Signals do Qt.

Único arquivo do lado do core que importa PySide6. Os sinais são emitidos de threads do core; como o
``PipelineBridge`` vive na thread da GUI, o Qt entrega os sinais lá (conexão enfileirada automática).
"""
from __future__ import annotations

import threading
from dataclasses import replace
from datetime import datetime

from PySide6.QtCore import QObject, Signal

from app.config import Config, SqlServerConfig
from app.core import sql_conexao
from app.core.pipeline import Pipeline


class PipelineBridge(QObject):
    status_alterado = Signal(str, str)       # (estado, detalhe): "desconectado" | "conectando" | "conectado" | "erro"
    log = Signal(str, str, str)              # (hora "HH:MM:SS", nivel "INFO" | "AVISO" | "ERRO", mensagem)
    contadores = Signal(int, int, int)       # (mensagens recebidas, registros gravados, erros), máx. ~5x/s
    teste_sql_concluido = Signal(bool, str)  # (sucesso, mensagem multilinha em português)
    proxima_gravacao = Signal(float)         # epoch (s) do próximo ciclo; só com amostragem (intervalo > 0)
    ultima_gravacao_ok = Signal(str)         # "AAAA-MM-DDTHH:MM:SS" local do último INSERT com commit no SQL Server
    ultima_leitura = Signal(str)             # "AAAA-MM-DDTHH:MM:SS" local da última mensagem MQTT (máx. 1 a cada 5 s)
    # (device_id, tópico, ISO local) só de leitura de contagem (channels); eventos, diag e respostas não emitem;
    # máx. 1 a cada 5 s por device. O vigia usa ultima_leitura (qualquer mensagem), não este.
    dispositivo_visto = Signal(str, str, str)
    erro_registrado = Signal(str)            # JSON de UM erro (app.core.erros); 1 sinal para cada +1 no contador de erros
    canais_vistos = Signal(str, str)         # (device_id, JSON lista ["chd1","chd2","chd3"]) quando o conjunto muda
    diagnostico_dispositivo = Signal(str, str)  # (device_id, JSON do objeto "diag" do DigiRail: sn, fw_v, rssi...)
    resposta_comando = Signal(str, str, bool, str)  # (device_id, "reset_counters"|"diag"|"reiniciar", sucesso, msg)
    # (device_id, etapa do reinício pelo relé: "Relé acionado (saída 1)", "Aguardando o aparelho voltar…"); o
    # resultado final sai em resposta_comando(device, "reiniciar", ...)
    reinicio_etapa = Signal(str, str)
    # JSON lista do cadastro OEE.dbo.dispositivos_iot (ativos), ao conectar e a cada 60 s, só se o OEE existir:
    # [{"codigo", "nome", "endereco", "topico", "ultimo_sinal_em" (ISO local|null), "protocolo",
    #   "maquina" ("código – nome"|null), "unidade"}]
    dispositivos_oee = Signal(str)
    # JSON do ciclo dos apontamentos reais (app.core.apontamentos_oee), 1 por ciclo concluído (ou falho):
    # {"quando", "proxima" (ISO local|null), "minutos", "ok", "mensagem", "criados", "alterados", "excluidos",
    #  "maquinas": [{"maquina_id", "codigo", "nome", "canais", "micro_s", "macro_s", "legenda", "legenda_descricao",
    #                "desde", "ultima_producao", "apontamentos_hoje", "ultima_leitura", "aviso",
    #                "diag" ("ok"|"sem resposta"|"sem MQTT"|null), "turno" ("dentro"|"fora"|"sem jornada"),
    #                "jornada" (ex. "Seg–Qui 05:00–14:48, Sex 05:00–13:48" | null)}]}
    apontamentos_atualizados = Signal(str)
    recalculo_concluido = Signal(int, str, bool, str)  # (maquina_id, dia "AAAA-MM-DD", sucesso, mensagem em português)

    def __init__(self, config: Config, parent=None) -> None:
        super().__init__(parent)
        self._config = config
        self._pipeline = Pipeline(
            config,
            ao_status=self.status_alterado.emit,
            ao_log=self._emitir_log,
            ao_contadores=self.contadores.emit,
            ao_proxima_gravacao=self.proxima_gravacao.emit,
            ao_gravacao_ok=self.ultima_gravacao_ok.emit,
            ao_leitura=self.ultima_leitura.emit,
            ao_dispositivo=self.dispositivo_visto.emit,
            ao_erro=self.erro_registrado.emit,
            ao_canais=self.canais_vistos.emit,
            ao_diagnostico=self.diagnostico_dispositivo.emit,
            ao_resposta_comando=self.resposta_comando.emit,
            ao_apontamentos=self.apontamentos_atualizados.emit,
            ao_recalculo=self.recalculo_concluido.emit,
            ao_reinicio_etapa=self.reinicio_etapa.emit,
            ao_dispositivos_oee=self.dispositivos_oee.emit,
        )

    def _emitir_log(self, nivel: str, mensagem: str) -> None:
        self.log.emit(datetime.now().strftime("%H:%M:%S"), nivel, mensagem)

    def definir_sql(self, sql: SqlServerConfig) -> None:
        """Troca a configuração do SQL Server; se conectado, vale a partir do próximo lote."""
        self._pipeline.definir_sql(replace(sql))

    def testar_sql(self, sql: SqlServerConfig) -> None:
        """Não bloqueia: testa numa thread e responde em ``teste_sql_concluido``."""
        copia = replace(sql)

        def trabalho() -> None:
            try:
                ok, msg = sql_conexao.testar_conexao(copia)
            except Exception as exc:  # noqa: BLE001
                ok, msg = False, sql_conexao.traduzir_erro(exc)
            self.teste_sql_concluido.emit(ok, msg)

        threading.Thread(target=trabalho, name="teste-sql", daemon=True).start()

    def conectar(self, broker: str, porta: int, topico: str, qos: int) -> None:
        self._pipeline.conectar(broker, porta, topico, qos)

    def desconectar(self) -> None:
        self._pipeline.desconectar()

    def encerrar(self, timeout: float = 5.0) -> None:
        self._pipeline.encerrar(timeout)

    def definir_periodicidade_apontamentos(self, minutos: float) -> None:
        """Não bloqueia: periodicidade (min) dos apontamentos reais; vale já (reagenda a próxima execução)."""
        self._pipeline.definir_periodicidade_apontamentos(float(minutos))

    def executar_apontamentos_agora(self) -> None:
        """Não bloqueia: roda um ciclo de apontamentos reais já; responde em ``apontamentos_atualizados``."""
        self._pipeline.executar_apontamentos_agora()

    def recalcular_dia_apontamentos(self, maquina_id: int, dia: str) -> None:
        """Não bloqueia: refaz a timeline da máquina desde 00:00 local de ``dia`` ("AAAA-MM-DD", de hoje até
        ``[apontamentos] dias_correcao`` dias atrás; fora disso responde falha). Os apontamentos manuais da máquina
        nesse período são apagados. Responde em ``recalculo_concluido`` e em ``apontamentos_atualizados``."""
        self._pipeline.recalcular_dia_apontamentos(int(maquina_id), str(dia))

    def zerar_contadores(self, device_id: str, canais: list[int]) -> None:
        """Não bloqueia: publica ``reset_counters`` (canais 1–6) e responde em ``resposta_comando``."""
        self._pipeline.zerar_contadores(device_id, list(canais))

    def pedir_diagnostico(self, device_id: str) -> None:
        """Não bloqueia: publica ``diag``; responde em ``diagnostico_dispositivo`` e ``resposta_comando``."""
        self._pipeline.pedir_diagnostico(device_id)

    def reiniciar_dispositivo(self, device_id: str) -> None:
        """Não bloqueia: liga a saída ``[reinicio] saida`` (relé que corta a alimentação do aparelho) e acompanha a
        volta; etapas em ``reinicio_etapa`` e o resultado em ``resposta_comando(device, "reiniciar", ...)``."""
        self._pipeline.reiniciar_dispositivo(device_id)

    # -- tópicos por dispositivo (v0.1.11): chaves "events", "config", "ack_config", "command", "ack_command" --

    def topicos_do_dispositivo(self, device_id: str) -> dict[str, str]:
        """Os 5 tópicos efetivos do device: os salvos em dispositivos.json ou o padrão de [dispositivos]."""
        return self._pipeline.topicos_do_dispositivo(device_id)

    def salvar_topicos_dispositivo(self, device_id: str, topicos: dict[str, str]) -> None:
        """Grava na hora (arquivo pequeno) e (des)assina os extras na thread de controle; o resultado vai para o log.

        Levanta ``ValueError`` (mensagem em português) se algum tópico for inválido (vazio, com '+'/'#', nulo ou
        longo demais) ou se o arquivo não puder ser gravado; nada é salvo nesse caso.
        """
        self._pipeline.salvar_topicos_dispositivo(device_id, dict(topicos))

    def restaurar_topicos_padrao(self, device_id: str) -> None:
        """Apaga os tópicos salvos do device (volta ao padrão) e desassina os extras que sobraram."""
        self._pipeline.restaurar_topicos_padrao(device_id)

    def topicos_ouvidos(self) -> list[str]:
        """Filtros assinados agora (assinatura do usuário + extras); vazio sem conexão."""
        return self._pipeline.topicos_ouvidos()

    def topico_coberto(self, topico: str) -> bool:
        """O integrador recebe mensagens desse tópico? (sem conexão: pela última assinatura usada)"""
        return self._pipeline.topico_coberto(topico)
