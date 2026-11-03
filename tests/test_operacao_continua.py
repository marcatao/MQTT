"""Operação contínua na tela: contagem até a próxima gravação, última gravação, limpeza à meia-noite e vigia.

Tudo com relógio falso: nenhum teste espera minutos de verdade (QT_QPA_PLATFORM=offscreen).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

pytest.importorskip("PySide6")

from PySide6.QtCore import QCoreApplication, QSettings  # noqa: E402

from app.config import Config, carregar  # noqa: E402
from app.gui import tempo  # noqa: E402
from app.gui.vigia import ARG_AUTO, Comando, Vigia, comando_reinicio, reiniciar_programa  # noqa: E402
from tests.fake_bridge import FakePipelineBridge  # noqa: E402
from tests.test_janela import CofreFalso, esperar, qapp  # noqa: E402,F401

INICIO = datetime(2026, 11, 3, 14, 30, 18).timestamp()


class Relogio:
    def __init__(self, t: float = INICIO):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def avancar(self, segundos: float) -> None:
        self.t += segundos


def iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%dT%H:%M:%S")


@pytest.fixture
def montar(qapp, tmp_path):  # noqa: F811
    criadas = []

    def _montar(minutos=10, servidor="TERRA\\SOLO", lembrar=False, relogio=None, **kw):
        relogio = relogio or Relogio()
        config = Config()
        config.vigia.minutos = minutos
        settings = QSettings(str(tmp_path / "gui.ini"), QSettings.Format.IniFormat)
        kw.setdefault("intervalo", 3600)  # sem mensagens simuladas: o teste emite os sinais com o relógio falso
        bridge = FakePipelineBridge(config, ciclo=0, **kw)
        iniciados: list[Comando] = []
        saidas: list[bool] = []

        def iniciar(cmd):
            iniciados.append(cmd)
            return True

        from app.gui.janela import JanelaPrincipal

        janela = JanelaPrincipal(bridge, config, settings=settings, tema="claro", cofre=CofreFalso(),
                                 relogio=relogio, iniciar_processo=iniciar, sair=lambda: saidas.append(True))
        janela.campo_servidor.setText(servidor)
        janela.opcao_lembrar.setChecked(lembrar)
        janela.show()
        janela._timer_tique.stop()  # o teste dá os tiques com o relógio falso
        criadas.append(janela)
        return janela, bridge, relogio, iniciados, saidas

    yield _montar
    for j in criadas:
        j.close()


# ------------------------------------------------------------------ formatação
@pytest.mark.parametrize("segundos, esperado", [
    (42, "0:42"), (41.2, "0:42"), (60, "1:00"), (59.9, "1:00"), (0.3, "0:01"), (605, "10:05"), (3725, "1:02:05"),
])
def test_contagem_formata(segundos, esperado):
    assert tempo.formatar_contagem(segundos) == esperado


def test_contagem_regressiva_no_chip(montar):
    janela, bridge, relogio, _, _ = montar()
    chip = janela.logs.chip_proxima
    assert chip.texto() == "Próxima gravação —"  # desconectado
    janela.botao_conectar.click()
    assert esperar(lambda: janela.estado == "conectado")
    bridge.proxima_gravacao.emit(relogio() + 42)
    QCoreApplication.processEvents()
    assert chip.texto() == "Próxima gravação em 0:42"
    relogio.avancar(1)
    janela._tique()
    assert chip.texto() == "Próxima gravação em 0:41"
    relogio.avancar(41)
    janela._tique()
    assert chip.texto() == "Próxima gravação: gravando…"
    relogio.avancar(5)
    janela._tique()
    assert chip.valor() == "gravando…"  # continua até o próximo valor chegar
    bridge.proxima_gravacao.emit(relogio() + 60)
    QCoreApplication.processEvents()
    assert chip.texto() == "Próxima gravação em 1:00"
    janela.botao_conectar.click()  # Desconectar
    assert esperar(lambda: janela.estado == "desconectado")
    assert chip.valor() == "—"


def test_ultima_gravacao_formata(montar):
    assert tempo.formatar_data_hora("2026-11-03T14:31:00") == "03/11/2026 14:31:00"
    assert tempo.texto_ultima(None) == ("Última gravação:", "ainda não houve")
    janela, bridge, _, _, _ = montar()
    chip = janela.logs.chip_ultima
    assert chip.texto() == "Última gravação: ainda não houve"
    bridge.ultima_gravacao_ok.emit("2026-11-03T14:31:00")
    QCoreApplication.processEvents()
    assert chip.texto() == "Última gravação: 03/11/2026 14:31:00"
    assert chip.estado == "ok"


# ------------------------------------------------------------------ meia-noite
def test_proxima_meia_noite():
    assert tempo.proxima_meia_noite(datetime(2026, 11, 3, 23, 59, 59)) == datetime(2026, 11, 4)
    assert tempo.proxima_meia_noite(datetime(2026, 11, 4, 0, 0, 0)) == datetime(2026, 11, 5)
    assert tempo.proxima_meia_noite(datetime(2026, 12, 31, 8)) == datetime(2027, 1, 1)
    agora = datetime(2026, 11, 3, 23, 59, 0).timestamp()
    assert 60_000 <= tempo.ms_ate_meia_noite(agora) <= 61_000


def test_limpa_a_meia_noite(montar):
    relogio = Relogio(datetime(2026, 11, 3, 23, 58, 0).timestamp())
    janela, _, _, _, _ = montar(relogio=relogio)
    # o disparo foi agendado para a próxima meia-noite (2 min), não para daqui a 24 h
    assert 115_000 <= janela._timer_meia_noite.remainingTime() <= 121_000
    janela.logs.adicionar("23:58:00", "INFO", "linha de ontem")
    janela.logs.descarregar()
    relogio.avancar(30)
    janela._ao_meia_noite()  # disparo adiantado: ainda é dia 03, nada muda
    assert "linha de ontem" in janela.logs.texto()

    relogio.t = datetime(2026, 11, 4, 0, 0, 0, 100_000).timestamp()
    janela._ao_meia_noite()
    texto = janela.logs.texto()
    assert "linha de ontem" not in texto
    assert texto.splitlines() == ["00:00:00  INFO   Atividade de 04/11/2026"]
    assert janela._timer_meia_noite.remainingTime() > 23 * 3600 * 1000  # reagendado para a próxima
    janela._tique()  # o tique do mesmo dia não limpa de novo
    assert janela.logs.linhas == 1


# ------------------------------------------------------------------ vigia (lógica pura)
def test_vigia_logica_pura():
    relogio = Relogio()
    disparos = []
    sql = {"ok": False}
    v = Vigia(10, disparos.append, sql_configurado=lambda: sql["ok"], relogio=relogio)
    assert v.verificar() is None  # sem conectar não vigia
    v.iniciar()
    relogio.avancar(9 * 60)
    v.registrar_leitura()
    relogio.avancar(9 * 60)  # 18 min desde o início, 9 desde a última leitura
    assert v.verificar() is None
    sql["ok"] = True  # agora com SQL: nunca gravou, e o início foi há 18 min
    assert "Sem gravação no SQL Server há 10 min" in v.verificar()
    assert len(disparos) == 1
    assert v.verificar() is None  # dispara uma vez só


# ------------------------------------------------------------------ vigia na janela
def _conectar(janela):
    janela.botao_conectar.click()
    assert esperar(lambda: janela.estado == "conectado")


def test_vigia_dispara_sem_leitura(montar, caplog):
    janela, bridge, relogio, iniciados, saidas = montar(servidor="")
    _conectar(janela)
    relogio.avancar(5 * 60)
    bridge.ultima_leitura.emit(iso(relogio()))
    QCoreApplication.processEvents()
    relogio.avancar(9 * 60)  # 14 min desde a conexão, 9 desde a leitura
    janela._tique()
    assert not iniciados
    relogio.avancar(61)
    with caplog.at_level(logging.ERROR, logger="app.vigia"):
        janela._tique()
    assert "Sem leitura há 10 min — reiniciando o programa" in janela.logs.texto()
    assert " ERRO " in janela.logs.texto()
    assert any("Sem leitura há 10 min" in r.getMessage() for r in caplog.records)  # vai para o arquivo
    assert ("encerrar", 8.0) in bridge.chamadas
    assert len(iniciados) == 1 and iniciados[0].argumentos[-1] == ARG_AUTO
    assert saidas == [True]


def test_vigia_falta_de_gravacao_so_com_sql(montar):
    # Sem SQL: leituras chegando bastam, mesmo sem nenhuma gravação.
    janela, bridge, relogio, iniciados, _ = montar(servidor="")
    _conectar(janela)
    for _ in range(4):
        relogio.avancar(5 * 60)
        bridge.ultima_leitura.emit(iso(relogio()))
        QCoreApplication.processEvents()
        janela._tique()
    assert not iniciados

    # Com SQL: leituras chegando mas nenhuma gravação ok em 10 min → reinicia.
    janela2, bridge2, relogio2, iniciados2, saidas2 = montar(servidor="TERRA\\SOLO")
    _conectar(janela2)
    bridge2.ultima_gravacao_ok.emit(iso(relogio2() + 60))
    for _ in range(2):
        relogio2.avancar(5 * 60)
        bridge2.ultima_leitura.emit(iso(relogio2()))
        QCoreApplication.processEvents()
        janela2._tique()
    assert not iniciados2  # a gravação em +1 min conta: só 9 min sem gravar
    relogio2.avancar(2 * 60)
    bridge2.ultima_leitura.emit(iso(relogio2()))
    QCoreApplication.processEvents()
    janela2._tique()
    assert "Sem gravação no SQL Server há 10 min" in janela2.logs.texto()
    assert len(iniciados2) == 1 and saidas2 == [True]


def test_vigia_nao_dispara_apos_desconectar(montar):
    janela, bridge, relogio, iniciados, _ = montar()
    _conectar(janela)
    janela.botao_conectar.click()  # Desconectar
    assert esperar(lambda: janela.estado == "desconectado")
    relogio.avancar(60 * 60)
    janela._tique()
    assert not iniciados and ("encerrar", 8.0) not in bridge.chamadas


def test_vigia_desligado_com_zero(montar):
    janela, bridge, relogio, iniciados, _ = montar(minutos=0)
    _conectar(janela)
    relogio.avancar(24 * 3600)
    janela._tique()
    assert not iniciados
    assert "Lembrar senha" not in janela.logs.texto()  # sem vigia, sem aviso


def test_aviso_lembrar_senha(montar):
    janela, _, _, _, _ = montar(lembrar=False)
    _conectar(janela)
    assert esperar(lambda: "marque Lembrar senha" in janela.logs.texto())

    janela2, _, _, _, _ = montar(lembrar=True)
    janela2.logs.limpar()
    _conectar(janela2)
    assert "Lembrar senha" not in janela2.logs.texto()


# ------------------------------------------------------------------ reinício
def test_reinicio_chama_encerrar_e_acao(tmp_path):
    ordem = []
    cmd = Comando("prog", ["--auto-conectar"], str(tmp_path))
    ok = reiniciar_programa(
        "Sem leitura há 10 min — reiniciando o programa",
        registrar=lambda nivel, msg: ordem.append(("registrar", nivel, msg)),
        salvar=lambda: ordem.append(("salvar",)),
        encerrar=lambda timeout: ordem.append(("encerrar", timeout)),
        iniciar=lambda c: ordem.append(("iniciar", c)) or True,
        sair=lambda: ordem.append(("sair",)),
        comando=cmd,
    )
    assert ok
    assert [o[0] for o in ordem] == ["registrar", "salvar", "encerrar", "iniciar", "sair"]
    assert ordem[0][1] == "ERRO" and ordem[2] == ("encerrar", 8.0) and ordem[3][1] is cmd

    # Se não conseguir abrir a nova instância, não sai.
    ordem.clear()
    ok = reiniciar_programa("x", registrar=lambda *a: ordem.append("registrar"), salvar=lambda: None,
                            encerrar=lambda timeout: None, iniciar=lambda c: False,
                            sair=lambda: ordem.append("sair"), comando=cmd)
    assert not ok and "sair" not in ordem


def test_comando_reinicio(tmp_path):
    c = comando_reinicio(frozen=True, executavel=r"C:\App\MQTT-SQLServer.exe",
                         argv=[r"C:\App\MQTT-SQLServer.exe", ARG_AUTO], pasta=str(tmp_path))
    assert (c.programa, c.argumentos, c.pasta) == (r"C:\App\MQTT-SQLServer.exe", [ARG_AUTO], str(tmp_path))
    c = comando_reinicio(frozen=False, executavel="/venv/bin/python", argv=["/x/app/main.py"], pasta="/x")
    assert (c.programa, c.argumentos, c.pasta) == ("/venv/bin/python", ["-m", "app.main", ARG_AUTO], "/x")


# ------------------------------------------------------------------ --auto-conectar
def test_auto_conectar_conecta(qapp, tmp_path):  # noqa: F811
    from app.main import ler_argumentos, montar_janela

    assert ler_argumentos(["app", ARG_AUTO]).auto_conectar
    assert not ler_argumentos(["app", "-platform", "offscreen"]).auto_conectar
    settings = QSettings(str(tmp_path / "gui.ini"), QSettings.Format.IniFormat)
    settings.setValue("mqtt/broker", "10.0.0.5")
    settings.setValue("mqtt/porta", 1884)
    settings.setValue("mqtt/topico", "NOVUS/x/#")
    settings.setValue("mqtt/qos", 1)
    settings.sync()
    janela = montar_janela(["app", ARG_AUTO], config=Config(), bridge_cls=FakePipelineBridge, settings=settings,
                           tema="claro", cofre=CofreFalso())
    try:
        bridge = janela.bridge
        assert esperar(lambda: any(c[0] == "conectar" for c in bridge.chamadas))
        nomes = [c[0] for c in bridge.chamadas]
        i = nomes.index("conectar")
        assert "definir_sql" in nomes[:i] and nomes[i - 1] == "definir_sql"
        assert bridge.chamadas[i] == ("conectar", "10.0.0.5", 1884, "NOVUS/x/#", 1)
        assert esperar(lambda: janela.estado == "conectado")
        assert janela.vigia.ativo
    finally:
        janela.close()

    config = Config()
    config.inicio.conectar_automaticamente = False  # sem --auto-conectar e com a opção desligada: não conecta
    janela = montar_janela(["app"], config=config, bridge_cls=FakePipelineBridge, settings=settings,
                           tema="claro", cofre=CofreFalso())
    try:
        esperar(lambda: False, timeout=0.6)
        assert not any(c[0] == "conectar" for c in janela.bridge.chamadas)
    finally:
        janela.close()


def test_config_vigia(tmp_path):
    assert Config().vigia.minutos == 10
    arq = tmp_path / "config.toml"
    arq.write_text("[vigia]\nminutos = 0\n", encoding="utf-8")
    assert carregar(arq).vigia.minutos == 0
