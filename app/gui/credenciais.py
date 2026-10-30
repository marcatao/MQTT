"""Senha do SQL Server no cofre do sistema (Gerenciador de Credenciais do Windows) via `keyring`.

A senha NUNCA vai para QSettings, config.toml ou log. Toda falha vira uma mensagem amigável para o log,
sem derrubar o app (keyring ausente, cofre indisponível no Linux etc.).
"""
from __future__ import annotations

SERVICO = "MQTT SQL Server"


class CofreSenhas:
    """Interface mínima usada pela janela. Cada método devolve (resultado, erro_ou_None)."""

    def ler(self, usuario: str) -> tuple[str | None, str | None]:
        try:
            import keyring

            return keyring.get_password(SERVICO, usuario), None
        except Exception as exc:  # noqa: BLE001 - qualquer falha do cofre é só um aviso
            return None, _descrever(exc)

    def salvar(self, usuario: str, senha: str) -> str | None:
        try:
            import keyring

            keyring.set_password(SERVICO, usuario, senha)
            return None
        except Exception as exc:  # noqa: BLE001
            return _descrever(exc)

    def apagar(self, usuario: str) -> str | None:
        try:
            import keyring
            from keyring.errors import PasswordDeleteError

            try:
                keyring.delete_password(SERVICO, usuario)
            except PasswordDeleteError:
                pass  # não havia senha salva
            return None
        except Exception as exc:  # noqa: BLE001
            return _descrever(exc)


def _descrever(exc: Exception) -> str:
    if isinstance(exc, ImportError):
        return "o pacote “keyring” não está instalado"
    return f"{type(exc).__name__}: {exc}".strip(": ")
