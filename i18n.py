"""Lightweight runtime localization strings (Russian default + English)."""
from __future__ import annotations

from typing import Any

_TRANSLATIONS: dict[str, dict[str, str]] = {
    "ru": {
        "auth_unauthorized": "Не авторизован",
        "auth_login_required": "Требуется вход",
        "auth_code": "Код: {code}",
        "auth_activation_prompt": "Требуется активация аккаунта! Перейдите по {uri} и введите код: {code}",
        "auth_code_expired": "Код активации истёк, запрашиваю новый...",
        "auth_checking": "Проверка входа...",
        "auth_restoring": "Восстановление сессии из cookies.jar",
        "auth_expired_reauth": "Сохранённая сессия устарела, требуется переавторизация",
        "auth_client_mismatch": "Несовпадение Client-ID cookie, очистка сессии",
        "auth_authorized": "Авторизован ({username})",
        "auth_logged_in": "Вход выполнен: {username} (ID: {user_id})",
        "status_stopped": "Остановлен",
        "status_starting": "Запуск...",
        "status_auth": "Авторизация...",
        "status_idle": "Ожидание кампаний (IDLE)",
        "status_inv_sync": "Синхронизация инвентаря...",
        "status_cleanup": "Очистка каналов...",
        "status_no_campaigns": "Нет активных кампаний для фарма.",
        "status_searching": "Поиск стримов...",
        "status_watching": "Смотрит: {channel}",
        "status_mining": "Фарм: {channel}",
        "status_standby": "Дежурство: {channel}",
        "status_no_streams": "Нет подходящих онлайн-стримов для просмотра.",
        "log_watch_start": "Начал просмотр {channel} [{game}]",
        "log_standby_start": "Режим дежурства (Standby) на {channel} [{game}]",
        "log_channel_online_switch": "{channel} вышел в онлайн — переключаюсь.",
        "log_channel_offline": "{channel} завершил стрим.",
        "log_drop_claimed": "Дроп получен: {game} — {drop}",
        "err_critical": "Ошибка: {error}",
        "err_login": "Ошибка входа: {error}",
        "log_critical_restart": "Критическая ошибка задачи ({error}), перезапуск...",
        "worker_restarting": "Перезапуск воркера...",
        "worker_crash_retry": "Сбой воркера ({error}), перезапуск через {delay}с",
        "worker_login_retry": "Ошибка входа ({error}), повтор через {delay}с",
        "http_unavailable": "Сервер недоступен ({status}), повтор через {delay}с",
        "http_network_err": "Сетевая ошибка ({error}), повтор через {delay}с",
    },
    "en": {
        "auth_unauthorized": "Not authorized",
        "auth_login_required": "Login required",
        "auth_code": "Code: {code}",
        "auth_activation_prompt": "Account activation required! Open {uri} and enter code: {code}",
        "auth_code_expired": "Activation code expired, requesting a new one...",
        "auth_checking": "Verifying login...",
        "auth_restoring": "Restoring session from cookies.jar",
        "auth_expired_reauth": "Saved session expired, re-authentication required",
        "auth_client_mismatch": "Client-ID cookie mismatch, clearing session",
        "auth_authorized": "Authorized ({username})",
        "auth_logged_in": "Logged in: {username} (ID: {user_id})",
        "status_stopped": "Stopped",
        "status_starting": "Starting...",
        "status_auth": "Authenticating...",
        "status_idle": "Waiting for campaigns (IDLE)",
        "status_inv_sync": "Syncing inventory...",
        "status_cleanup": "Cleaning up channels...",
        "status_no_campaigns": "No active campaigns to mine.",
        "status_searching": "Searching for streams...",
        "status_watching": "Watching: {channel}",
        "status_mining": "Mining: {channel}",
        "status_standby": "Standby: {channel}",
        "status_no_streams": "No suitable online streams available.",
        "log_watch_start": "Started watching {channel} [{game}]",
        "log_standby_start": "Standby mode on {channel} [{game}]",
        "log_channel_online_switch": "{channel} went online — switching.",
        "log_channel_offline": "{channel} went offline.",
        "log_drop_claimed": "Drop claimed: {game} — {drop}",
        "err_critical": "Error: {error}",
        "err_login": "Login error: {error}",
        "log_critical_restart": "Critical task error ({error}), restarting...",
        "worker_restarting": "Restarting worker...",
        "worker_crash_retry": "Worker crashed ({error}), restarting in {delay}s",
        "worker_login_retry": "Login error ({error}), retrying in {delay}s",
        "http_unavailable": "Server unavailable ({status}), retrying in {delay}s",
        "http_network_err": "Network error ({error}), retrying in {delay}s",
    },
}


def normalize_language(lang: str | None) -> str:
    clean = str(lang or "ru").strip().lower()
    return clean if clean in _TRANSLATIONS else "ru"


def tr(key: str, lang: str = "ru", **kwargs: Any) -> str:
    loc = _TRANSLATIONS[normalize_language(lang)]
    template = loc.get(key) or _TRANSLATIONS["ru"].get(key) or key
    if not kwargs:
        return template
    try:
        return template.format(**kwargs)
    except Exception:
        return template
