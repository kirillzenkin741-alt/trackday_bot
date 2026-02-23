import os
from dataclasses import dataclass


class ConfigError(ValueError):
    pass


def _require_str(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _require_int(name: str) -> int:
    raw = _require_str(name)
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be an integer, got: {raw}") from exc


@dataclass(frozen=True)
class Settings:
    bot_token: str
    group_id: int
    admin_id: int
    spreadsheet_id: str
    credentials_file: str
    timezone: str


def load_settings() -> Settings:
    return Settings(
        bot_token=_require_str("BOT_TOKEN"),
        group_id=_require_int("GROUP_ID"),
        admin_id=_require_int("ADMIN_ID"),
        spreadsheet_id=os.getenv("SPREADSHEET_ID", "1izQKvuzt9iXuTjCpHoXeWwi8GgI5yanDW-fRBtHGzzg").strip(),
        credentials_file=os.getenv("CREDENTIALS_FILE", "credentials.json").strip(),
        timezone=os.getenv("TZ", "Asia/Tomsk").strip(),
    )

