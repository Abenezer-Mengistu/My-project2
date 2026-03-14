import os
import json


__NODE_ENV_DEV = "development"
__NODE_ENV_PROD = "production"


def _parse_proxy_credentials(env_val: str | None) -> dict:
    if not env_val:
        return {"username": "", "password": ""}
    try:
        creds = json.loads(env_val)
    except (json.JSONDecodeError, TypeError):
        creds = {}
    return {
        "username": creds.get("username", ""),
        "password": creds.get("password", ""),
    }


# ── App Config ────────────────────────────────────────────────────────────────
app_config = {
    "node_env": os.environ.get("NODE_ENV", __NODE_ENV_DEV),
    "port": int(os.environ.get("APP_PORT", "5556")),
    "auth_token": os.environ.get("APP_AUTH_TOKEN"),
    "developer": os.environ.get("DEVELOPER_NAME", "searchland"),
    "got_endpoint": os.environ.get("GOT_ENDPOINT", "http://localhost:5556"),
    "puppeteer_endpoint": os.environ.get("PUPPETEER_ENDPOINT", "http://localhost:5556"),
    "document_endpoint": os.environ.get("DOCUMENT_ENDPOINT", "http://localhost:5556"),
    "storage_path": "storage",
    "downloads_path": "storage/downloads",
    "static_path": "storage/static",
    "chrome_user_data_file": "chrome-user-data.zip",
    "chrome_storage_state_file": "chrome-storage-state.json",
    "default_user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/141.0.7390.122 Safari/537.36"
    ),
    "default_sec_ch_ua": (
        '"Not;A=Brand";v="99", "Google Chrome";v="141", "Chromium";v="141"'
    ),
}

# ── DB Config ────────────────────────────────────────────────────────────────
db_config = {
    "default": {
        "host": os.environ.get("DB_HOST", "bc-db-host"),
        "name": os.environ.get("DB_NAME", ""),
        "username": os.environ.get("DB_USERNAME", ""),
        "password": os.environ.get("DB_PASSWORD", ""),
        "port": int(os.environ.get("DB_PORT", "5433")),
    }
}

# ── GCloud Config ─────────────────────────────────────────────────────────────
gcloud_config = {
    "key": os.environ.get("GC_KEY"),
    "project_id": os.environ.get("GC_PROJECT_ID"),
    "project_location": os.environ.get("GC_PROJECT_LOCATION"),
    "service_email": os.environ.get("GC_SERVICE_EMAIL"),
    "task_url": os.environ.get("GC_TASK_URL"),
    "task_port": os.environ.get("GC_TASK_PORT"),
    "tasks_location": os.environ.get("GC_TASKS_LOCATION", "us-central1"),
    "storage_prefix": os.environ.get("GC_STORAGE_PREFIX"),
}

# ── Services Config ───────────────────────────────────────────────────────────
services_config = {
    "twocaptcha": {"key": os.environ.get("TWO_CATCHA_API_KEY")},
    "osplaces": {"key": os.environ.get("OS_PLACES_API_KEY")},
    "ip_royal": {"key": os.environ.get("IP_ROYAL_API_KEY")},
    "tascomi": {
        "login": os.environ.get("TASCOMI_LOGIN", ""),
        "password": os.environ.get("TASCOMI_PASSWORD", ""),
    },
}

# ── Proxy Config ──────────────────────────────────────────────────────────────
_premium = _parse_proxy_credentials(os.environ.get("PROXY_PREMIUM_UK"))
_london = _parse_proxy_credentials(os.environ.get("PROXY_LONDON_SOCIAL"))
_webshare_uk = _parse_proxy_credentials(os.environ.get("PROXY_WEBSHARE_UK"))
_webshare_us = _parse_proxy_credentials(os.environ.get("PROXY_WEBSHARE_US"))
_webshare_au = _parse_proxy_credentials(os.environ.get("PROXY_WEBSHARE_AU"))
_webshare_lim = _parse_proxy_credentials(os.environ.get("PROXY_WEBSHARE_UK_LIM"))


def _proxy_url(credentials: dict, host: str) -> str:
    u, p = credentials["username"], credentials["password"]
    return f"http://{u}:{p}@{host}"


proxy_config: dict[str, dict] = {
    "premium_uk_proxy": {
        "origin": "http://proxy.packetstream.io:31112",
        **_premium,
        "url": _proxy_url(_premium, "proxy.packetstream.io:31112"),
    },
    "london_social_proxy": {
        "origin": "http://london1.thesocialproxy.com:10000",
        **_london,
        "url": _proxy_url(_london, "london1.thesocialproxy.com:10000"),
    },
    "webshare_uk_proxy": {
        "origin": "http://p.webshare.io:80",
        **_webshare_uk,
        "url": _proxy_url(_webshare_uk, "p.webshare.io:80"),
    },
    "webshare_us_proxy": {
        "origin": "http://p.webshare.io:80",
        **_webshare_us,
        "url": _proxy_url(_webshare_us, "p.webshare.io:80"),
    },
    "webshare_au_proxy": {
        "origin": "http://p.webshare.io:80",
        **_webshare_au,
        "url": _proxy_url(_webshare_au, "p.webshare.io:80"),
    },
    "ip_royal_proxy": {
        "origin": "http://localhost:8627",
        "username": None,
        "password": None,
        "url": "http://localhost:8627",
    },
    "webshare_proxy_lim": {
        "origin": "http://p.webshare.io:80",
        **_webshare_lim,
        "url": _proxy_url(_webshare_lim, "p.webshare.io:80"),
    },
}

# ── Ticketing / pipeline defaults ─────────────────────────────────────────────
# Use "file" to read venues from Excel; use "db" to read from database.
ticketing_config = {
    "default_source": os.environ.get("TICKETING_SOURCE", "file"),
    "excel_path": os.environ.get("TICKETING_EXCEL_PATH", "venues.xlsx"),
    "max_venues": int(os.environ.get("TICKETING_MAX_VENUES", "10000")),
    "phase2_limit": int(os.environ.get("TICKETING_PHASE2_LIMIT", "10000")),
}

# ── Unified Config ────────────────────────────────────────────────────────────
CONFIG = {
    "app": app_config,
    "db": db_config,
    "services": services_config,
    "gcloud": gcloud_config,
    "proxies": proxy_config,
    "ticketing": ticketing_config,
}

__all__ = ["CONFIG", "__NODE_ENV_DEV", "__NODE_ENV_PROD"]
