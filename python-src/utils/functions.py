import asyncio
import random
import zipfile
from datetime import date
from typing import Any, Callable


async def retry(
    func: Callable,
    limit: int = 5,
    delay_ms: int = 0,
    count_attempt: Callable[[Any], bool] = lambda _: True,
) -> Any:
    """Retry an async function up to `limit` times with optional delay."""
    attempt = 0
    while attempt < limit:
        try:
            return await func()
        except Exception as err:
            if count_attempt(err):
                attempt += 1
            if attempt >= limit:
                raise
            if delay_ms:
                await delay(delay_ms)


async def has_exception(func: Callable) -> bool:
    """Return True if the async callable raises an exception."""
    try:
        await func()
    except Exception:
        return True
    return False


async def delay(ms: int) -> None:
    """Sleep for `ms` milliseconds."""
    await asyncio.sleep(ms / 1000)


async def delay_in_range(min_s: float, max_s: float) -> None:
    """Sleep for a random duration between min_s and max_s seconds."""
    seconds = random_int_in_range(int(min_s), int(max_s))
    await asyncio.sleep(seconds)


def random_int_in_range(min_val: int, max_val: int) -> int:
    """Return a random integer in [min_val, max_val] inclusive."""
    return random.randint(min_val, max_val)


def split_date_range(
    from_date: date, to_date: date, parts: int = 2
) -> list[dict[str, date]]:
    """Split a date range into `parts` equal sub-ranges."""
    from_ts = from_date.timestamp() if hasattr(from_date, "timestamp") else float(from_date.strftime("%s"))  # type: ignore[attr-defined]
    to_ts = to_date.timestamp() if hasattr(to_date, "timestamp") else float(to_date.strftime("%s"))  # type: ignore[attr-defined]

    if from_ts >= to_ts:
        raise ValueError("Wrong date range to split")
    if to_ts - from_ts < 86400:
        raise ValueError("Date range too small to split")

    import datetime as dt

    from_dt = dt.datetime.fromtimestamp(from_ts)
    to_dt = dt.datetime.fromtimestamp(to_ts)

    if to_ts - from_ts < 172800:
        return [{"from": from_dt, "to": from_dt}, {"from": to_dt, "to": to_dt}]

    ranges = []
    step = (to_ts - from_ts) / parts
    current = from_ts
    while current + step <= to_ts + 1:
        ranges.append(
            {
                "from": dt.datetime.fromtimestamp(current),
                "to": dt.datetime.fromtimestamp(current + step),
            }
        )
        current += step
    return ranges


def is_same_day_and_diff_leq_1_day(a: Any, b: Any) -> bool:
    """Return True if a and b fall on the same day and are ≤1 day apart."""
    import datetime as dt

    def to_datetime(v: Any) -> dt.datetime | None:
        if isinstance(v, dt.datetime):
            return v
        if isinstance(v, dt.date):
            return dt.datetime(v.year, v.month, v.day)
        try:
            return dt.datetime.fromisoformat(str(v))
        except Exception:
            return None

    d1, d2 = to_datetime(a), to_datetime(b)
    if d1 is None or d2 is None:
        return False

    one_day = dt.timedelta(days=1)
    diff_ok = abs(d1 - d2) <= one_day
    same_day = d1.date() == d2.date()
    return diff_ok and same_day


def unzip_sync(file_path: str, to_dir: str) -> None:
    """Extract all files from a zip archive to `to_dir`."""
    with zipfile.ZipFile(file_path, "r") as z:
        z.extractall(to_dir)


def extract_form_params(soup: Any) -> dict:
    """Extract form input name/value pairs from a BeautifulSoup object."""
    params: dict = {}
    for inp in soup.select("input[name]"):
        inp_type = inp.get("type", "").lower()
        if inp_type == "submit":
            continue
        name = inp.get("name")
        if name:
            params[name] = inp.get("value", "")
    return params
