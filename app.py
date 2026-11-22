"""
База знаний по дендрологии — рабочий прототип (без ИИ).
FastAPI + SQLite.

Слои системы:
  1. Хранение знаний   — species_reference: признаки вида (тип, листва, лист, плод, свет,
                         место, высота...), таксономия, алиасы названий, связи «родственные виды».
  2. Обработка знаний  — нечёткий поиск/автокоррекция (rapidfuzz), определитель по признакам
                         (взвешенное сопоставление с объяснением результата) — всё без ИИ.
  3. Живые источники   — GBIF (таксономия, фото), Wikipedia (описание, фото),
                         iNaturalist (фото, название); у каждого факта виден источник,
                         состояние источников проверяется (/api/sources/check).
  4. Пополнение знаний — добавление породы/дерева/участка, сохранение найденного вживую
                         в собственную базу.

Запуск:
    pip install -r requirements.txt
    python app.py
Открыть: http://localhost:8000
"""

import html
import json
import os
import re
import sqlite3
import random
import time
import uuid
try:                                   # поиск видео без ключей Google; без пакета сайт работает, но список видео пуст
    from youtubesearchpython import VideosSearch, CustomSearch, VideoSortOrder, VideoUploadDateFilter
except Exception:                      # noqa: BLE001
    VideosSearch = CustomSearch = VideoSortOrder = VideoUploadDateFilter = None
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import requests
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from rapidfuzz import fuzz, process

import fresh                           # свежие данные из разных онлайн-источников

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "dendro.db"
SEED_PATH = BASE_DIR / "data" / "species_seed.json"
ALIASES_PATH = BASE_DIR / "data" / "species_aliases.json"
ARTICLES_PATH = BASE_DIR / "data" / "hidden_articles.json"
YT_KEY_FILE = BASE_DIR / "youtube_api_key.txt"
VIDEO_CACHE_TTL = 7 * 24 * 3600

CACHE_TTL_SECONDS = 24 * 60 * 60
RETRY_EMPTY_AFTER = 5 * 60          # если в кэше нет ни текста, ни фото — перепроверим через 5 минут
IMAGE_TTL_HIT = 30 * 24 * 3600
IMAGE_TTL_MISS = 5 * 60

# Wikimedia и iNaturalist просят представляться; безымянный python-requests они могут отклонять (HTTP 403).
UA = "DendroKB/1.0 (university coursework project) python-requests"
HEADERS = {"User-Agent": UA, "Accept": "application/json"}

app = FastAPI(title="База знаний по дендрологии")

# Сессия = cookie браузера (живёт, пока открыт браузер). В ней: бросок костей (один за сессию) и остаток поисков.
SESSIONS: dict[str, dict] = {}


def new_session_state() -> dict:
    return {"remaining": 0, "rolled": False, "last_roll": None, "bonus_no": None}


def session_dep(request: Request, response: Response) -> dict:
    sid = request.cookies.get("dendro_sid")
    uid = request.cookies.get("dendro_uid")
    if not uid:  # долгоживущий идентификатор браузера: помнит, какие бонусные статьи уже показывались
        uid = uuid.uuid4().hex
        response.set_cookie("dendro_uid", uid, httponly=True, samesite="lax", path="/", max_age=365 * 24 * 3600)
    if not sid or sid not in SESSIONS:
        sid = uuid.uuid4().hex
        SESSIONS[sid] = new_session_state()
        response.set_cookie("dendro_sid", sid, httponly=True, samesite="lax", path="/")  # без max_age — сессионная
    SESSIONS[sid]["uid"] = uid
    return SESSIONS[sid]

REFERENCE_COLUMNS = {
    "common_en": "TEXT", "avg_height_m": "TEXT", "lifespan_years": "TEXT", "habitat": "TEXT",
    "source_note": "TEXT", "foliage": "TEXT", "leaf_form": "TEXT", "fruit": "TEXT",
    "light": "TEXT", "habitat_tags": "TEXT",
}
CACHE_COLUMNS = {
    "image_credit": "TEXT", "wiki_url": "TEXT", "common_ru": "TEXT",
    "gbif_key": "INTEGER", "source_log": "TEXT",
}
IMAGE_COLUMNS = {"credit": "TEXT"}

# столбцы, которые при старте дозаполняются из seed только если в БД они пустые (правки пользователя не затираются)
SEED_FILL_COLUMNS = ["name_latin", "family", "genus", "type", "description", "common_en", "avg_height_m",
                     "lifespan_years", "habitat", "source_note", "foliage", "leaf_form", "fruit",
                     "light", "habitat_tags"]


# ---------------------------------------------------------------- DB

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def ensure_columns(conn, table: str, cols: dict):
    existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, typ in cols.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")


def init_db():
    with get_db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS species_reference (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name_ru TEXT NOT NULL UNIQUE,
            name_latin TEXT, family TEXT, genus TEXT, type TEXT, description TEXT,
            common_en TEXT, avg_height_m TEXT, lifespan_years TEXT, habitat TEXT, source_note TEXT,
            foliage TEXT, leaf_form TEXT, fruit TEXT, light TEXT, habitat_tags TEXT
        );

        CREATE TABLE IF NOT EXISTS species_alias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alias_ru TEXT NOT NULL UNIQUE,
            reference_id INTEGER NOT NULL REFERENCES species_reference(id)
        );

        CREATE TABLE IF NOT EXISTS species_image (
            reference_id INTEGER PRIMARY KEY REFERENCES species_reference(id),
            image_url TEXT, credit TEXT,
            fetched_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS plots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, forest_type TEXT, area REAL
        );

        CREATE TABLE IF NOT EXISTS species_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference_id INTEGER REFERENCES species_reference(id),
            query_name TEXT NOT NULL,
            scientific_name TEXT, kingdom TEXT, phylum TEXT, class_name TEXT, order_name TEXT,
            family TEXT, genus TEXT, rank TEXT,
            own_description TEXT, live_summary TEXT, wiki_image_url TEXT,
            online_ok INTEGER, fetched_at INTEGER NOT NULL,
            image_credit TEXT, wiki_url TEXT, common_ru TEXT, gbif_key INTEGER, source_log TEXT
        );

        CREATE TABLE IF NOT EXISTS species_video (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_key TEXT NOT NULL, youtube_id TEXT NOT NULL,
            title TEXT, channel TEXT, lang TEXT, added_at INTEGER NOT NULL,
            UNIQUE(subject_key, youtube_id)
        );

        CREATE TABLE IF NOT EXISTS seen_bonus (
            uid TEXT NOT NULL, article_no TEXT NOT NULL, seen_at INTEGER NOT NULL,
            PRIMARY KEY (uid, article_no)
        );

        CREATE TABLE IF NOT EXISTS video_cache (
            cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL, fetched_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS species_link (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_key TEXT NOT NULL, url TEXT NOT NULL, platform TEXT, title TEXT, thumb TEXT, added_at INTEGER NOT NULL,
            UNIQUE(subject_key, url)
        );

        CREATE TABLE IF NOT EXISTS trees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plot_id INTEGER NOT NULL REFERENCES plots(id) ON DELETE CASCADE,
            species_cache_id INTEGER REFERENCES species_cache(id),
            age INTEGER, height REAL, trunk_diameter REAL,
            added_at INTEGER NOT NULL
        );
        """)
        # миграции для баз, созданных прежними версиями прототипа
        ensure_columns(conn, "species_reference", REFERENCE_COLUMNS)
        ensure_columns(conn, "species_cache", CACHE_COLUMNS)
        ensure_columns(conn, "species_image", IMAGE_COLUMNS)
        sync_seed(conn)

        if conn.execute("SELECT COUNT(*) AS c FROM species_alias").fetchone()["c"] == 0 and ALIASES_PATH.exists():
            for a in json.load(open(ALIASES_PATH, encoding="utf-8")):
                ref = conn.execute("SELECT id FROM species_reference WHERE name_ru = ?",
                                   (a["canonical_name_ru"],)).fetchone()
                if ref:
                    conn.execute("INSERT OR IGNORE INTO species_alias (alias_ru, reference_id) VALUES (?,?)",
                                 (a["alias_ru"], ref["id"]))


def sync_seed(conn):
    """Вставляет недостающие виды из seed и дозаполняет пустые поля у существующих."""
    if not SEED_PATH.exists():
        return
    seed = json.load(open(SEED_PATH, encoding="utf-8"))
    existing = {r["name_ru"]: r for r in conn.execute("SELECT * FROM species_reference").fetchall()}
    cols = ["name_ru"] + SEED_FILL_COLUMNS
    for it in seed:
        row = existing.get(it["name_ru"])
        if row is None:
            conn.execute(
                f"INSERT INTO species_reference ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                [it.get(c) for c in cols],
            )
        else:
            for c in SEED_FILL_COLUMNS:
                if row[c] is None and it.get(c) is not None:
                    conn.execute(f"UPDATE species_reference SET {c}=? WHERE id=?", (it[c], row["id"]))


def load_lookup(conn):
    refs = conn.execute("SELECT * FROM species_reference").fetchall()
    ref_by_id = {r["id"]: r for r in refs}
    name_to_ref = {r["name_ru"].lower(): r for r in refs}
    alias_to_ref = {}
    for a in conn.execute("SELECT * FROM species_alias").fetchall():
        ref = ref_by_id.get(a["reference_id"])
        if ref:
            alias_to_ref[a["alias_ru"].lower()] = ref
    return name_to_ref, alias_to_ref


# Автокоррекция: латинские «двойники» кириллицы и транслитерация (lipa → липа)
_HOMOGLYPHS = str.maketrans("aceopxykmt", "асеорхукмт")
_TRANSLIT_DI = [("shch", "щ"), ("sh", "ш"), ("ch", "ч"), ("zh", "ж"), ("kh", "х"), ("ts", "ц"), ("yu", "ю"), ("ya", "я"), ("yo", "ё")]


def translit_ru(text: str) -> str:
    """Простая транслитерация латиницы в кириллицу: dub → дуб, lipa → липа, bereza → береза."""
    for lat, cyr in _TRANSLIT_DI:
        text = text.replace(lat, cyr)
    one = {"a": "а", "b": "б", "c": "к", "d": "д", "e": "е", "f": "ф", "g": "г", "h": "х", "i": "и", "j": "й", "k": "к",
           "l": "л", "m": "м", "n": "н", "o": "о", "p": "п", "q": "к", "r": "р", "s": "с", "t": "т", "u": "у", "v": "в",
           "w": "в", "x": "кс", "y": "ы", "z": "з"}
    return "".join(one.get(ch, ch) for ch in text)


def resolve_reference(conn, query_lower: str):
    """(строка species_reference | None, ключ, которым нашли | None). Сравнение в Python: SQLite lower() не знает кириллицу.
    Шаги: 1) точное совпадение с алиасом/названием; 2) смесь алфавитов → замена латинских двойников;
    3) только латиница → точное совпадение с латинским названием, затем транслитерация и нечёткий поиск по русским
    названиям (порог 80), затем нечёткий поиск по латинским названиям (порог 85); 4) кириллица → нечёткий поиск (порог 72)."""
    name_to_ref, alias_to_ref = load_lookup(conn)
    has_cyr, has_lat = bool(re.search(r"[а-яё]", query_lower)), bool(re.search(r"[a-z]", query_lower))

    def exact(k):
        return alias_to_ref.get(k) or name_to_ref.get(k)

    if exact(query_lower):
        return exact(query_lower), query_lower
    if has_cyr and has_lat:                                   # «дyб» с латинской y
        fixed = query_lower.translate(_HOMOGLYPHS)
        if exact(fixed):
            return exact(fixed), fixed
        query_lower, has_lat = fixed, bool(re.search(r"[a-z]", fixed))

    cyr_keys = [k for k in list(name_to_ref) + list(alias_to_ref) if re.search(r"[а-яё]", k)]
    lat_keys = [k for k in name_to_ref if re.search(r"[a-z]", k)]
    lat_by_latin = {(r["name_latin"] or "").lower(): r for r in name_to_ref.values() if r["name_latin"]}

    if has_lat and not has_cyr:                               # запрос латиницей
        if query_lower in lat_by_latin:
            return lat_by_latin[query_lower], query_lower
        tr = translit_ru(query_lower)
        if exact(tr):
            return exact(tr), tr
        m = process.extractOne(tr, cyr_keys, scorer=fuzz.WRatio, score_cutoff=80)
        if m:
            return (name_to_ref.get(m[0]) or alias_to_ref.get(m[0])), m[0]
        m = process.extractOne(query_lower, lat_keys + list(lat_by_latin), scorer=fuzz.WRatio, score_cutoff=85)
        if m:
            return (name_to_ref.get(m[0]) or lat_by_latin.get(m[0])), m[0]
        return None, None

    match = process.extractOne(query_lower, cyr_keys, scorer=fuzz.WRatio, score_cutoff=72)
    if match:
        key = match[0]
        return (name_to_ref.get(key) or alias_to_ref.get(key)), key
    return None, None


# ---------------------------------------------------------------- модели

class PlotIn(BaseModel):
    name: str
    forest_type: str | None = None
    area: float | None = None


class TreeIn(BaseModel):
    plot_id: int
    species_cache_id: int
    age: int | None = None
    height: float | None = None
    trunk_diameter: float | None = None


class SpeciesIn(BaseModel):
    name_ru: str
    name_latin: str | None = None
    family: str | None = None
    genus: str | None = None
    type: str | None = None
    foliage: str | None = None
    leaf_form: str | None = None
    fruit: str | None = None
    light: str | None = None
    habitat_tags: list[str] | None = None
    avg_height_m: str | None = None
    lifespan_years: str | None = None
    habitat: str | None = None
    description: str | None = None


class IdentifyIn(BaseModel):
    type: str | None = None
    foliage: str | None = None
    leaf_form: str | None = None
    fruit: str | None = None
    light: str | None = None
    height: str | None = None
    habitats: list[str] = []


# ---------------------------------------------------------------- живые источники (обычные HTTP-запросы, без ИИ)

def http_json(source: str, url: str, params: dict | None = None, timeout: float = 5.0):
    """Возвращает (данные | None, статус). Статус: ok/detail/ms — чтобы причина сбоя была видна, а не глоталась молча."""
    t0 = time.time()
    st = {"source": source, "ok": False, "detail": "", "ms": 0}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        st["ms"] = int((time.time() - t0) * 1000)
        if r.status_code == 404:
            st.update(ok=True, detail="не найдено")
            return None, st
        if r.status_code != 200:
            st["detail"] = f"HTTP {r.status_code}"
            return None, st
        data = r.json()
        st.update(ok=True, detail="OK")
        return data, st
    except requests.Timeout:
        st["detail"] = "таймаут"
    except requests.ConnectionError:
        st["detail"] = "нет соединения"
    except (requests.RequestException, ValueError) as e:
        st["detail"] = type(e).__name__
    st["ms"] = int((time.time() - t0) * 1000)
    return None, st


def merge_status(source: str, statuses: list[dict]) -> dict:
    """Итог по источнику за один поиск: ошибка, если были сбои и ничего не получено."""
    bad = [s for s in statuses if not s["ok"]]
    if bad:
        return {"source": source, "ok": False, "detail": bad[0]["detail"], "ms": bad[0]["ms"]}
    if statuses:
        return {"source": source, "ok": True, "detail": statuses[-1]["detail"], "ms": statuses[-1]["ms"]}
    return {"source": source, "ok": False, "detail": "не запрашивался", "ms": 0}


def license_label(url: str | None) -> str:
    if not url:
        return ""
    m = re.search(r"creativecommons\.org/licenses/([a-z\-]+)/([\d.]+)", url)
    if m:
        return f"CC {m.group(1).upper()} {m.group(2)}"
    if "publicdomain/zero" in url or url.upper() == "CC0_1_0":
        return "CC0"
    return url if len(url) < 24 else ""


def binomial(sci: str | None) -> str:
    toks = re.sub(r"[()×]", " ", sci or "").split()
    if not toks:
        return ""
    if len(toks) >= 2 and re.fullmatch(r"[a-z\-]+", toks[1]):
        return f"{toks[0]} {toks[1]}"
    return toks[0]


def trim_text(text: str | None, limit: int = 900) -> str | None:
    if not text:
        return None
    if len(text) <= limit:
        return text
    cut = text[:limit]
    end = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    return (cut[:end + 1] if end > 200 else cut.rstrip() + "…")


def fetch_gbif(query: str):
    data, st = http_json("gbif", "https://api.gbif.org/v1/species/search",
                         {"q": query, "rank": "SPECIES", "highertaxonKey": 6, "limit": 1})
    results = (data or {}).get("results", [])
    if not results:
        return None, st
    hit = results[0]
    return {
        "scientific_name": hit.get("scientificName") or hit.get("canonicalName"),
        "family": hit.get("family"), "genus": hit.get("genus"),
        "kingdom": hit.get("kingdom"), "phylum": hit.get("phylum"),
        "class_name": hit.get("class"), "order_name": hit.get("order"),
        "rank": hit.get("rank"), "key": hit.get("nubKey") or hit.get("key"),
    }, st


def fetch_gbif_image(key: int):
    """Фото вида из GBIF: сначала медиа таксона, затем фото наблюдений. Автор и лицензия сохраняются."""
    statuses = []
    data, st = http_json("gbif", f"https://api.gbif.org/v1/species/{key}/media", {"limit": 5})
    statuses.append(st)
    for m in (data or {}).get("results", []):
        if m.get("type") == "StillImage" and str(m.get("identifier", "")).startswith("http"):
            return m["identifier"], gbif_credit(m), merge_status("gbif", statuses)
    data, st = http_json("gbif", "https://api.gbif.org/v1/occurrence/search",
                         {"taxonKey": key, "mediaType": "StillImage", "limit": 5})
    statuses.append(st)
    for occ in (data or {}).get("results", []):
        for m in occ.get("media", []):
            if m.get("type") == "StillImage" and str(m.get("identifier", "")).startswith("http"):
                return m["identifier"], gbif_credit(m), merge_status("gbif", statuses)
    return None, None, merge_status("gbif", statuses)


def gbif_credit(m: dict) -> str:
    who = m.get("creator") or m.get("rightsHolder") or "автор не указан"
    lic = license_label(m.get("license"))
    return f"Фото (GBIF): {who}" + (f", {lic}" if lic else "")


def fetch_wikipedia(title: str, lang: str = "ru"):
    title = (title or "").strip().replace(" ", "_")
    if not title:
        return None, {"source": "wikipedia", "ok": True, "detail": "пусто", "ms": 0}
    title = title[:1].upper() + title[1:]
    data, st = http_json("wikipedia",
                         f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{requests.utils.quote(title, safe='')}",
                         {"redirect": "true"})
    if not data or data.get("type") == "disambiguation":
        return None, st
    img = (data.get("thumbnail") or {}).get("source") or (data.get("originalimage") or {}).get("source")
    if img:
        img = re.sub(r"/(\d+)px-", "/320px-", img, count=1)  # 640px не входит в стандартные размеры миниатюр Wikimedia
    return {
        "summary": trim_text(data.get("extract")),
        "image_url": img,
        "page_url": ((data.get("content_urls") or {}).get("desktop") or {}).get("page"),
        "lang": lang,
    }, st


def wiki_lookup(title_ru: str | None, latin: str):
    """Порядок: ru по латинскому названию (у ruwiki есть перенаправления) → ru по русскому → en по латинскому."""
    attempts, seen = [], set()
    for t, l in ((latin, "ru"), (title_ru, "ru"), (latin, "en")):
        if t and (t.lower(), l) not in seen:
            seen.add((t.lower(), l))
            attempts.append((t, l))
    best, statuses = None, []
    for t, l in attempts:
        w, st = fetch_wikipedia(t, l)
        statuses.append(st)
        if not st["ok"]:
            break  # источник недоступен/блокирует — остальные попытки бессмысленны
        if not w:
            continue
        if best is None:
            best = w
        elif not best.get("image_url") and w.get("image_url"):
            best["image_url"], best["image_lang"] = w["image_url"], w["lang"]
        if best.get("summary") and best.get("image_url"):
            break
    return best, merge_status("wikipedia", statuses)


def fetch_inat(query: str):
    data, st = http_json("inaturalist", "https://api.inaturalist.org/v1/taxa",
                         {"q": query, "per_page": 1, "locale": "ru", "is_active": "true"})
    results = (data or {}).get("results", [])
    if not results:
        return None, st
    r = results[0]
    photo = r.get("default_photo") or {}
    url = photo.get("medium_url") or photo.get("url")
    credit = f"Фото (iNaturalist): {photo.get('attribution')}" if photo.get("attribution") else "Фото: iNaturalist"
    return {"image_url": url, "image_credit": credit if url else None,
            "common_ru": r.get("preferred_common_name"), "name": r.get("name")}, st


def gather_live(display_name: str, latin_hint: str | None, title_ru: str | None):
    """Собирает данные из трёх живых источников; фото — по цепочке Wikipedia → iNaturalist → GBIF."""
    term = binomial(latin_hint) if latin_hint else display_name
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_g, f_i = ex.submit(fetch_gbif, term), ex.submit(fetch_inat, term)
        (gbif, st_g), (inat, st_i) = f_g.result(), f_i.result()

    sci = (gbif or {}).get("scientific_name") or latin_hint or (inat or {}).get("name") or ""
    wiki, st_w = wiki_lookup(title_ru, binomial(sci))

    image_url = image_credit = None
    if inat and inat.get("image_url"):
        image_url, image_credit = inat["image_url"], inat["image_credit"]
    elif wiki and wiki.get("image_url"):
        image_url = wiki["image_url"]
        image_credit = f"Фото: Википедия ({wiki.get('image_lang') or wiki.get('lang', 'ru')})"
    elif gbif and gbif.get("key"):
        image_url, image_credit, st_gm = fetch_gbif_image(gbif["key"])
        st_g = merge_status("gbif", [st_g, st_gm])

    sources = {"gbif": merge_status("gbif", [st_g]), "wikipedia": st_w, "inaturalist": merge_status("inaturalist", [st_i])}
    return {
        "gbif": gbif, "wiki": wiki, "inat": inat,
        "image_url": image_url, "image_credit": image_credit,
        "sources": sources,
        "online_ok": any(s["ok"] and s["detail"] == "OK" for s in sources.values()),
    }


# ---------------------------------------------------------------- вспомогательное

def cap(s: str | None) -> str:
    return (s[:1].upper() + s[1:]) if s else ""


def related_for(conn, ref):
    genus_rows, family_rows = [], []
    if ref["genus"]:
        genus_rows = conn.execute(
            "SELECT id, name_ru FROM species_reference WHERE genus=? AND id!=? ORDER BY (description IS NULL), name_ru LIMIT 8",
            (ref["genus"], ref["id"])).fetchall()
    if ref["family"]:
        family_rows = conn.execute(
            """SELECT id, name_ru FROM species_reference
               WHERE family=? AND id!=? AND (genus IS NULL OR genus != COALESCE(?, ''))
               ORDER BY (description IS NULL), name_ru LIMIT 8""",
            (ref["family"], ref["id"], ref["genus"])).fetchall()
    return {"genus": [dict(r) for r in genus_rows], "family": [dict(r) for r in family_rows]}


def finalize(conn, d: dict) -> dict:
    """Добавляет к результату поля из species_reference, родственные виды и статусы источников."""
    rid = d.get("reference_id")
    if rid:
        r = conn.execute("SELECT * FROM species_reference WHERE id=?", (rid,)).fetchone()
        if r:
            for k in ("common_en", "avg_height_m", "lifespan_years", "habitat", "type", "source_note",
                      "foliage", "leaf_form", "fruit", "light", "habitat_tags"):
                d[k] = r[k]
            d["related"] = related_for(conn, r)
    log = d.pop("source_log", None)
    if log:
        try:
            d["sources"] = json.loads(log)
        except ValueError:
            pass
    return d


# ---------------------------------------------------------------- кости (один бросок за сессию)

def dice_view(sess: dict) -> dict:
    lr = sess["last_roll"]
    return {"remaining": sess["remaining"], "rolled": sess["rolled"], "last_roll": lr,
            "article_no": (lr[0] + lr[1]) if lr else None, "is_double": bool(lr and lr[0] == lr[1])}


def bonus_pool(arts: dict) -> list[str]:
    return sorted((k for k in arts if k.startswith("bonus")), key=lambda k: int(re.sub(r"\D", "", k) or 0))


def pick_bonus(uid: str, die_value: int) -> str | None:
    """Бонус за дубль: статья выбирается по значению кубика, но уже показанные этому браузеру пропускаются.
    Пока не пройден весь круг (6 статей), одна и та же бонусная статья не повторяется."""
    pool = bonus_pool(load_articles())
    if not pool:
        return None
    start = (die_value - 1) % len(pool)
    order = pool[start:] + pool[:start]
    with get_db() as conn:
        seen = {r["article_no"] for r in conn.execute("SELECT article_no FROM seen_bonus WHERE uid=?", (uid,)).fetchall()}
        for k in order:
            if k not in seen:
                return k
        conn.execute("DELETE FROM seen_bonus WHERE uid=?", (uid,))   # круг пройден — начинаем новый
    return order[0]


@app.post("/api/dice/roll")
def roll_dice(sess: dict = Depends(session_dep)):
    if sess["rolled"]:
        raise HTTPException(409, "В этой сессии кости уже брошены")
    d1, d2 = random.randint(1, 6), random.randint(1, 6)
    bonus = pick_bonus(sess.get("uid", ""), d1) if d1 == d2 else None
    sess.update(rolled=True, remaining=d1 + d2, last_roll=[d1, d2], bonus_no=bonus)
    return {"d1": d1, "d2": d2, "sum": d1 + d2, **dice_view(sess)}


@app.get("/api/dice/status")
def dice_status(sess: dict = Depends(session_dep)):
    return dice_view(sess)


@app.post("/api/session/reset")
def reset_session(sess: dict = Depends(session_dep)):
    """Новая сессия — для показа преподавателю: можно бросить кости заново."""
    uid = sess.get("uid")
    sess.update(new_session_state())
    sess["uid"] = uid
    return dice_view(sess)


def load_articles() -> dict:
    if not ARTICLES_PATH.exists():
        return {}
    return {str(a["no"]): a for a in json.load(open(ARTICLES_PATH, encoding="utf-8"))}


@app.get("/api/hidden/current")
def hidden_current(sess: dict = Depends(session_dep)):
    """Скрытая статья открывается только тому, кто бросил кости: номер статьи = сумма кубиков;
    дубль открывает ещё и бонусную. Угадать адрес статьи без броска нельзя."""
    if not sess["rolled"]:
        raise HTTPException(403, "Сначала бросьте кости")
    arts, view = load_articles(), dice_view(sess)
    bonus, progress = None, None
    if view["is_double"] and sess.get("bonus_no"):
        bonus = arts.get(sess["bonus_no"])
        with get_db() as conn:
            conn.execute("INSERT OR IGNORE INTO seen_bonus (uid, article_no, seen_at) VALUES (?,?,?)",
                         (sess.get("uid", ""), sess["bonus_no"], int(time.time())))
            n = conn.execute("SELECT COUNT(*) AS c FROM seen_bonus WHERE uid=?", (sess.get("uid", ""),)).fetchone()["c"]
        progress = {"seen": n, "total": len(bonus_pool(arts))}
    return {"main": arts.get(str(view["article_no"])), "bonus": bonus, "bonus_progress": progress,
            "article_no": view["article_no"], "is_double": view["is_double"]}


# ---------------------------------------------------------------- диагностика источников

@app.get("/api/sources/check")
def check_sources():
    probes = [
        ("GBIF", "gbif", "https://api.gbif.org/v1/species/search", {"q": "Acer platanoides", "limit": 1}),
        ("Wikipedia", "wikipedia", "https://ru.wikipedia.org/api/rest_v1/page/summary/Acer_platanoides", None),
        ("iNaturalist", "inaturalist", "https://api.inaturalist.org/v1/taxa", {"q": "Acer platanoides", "per_page": 1}),
    ]
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = [(n, ex.submit(http_json, key, url, params)) for n, key, url, params in probes]
        out = []
        for name, f in futs:
            _, st = f.result()
            out.append({"name": name, "ok": st["ok"], "detail": st["detail"], "ms": st["ms"]})
    out.append({"name": "YouTube (авто-поиск)", "ok": VideosSearch is not None, "warn": VideosSearch is None, "ms": 0,
                "detail": "работает через парсер (бесплатно, без ключей)" if VideosSearch is not None
                else "нет пакета youtube-search-python — только свои видео"})
    return out


# ---------------------------------------------------------------- поиск породы

@app.get("/api/species/search")
def search_species(q: str, sess: dict = Depends(session_dep)):
    if not q or not q.strip():
        raise HTTPException(400, "Пустой запрос")
    if sess["remaining"] <= 0:
        raise HTTPException(429, "Поиски в этой сессии закончились — начните новую сессию, чтобы бросить кости снова"
                            if sess["rolled"] else "Сначала бросьте кости — они дают запас поисков")

    with get_db() as conn:
        q_lower = q.strip().lower()
        ref, matched_key = resolve_reference(conn, q_lower)
        if ref:
            display_name = ref["name_ru"].lower()
            corrected = matched_key if matched_key != q_lower else None
        else:
            display_name, corrected = q_lower, None

        cached = conn.execute(
            "SELECT * FROM species_cache WHERE query_name = ? ORDER BY fetched_at DESC LIMIT 1",
            (display_name,)).fetchone()
        now = int(time.time())
        if cached:
            age = now - cached["fetched_at"]
            empty = not cached["live_summary"] and not cached["wiki_image_url"]
            if age < CACHE_TTL_SECONDS and not (empty and age > RETRY_EMPTY_AFTER):
                sess["remaining"] -= 1
                result = finalize(conn, dict(cached))
                result["from_cache"], result["corrected_from"] = True, corrected
                return result

        latin_hint = ref["name_latin"] if ref else None
        title_ru = None
        if ref and not re.search(r"[A-Za-z]", ref["name_ru"]):
            title_ru = ref["name_ru"]
        elif not ref and not re.search(r"[A-Za-z]", display_name):
            title_ru = display_name

        live = gather_live(display_name, latin_hint, title_ru)
        gbif, wiki, inat = live["gbif"] or {}, live["wiki"] or {}, live["inat"] or {}

        values = {
            "reference_id": ref["id"] if ref else None, "query_name": display_name,
            "scientific_name": gbif.get("scientific_name") or latin_hint,
            "kingdom": gbif.get("kingdom") or ("Plantae" if ref else None),
            "phylum": gbif.get("phylum"), "class_name": gbif.get("class_name"), "order_name": gbif.get("order_name"),
            "family": gbif.get("family") or (ref["family"] if ref else None),
            "genus": gbif.get("genus") or (ref["genus"] if ref else None),
            "rank": gbif.get("rank") or ("SPECIES" if ref else None),
            "own_description": ref["description"] if ref else None,
            "live_summary": wiki.get("summary"),
            "wiki_image_url": live["image_url"], "image_credit": live["image_credit"],
            "wiki_url": wiki.get("page_url"), "common_ru": inat.get("common_ru"),
            "gbif_key": gbif.get("key"), "online_ok": int(live["online_ok"]), "fetched_at": now,
            "source_log": json.dumps(live["sources"], ensure_ascii=False),
        }
        if cached:  # обновляем ту же строку: id остаётся прежним, деревья на него ссылаются
            sets = ", ".join(f"{k}=?" for k in values)
            conn.execute(f"UPDATE species_cache SET {sets} WHERE id=?", [*values.values(), cached["id"]])
            row_id = cached["id"]
        else:
            cols = ", ".join(values)
            cur = conn.execute(f"INSERT INTO species_cache ({cols}) VALUES ({', '.join('?' * len(values))})",
                               list(values.values()))
            row_id = cur.lastrowid
        sess["remaining"] -= 1

        row = conn.execute("SELECT * FROM species_cache WHERE id=?", (row_id,)).fetchone()
        result = finalize(conn, dict(row))
        result["online_ok"] = bool(live["online_ok"])
        result["from_cache"], result["corrected_from"] = False, corrected
        return result


@app.get("/api/species")
def list_species():
    with get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM species_cache ORDER BY fetched_at DESC").fetchall()]


@app.get("/api/species/reference")
def list_reference():
    with get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM species_reference ORDER BY name_ru").fetchall()]


# ---------------------------------------------------------------- пополнение знаний

def clean(v):
    if v is None:
        return None
    v = str(v).strip()
    return v or None


@app.post("/api/species")
def create_species(sp: SpeciesIn):
    name = clean(sp.name_ru)
    if not name:
        raise HTTPException(400, "Укажите название породы")
    latin = clean(sp.name_latin)
    genus = clean(sp.genus) or (latin.split()[0] if latin else None)
    tags = ", ".join(t.strip() for t in (sp.habitat_tags or []) if t.strip()) or None
    with get_db() as conn:
        try:
            cur = conn.execute(
                """INSERT INTO species_reference
                   (name_ru, name_latin, family, genus, type, description, avg_height_m, lifespan_years,
                    habitat, source_note, foliage, leaf_form, fruit, light, habitat_tags)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (cap(name), latin, clean(sp.family), genus, clean(sp.type), clean(sp.description),
                 clean(sp.avg_height_m), clean(sp.lifespan_years), clean(sp.habitat),
                 "добавлено пользователем", clean(sp.foliage), clean(sp.leaf_form), clean(sp.fruit),
                 clean(sp.light), tags))
        except sqlite3.IntegrityError:
            raise HTTPException(409, "Порода с таким названием уже есть в базе")
        return {"id": cur.lastrowid, "name_ru": cap(name)}


@app.post("/api/species/from-cache/{cache_id}")
def save_from_cache(cache_id: int):
    """Знание, найденное вживую, становится записью собственной базы."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM species_cache WHERE id=?", (cache_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Результат поиска не найден")
        if row["reference_id"]:
            raise HTTPException(409, "Эта порода уже есть в базе")
        name = cap(row["query_name"])
        try:
            cur = conn.execute(
                """INSERT INTO species_reference (name_ru, name_latin, family, genus, source_note)
                   VALUES (?,?,?,?,?)""",
                (name, row["scientific_name"], row["family"], row["genus"],
                 "сохранено из живого поиска (GBIF / Wikipedia / iNaturalist)"))
        except sqlite3.IntegrityError:
            raise HTTPException(409, "Порода с таким названием уже есть в базе")
        conn.execute("UPDATE species_cache SET reference_id=? WHERE id=?", (cur.lastrowid, cache_id))
        return {"id": cur.lastrowid, "name_ru": name}


# ---------------------------------------------------------------- видео внутри сайта

ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")


def yt_key() -> str | None:
    k = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not k and YT_KEY_FILE.exists():
        k = YT_KEY_FILE.read_text(encoding="utf-8").strip()
    return k or None


def parse_youtube_id(text: str) -> str | None:
    text = (text or "").strip()
    if ID_RE.match(text):
        return text
    m = re.search(r"(?:youtu\.be/|youtube(?:-nocookie)?\.com/(?:watch\?(?:[^#]*&)?v=|embed/|shorts/|live/|v/))([A-Za-z0-9_-]{11})", text)
    return m.group(1) if m else None


def subject_key(subject: str) -> str:
    sub = (subject or "").strip()
    return (binomial(sub) if re.search(r"[A-Za-z]", sub) else sub).lower()


def yt_get(path: str, params: dict):
    """Официальный YouTube Data API v3. Возвращает (данные | None, причина ошибки | None)."""
    try:
        r = requests.get(f"https://www.googleapis.com/youtube/v3/{path}", params=params, headers=HEADERS, timeout=6)
        if r.status_code != 200:
            try:
                msg = r.json()["error"]["errors"][0]["reason"]   # quotaExceeded, keyInvalid, ...
            except Exception:
                msg = f"HTTP {r.status_code}"
            return None, msg
        return r.json(), None
    except requests.RequestException as e:
        return None, type(e).__name__


def iso_duration(iso: str | None) -> str | None:
    m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso or "")
    if not m:
        return None
    h, mi, se = (int(x or 0) for x in m.groups())
    return f"{h}:{mi:02d}:{se:02d}" if h else f"{mi}:{se:02d}"


def thumb_url(vid: str) -> str:
    return f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg"


def search_videos_api(conn, name_ru: str | None, latin: str):
    """Видео по породе (youtube-search-python, без ключей). Чтобы не получать только старые ролики, на каждый запрос
    делаются три поиска: «за последний год», «сначала новые» и обычный по релевантности. «ё» заменяется на «е»,
    потому что в заголовках пишут «клен». Дата публикации переводится из «3 weeks ago» в ISO."""
    if VideosSearch is None:
        return [], "не установлен пакет youtube-search-python"
    queries = []
    if name_ru and not re.search(r"[A-Za-z]", name_ru):
        clean = re.sub(r"\([^)]*\)", "", name_ru).strip().replace("ё", "е").replace("Ё", "Е")
        queries.append((clean, "ru"))
    if latin:
        queries.append((binomial(latin), "en"))
    if not queries:
        return [], None

    ckey = "v2|" + "|".join(f"{lang}:{q}" for q, lang in queries).lower()
    row = conn.execute("SELECT * FROM video_cache WHERE cache_key=?", (ckey,)).fetchone()
    if row and time.time() - row["fetched_at"] < VIDEO_CACHE_TTL:
        return json.loads(row["payload"]), None

    found, seen, err = [], set(), None
    for q, lang in queries:
        searches = [lambda q=q, lang=lang: CustomSearch(q, VideoUploadDateFilter.thisYear, limit=20, language=lang),
                    lambda q=q, lang=lang: CustomSearch(q, VideoSortOrder.uploadDate, limit=20, language=lang),
                    lambda q=q, lang=lang: VideosSearch(q, limit=6, language=lang)]
        for make in searches:
            try:
                results = make().result()
            except Exception as e:  # noqa: BLE001 — парсер неофициальный: сбой одного запроса не должен ронять сайт
                err = f"Ошибка парсера: {type(e).__name__} - {e}"
                continue
            for it in ((results or {}).get("result") or []):
                vid = it.get("id")
                if not vid or vid in seen:
                    continue
                seen.add(vid)
                raw_title = it.get("title")
                channel_data = it.get("channel")
                found.append({
                    "id": vid,
                    "title": html.unescape(raw_title) if raw_title else "Без названия",
                    "channel": channel_data.get("name") if isinstance(channel_data, dict) and channel_data.get("name") else "Неизвестный канал",
                    "published": it.get("publishedTime"), "lang": lang, "thumb": thumb_url(vid),
                    "duration": it.get("duration"), "source": "YouTube",
                })
    terms = fresh.build_terms(name_ru, latin)       # только по теме и не старше 2 лет
    found = [v for v in found
             if fresh.is_fresh({"published": fresh.rel_to_iso(v.get("published"))})
             and fresh.is_relevant({"title": v["title"], "snippet": ""}, terms)]
    found.sort(key=lambda v: fresh.rel_to_iso(v.get("published")) or "", reverse=True)   # новые сверху
    if found:
        conn.execute("INSERT OR REPLACE INTO video_cache (cache_key, payload, fetched_at) VALUES (?,?,?)",
                     (ckey, json.dumps(found, ensure_ascii=False), int(time.time())))
    return found, err


class VideoIn(BaseModel):
    subject: str
    url: str


@app.get("/api/videos")
def list_videos(subject: str, name: str = ""):
    sk = subject_key(subject)
    with get_db() as conn:
        own = [{"id": r["youtube_id"], "db_id": r["id"], "title": r["title"], "channel": r["channel"],
                "published": None, "lang": r["lang"], "thumb": thumb_url(r["youtube_id"]),
                "duration": None, "source": "своя база"}
               for r in conn.execute("SELECT * FROM species_video WHERE subject_key=? ORDER BY id DESC", (sk,)).fetchall()]
        latin = subject if re.search(r"[A-Za-z]", subject) else ""
        api, err = search_videos_api(conn, name or (subject if not latin else ""), latin)
    own_ids = {v["id"] for v in own}
    return {"videos": own + [v for v in api if v["id"] not in own_ids],
            "api_configured": True, "api_error": err}


@app.post("/api/videos")
def add_video(v: VideoIn):
    vid = parse_youtube_id(v.url)
    if not vid:
        raise HTTPException(400, "Не похоже на ссылку на видео YouTube")
    data, st = http_json("youtube", "https://www.youtube.com/oembed",
                         {"url": f"https://www.youtube.com/watch?v={vid}", "format": "json"})
    if data is None and st["ok"]:
        raise HTTPException(404, "Видео не найдено или закрыто автором")
    if data is None and st["detail"] == "HTTP 401":
        raise HTTPException(422, "Автор запретил встраивание — это видео нельзя показать на сайте")
    title = (data or {}).get("title") or f"Видео YouTube ({vid})"
    channel = (data or {}).get("author_name")
    lang = "ru" if re.search(r"[А-Яа-яЁё]", title) else "en"
    with get_db() as conn:
        try:
            cur = conn.execute("""INSERT INTO species_video (subject_key, youtube_id, title, channel, lang, added_at)
                                  VALUES (?,?,?,?,?,?)""", (subject_key(v.subject), vid, title, channel, lang, int(time.time())))
        except sqlite3.IntegrityError:
            raise HTTPException(409, "Это видео уже добавлено")
    return {"id": vid, "db_id": cur.lastrowid, "title": title, "channel": channel, "lang": lang}


@app.delete("/api/videos/{db_id}")
def delete_video(db_id: int):
    with get_db() as conn:
        conn.execute("DELETE FROM species_video WHERE id=?", (db_id,))
    return {"deleted": db_id}


# ---------------------------------------------------------------- определитель (экспертная система без ИИ)

HEIGHT_CLASSES = ["до 5 м", "5–15 м", "15–25 м", "выше 25 м"]
FEATURES = [  # ключ, подпись, вес
    ("type", "Тип", 3), ("leaf_form", "Лист", 3), ("fruit", "Плод", 3),
    ("foliage", "Листва", 2), ("height", "Высота", 2), ("habitats", "Место", 2), ("light", "Свет", 1),
]


def height_class(text: str | None) -> str | None:
    nums = [float(x.replace(",", ".")) for x in re.findall(r"\d+(?:[.,]\d+)?", text or "")]
    if not nums:
        return None
    m = sum(nums) / len(nums)
    return HEIGHT_CLASSES[0] if m <= 5 else HEIGHT_CLASSES[1] if m <= 15 else HEIGHT_CLASSES[2] if m <= 25 else HEIGHT_CLASSES[3]


def tags_of(r) -> set:
    return {t.strip() for t in (r["habitat_tags"] or "").split(",") if t.strip()}


@app.get("/api/identify/options")
def identify_options():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM species_reference WHERE leaf_form IS NOT NULL").fetchall()
    def distinct(col):
        return sorted({r[col] for r in rows if r[col]})
    habitats = sorted({t for r in rows for t in tags_of(r)})
    return {
        "considered": len(rows),
        "type": distinct("type"), "foliage": distinct("foliage"), "leaf_form": distinct("leaf_form"),
        "fruit": distinct("fruit"), "light": distinct("light"),
        "height": HEIGHT_CLASSES, "habitats": habitats,
    }


@app.post("/api/identify")
def identify(sel: IdentifyIn):
    """Определитель: сопоставляет выбранные признаки с признаками видов; результат объясняется
    (что совпало, что нет, что неизвестно). Не использует ИИ и сеть."""
    chosen = {k: v for k, v in sel.model_dump().items() if v}
    if not chosen:
        return {"items": [], "considered": 0, "selected": 0}
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM species_reference WHERE leaf_form IS NOT NULL").fetchall()

    items = []
    for r in rows:
        vals = {"type": r["type"], "leaf_form": r["leaf_form"], "fruit": r["fruit"], "foliage": r["foliage"],
                "light": r["light"], "height": height_class(r["avg_height_m"]), "habitats": tags_of(r)}
        total = got = lost = 0
        matched, mismatched, unknown = [], [], []
        for key, label, w in FEATURES:
            want = chosen.get(key)
            if not want:
                continue
            total += w
            v = vals[key]
            shown = ", ".join(want) if isinstance(want, list) else want
            if not v:
                unknown.append(f"{label}: {shown}")
            elif (isinstance(want, list) and v & set(want)) or (not isinstance(want, list) and v == want):
                got += w
                matched.append(f"{label}: {shown}")
            else:
                lost += w
                mismatched.append(f"{label}: {shown}")
        if got == 0:
            continue
        items.append({
            "id": r["id"], "name_ru": r["name_ru"], "name_latin": r["name_latin"], "family": r["family"],
            "percent": round(100 * got / total), "score": got - 0.5 * lost,
            "matched": matched, "mismatched": mismatched, "unknown": unknown,
        })
    items.sort(key=lambda x: (-x["score"], x["name_ru"]))
    return {"items": items[:12], "considered": len(rows), "selected": len(chosen)}


# ---------------------------------------------------------------- каталог и карточки

@app.get("/api/stats")
def stats():
    with get_db() as conn:
        one = lambda sql: conn.execute(sql).fetchone()["c"]
        fam = conn.execute(
            """SELECT family, COUNT(*) AS c FROM species_reference
               WHERE family IS NOT NULL AND family != '' GROUP BY family ORDER BY c DESC LIMIT 5""").fetchall()
        return {
            "species_total": one("SELECT COUNT(*) AS c FROM species_reference"),
            "curated_total": one("SELECT COUNT(*) AS c FROM species_reference WHERE description IS NOT NULL"),
            "identifiable_total": one("SELECT COUNT(*) AS c FROM species_reference WHERE leaf_form IS NOT NULL"),
            "trees_total": one("SELECT COUNT(*) AS c FROM trees"),
            "plots_total": one("SELECT COUNT(*) AS c FROM plots"),
            "searched_total": one("SELECT COUNT(*) AS c FROM species_cache"),
            "top_families": [{"family": r["family"], "count": r["c"]} for r in fam],
        }


@app.get("/api/species/reference/{ref_id}")
def reference_detail(ref_id: int):
    """Карточка вида: собственные данные + последний живой кэш. Не расходует «кости»."""
    with get_db() as conn:
        ref = conn.execute("SELECT * FROM species_reference WHERE id=?", (ref_id,)).fetchone()
        if not ref:
            raise HTTPException(404, "Вид не найден")
        cached = conn.execute("SELECT * FROM species_cache WHERE reference_id=? ORDER BY fetched_at DESC LIMIT 1",
                              (ref_id,)).fetchone()
        d = {
            "reference_id": ref["id"], "query_name": ref["name_ru"].lower(),
            "scientific_name": ref["name_latin"], "family": ref["family"], "genus": ref["genus"],
            "kingdom": "Plantae" if ref["family"] else None, "own_description": ref["description"],
            "online_ok": None, "has_live": False,
        }
        if cached:
            for k in ("kingdom", "phylum", "class_name", "order_name", "rank", "live_summary",
                      "wiki_image_url", "image_credit", "wiki_url", "common_ru", "gbif_key", "source_log"):
                d[k] = cached[k]
            d["scientific_name"] = cached["scientific_name"] or d["scientific_name"]
            d["family"] = cached["family"] or d["family"]
            d["genus"] = cached["genus"] or d["genus"]
            d["online_ok"], d["has_live"] = bool(cached["online_ok"]), True
        if not d.get("wiki_image_url"):
            img = conn.execute("SELECT * FROM species_image WHERE reference_id=?", (ref_id,)).fetchone()
            if img and img["image_url"]:
                d["wiki_image_url"], d["image_credit"] = img["image_url"], img["credit"]
        return finalize(conn, d)


@app.get("/api/species/reference/{ref_id}/image")
def reference_image(ref_id: int):
    """Фото для карточки (Wikipedia → iNaturalist → GBIF). Результат, в том числе отрицательный, кэшируется."""
    with get_db() as conn:
        ref = conn.execute("SELECT * FROM species_reference WHERE id=?", (ref_id,)).fetchone()
        if not ref:
            raise HTTPException(404, "Вид не найден")
        c = conn.execute("""SELECT wiki_image_url, image_credit FROM species_cache
                            WHERE reference_id=? AND wiki_image_url IS NOT NULL ORDER BY fetched_at DESC LIMIT 1""",
                         (ref_id,)).fetchone()
        if c:
            return {"image_url": c["wiki_image_url"], "credit": c["image_credit"], "cached": True}
        row = conn.execute("SELECT * FROM species_image WHERE reference_id=?", (ref_id,)).fetchone()
        now = int(time.time())
        if row:
            ttl = IMAGE_TTL_HIT if row["image_url"] else IMAGE_TTL_MISS
            if now - row["fetched_at"] < ttl:
                return {"image_url": row["image_url"], "credit": row["credit"], "cached": True}
        title_ru = None if re.search(r"[A-Za-z]", ref["name_ru"]) else ref["name_ru"]
        live = gather_live(ref["name_ru"].lower(), ref["name_latin"], title_ru)
        conn.execute("INSERT OR REPLACE INTO species_image (reference_id, image_url, credit, fetched_at) VALUES (?,?,?,?)",
                     (ref_id, live["image_url"], live["image_credit"], now))
        return {"image_url": live["image_url"], "credit": live["image_credit"], "cached": False}


@app.get("/api/species/catalog")
def species_catalog(page: int = 1, page_size: int = 24, q: str = "", kind: str = ""):
    page = max(page, 1)
    page_size = min(max(page_size, 1), 100)
    needle = q.strip().lower()
    with get_db() as conn:
        rows = conn.execute(
            """SELECT r.*,
                      COALESCE(
                        (SELECT wiki_image_url FROM species_cache c
                         WHERE c.reference_id = r.id AND c.wiki_image_url IS NOT NULL
                         ORDER BY c.fetched_at DESC LIMIT 1),
                        (SELECT image_url FROM species_image i WHERE i.reference_id = r.id)
                      ) AS image_url
               FROM species_reference r
               ORDER BY (r.description IS NULL), r.name_ru""").fetchall()
    items = [dict(r) for r in rows]
    if needle:
        items = [it for it in items if needle in " ".join(
            str(it.get(k) or "") for k in ("name_ru", "name_latin", "family", "genus", "common_en")).lower()]
    if kind:
        items = [it for it in items if (it.get("type") or "") == kind]
    total, offset = len(items), (page - 1) * page_size
    return {"items": items[offset:offset + page_size], "total": total, "page": page,
            "page_size": page_size, "has_more": offset + page_size < total}


# ---------------------------------------------------------------- участки и деревья

@app.get("/api/plots")
def list_plots():
    with get_db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM plots ORDER BY id DESC").fetchall()]


@app.post("/api/plots")
def create_plot(plot: PlotIn):
    with get_db() as conn:
        cur = conn.execute("INSERT INTO plots (name, forest_type, area) VALUES (?,?,?)",
                           (plot.name, plot.forest_type, plot.area))
        return {"id": cur.lastrowid, **plot.model_dump()}


@app.get("/api/trees")
def list_trees():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT t.id, t.age, t.height, t.trunk_diameter, t.added_at,
                   p.name AS plot_name, p.id AS plot_id,
                   s.query_name AS species_name, s.scientific_name
            FROM trees t
            JOIN plots p ON p.id = t.plot_id
            LEFT JOIN species_cache s ON s.id = t.species_cache_id
            ORDER BY t.id DESC""").fetchall()
        return [dict(r) for r in rows]


@app.post("/api/trees")
def create_tree(tree: TreeIn):
    with get_db() as conn:
        if not conn.execute("SELECT id FROM plots WHERE id=?", (tree.plot_id,)).fetchone():
            raise HTTPException(404, "Участок не найден")
        if not conn.execute("SELECT id FROM species_cache WHERE id=?", (tree.species_cache_id,)).fetchone():
            raise HTTPException(404, "Порода не найдена — сначала найдите её через поиск")
        cur = conn.execute(
            """INSERT INTO trees (plot_id, species_cache_id, age, height, trunk_diameter, added_at)
               VALUES (?,?,?,?,?,?)""",
            (tree.plot_id, tree.species_cache_id, tree.age, tree.height, tree.trunk_diameter, int(time.time())))
        return {"id": cur.lastrowid, **tree.model_dump()}


# ---------------------------------------------------------------- статика

init_db()
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


# ---------------------------------------------------------------- свежие данные из сети (все площадки)

FRESH_TTL = 60 * 60   # час: «свежее» не должно устаревать надолго


class LinkIn(BaseModel):
    subject: str
    url: str


@app.get("/api/fresh")
def fresh_data(subject: str, name: str = "", refresh: int = 0):
    """Свежие видео, статьи, новости, научные публикации и посты по породе из многих открытых источников.
    Результат кэшируется на час (таблица video_cache), у каждого источника виден статус."""
    sk = subject_key(subject)
    latin = subject if re.search(r"[A-Za-z]", subject) else ""
    name_ru = name or (subject if not latin else "")
    ckey = f"fresh:{sk}:{(name_ru or '').lower()}"
    with get_db() as conn:
        row = conn.execute("SELECT * FROM video_cache WHERE cache_key=?", (ckey,)).fetchone()
        if row and not refresh and time.time() - row["fetched_at"] < FRESH_TTL:
            data = json.loads(row["payload"])
            data["from_cache"] = True
        else:
            def yt():
                with get_db() as c2:        # соединение SQLite нельзя использовать из другого потока
                    vids, err = search_videos_api(c2, name_ru or None, latin)
                if err and not vids:
                    raise RuntimeError(err)
                return [fresh.item("video", "YouTube", v["title"], f"https://www.youtube.com/watch?v={v['id']}", thumb=v["thumb"],
                                   published=fresh.rel_to_iso(v.get("published")), embed=f"https://www.youtube-nocookie.com/embed/{v['id']}",
                                   snippet=v["channel"], lang=v["lang"], duration=v.get("duration")) for v in vids]
            data = fresh.gather_fresh(name_ru, latin, extra_video=yt)
            data["from_cache"] = False
            if any(x["ok"] and x["count"] for x in data["sources"]):
                conn.execute("INSERT OR REPLACE INTO video_cache (cache_key, payload, fetched_at) VALUES (?,?,?)",
                             (ckey, json.dumps(data, ensure_ascii=False), int(time.time())))
        data["my_links"] = [dict(r) for r in conn.execute(
            "SELECT id, url, platform, title, thumb FROM species_link WHERE subject_key=? ORDER BY id DESC", (sk,)).fetchall()]
    return data


@app.post("/api/links")
def add_link(link: LinkIn):
    """Добавить ссылку на ролик/пост с любой площадки (TikTok, Instagram, VK ...) в собственную базу."""
    url = link.url.strip()
    if not re.match(r"^https?://", url):
        raise HTTPException(400, "Нужна ссылка, начинающаяся с http:// или https://")
    meta = fresh.oembed(url) or {}
    with get_db() as conn:
        try:
            cur = conn.execute("INSERT INTO species_link (subject_key, url, platform, title, thumb, added_at) VALUES (?,?,?,?,?,?)",
                               (subject_key(link.subject), url, fresh.platform_of(url), meta.get("title"), meta.get("thumb"), int(time.time())))
        except sqlite3.IntegrityError:
            raise HTTPException(409, "Эта ссылка уже есть в базе")
    return {"id": cur.lastrowid, "platform": fresh.platform_of(url), "title": meta.get("title")}


@app.delete("/api/links/{link_id}")
def delete_link(link_id: int):
    with get_db() as conn:
        conn.execute("DELETE FROM species_link WHERE id=?", (link_id,))
    return {"ok": True}



@app.get("/")
def index():
    return FileResponse(BASE_DIR / "static" / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
