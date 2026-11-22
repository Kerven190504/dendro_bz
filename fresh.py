"""
Свежие данные из интернета по теме запроса (без ИИ).

Каждый источник — отдельная функция-«коннектор»: она делает один HTTP-запрос к открытому API и приводит
ответ к единому формату элемента:
    {kind: video|article|news|science|social, source, title, url, thumb, published (ISO), snippet, embed, lang}
Сбой одного источника не мешает остальным: статус каждого источника возвращается отдельно.

TikTok и Instagram не имеют открытого API поиска, поэтому для них формируются ссылки на их страницы поиска
(open-in), а конкретные ссылки на ролики можно добавить вручную (см. app.py: /api/links).
"""
import html
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, quote_plus

import requests

UA = "DendroKB/1.0 (university coursework project) python-requests"
HEADERS = {"User-Agent": UA, "Accept": "application/json"}
TIMEOUT = 6.0


def _get(url, params=None, as_text=False):
    r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    return r.text if as_text else r.json()


def _iso_from_ts(ts):
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError, OSError):
        return None


def _strip_tags(s):
    return html.unescape(re.sub(r"<[^>]+>", "", s or "")).strip()


def item(kind, source, title, url, **kw):
    return {"kind": kind, "source": source, "title": title or "Без названия", "url": url,
            "thumb": kw.get("thumb"), "published": kw.get("published"), "snippet": kw.get("snippet"),
            "embed": kw.get("embed"), "lang": kw.get("lang"), "duration": kw.get("duration")}


# ---------------------------------------------------------------- парсеры (чистые функции — проверяются без сети)

def parse_wikipedia(data, lang):
    out = []
    for r in ((data or {}).get("query") or {}).get("search", []):
        t = r.get("title")
        out.append(item("article", f"Wikipedia ({lang})", t, f"https://{lang}.wikipedia.org/wiki/{quote(t.replace(' ', '_'))}",
                        published=r.get("timestamp"), snippet=_strip_tags(r.get("snippet")), lang=lang))
    return out


def parse_commons(data):
    out = []
    pages = ((data or {}).get("query") or {}).get("pages", {}) or {}
    for p in pages.values():
        info = (p.get("imageinfo") or [{}])[0]
        out.append(item("video", "Wikimedia Commons", re.sub(r"^File:", "", p.get("title", "")), info.get("descriptionurl") or info.get("url"),
                        thumb=info.get("thumburl"), published=info.get("timestamp"), embed=None))
    return out


def parse_dailymotion(data):
    out = []
    for v in (data or {}).get("list", []):
        vid = v.get("id")
        out.append(item("video", "Dailymotion", v.get("title"), v.get("url") or f"https://www.dailymotion.com/video/{vid}",
                        thumb=v.get("thumbnail_360_url"), published=_iso_from_ts(v.get("created_time")),
                        embed=f"https://www.dailymotion.com/embed/video/{vid}", snippet=v.get("owner.screenname"),
                        duration=v.get("duration")))
    return out


def parse_peertube(data):
    out = []
    for v in (data or {}).get("data", []):
        ch = (v.get("channel") or {}).get("displayName")
        out.append(item("video", "PeerTube", v.get("name"), v.get("url"), thumb=v.get("thumbnailUrl"),
                        published=v.get("publishedAt"), embed=v.get("embedUrl"), snippet=ch, duration=v.get("duration")))
    return out


def parse_rutube(data):
    out = []
    for v in (data or {}).get("results", []):
        vid = v.get("id")
        out.append(item("video", "RuTube", v.get("title"), v.get("video_url") or f"https://rutube.ru/video/{vid}/",
                        thumb=v.get("thumbnail_url"), published=v.get("created_ts") or v.get("publication_ts"),
                        embed=f"https://rutube.ru/play/embed/{vid}", snippet=(v.get("author") or {}).get("name")))
    return out


def parse_archive(data):
    out = []
    for d in ((data or {}).get("response") or {}).get("docs", []):
        i = d.get("identifier")
        t = d.get("title")
        out.append(item("video", "Internet Archive", t[0] if isinstance(t, list) else t, f"https://archive.org/details/{i}",
                        thumb=f"https://archive.org/services/img/{i}", published=d.get("date"),
                        embed=f"https://archive.org/embed/{i}"))
    return out


def parse_openalex(data):
    out = []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for w in (data or {}).get("results", []):
        d = w.get("publication_date")
        if d and d > today:
            continue
        out.append(item("science", "OpenAlex", w.get("title"), w.get("doi") or w.get("id"), published=d))
    return out


def parse_news_rss(text, lang):
    out = []
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return out
    for it in root.iter("item"):
        pub = it.findtext("pubDate")
        try:
            pub = parsedate_to_datetime(pub).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if pub else None
        except (TypeError, ValueError):
            pub = None
        out.append(item("news", "Google News", html.unescape(it.findtext("title") or ""), it.findtext("link"),
                        published=pub, snippet=it.findtext("source"), lang=lang))
    return out


def parse_reddit(data):
    out = []
    for c in ((data or {}).get("data") or {}).get("children", []):
        d = c.get("data") or {}
        thumb = d.get("thumbnail") if str(d.get("thumbnail", "")).startswith("http") else None
        out.append(item("social", f"Reddit r/{d.get('subreddit')}", d.get("title"), "https://www.reddit.com" + (d.get("permalink") or ""),
                        thumb=thumb, published=_iso_from_ts(d.get("created_utc")), snippet=(d.get("selftext") or "")[:200]))
    return out



# ---------------------------------------------------------------- отбор: только по теме и не старше 2 лет

MAX_AGE_DAYS = 730
CONTEXT = re.compile(r"дерев|лес\b|лесн|растен|сад\b|садов|посадк|ствол|листв|порода|ботани|парк|аллея|насажд|кустарник|"
                     r"\btrees?\b|wood|forest|botan|plant|arbor|species|shrub|leaf|leaves|bark", re.I)
_END = (r"(?:ого|его|ому|ему|ыми|ими|ами|ями|ых|их|ым|им|ую|юю|ый|ий|ой|ая|яя|ое|ее|ые|ие|ов|ев|ом|ем|ах|ях|ей|"
        r"ью|ия|ию|ии|а|я|ы|и|у|ю|е|ь)?")
_ADJ_END = ("ый", "ий", "ой", "ая", "яя", "ое", "ее", "ые", "ие")


def _stem_rx(word):
    if word.endswith(_ADJ_END):
        base = word[:-2]
    elif word[-1] in "аяыиуюеоьй":
        base = word[:-1]
    else:
        base = word
    return re.escape(base) + _END


def build_terms(name_ru, latin):
    """Шаблоны для проверки «текст действительно про эту породу»."""
    ru = re.sub(r"\([^)]*\)", "", name_ru or "").strip().lower().replace("ё", "е")
    ru = ru if ru and not re.search(r"[a-z]", ru) else ""
    words = ru.split()
    lat_words = re.findall(r"[A-Za-z]+", latin or "")
    lat_words = [w.lower() for w in lat_words if w.lower() not in ("spp", "sp", "subsp", "var", "x")][:2]
    strong, noun = [], None
    if len(lat_words) == 2:
        strong.append(re.compile(rf"\b{lat_words[0]}\s+{lat_words[1]}\b", re.I))
    if lat_words and len(lat_words[0]) >= 5:
        strong.append(re.compile(rf"\b{lat_words[0]}\b", re.I))
    if len(words) >= 2:   # «акация белая» / «белая акация», с любыми окончаниями
        a, b = _stem_rx(words[0]), _stem_rx(words[1])
        strong.append(re.compile(rf"\b{a}\W+{b}\b|\b{b}\W+{a}\b", re.I))
    if words:
        noun = re.compile(rf"\b{_stem_rx(words[0])}\b", re.I)   # само существительное: нужно ещё слово о деревьях
    return strong, noun


def is_relevant(it, terms):
    strong, noun = terms
    text = f"{it.get('title') or ''} {it.get('snippet') or ''}".replace("ё", "е").replace("Ё", "Е")
    if any(rx.search(text) for rx in strong):
        return True
    return bool(noun and noun.search(text) and CONTEXT.search(text))


_REL_UNITS = {"second": 0, "minute": 0, "hour": 0, "day": 1, "week": 7, "month": 30, "year": 365}


def rel_to_iso(text):
    """«2 years ago» → примерная дата ISO (для YouTube, который отдаёт относительное время)."""
    m = re.search(r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago", text or "", re.I)
    if not m:
        return None
    days = int(m.group(1)) * _REL_UNITS[m.group(2).lower()]
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")


def cutoff_date():
    return (datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)).strftime("%Y-%m-%d")


def is_fresh(it):
    d = (it.get("published") or "")[:10]
    return bool(re.match(r"\d{4}-\d{2}-\d{2}$", d)) and cutoff_date() <= d <= datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------- коннекторы (по одному запросу на источник)

def c_wikipedia(q, lang):
    return parse_wikipedia(_get(f"https://{lang}.wikipedia.org/w/api.php", {
        "action": "query", "list": "search", "srsearch": q, "srlimit": 10, "srsort": "just_match", "format": "json", "utf8": 1}), lang)


def c_commons(q):
    return parse_commons(_get("https://commons.wikimedia.org/w/api.php", {
        "action": "query", "generator": "search", "gsrsearch": f"{q} filetype:video", "gsrnamespace": 6, "gsrlimit": 20,
        "prop": "imageinfo", "iiprop": "url|timestamp", "iiurlwidth": 320, "format": "json"}))


def c_dailymotion(q):
    return parse_dailymotion(_get("https://api.dailymotion.com/videos", {
        "search": q, "sort": "recent", "limit": 30, "created_after": int(time.time()) - MAX_AGE_DAYS * 86400, "fields": "id,title,url,thumbnail_360_url,created_time,owner.screenname,duration"}))


def c_peertube(q):
    return parse_peertube(_get("https://sepiasearch.org/api/v1/search/videos", {"search": q, "sort": "-publishedAt", "count": 30, "startDate": cutoff_date() + "T00:00:00Z"}))


def c_rutube(q):
    return parse_rutube(_get("https://rutube.ru/api/search/video/", {"query": q, "format": "json"}))


def c_archive(q):
    return parse_archive(_get("https://archive.org/advancedsearch.php", {
        "q": f"({q}) AND mediatype:movies AND date:[{cutoff_date()} TO 9999]", "fl[]": ["identifier", "title", "date"], "rows": 20, "sort[]": "date desc", "output": "json"}))


def c_openalex(q):
    return parse_openalex(_get("https://api.openalex.org/works", {
        "search": q, "filter": f"from_publication_date:{cutoff_date()}", "sort": "publication_date:desc", "per-page": 25, "select": "id,title,publication_date,doi"}))


def c_news(q, lang):
    loc = {"ru": ("ru", "BY", "BY:ru"), "en": ("en-US", "US", "US:en")}[lang]
    return parse_news_rss(_get("https://news.google.com/rss/search", {"q": f"{q} when:2y", "hl": loc[0], "gl": loc[1], "ceid": loc[2]}, as_text=True), lang)[:30]


def c_reddit(q):
    return parse_reddit(_get("https://www.reddit.com/search.json", {"q": q, "sort": "new", "limit": 25, "t": "year"}))


def social_links(q, tag):
    """Платформы без открытого поиска: ссылки на страницы поиска (открываются в новой вкладке)."""
    qq, tg = quote_plus(q), quote(re.sub(r"\W+", "", tag or q).lower())
    return [
        ("TikTok", f"https://www.tiktok.com/search?q={qq}"),
        ("Instagram", f"https://www.instagram.com/explore/tags/{tg}/"),
        ("Pinterest", f"https://www.pinterest.com/search/pins/?q={qq}"),
        ("X (Twitter)", f"https://x.com/search?q={qq}&f=live"),
        ("VK Видео", f"https://vk.com/video?q={qq}"),
        ("Facebook", f"https://www.facebook.com/search/videos?q={qq}"),
    ]


def oembed(url):
    """Название и миниатюра по ссылке на ролик: TikTok / Vimeo / YouTube — через публичный oEmbed. Instagram требует токен."""
    ep = None
    if "tiktok.com" in url:
        ep = "https://www.tiktok.com/oembed"
    elif "vimeo.com" in url:
        ep = "https://vimeo.com/api/oembed.json"
    elif "youtube.com" in url or "youtu.be" in url:
        ep = "https://www.youtube.com/oembed"
    if not ep:
        return None
    try:
        d = _get(ep, {"url": url, "format": "json"})
        return {"title": d.get("title"), "author": d.get("author_name"), "thumb": d.get("thumbnail_url")}
    except Exception:  # noqa: BLE001
        return None


def platform_of(url):
    host = re.sub(r"^https?://(www\.|m\.)?", "", url.lower()).split("/")[0]
    for key, name in [("tiktok", "TikTok"), ("instagram", "Instagram"), ("youtu", "YouTube"), ("vimeo", "Vimeo"),
                      ("vk.com", "VK"), ("facebook", "Facebook"), ("x.com", "X"), ("twitter", "X"), ("rutube", "RuTube"),
                      ("dailymotion", "Dailymotion"), ("pinterest", "Pinterest")]:
        if key in host:
            return name
    return host or "Ссылка"


# ---------------------------------------------------------------- сборка

def gather_fresh(name_ru, latin, extra_video=None):
    """Параллельно опрашивает источники, оставляет только материалы, в тексте которых есть именно эта порода,
    и только за последние 2 года. extra_video — функция, возвращающая элементы YouTube."""
    ru = re.sub(r"\([^)]*\)", "", name_ru or "").strip().replace("ё", "е").replace("Ё", "Е")
    ru = ru if ru and not re.search(r"[A-Za-z]", ru) else None
    lw = [w for w in re.findall(r"[A-Za-z]+", latin or "") if w.lower() not in ("spp", "sp", "subsp", "var", "x")][:2]
    lat = " ".join(lw) or None
    terms = build_terms(name_ru, latin)
    q_ru = f'"{ru}"' if ru else None                   # точная фраза, а не набор отдельных слов
    q_lat = f'"{lat}"' if lat else None
    q_main = lat or ru or ""

    jobs = []
    if ru:
        jobs += [("Wikipedia (ru)", lambda: c_wikipedia(f'{q_ru} OR {q_lat}' if q_lat else q_ru, "ru")),
                 ("Google News (ru)", lambda: c_news(q_ru, "ru")), ("RuTube", lambda: c_rutube(ru + (" дерево" if len(ru.split()) == 1 else ""))),
                 ("Dailymotion", lambda: c_dailymotion(q_ru))]
    if lat:
        jobs += [("Wikipedia (en)", lambda: c_wikipedia(q_lat, "en")), ("Google News (en)", lambda: c_news(q_lat, "en")),
                 ("Wikimedia Commons", lambda: c_commons(lat)), ("PeerTube", lambda: c_peertube(q_lat)),
                 ("Internet Archive", lambda: c_archive(f'"{lat}"')), ("OpenAlex", lambda: c_openalex(lat)),
                 ("Reddit", lambda: c_reddit(q_lat))]
    if extra_video:
        jobs.append(("YouTube", extra_video))

    items, sources = [], []

    def run(job):
        nm, fn = job
        t0 = time.time()
        try:
            raw = fn() or []
            res = [i for i in raw if is_fresh(i) and is_relevant(i, terms)][:8]
            det = "OK" if res else (f"найдено {len(raw)}, но не по теме или старше 2 лет" if raw else "ничего не найдено")
            return nm, res, {"source": nm, "ok": True, "detail": det, "count": len(res), "raw": len(raw), "ms": int((time.time() - t0) * 1000)}
        except Exception as e:  # noqa: BLE001 — сбой одного источника не должен ронять остальные
            d = str(e) if isinstance(e, RuntimeError) else ("таймаут" if isinstance(e, requests.Timeout) else "нет соединения" if isinstance(e, requests.ConnectionError) else type(e).__name__)
            return nm, [], {"source": nm, "ok": False, "detail": d, "count": 0, "raw": 0, "ms": int((time.time() - t0) * 1000)}

    with ThreadPoolExecutor(max_workers=8) as ex:
        for nm, res, st in ex.map(run, jobs):
            items += res
            sources.append(st)

    seen, uniq = set(), []                              # одинаковые ссылки и заголовки — один раз
    for it in sorted(items, key=lambda x: x["published"] or "", reverse=True):
        k = (it["url"] or "", (it["title"] or "").lower())
        if k[0] in seen or k[1] in seen:
            continue
        seen.update(k)
        uniq.append(it)
    return {"items": uniq, "sources": sources, "max_age_days": MAX_AGE_DAYS,
            "social_links": [{"name": n, "url": u} for n, u in social_links(q_main or "дерево", lat or ru)]}
