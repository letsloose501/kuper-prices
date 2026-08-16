#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Купер (kuper.ru) — сбор цен по всем магазинам сразу, без браузера.

Скрипт дёргает товарный API, отсеивает мусор из выдачи, считает цену за кг/л/шт
и печатает готовую отсортированную таблицу. Задуман как «руки» для ИИ-агента:
агент получает чистые данные и только собирает из них корзину и считает,
вместо того чтобы листать карточки товаров в браузере.

КАК СНИМАЕТСЯ 403. Нужны три вещи одновременно, поодиночке не работает:
  1) TLS-отпечаток Chrome: curl_cffi с impersonate="chrome" (без него 403 всегда);
  2) заголовки referer / origin / x-requested-with (без них 403 даже с отпечатком);
  3) прогрев: GET на главную, чтобы получить куки spid/spsc.
Живая сессия пользователя, вход в аккаунт и открытый браузер НЕ нужны.

Запуск:
    python scripts/kuper.py --discover --lat 55.75 --lon 37.62   # СНАЧАЛА: магазины по адресу
    python scripts/kuper.py "рис круглозерный:рис" "куриное филе:филе,куриное"
    python scripts/kuper.py --file queries.txt --json
    python scripts/kuper.py "яйцо с0:яйцо" --top 5 --stores METRO,Лента

Синтаксис запроса:  запрос : якорь1, якорь2 ! исключение1, исключение2
    якоря      — слова, которые ОБЯЗАНЫ быть в названии товара (иначе мусор:
                 «куриное филе» → сосиски, «сердечки» → воздушные шары);
    исключения — дополнительно к общему чёрному списку BAD.

Выход: markdown-таблица (по умолчанию) или --json для машинной обработки.

АДРЕС. У Купера география едет на координатах, а не на названии города: список
магазинов и их store_id зависят от точки доставки — по соседним адресам одной сети
бывают разные id. Поэтому в коде адреса нет вовсе: первый запуск обязательно
`--discover --lat … --lon …`. Он спросит у Купера, кто возит на эту точку, проверит
каждый id живым запросом и запишет подтверждённые в `kuper_stores.json` рядом со
скриптом. Этот файл личный — он в .gitignore, публиковать его не надо.

БУДЬ ВЕЖЛИВ. Не задирай --workers: чем больше одновременных запросов, тем скорее
поймаешь временную блокировку. Скрипт для личного сравнения цен, а не для выкачивания
каталога — не гоняй его в цикле.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

try:
    from curl_cffi import requests
except ImportError:
    sys.exit("нужен curl_cffi:  pip install curl_cffi")

BASE = "https://web.kuper.ru"
CITY = "moscow"      # только для прогрева куки; реальная география — из координат в CONFIG
CONFIG = Path(__file__).with_name("kuper_stores.json")   # адрес и магазины (личное, в .gitignore)
DEFAULT_WORKERS = 4  # одновременных запросов: 8 потоков с отдельным прогревом ловили 403
WORKERS = DEFAULT_WORKERS
HEAD_WORDS = 3       # в скольких первых словах названия обязан стоять главный якорь

# Магазинов в коде намеренно нет: store_id привязаны к точке доставки, а не к городу,
# и чужой список молча вернёт цены не того магазина. Список заводится через --discover.
STORES: dict[str, str] = {}

# Мусор, который поиск Купера подмешивает к продуктовым запросам.
# NB: 'молок' сюда писать нельзя — оно поймает и само молоко (разные корни у «молочный»).
BAD = [
    "семена", "семя", "грунт", "рассад", "удобрен",             # садовое
    "корм", "лакомств",                                          # зоотовары
    "игрушк", "нож ", "набор шар", "заколк", "брелок", "маска",  # непрод
    "шампун", "гель", "крем ",
    "нектар", "сок ", "коктейль", "молочн", "напиток",           # напитки
    "пряник", "печенье", "батончик", "конфет", "булочк", "кекс",  # кондитерка
    "круассан", "пирог", "торт", "вафл", "хлебцы", "палочки", "сухар",
    "макарон", "ригатони", "кетчуп", "чипсы", "каша", "мюсли", "гранол",
    "маринован", "консервирован", "солен", "сушен", "вялен",     # не свежее
    "творожок", "мороженое", "сырок", "десерт", "пудинг", "растишк", "агуша",
]

HEADERS = {
    "content-type": "application/json",
    "accept": "application/json",
    "referer": f"{BASE}/",
    "origin": BASE,
    "x-requested-with": "XMLHttpRequest",
}

_local = threading.local()
_warm_lock = threading.Lock()
_warm_cookies: dict[str, str] = {}


class Blocked(RuntimeError):
    """Купер ответил 403 — защита от ботов, а не «ничего не найдено».

    Разделять обязательно: молчаливый возврат пустого списка при 403 однажды
    подсунул таблицу, где все магазины «не отвечают», и чуть не затёр конфиг.
    """


def warm(force: bool = False) -> dict[str, str]:
    """Один прогрев на весь запуск: куки spid/spsc с главной, дальше их переиспользуем.

    Прогревать в каждом потоке нельзя — пачка одновременных заходов на главную
    выглядит для защиты как бот и приводит к 403 на всё.
    """
    global _warm_cookies
    with _warm_lock:
        if _warm_cookies and not force:
            return _warm_cookies
        s = requests.Session(impersonate="chrome")
        r = s.get(f"{BASE}/?city_name={CITY}", timeout=30)
        if r.status_code == 403:
            raise Blocked("Купер отдаёт 403 уже на главной")
        _warm_cookies = {k: v for k, v in s.cookies.items()}
        return _warm_cookies


def session():
    """По сессии на поток (curl_cffi не обещает потокобезопасность одной), но куки общие."""
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session(impersonate="chrome")
        for k, v in warm().items():
            s.cookies.set(k, v)
        _local.s = s
    return s


def search(query: str, store_id: str, retry: bool = True) -> list[dict]:
    """Доступные товары одного магазина. 403 → один прогрев заново, потом сдаёмся с Blocked."""
    s = session()          # снаружи try: Blocked обязан всплыть, а не утонуть в except ниже
    try:
        r = s.post(
            f"{BASE}/api/web/v1/products",
            data=json.dumps({"q": query, "store_id": str(store_id)}),
            headers=HEADERS, timeout=30,
        )
    except Exception:      # сетевой сбой одного магазина — не повод валить весь прогон
        return []
    if r.status_code == 403:
        if not retry:
            raise Blocked(f"403 на запросе «{query}» (магазин {store_id})")
        _local.s = None
        warm(force=True)
        time.sleep(2)
        return search(query, store_id, retry=False)
    if r.status_code != 200:
        return []
    try:
        return [p for p in (r.json().get("products") or []) if p.get("available")]
    except Exception:
        return []


def discover_stores(lat: float, lon: float) -> list[dict]:
    """Какие магазины доставляют по этим координатам.

    Полка `ui_type == "shop"` эндпоинта mweb/shelves отдаёт ровно то, что нужно:
    `id` — это и есть store_id для /products, плюс сеть и минимальная сумма заказа.
    Полка отдаёт первую десятку (`has_more`), но продуктовые сети в неё попадают.
    """
    r = session().get(
        f"{BASE}/api/v3/mweb/shelves?lat={lat}&lon={lon}&shelves_codes_to_exclude=alcohol",
        headers={k: v for k, v in HEADERS.items() if k != "content-type"}, timeout=30)
    if r.status_code != 200:
        raise SystemExit(f"Купер ответил {r.status_code} на список магазинов")
    shop = ((r.json().get("data") or {}).get("shelves_data") or {}).get("shop") or {}
    out = []
    for s in shop.values():
        retailer = s.get("retailer") or {}
        out.append({
            "id": str(s.get("id")),
            "name": retailer.get("name") or "?",
            "slug": retailer.get("slug") or "",
            "min_order": s.get("minimum_order_amount"),
        })
    return sorted(out, key=lambda x: x["name"])


VOL_RE = [
    (re.compile(r"([\d.]+)\s*кг"), "кг", 1.0),
    (re.compile(r"([\d.]+)\s*мл"), "л", 0.001),      # мл раньше «л», иначе «мл» съест «л»
    (re.compile(r"([\d.]+)\s*л"), "л", 1.0),
    (re.compile(r"([\d.]+)\s*г"), "кг", 0.001),
    (re.compile(r"([\d.]+)\s*шт"), "шт", 1.0),
]


def parse_volume(product: dict) -> tuple[str, float] | tuple[None, None]:
    """Объём товара → (базовая единица, количество в ней). Пример: «900 г» → ('кг', 0.9).

    Сначала строка human_volume (в ней видно л это или кг), затем численный
    grams_per_unit как запасной путь — поле у Купера заполнено почти всегда.
    """
    s = str(product.get("human_volume") or "").lower().replace(",", ".")
    for rx, base, mult in VOL_RE:
        m = rx.search(s)
        if m:
            try:
                return base, float(m.group(1)) * mult
            except ValueError:
                break
    g = product.get("grams_per_unit")
    if isinstance(g, (int, float)) and g > 0:
        return "кг", g / 1000.0
    return None, None


def offer(product: dict, store: str) -> dict:
    base, amount = parse_volume(product)
    price = product.get("price")
    per = round(price / amount, 1) if (base and amount and price) else None
    return {
        "store": store,
        "name": (product.get("name") or "")[:60],
        "price": price,
        "vol": product.get("human_volume") or "",
        "base": base or "?",
        "per": per,                                    # цена за кг/л/шт — по ней сортируем
        "discount": product.get("discount_percent") or 0,
    }


def filter_and_sort(raw_offers: list[dict], anchors: list[str], excludes: list[str],
                    top: int, no_bad: bool) -> list[dict]:
    """Отсев мусора и сортировка по цене за базовую единицу.

    Общая для обоих путей добычи (прямые запросы и сбор браузером) — чтобы результат
    не зависел от того, каким входом пришли данные.
    Каждый элемент raw_offers — товар Купера плюс ключ `store` с названием магазина.
    """
    bad = [] if no_bad else BAD + excludes
    offers = []
    for p in raw_offers:
        if not p.get("available", True):
            continue
        name = (p.get("name") or "").lower()
        if not all(a.lower() in name for a in anchors):
            continue
        # Первый якорь обязан стоять в начале названия. У Купера название начинается
        # с самого продукта («Творог рассыпчатый…», «Филе куриное…»), поэтому такой
        # разрез отсекает «Йогурт питьевой с ЧЕРНИКОЙ» на запрос «черника» и
        # «Халва в ШОКОЛАДНОЙ глазури» на «шоколад» — их якорь стоит в хвосте.
        if anchors and not any(anchors[0].lower() in w for w in name.split()[:HEAD_WORDS]):
            continue
        if any(b in name for b in bad):
            continue
        offers.append(offer(p, p.get("store", "?")))
    # по цене за базовую единицу; без единицы — в конец по абсолютной цене
    offers.sort(key=lambda o: (o["per"] is None, o["per"] if o["per"] is not None else o["price"]))
    return offers[:top]


def collect(query: str, anchors: list[str], excludes: list[str],
            stores: dict[str, str], top: int, depth: int, no_bad: bool) -> list[dict]:
    """Опросить все магазины по одному запросу и вернуть топ дешёвых за базовую единицу."""
    def one(item):
        store, sid = item
        return [dict(p, store=store) for p in search(query, sid)[:depth]]

    raw: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(WORKERS, len(stores))) as pool:
        for chunk in pool.map(one, stores.items()):
            raw.extend(chunk)
    return filter_and_sort(raw, anchors, excludes, top, no_bad)


def browser_snippet(queries: list[str], stores: dict[str, str], depth: int) -> str:
    """JS для сбора того же сырья в браузере — запасной вход, когда прямые запросы под 403.

    Выполняется в открытом Купере (Claude in Chrome → javascript_tool, REPL-семантика:
    top-level await, результат последнего выражения возвращается сам). Отдаёт JSON
    в том же виде, что ждёт --from-json: {запрос: [товар с полем store, ...]}.
    """
    plain = [parse_query(q)[0] for q in queries]
    return f"""// Сбор сырья по {len(plain)} запросам. Выполнить в открытом Купере;
// результат сам ляжет в буфер обмена → python kuper.py --from-clipboard <те же запросы>
const STORES = {json.dumps(stores, ensure_ascii=False)};
const QUERIES = {json.dumps(plain, ensure_ascii=False)};
const out = {{}};
for (const q of QUERIES) {{
  const acc = [];
  await Promise.all(Object.entries(STORES).map(async ([store, sid]) => {{
    try {{
      const r = await fetch('/api/web/v1/products', {{
        method: 'POST',
        headers: {{'content-type': 'application/json', 'accept': 'application/json'}},
        body: JSON.stringify({{q, store_id: String(sid)}})
      }});
      if (!r.ok) return;
      const j = await r.json();
      // только поля, которые нужны обработке — иначе дамп раздувается в сотни КБ
      for (const p of (j.products || []).slice(0, {depth})) acc.push({{
        store, name: p.name, price: p.price, human_volume: p.human_volume,
        grams_per_unit: p.grams_per_unit, discount_percent: p.discount_percent,
        available: p.available
      }});
    }} catch (e) {{}}
  }}));
  out[q] = acc;
}}
// Сырьё уезжает в буфер обмена, а не в ответ: дамп на десятки КБ обрезается по дороге.
// Запасной путь — window.__KUPER_RAW, если браузер не дал доступ к буферу.
window.__KUPER_RAW = JSON.stringify(out);
// Сырьё уходит мимо чата: дамп на ~100 КБ в ответе обрезается. Буфер обмена требует,
// чтобы вкладка была в фокусе — если не сработал, жми кнопку и получишь файл.
let copied = false;
try {{ await navigator.clipboard.writeText(window.__KUPER_RAW); copied = true; }} catch (e) {{}}
if (!copied) {{
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([window.__KUPER_RAW], {{type: 'application/json'}}));
  a.download = 'kuper_raw.json';
  a.textContent = '⬇ скачать kuper_raw.json';
  a.style.cssText = 'position:fixed;z-index:99999;top:12px;left:12px;padding:10px 14px;'
                  + 'background:#111;color:#fff;border-radius:8px;font:14px sans-serif';
  document.body.appendChild(a);
}}
JSON.stringify({{copied, queries: QUERIES.length,
                 bytes: window.__KUPER_RAW.length,
                 total: Object.values(out).reduce((n, a) => n + a.length, 0),
                 next: copied ? 'python kuper.py --from-clipboard <те же запросы>'
                              : 'кликни кнопку слева сверху, потом: '
                                + 'python kuper.py --from-json <файл> <те же запросы>'}})"""


def parse_query(raw: str) -> tuple[str, list[str], list[str]]:
    """«рис круглозерный : рис ! бурый» → ('рис круглозерный', ['рис'], ['бурый'])."""
    excludes: list[str] = []
    if "!" in raw:
        raw, exc = raw.split("!", 1)
        excludes = [w.strip().lower() for w in exc.split(",") if w.strip()]
    anchors: list[str] = []
    if ":" in raw:
        raw, anc = raw.split(":", 1)
        anchors = [w.strip().lower() for w in anc.split(",") if w.strip()]
    return raw.strip(), anchors, excludes


def load_config() -> dict:
    """kuper_stores.json: {address, lat, lon, stores:{сеть:id}, min_order:{}}.

    Старый плоский формат {"Магазин": "id"} тоже понимаем — чтобы файл,
    написанный руками, не ломал запуск.
    """
    if not CONFIG.exists():
        return {}
    data = json.loads(CONFIG.read_text(encoding="utf-8"))
    if "stores" not in data:
        return {"stores": data}
    return data


# Тупик, чтобы не изобретать заново (проверено 17.08.2026): передать дамп из браузера
# через локальный приёмник (python -m http.server на 127.0.0.1) НЕЛЬЗЯ — Chrome со
# страницы Купера рубит любой запрос на localhost («Failed to fetch») ещё до preflight,
# и заголовки CORS/Access-Control-Allow-Private-Network делу не помогают. Рабочие
# способы отдать сырьё скрипту — буфер обмена и файл, они ниже.


def read_clipboard() -> str:
    """Содержимое буфера обмена (Windows). Туда снипет из --emit-js кладёт сырьё."""
    import subprocess
    r = subprocess.run(["powershell", "-NoProfile", "-Command", "Get-Clipboard -Raw"],
                       capture_output=True, text=True, encoding="utf-8")
    data = (r.stdout or "").strip()
    if not data.startswith("{"):
        raise SystemExit("в буфере обмена не JSON — сперва выполни снипет из --emit-js в браузере")
    return data


def read_queries(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]


def render(results: dict[str, list[dict]]) -> str:
    out = []
    for query, offers in results.items():
        out.append(f"\n### {query}")
        if not offers:
            out.append("_ничего не найдено — ослабь якоря или проверь запрос_")
            continue
        out.append("| ₽/ед | цена | объём | магазин | товар |")
        out.append("|---:|---:|---|---|---|")
        for o in offers:
            per = f"{o['per']:.0f} ₽/{o['base']}" if o["per"] is not None else "—"
            skidka = f" −{o['discount']}%" if o["discount"] else ""
            out.append(f"| {per} | {o['price']:.0f}{skidka} | {o['vol']} | {o['store']} | {o['name']} |")
    return "\n".join(out)


def main() -> int:
    global WORKERS
    ap = argparse.ArgumentParser(
        description="Цены Купера по всем магазинам: фильтр, цена за кг/л/шт, сортировка.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='пример: python scripts/kuper.py "творог 5:творог" "рис:рис ! бурый"')
    ap.add_argument("queries", nargs="*", help="запрос : якорь1, якорь2 ! исключение1")
    ap.add_argument("--file", type=Path, help="файл со списком запросов (по одному в строке, # — комментарий)")
    ap.add_argument("--top", type=int, default=3, help="сколько дешёвых показать на запрос (по умолчанию 3)")
    ap.add_argument("--depth", type=int, default=30, help="сколько товаров смотреть в выдаче магазина (30)")
    ap.add_argument("--stores", help="только эти магазины, через запятую")
    ap.add_argument("--json", action="store_true", help="выдать JSON вместо таблицы")
    ap.add_argument("--no-bad", action="store_true", help="не применять чёрный список (отладка пустой выдачи)")
    ap.add_argument("--discover", action="store_true",
                    help="показать магазины по координатам и записать их в kuper_stores.json")
    ap.add_argument("--lat", type=float, help="широта точки доставки (для --discover)")
    ap.add_argument("--lon", type=float, help="долгота точки доставки (для --discover)")
    ap.add_argument("--address", default="", help="подпись адреса для kuper_stores.json")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                    help=f"сколько магазинов опрашивать разом (по умолчанию {DEFAULT_WORKERS}; "
                         "больше — выше риск словить 403)")
    ap.add_argument("--emit-js", action="store_true",
                    help="выдать JS для сбора тех же данных в браузере (запасной вход)")
    ap.add_argument("--from-json", type=Path,
                    help="взять сырьё из файла, собранного браузером, и обработать как обычно")
    ap.add_argument("--from-clipboard", action="store_true",
                    help="взять сырьё из буфера обмена (туда его кладёт снипет из --emit-js)")
    args = ap.parse_args()
    WORKERS = max(1, args.workers)

    cfg = load_config()

    if args.discover:
        lat = args.lat if args.lat is not None else cfg.get("lat")
        lon = args.lon if args.lon is not None else cfg.get("lon")
        if lat is None or lon is None:
            return print("нужны --lat и --lon точки доставки "
                         "(координаты адреса — из карт: правый клик → «что здесь»)") or 2

        found = discover_stores(lat, lon)
        # Полка отдаёт только первую десятку (has_more), и в неё попадают не все сети —
        # поэтому известные магазины не выбрасываем, а проверяем наравне с найденными.
        known = {**STORES, **(cfg.get("stores") or {})}
        by_id = {s["id"]: s for s in found}
        for name, sid in known.items():
            by_id.setdefault(str(sid), {"id": str(sid), "name": name, "slug": "",
                                        "min_order": (cfg.get("min_order") or {}).get(name)})

        # Живая проверка: магазин, который не отвечает на пробный запрос, в конфиг не пишем.
        def probe(s):
            try:
                return len(search("молоко", s["id"]))
            except Blocked:
                return -1                     # −1 = «не знаем», это не то же самое, что 0
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            counts = list(pool.map(probe, by_id.values()))

        print(f"Магазины по координатам {lat}, {lon}:\n")
        print("| store_id | сеть | товаров на пробу | мин. заказ | slug |")
        print("|---|---|---:|---:|---|")
        alive, unknown = {}, 0
        for s, n in sorted(zip(by_id.values(), counts), key=lambda p: -p[1]):
            mark = {0: "  ⛔ не отвечает", -1: "  ⚠️ проверка не прошла (403)"}.get(n, "")
            shown = "?" if n < 0 else n
            print(f"| {s['id']} | {s['name']}{mark} | {shown} | {s['min_order']} | {s['slug']} |")
            if n > 0:
                alive[s["name"]] = s["id"]
            elif n < 0:
                unknown += 1

        # Ничего не подтвердилось — прежний список ценнее пустого. Не трогаем файл.
        if not alive:
            print(f"\n⛔ Ни один магазин не подтверждён "
                  f"({'Купер блокирует запросы' if unknown else 'все ответили пусто'}) — "
                  f"{CONFIG.name} НЕ перезаписан, прежний список цел.")
            return 1

        CONFIG.write_text(json.dumps({
            "address": args.address or cfg.get("address", ""),
            "lat": lat, "lon": lon,
            "stores": alive,
            "min_order": {s["name"]: s["min_order"] for s in by_id.values() if s["name"] in alive},
            "verified": time.strftime("%Y-%m-%d"),
        }, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"\nПодтверждено магазинов: {len(alive)} — записаны в {CONFIG.name}."
              + (f" Не проверено из-за 403: {unknown} — прогони ещё раз позже." if unknown else ""))
        return 0

    raw = list(args.queries) + (read_queries(args.file) if args.file else [])
    if not raw:
        ap.print_help()
        return 2

    stores = cfg.get("stores") or dict(STORES)
    # Магазины нужны, только когда идём в сеть сами или печатаем снипет для браузера;
    # готовый дамп обрабатывается и без них.
    if not stores and not (args.from_json or args.from_clipboard):
        return print(
            f"Не задан ни один магазин: рядом со скриптом нет {CONFIG.name}.\n"
            "store_id привязаны к точке доставки, поэтому список надо завести под свой адрес:\n"
            "  python scripts/kuper.py --discover --lat <широта> --lon <долгота>\n"
            "Координаты адреса берутся из любых карт (правый клик → «что здесь»).") or 2
    if args.stores:
        want = {s.strip().lower() for s in args.stores.split(",")}
        known = ", ".join(stores)
        stores = {k: v for k, v in stores.items() if k.lower() in want}
        if not stores:
            return print("нет таких магазинов; в конфиге есть:", known) or 2

    if args.emit_js:
        print(browser_snippet(raw, stores, args.depth))
        return 0

    results: dict[str, list[dict]] = {}
    if args.from_json or args.from_clipboard:   # сырьё собрано браузером — обработка та же
        dump = json.loads(args.from_json.read_text(encoding="utf-8")
                          if args.from_json else read_clipboard())
        for item in raw:
            query, anchors, excludes = parse_query(item)
            results[query] = filter_and_sort(dump.get(query) or [], anchors, excludes,
                                             args.top, args.no_bad)
    else:
        try:
            for item in raw:
                query, anchors, excludes = parse_query(item)
                results[query] = collect(query, anchors, excludes, stores,
                                         args.top, args.depth, args.no_bad)
        except Blocked as e:
            print(f"⛔ Купер блокирует прямые запросы ({e}).\n"
                  "Данные не потеряны — собери их браузером и прогони через ту же обработку:\n"
                  f"  1) python {Path(__file__).name} --emit-js <те же запросы>  → выполнить в Купере\n"
                  f"  2) сохранить ответ в raw.json\n"
                  f"  3) python {Path(__file__).name} --from-json raw.json <те же запросы>\n"
                  "Либо подождать 10–15 минут: блокировка временная.", file=sys.stderr)
            return 3

    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=1))
    else:
        print(render(results))
        empty = [q for q, o in results.items() if not o]
        if empty:
            print(f"\n_пусто по запросам: {', '.join(empty)} — попробуй --no-bad или другие якоря_")
    return 0


if __name__ == "__main__":
    sys.exit(main())
