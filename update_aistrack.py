#!/usr/bin/env python3
"""
Робот обновления трека судна: aisstream.io -> geojson/aistrack.geojson в папке Google Drive.

Запускается по расписанию (GitHub Actions, каждые 5 минут). За один запуск:
  1. читает config.json из папки VesselTrack (судно, MMSI/IMO, период наблюдения);
  2. если сейчас вне периода наблюдения — ничего не делает;
  3. слушает поток АИС aisstream.io по MMSI судна LISTEN_SECONDS секунд;
  4. дописывает новые позиции в aistrack.geojson (формат как у выгрузки с VesselFinder)
     и загружает файл обратно в Drive.

Переменные окружения:
  GOOGLE_SA_JSON     — содержимое JSON-ключа сервисного аккаунта Google
  DRIVE_FOLDER_ID    — ID (или ссылка) папки VesselTrack
  AISSTREAM_API_KEY  — ключ aisstream.io
  RUN_MINUTES        — длительность одного запуска, минуты (по умолчанию 330)
  CYCLE_SECONDS      — как часто записывать накопленное в Drive (по умолчанию 300)

Локальная проверка без сети:
  python update_aistrack.py --offline config.json aistrack.geojson messages.jsonl
"""
import asyncio
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone

DEFAULT_ZONE = timezone(timedelta(hours=3))
TRACK_FILE = "aistrack.geojson"
GEOJSON_FOLDER = "geojson"
CONFIG_FILE = "config.json"
POSITION_TYPES = ["PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"]


def log(msg):
    print(msg, flush=True)


def warn(msg):
    # отображается как предупреждение в интерфейсе GitHub Actions
    print(f"::warning::{msg}", flush=True)


# ---------------------------------------------------------------- конфиг

def parse_zone(s):
    if not s:
        return DEFAULT_ZONE
    m = re.fullmatch(r"\s*(?:UTC)?\s*([+-])(\d{1,2}):?(\d{2})?\s*", str(s))
    if not m:
        return DEFAULT_ZONE
    sign = 1 if m.group(1) == "+" else -1
    return timezone(sign * timedelta(hours=int(m.group(2)), minutes=int(m.group(3) or 0)))


def parse_local_time(s, zone):
    """Время из конфига: с явным поясом — как есть, без пояса — в поясе zone. Возвращает unix-секунды."""
    if s is None:
        return None
    t = str(s).strip().replace(" ", "T")
    if not t:
        return None
    try:
        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=zone)
        return int(dt.timestamp())
    except ValueError:
        pass
    for fmt in ("%d.%m.%YT%H:%M:%S", "%d.%m.%YT%H:%M", "%d.%m.%Y"):
        try:
            return int(datetime.strptime(t, fmt).replace(tzinfo=zone).timestamp())
        except ValueError:
            pass
    raise ValueError(f"не удалось разобрать время «{s}»")


def read_config(text):
    c = json.loads(text)
    zone = parse_zone(c.get("timezone"))
    v = c.get("vessel") or c
    obs = c.get("observation") or c
    mmsi = str(v.get("mmsi") or "").strip()
    if not re.fullmatch(r"\d{9}", mmsi):
        raise ValueError(f"в config.json нет корректного MMSI (9 цифр): «{mmsi}»")
    imo = str(v.get("imo") or "").strip() or None
    ais = c.get("ais") or {}
    return {
        "name": v.get("name") or "Судно",
        "imo": imo,
        "mmsi": mmsi,
        "from": parse_local_time(obs.get("from"), zone),
        "to": parse_local_time(obs.get("to"), zone),
        "min_interval": int(float(ais.get("min_interval_min", 5)) * 60),
    }


# ---------------------------------------------------------------- АИС

def parse_ais_time(s):
    # "2026-10-17 17:20:01.987654321 +0000 UTC"
    m = re.match(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", s or "")
    if not m:
        return None
    return int(datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}+00:00").timestamp())


def _meta(msg):
    for k in ("MetaData", "Metadata", "metadata", "metaData"):
        if isinstance(msg.get(k), dict):
            return msg[k]
    return {}


def _body(msg):
    """Тело сообщения о позиции: по MessageType или по первому известному ключу внутри Message."""
    m = msg.get("Message") or {}
    t = msg.get("MessageType")
    if t in POSITION_TYPES and isinstance(m.get(t), dict):
        return m[t]
    for k in POSITION_TYPES:
        if isinstance(m.get(k), dict):
            return m[k]
    return None


def msg_mmsi(msg):
    meta = _meta(msg)
    v = meta.get("MMSI") or meta.get("mmsi")
    if not v:
        b = _body(msg) or {}
        v = b.get("UserID")
    return str(v) if v else None


def extract_position(msg, received_ts=None):
    """Сообщение aisstream -> позиция или None. Время — из MetaData.time_utc, иначе время приёма."""
    body = _body(msg)
    if body is None:
        return None
    meta = _meta(msg)
    lat = body.get("Latitude", meta.get("latitude"))
    lon = body.get("Longitude", meta.get("longitude"))
    ts = parse_ais_time(meta.get("time_utc")) or received_ts
    if lat is None or lon is None or ts is None:
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
        return None  # 91/181 — «нет данных»
    sog = body.get("Sog")
    cog = body.get("Cog")
    hdg = body.get("TrueHeading")
    return {
        "ts": ts,
        "lon": round(float(lon), 5),
        "lat": round(float(lat), 5),
        "sog": None if sog is None or sog >= 102.2 else round(float(sog), 1),
        "cog": None if cog is None or cog >= 360 else round(float(cog), 1),
        "heading": None if hdg is None or hdg >= 360 else int(hdg),
        "nav": body.get("NavigationalStatus"),
    }


async def listen(api_key, mmsi, seconds):
    import websockets  # pip install websockets

    positions, static = [], []
    unknown = 0
    sub = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FiltersShipMMSI": [mmsi],
        "FilterMessageTypes": POSITION_TYPES + ["ShipStaticData"],
    }
    loop = asyncio.get_running_loop()
    end = loop.time() + seconds
    async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=30) as ws:
        await ws.send(json.dumps(sub))
        while True:
            left = end - loop.time()
            if left <= 0:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=left)
            except asyncio.TimeoutError:
                break
            msg = json.loads(raw)
            if "error" in msg:
                raise RuntimeError(f"aisstream: {msg['error']}")
            if msg.get("MessageType") == "ShipStaticData":
                static.append(msg["Message"]["ShipStaticData"])
                continue
            p = extract_position(msg, int(datetime.now(timezone.utc).timestamp()))
            if p:
                positions.append(p)
            elif unknown < 3:
                unknown += 1
                warn(f"Сообщение не разобрано: {raw[:400]}")
    return positions, static


# ---------------------------------------------------------------- трек

def load_points(text):
    if not text or not text.strip():
        return [], {}
    gj = json.loads(text)
    pts, line_props = [], {}
    for f in gj.get("features", []):
        g = f.get("geometry") or {}
        pr = f.get("properties") or {}
        if g.get("type") == "LineString":
            line_props = pr
        elif g.get("type") == "Point" and "ts" in pr:
            lon, lat = g["coordinates"][:2]
            pts.append({
                "ts": int(pr["ts"]), "lon": lon, "lat": lat,
                "sog": pr.get("sog_kn"), "cog": pr.get("cog_deg"),
                "heading": pr.get("heading"), "nav": pr.get("nav_status"),
                "src": pr.get("src"),
            })
    return pts, line_props


PATCH_FILE = "vf_patch.json"


def load_patch(text):
    """vf_patch.json от задачи VesselFinder: {"points": [[ts, lon, lat, sog, cog], ...]}."""
    if not text or not text.strip():
        return []
    out = []
    for r in json.loads(text).get("points", []):
        try:
            out.append({"ts": int(r[0]), "lon": round(float(r[1]), 5), "lat": round(float(r[2]), 5),
                        "sog": r[3] if len(r) > 3 else None, "cog": r[4] if len(r) > 4 else None,
                        "src": "vesselfinder"})
        except (TypeError, ValueError, IndexError):
            continue
    return out


def fill_gaps(existing, patch, tolerance=120):
    """Добавляет точки VesselFinder, которых нет в треке (нет точки ближе tolerance секунд)."""
    if not patch:
        return existing, 0
    ts_sorted = sorted(p["ts"] for p in existing)
    import bisect
    added = []
    for p in patch:
        i = bisect.bisect_left(ts_sorted, p["ts"])
        near = [ts_sorted[j] for j in (i - 1, i) if 0 <= j < len(ts_sorted)]
        if any(abs(t - p["ts"]) <= tolerance for t in near):
            continue
        added.append(p)
    return existing + added, len(added)


def merge(existing, new, cfg, now_ts):
    """Объединяет точки, отбрасывает дубли и точки вне периода, прореживает новые по min_interval."""
    lo = cfg["from"] if cfg["from"] is not None else -1
    hi = cfg["to"] if cfg["to"] is not None else 2 ** 62
    hi = min(hi, now_ts + 600)  # защита от точек «из будущего»
    by_ts = {p["ts"]: p for p in existing if lo <= p["ts"] <= hi}
    last = max(by_ts) if by_ts else None
    added = 0
    for p in sorted(new, key=lambda x: x["ts"]):
        if not (lo <= p["ts"] <= hi) or p["ts"] in by_ts:
            continue
        if last is not None and p["ts"] < last + cfg["min_interval"]:
            continue
        p = dict(p, src="aisstream")
        by_ts[p["ts"]] = p
        last = p["ts"] if last is None else max(last, p["ts"])
        added += 1
    return [by_ts[k] for k in sorted(by_ts)], added


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_geojson(points, cfg, old_props, now_ts):
    def num(x):
        return int(x) if x and str(x).isdigit() else x

    line = {
        "type": "Feature",
        "properties": {
            "name": cfg["name"], "imo": num(cfg["imo"]), "mmsi": num(cfg["mmsi"]),
            "source": old_props.get("source") or "VesselFinder + aisstream.io",
            "points": len(points),
            "start": iso(points[0]["ts"]) if points else None,
            "end": iso(points[-1]["ts"]) if points else None,
            "updated": iso(now_ts),
        },
        "geometry": {"type": "LineString", "coordinates": [[p["lon"], p["lat"]] for p in points]},
    }
    feats = [line]
    for i, p in enumerate(points):
        pr = {"n": i, "time": iso(p["ts"]), "ts": p["ts"], "sog_kn": p.get("sog"), "cog_deg": p.get("cog")}
        for k_src, k_dst in (("heading", "heading"), ("nav", "nav_status"), ("src", "src")):
            if p.get(k_src) is not None:
                pr[k_dst] = p[k_src]
        feats.append({"type": "Feature", "properties": pr,
                      "geometry": {"type": "Point", "coordinates": [p["lon"], p["lat"]]}})
    return json.dumps({"type": "FeatureCollection", "features": feats}, ensure_ascii=False, separators=(",", ":"))


def in_window(cfg, now_ts):
    if cfg["from"] is not None and now_ts < cfg["from"]:
        return False, f"период наблюдения ещё не начался (с {iso(cfg['from'])})"
    if cfg["to"] is not None and now_ts > cfg["to"]:
        return False, f"период наблюдения завершён ({iso(cfg['to'])})"
    return True, ""


def check_imo(static, cfg):
    if not cfg["imo"]:
        return
    for s in static:
        imo = str(s.get("ImoNumber") or "")
        if imo and imo != "0" and imo != cfg["imo"]:
            warn(f"MMSI {cfg['mmsi']} передаёт IMO {imo}, а в config.json указан IMO {cfg['imo']} — проверьте конфиг")
            return
    if static:
        log(f"IMO подтверждён по статическим данным АИС: {cfg['imo']}")


# ---------------------------------------------------------------- Google Drive

class Drive:
    API = "https://www.googleapis.com/drive/v3/files"
    UPLOAD = "https://www.googleapis.com/upload/drive/v3/files"

    def __init__(self, sa_json):
        from google.oauth2 import service_account
        from google.auth.transport.requests import AuthorizedSession

        creds = service_account.Credentials.from_service_account_info(
            json.loads(sa_json), scopes=["https://www.googleapis.com/auth/drive"])
        self.email = creds.service_account_email
        self.s = AuthorizedSession(creds)

    def _check(self, r, what):
        if r.status_code >= 400:
            raise RuntimeError(f"Drive: {what}: HTTP {r.status_code} {r.text[:400]}")
        return r

    def list(self, folder_id):
        out, token = [], None
        while True:
            params = {"q": f"'{folder_id}' in parents and trashed = false",
                      "fields": "nextPageToken,files(id,name,mimeType,modifiedTime)",
                      "pageSize": 1000, "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"}
            if token:
                params["pageToken"] = token
            r = self._check(self.s.get(self.API, params=params), "список папки")
            j = r.json()
            out += j.get("files", [])
            token = j.get("nextPageToken")
            if not token:
                return out

    def download(self, file_id):
        r = self._check(self.s.get(f"{self.API}/{file_id}", params={"alt": "media", "supportsAllDrives": "true"}),
                        "скачивание")
        return r.content.decode("utf-8-sig")

    def update(self, file_id, text):
        self._check(self.s.patch(f"{self.UPLOAD}/{file_id}",
                                 params={"uploadType": "media", "supportsAllDrives": "true"},
                                 data=text.encode("utf-8"), headers={"Content-Type": "application/geo+json"}),
                    "запись файла")

    def set_description(self, file_id, text):
        """Отметка о проверке в описании файла — видна в приложении и в свойствах файла в Drive."""
        self._check(self.s.patch(f"{self.API}/{file_id}", params={"supportsAllDrives": "true", "fields": "id"},
                                 json={"description": text}), "отметка о проверке")

    def create(self, folder_id, name, text):
        boundary = "vesseltrack-boundary"
        meta = json.dumps({"name": name, "parents": [folder_id], "mimeType": "application/geo+json"})
        body = (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{meta}\r\n"
                f"--{boundary}\r\nContent-Type: application/geo+json\r\n\r\n{text}\r\n--{boundary}--").encode("utf-8")
        r = self.s.post(self.UPLOAD, params={"uploadType": "multipart", "supportsAllDrives": "true"},
                        data=body, headers={"Content-Type": f"multipart/related; boundary={boundary}"})
        if r.status_code == 403 and "storageQuota" in r.text:
            raise RuntimeError(
                "Сервисный аккаунт не может создавать файлы в «Моём диске». Загрузите в папку geojson "
                "начальный файл aistrack.geojson (можно пустой) — робот будет его дописывать.")
        self._check(r, "создание файла")


def normalize_folder_id(s):
    m = re.search(r"/folders/([A-Za-z0-9_-]+)", s) or re.search(r"[?&]id=([A-Za-z0-9_-]+)", s)
    return m.group(1) if m else s.strip()


# ---------------------------------------------------------------- запуск

def load_config(drive, folder):
    root = drive.list(folder)
    if not root:
        raise RuntimeError(f"Папка пуста или недоступна. Откройте доступ «Редактор» для {drive.email}")
    cfg_file = next((f for f in root if f["name"].lower() == CONFIG_FILE), None)
    if not cfg_file:
        raise RuntimeError("В папке VesselTrack нет config.json")
    return root, read_config(drive.download(cfg_file["id"]))


def heartbeat(drive, track_id, heard, points_total, last_ts):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    last = iso(last_ts) if last_ts else "—"
    try:
        drive.set_description(track_id, f"robot {now} heard={heard} points={points_total} last={last}")
    except Exception as e:
        warn(f"Отметка о проверке не записана: {e}")


def sync(drive, folder, positions, static):
    """Перечитывает config.json, сливает накопленные позиции и vf_patch.json в aistrack.geojson.
    Возвращает cfg или None, если сейчас вне периода наблюдения."""
    now_ts = int(datetime.now(timezone.utc).timestamp())
    root, cfg = load_config(drive, folder)
    ok, why = in_window(cfg, now_ts)
    if not ok:
        log(f"Пропуск: {why}")
        return None
    check_imo(static, cfg)

    gj_folder = next((f for f in root if f["name"].lower() == GEOJSON_FOLDER
                      and f["mimeType"] == "application/vnd.google-apps.folder"), None)
    if not gj_folder:
        raise RuntimeError("В папке VesselTrack нет подпапки geojson")
    gj_files = drive.list(gj_folder["id"])
    track = next((f for f in gj_files if f["name"].lower() == TRACK_FILE), None)

    old_text = drive.download(track["id"]) if track else ""
    existing, props = load_points(old_text)
    # дозаполнение пропусков точками VesselFinder (их выкладывает задача с компьютера)
    filled = 0
    patch = [f for f in gj_files if f["name"].lower() == PATCH_FILE]
    if patch:
        newest = max(patch, key=lambda f: f.get("modifiedTime") or "")
        try:
            existing, filled = fill_gaps(existing, load_patch(drive.download(newest["id"])))
            if filled:
                log(f"Дозаполнено из VesselFinder: {filled} точек")
        except Exception as e:  # повреждённый патч не должен ломать обновление
            warn(f"vf_patch.json не прочитан: {e}")
            filled = 0
    points, added = merge(existing, positions, cfg, now_ts)
    added += filled
    dropped = len(existing) - (len(points) - (added - filled))
    if added == 0 and dropped == 0 and track:
        last = max((p["ts"] for p in existing), default=None)
        log("Новых точек нет" + (f"; последняя точка {iso(last)}" if last else ""))
        heartbeat(drive, track["id"], len(positions), len(existing), last)
        return cfg
    text = build_geojson(points, cfg, props, now_ts)
    if track:
        drive.update(track["id"], text)
        heartbeat(drive, track["id"], len(positions), len(points), points[-1]["ts"] if points else None)
    else:
        drive.create(gj_folder["id"], TRACK_FILE, text)
    log(f"Записано: +{added} точек, удалено вне периода {dropped}, всего {len(points)}")
    return cfg


def run_online():
    """
    Долгий запуск: слушает поток АИС непрерывно до RUN_MINUTES минут (по умолчанию 330 — почти
    6-часовой предел GitHub) и каждые CYCLE_SECONDS секунд записывает накопленное в Drive.
    Расписание GitHub ненадёжно (запуски «раз в 5 минут» на деле идут раз в 1–2 часа),
    поэтому один запуск покрывает несколько часов, а следующий встаёт в очередь и продолжает.
    """
    import time
    folder = normalize_folder_id(os.environ["DRIVE_FOLDER_ID"])
    drive = Drive(os.environ["GOOGLE_SA_JSON"])
    api_key = os.environ["AISSTREAM_API_KEY"]
    run_seconds = int(float(os.environ.get("RUN_MINUTES", "330")) * 60)
    cycle = int(os.environ.get("CYCLE_SECONDS", "300"))
    deadline = time.monotonic() + run_seconds

    _, cfg = load_config(drive, folder)
    log(f"Судно {cfg['name']}, MMSI {cfg['mmsi']}, IMO {cfg['imo']}, "
        f"период {iso(cfg['from']) if cfg['from'] else '…'} — {iso(cfg['to']) if cfg['to'] else '…'}; "
        f"работаю до {run_seconds // 60} мин, запись каждые {cycle // 60} мин")
    ok, why = in_window(cfg, int(datetime.now(timezone.utc).timestamp()))
    if not ok:
        log(f"Пропуск: {why}")
        return

    pending, static = [], []
    while True:
        left = deadline - time.monotonic()
        if left < 60:
            break
        try:
            got, st = asyncio.run(listen(api_key, cfg["mmsi"], min(cycle, left - 30)))
            pending += got
            static += st
            log(f"Позиций АИС за цикл: {len(got)}")
        except Exception as e:  # обрыв соединения — переподключимся в следующем цикле
            warn(f"aisstream: {e}")
            time.sleep(20)
        try:
            new_cfg = sync(drive, folder, pending, static)
            pending, static = [], []
            if new_cfg is None:
                break
            cfg = new_cfg
        except Exception as e:  # сбой Drive не должен останавливать прослушивание; точки сохранятся до следующей записи
            warn(f"Запись в Drive не удалась, повторю через цикл: {e}")
    if pending:
        try:
            sync(drive, folder, pending, static)
        except Exception as e:
            warn(f"Последняя запись не удалась: {e}")


async def probe(api_key, boxes, mmsi_filter, seconds):
    """Диагностика: сколько сообщений и судов aisstream отдаёт в заданном районе."""
    import websockets
    sub = {"APIKey": api_key, "BoundingBoxes": boxes, "FilterMessageTypes": POSITION_TYPES}
    if mmsi_filter:
        sub["FiltersShipMMSI"] = mmsi_filter
    msgs, ships, near = 0, {}, []
    loop = asyncio.get_running_loop()
    end = loop.time() + seconds
    async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=30) as ws:
        await ws.send(json.dumps(sub))
        while loop.time() < end:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=end - loop.time())
            except asyncio.TimeoutError:
                break
            m = json.loads(raw)
            if "error" in m:
                raise RuntimeError(f"aisstream: {m['error']}")
            msgs += 1
            md = _meta(m)
            mm = msg_mmsi(m)
            if not mm or not md:
                log(f"     [необычное сообщение] {raw[:500]}")
            pos = extract_position(m) or {}
            ships[mm or "?"] = (str(md.get("ShipName") or "").strip(), float(pos.get("lat") or md.get("latitude") or 0),
                                float(pos.get("lon") or md.get("longitude") or 0), str(md.get("time_utc") or "")[:19])
    return msgs, ships


def run_diagnose():
    """Проверка покрытия aisstream вокруг последней известной точки судна."""
    folder = normalize_folder_id(os.environ["DRIVE_FOLDER_ID"])
    drive = Drive(os.environ["GOOGLE_SA_JSON"])
    key = os.environ["AISSTREAM_API_KEY"]
    root, cfg = load_config(drive, folder)
    gj = next(f for f in root if f["name"].lower() == GEOJSON_FOLDER)
    track = next(f for f in drive.list(gj["id"]) if f["name"].lower() == TRACK_FILE)
    pts, _ = load_points(drive.download(track["id"]))
    last = max(pts, key=lambda p: p["ts"])
    lat, lon = last["lat"], last["lon"]
    log(f"Последняя точка судна: {lat:.3f}, {lon:.3f} ({iso(last['ts'])}, источник {last.get('src') or 'vesselfinder'})")

    box = [[[lat - 1.5, lon - 2.0], [lat + 1.5, lon + 2.0]]]
    msgs, ships = asyncio.run(probe(key, box, None, 120))
    log(f"1) Район ±1.5° вокруг судна, 2 мин, все суда: сообщений {msgs}, судов {len(ships)}")
    for mm, (name, la, lo, t) in list(ships.items())[:15]:
        log(f"     {mm} {name or '—'} {la:.3f},{lo:.3f} {t}")
    log(f"   Наше судно {cfg['mmsi']} в районе: {'ДА' if cfg['mmsi'] in ships else 'нет'}")

    msgs2, ships2 = asyncio.run(probe(key, [[[-90, -180], [90, 180]]], [cfg["mmsi"]], 120))
    log(f"2) Весь мир, фильтр по MMSI {cfg['mmsi']}, 2 мин: сообщений {msgs2}")
    for mm, (name, la, lo, t) in ships2.items():
        log(f"     {mm} {name or '—'} {la:.3f},{lo:.3f} {t}")

    seen = cfg["mmsi"] in ships or cfg["mmsi"] in ships2
    if msgs == 0:
        log("ВЫВОД: aisstream вообще не принимает суда в этом районе — нет береговых станций.")
    elif not seen:
        log("ВЫВОД: станции в районе есть, но наше судно за эти минуты не услышано.")
    else:
        log("ВЫВОД: aisstream видит наше судно — подписка работает.")


def run_offline(cfg_path, track_path, messages_path):
    now_ts = int(datetime.now(timezone.utc).timestamp())
    cfg = read_config(open(cfg_path, encoding="utf-8").read())
    msgs = [json.loads(l) for l in open(messages_path, encoding="utf-8") if l.strip()]
    positions = [p for p in (extract_position(m) for m in msgs) if p]
    check_imo([m["Message"]["ShipStaticData"] for m in msgs if m.get("MessageType") == "ShipStaticData"], cfg)
    old = open(track_path, encoding="utf-8").read() if os.path.exists(track_path) else ""
    existing, props = load_points(old)
    points, added = merge(existing, positions, cfg, now_ts)
    with open(track_path, "w", encoding="utf-8") as f:
        f.write(build_geojson(points, cfg, props, now_ts))
    log(f"offline: +{added}, всего {len(points)}, окно: {in_window(cfg, now_ts)}")


if __name__ == "__main__":
    try:
        if len(sys.argv) == 5 and sys.argv[1] == "--offline":
            run_offline(*sys.argv[2:])
        elif os.environ.get("MODE") == "diagnose":
            run_diagnose()
        else:
            run_online()
    except Exception as e:  # понятное сообщение в журнале Actions
        print(f"::error::{e}", flush=True)
        sys.exit(1)
