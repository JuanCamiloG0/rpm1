from flask import Flask, render_template, jsonify, request, url_for
import os
import time
import sqlite3
import hashlib
from pathlib import Path
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__, template_folder='templates', static_folder='static')

# ----------------------- Google Sheets (lazy + tolerante) --------------------
SHEET_ID = "1FTitPI148EqD1oTBqUDsK9HA9DnrbKfcLNVFzghWGGA"
WS_NAME = "Sesiones"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
CREDENTIALS_FILE = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
_gc = None


def get_gspread_client():
    global _gc
    if _gc is None:
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        _gc = gspread.authorize(creds)
    return _gc


_cache = {"data": None, "ts": 0}
RANK_TTL = int(os.environ.get("RANK_TTL_SECONDS", "15"))  # 0 desactiva cache


def get_rows(force=False, ttl=None):
    if ttl is None:
        ttl = RANK_TTL
    now = time.time()
    if (not force) and _cache["data"] and (ttl > 0) and (now - _cache["ts"] < ttl):
        return _cache["data"]
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet(WS_NAME)
        rows = ws.get_all_records(default_blank="")
        _cache["data"], _cache["ts"] = rows, now
        return rows
    except Exception as e:
        app.logger.exception("Error leyendo Google Sheet: %s", e)
        return _cache["data"] or []


# ------------------------------- Campos & BD ---------------------------------
LEVEL_FIELD = "Nivel de la sesión"   # 7.0 alto → 1.0 bajo
NAME_FIELD = "Nombre del jugador"
GENDER_FIELD = "Género"
OFFICIAL_CAT_FIELD = "CAT. RPM OFICIAL"

# posibles nombres de fecha en tu Sheet
DATE_CANDIDATES = [
    "Fecha", "fecha", "FECHA", "Día", "Dia", "Día de la sesión", "Dia de la sesión",
    "Fecha de la sesión", "Fecha de la sesion", "Timestamp", "Marca temporal", "Date"
]

DB_PATH = Path("rank.db")


def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS player_lastpos (
                player TEXT PRIMARY KEY,
                last_pos INTEGER NOT NULL,
                updated_at REAL NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS movement_cache (
                player TEXT PRIMARY KEY,
                movement TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
        """)
        con.execute(
            """CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)""")
        con.commit()


init_db()

# ------------------------------- Utilidades ----------------------------------


def normalize_name(name):
    return " ".join((name or "").split()).strip().lower()


def key_for(scope, player_key):
    """Guarda/lee posiciones por contexto (scope) usando la misma tabla."""
    return f"{scope}::{player_key}"


def scope_from_meta(scope):
    return f"rank_hash:{scope}"


def get_meta(k):
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return row[0] if row else None


def set_meta(k, v):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            INSERT INTO meta(k, v) VALUES (?, ?)
            ON CONFLICT(k) DO UPDATE SET v=excluded.v
        """, (k, v))
        con.commit()


def get_last_pos_map():
    with sqlite3.connect(DB_PATH) as con:
        return dict(con.execute("SELECT player, last_pos FROM player_lastpos").fetchall())


def set_current_positions(curr_pos_map):
    now = time.time()
    with sqlite3.connect(DB_PATH) as con:
        for player_key, pos in curr_pos_map.items():
            con.execute("""
                INSERT INTO player_lastpos(player, last_pos, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(player) DO UPDATE SET
                    last_pos=excluded.last_pos, updated_at=excluded.updated_at
            """, (player_key, pos, now))
        con.commit()


def get_movement_cache_map():
    with sqlite3.connect(DB_PATH) as con:
        return dict(con.execute("SELECT player, movement FROM movement_cache").fetchall())


def set_movement_cache(mov_map):
    now = time.time()
    with sqlite3.connect(DB_PATH) as con:
        for player_key, movement in mov_map.items():
            con.execute("""
                INSERT INTO movement_cache(player, movement, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(player) DO UPDATE SET
                    movement=excluded.movement, updated_at=excluded.updated_at
            """, (player_key, movement, now))
        con.commit()


def clear_movement_cache():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("DELETE FROM movement_cache")
        con.commit()

# --------------------------------- Helpers -----------------------------------


def get_level(row):
    s = str(row.get(LEVEL_FIELD, "")).strip()
    if not s:
        return 0.0
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def sort_rows_by_level(rows):
    # mayor nivel primero; luego alfabético
    return sorted(rows, key=lambda r: (-get_level(r), (r.get(NAME_FIELD) or "").strip().lower()))


def hash_rank(rows_sorted):
    parts = []
    for r in rows_sorted:
        parts.append(
            f"{normalize_name(r.get(NAME_FIELD, ''))}:{get_level(r):.3f}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def build_pos_map(rows_sorted, scope="ALL"):
    """Devuelve dos mapas: por clave combinada (scope::player) y por nombre (solo player)."""
    by_scope = {}
    by_name = {}
    for idx, r in enumerate(rows_sorted, start=1):
        name_key = normalize_name((r.get(NAME_FIELD) or ""))
        by_scope[key_for(scope, name_key)] = idx
        by_name[name_key] = idx
    return by_scope, by_name


def ensure_snapshot_and_movements(rows_sorted, scope="ALL"):
    """
    Calcula flechas SOLO en este 'scope' (vista).
    - Guarda/lee posiciones snapshot por (scope::player) en las tablas existentes.
    - Devuelve movement_map y pos_map **por nombre** (no combinadas) para enriquecer la vista.
    """
    pos_scope_map, pos_name_map = build_pos_map(rows_sorted, scope=scope)
    rank_key = scope_from_meta(scope)  # hash por scope
    new_hash = hash_rank(rows_sorted)

    old_hash = get_meta(rank_key)
    if old_hash != new_hash:
        last_map = get_last_pos_map()
        movement_scope_map = {}
        for combined_key, curr_pos in pos_scope_map.items():
            last_pos = last_map.get(combined_key)
            if last_pos is None:
                mv = "none"
            elif curr_pos < last_pos:
                mv = "up"
            elif curr_pos > last_pos:
                mv = "down"
            else:
                mv = "same"
            movement_scope_map[combined_key] = mv

        # persistir
        set_movement_cache(movement_scope_map)
        set_current_positions(pos_scope_map)
        set_meta(rank_key, new_hash)

    # construir movement por NOMBRE usando el cache actual:
    cache_all = get_movement_cache_map()
    movement_by_name = {}
    for combined_key, mv in cache_all.items():
        # solo entradas de este scope
        if not combined_key.startswith(f"{scope}::"):
            continue
        name_key = combined_key.split("::", 1)[1]
        movement_by_name[name_key] = mv

    return movement_by_name, pos_name_map


def enrich_view(rows_sorted_view, movement_map_by_name, pos_map_by_name):
    enriched = []
    for idx, r in enumerate(rows_sorted_view, start=1):
        key = normalize_name((r.get(NAME_FIELD) or ""))
        r2 = dict(r)
        r2["_pos"] = idx                       # posición dentro de ESTA vista
        r2["_pos_overall"] = pos_map_by_name.get(key)
        r2["_movement"] = movement_map_by_name.get(key, "none")
        enriched.append(r2)
    return enriched


def unique_nonempty(iterable):
    s = {str(v).strip() for v in iterable if str(v).strip()}
    return sorted(s)


def filter_rows(rows, genero=None, official_cat=None):
    """
    Filtra por género y/o por categoría oficial.
    - Para genero='M': excluye cualquier fila femenina (en GÉNERO o en la categoría).
    - Para genero='F': incluye filas femeninas (en GÉNERO o en la categoría).
    """
    genero = (genero or "").strip().upper()
    official_cat = (official_cat or "").strip()
    out = []
    for r in rows:
        g = str(r.get(GENDER_FIELD, "")).strip().lower()
        c = str(r.get(OFFICIAL_CAT_FIELD, "")).strip().lower()

        if genero == "M":
            if ("fem" in g) or ("femen" in g) or ("fem" in c) or ("femen" in c) or g == "f":
                continue
        elif genero == "F":
            if not (("fem" in g) or ("femen" in g) or ("fem" in c) or ("femen" in c) or g == "f"):
                continue

        if official_cat and c.lower() != official_cat.lower():
            continue
        out.append(r)
    return out

# ---------- Deduplicación: mejor sesión por jugador y día --------------------


DATE_PATTERNS = (
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y",
    "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",
)


def _parse_date_key(raw):
    s = str(raw or "").strip()
    if not s:
        return ""
    s_try = s.replace("T", " ").strip()
    for fmt in DATE_PATTERNS:
        try:
            dt = datetime.strptime(s_try, fmt)
            return dt.date().isoformat()
        except Exception:
            pass
    for fmt in ("%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M"):
        try:
            dt = datetime.strptime(s_try, fmt)
            return dt.date().isoformat()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.split(" ")[0]).date().isoformat()
    except Exception:
        return ""


def dedupe_best_per_day(rows):
    best = {}
    passthrough = []
    for r in rows:
        player_key = normalize_name(r.get(NAME_FIELD))
        day_key = ""
        for cand in DATE_CANDIDATES:
            if cand in r and str(r[cand]).strip():
                dk = _parse_date_key(r[cand])
                if dk:
                    day_key = dk
                    break
        if not day_key:
            passthrough.append(r)
            continue
        k = (player_key, day_key)
        if k not in best or get_level(r) > get_level(best[k]):
            best[k] = r
    return passthrough + list(best.values())

# --------- Canon de categorías + buckets SIN límite --------------------------


CATS_M = ['1ra', '2da', '2_3', '3ra', '4ta',
          '5ta', '6ta', '7ma']  # mayor → menor
# mayor → menor
CATS_F = ['1ra', 'A', 'B', 'C', 'D', 'E']


def canon_cat_m(raw):
    s = (raw or "").strip().lower()
    if not s:
        return None
    s = s.replace("º", "").replace("°", "").replace(
        "–", "-").replace("—", "-").replace("/", "-").replace(" ", "")
    if ("2" in s and "3" in s) or "2-3" in s or "2da3ra" in s or "2da-3ra" in s:
        return "2_3"
    if s.startswith("7"):
        return "7ma"
    if s.startswith("6"):
        return "6ta"
    if s.startswith("5"):
        return "5ta"
    if s.startswith("4"):
        return "4ta"
    if s.startswith("3"):
        return "3ra"
    if s.startswith("2"):
        return "2da"
    if s.startswith("1") or s.startswith("1a"):
        return "1ra"
    if "ma" in s and "7" in s:
        return "7ma"
    if "ta" in s and "6" in s:
        return "6ta"
    if "ta" in s and "5" in s:
        return "5ta"
    if "ta" in s and "4" in s:
        return "4ta"
    if "ra" in s and "3" in s:
        return "3ra"
    if "da" in s and "2" in s:
        return "2da"
    if "ra" in s and "1" in s:
        return "1ra"
    return None


def canon_cat_f(raw):
    s = (raw or "").strip().lower()
    if not s:
        return None
    for token in ["femenino", "categoria", "categoría", "cat", ".", "_", "-", " "]:
        s = s.replace(token, "")
    s = s.strip()
    if s in {"1", "1a", "1ra", "primera", "open"}:
        return "1ra"
    if s in {"a"}:
        return "A"
    if s in {"b", "2", "2a", "2da"}:
        return "B"
    if s in {"c", "3", "3a", "3ra"}:
        return "C"
    if s in {"d", "4", "4a", "4ta"}:
        return "D"
    if s in {"e", "5", "5a", "5ta"}:
        return "E"
    if s and s[0] in "abcde":
        return s[0].upper()
    return None


def assign_buckets_from_sheet(rows_sorted, cats, genero=None):
    groups = {c: [] for c in cats}
    canon = canon_cat_f if (genero == "F") else canon_cat_m
    for r in rows_sorted:
        c0 = canon(r.get(OFFICIAL_CAT_FIELD))
        if c0 not in groups:
            c0 = cats[0]
        r2 = dict(r)
        r2['cat'] = c0
        r2['_pos_cat'] = len(groups[c0]) + 1
        groups[c0].append(r2)
    default_cat = next((c for c in cats if groups[c]), cats[0])
    return groups, default_cat

# -------------------------- Metadatos de chips (iconos) ----------------------


def cats_meta_m():
    meta = {
        "1ra": {"label": "1ra",        "value": "1ra", "img": "img/1ra.png", "alt": "Categoría 1ra"},
        "2da": {"label": "2da",        "value": "2da", "img": "img/1ra.png", "alt": "Categoría 2da"},
        "2_3": {"label": "2da - 3ra",  "value": "2_3", "img": "img/2_3.png", "alt": "Categoría 2da-3ra"},
        "3ra": {"label": "3ra",        "value": "3ra", "img": "img/3ra.png", "alt": "Categoría 3ra"},
        "4ta": {"label": "4ta",        "value": "4ta", "img": "img/4ta.png", "alt": "Categoría 4ta"},
        "5ta": {"label": "5ta",        "value": "5ta", "img": "img/5ta.png", "alt": "Categoría 5ta"},
        "6ta": {"label": "6ta",        "value": "6ta", "img": "img/6ta.png", "alt": "Categoría 6ta"},
        "7ma": {"label": "7ma",        "value": "7ma", "img": "img/7ma.png", "alt": "Categoría 7ma"},
    }
    # Devuelve ordenado según CATS_M
    return [meta[k] for k in CATS_M if k in meta]


def cats_meta_f():
    return [
        {"label": "Femenino 1ra", "value": "1ra",
            "img": "img/feme1.png", "alt": "Femenino 1ra"},
        {"label": "Femenino A", "value": "A",
            "img": "img/femeA.png", "alt": "Femenino A"},
        {"label": "Femenino B", "value": "B",
            "img": "img/femeB.png", "alt": "Femenino B"},
        {"label": "Femenino C", "value": "C",
            "img": "img/femeC.png", "alt": "Femenino C"},
        {"label": "Femenino D", "value": "D",
            "img": "img/femeD.png", "alt": "Femenino D"},
        {"label": "Femenino E", "value": "E",
            "img": "img/femeE.png", "alt": "Femenino E"},
    ]

# -------------------------- Cache navegador: OFF ------------------------------


@app.after_request
def add_no_cache_headers(response):
    if not request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

# ---------------------------------- Rutas ------------------------------------


@app.route("/")
def home():
    rows = get_rows(force=bool(request.args.get("refresh")))
    return render_template("base.html", rows=rows)

# relativegeneral (sin filtro)


@app.route("/ranking")
def ranking():
    force = bool(request.args.get("refresh"))
    genero = request.args.get("genero")
    cat = request.args.get("cat")

    rows_all = get_rows(force=force)
    rows_all = dedupe_best_per_day(rows_all)
    rows_all_sorted = sort_rows_by_level(rows_all)

    # Movimiento por scope = ALL (solo cambia si cambia POS general)
    movement_map, pos_map = ensure_snapshot_and_movements(
        rows_all_sorted, scope="ALL")

    cats_m = unique_nonempty(r.get(OFFICIAL_CAT_FIELD, "")
                             for r in rows_all
                             if str(r.get(GENDER_FIELD, "")).strip().upper() == "M")
    cats_by_gender = {"M": cats_m, "F": ["1ra", "A", "B", "C", "D", "E"]}

    if genero or cat:
        subset = filter_rows(rows_all, genero=genero, official_cat=cat)
        subset_sorted = sort_rows_by_level(subset)
        rows_view = enrich_view(subset_sorted, movement_map, pos_map)
    else:
        rows_view = enrich_view(rows_all_sorted, movement_map, pos_map)

    view = {"genero": (genero or ""), "cat": (cat or "")}
    return render_template("ranking.html",
                           rows=rows_view,
                           cats_by_gender=cats_by_gender,
                           view=view,
                           is_general=True,
                           is_masculino=False,
                           is_femenino=False)

# Ranking masculino


@app.route("/ranking-masculino")
def ranking_masculino():
    force = bool(request.args.get("refresh"))
    rows_all = get_rows(force=force)
    rows_all = dedupe_best_per_day(rows_all)

    # Solo masculino
    rows_m = filter_rows(rows_all, genero="M")
    rows_sorted_m = sort_rows_by_level(rows_m)

    # Bucket por categorías (usa CATS_M con 1ra primero)
    groups, _top_cat_ignored = assign_buckets_from_sheet(
        rows_sorted_m, CATS_M, genero="M"
    )

    # Por defecto abre en 1ra
    current_cat = request.args.get("cat") or "1ra"
    if current_cat not in groups:
        current_cat = "1ra"

    rows_current = groups.get(current_cat, [])

    # Movimiento por scope = M:<cat>
    scope = f"M:{current_cat}"
    movement_map, pos_map = ensure_snapshot_and_movements(
        rows_current, scope=scope)
    rows_view = enrich_view(rows_current, movement_map, pos_map)

    # Meta de categorías (para el dropdown/chips)
    cats = cats_meta_m()
    # Etiqueta visible del botón (si usas {{ current_label }})
    current_label = next(
        (c["label"] for c in cats if c["value"] == current_cat), current_cat)

    view = {"genero": "M", "cat": current_cat}
    return render_template(
        "ranking.html",
        rows=rows_view,
        groups=groups,          # <-- requerido por la plantilla cuando hay categorías
        cats=cats,              # <-- requerido para listar opciones
        current_cat=current_cat,  # <-- requerido para marcar activa
        current_label=current_label,  # <-- si tu HTML muestra el texto del botón
        view=view,
        is_general=False,
        is_masculino=True,
        is_femenino=False
    )


@app.route("/ranking-femenino")
def ranking_femenino():
    force = bool(request.args.get("refresh"))
    rows_all = get_rows(force=force)
    rows_all = dedupe_best_per_day(rows_all)

    # Solo femenino
    rows_f = filter_rows(rows_all, genero="F")
    rows_sorted_f = sort_rows_by_level(rows_f)

    # Bucket por categorías femeninas
    groups, _top_cat_ignored = assign_buckets_from_sheet(
        rows_sorted_f, CATS_F, genero="F"
    )

    # Por defecto abre en 1ra
    current_cat = request.args.get("cat") or "1ra"
    if current_cat not in groups:
        current_cat = "1ra"

    rows_current = groups.get(current_cat, [])

    # Movimiento por scope = F:<cat>
    scope = f"F:{current_cat}"
    movement_map, pos_map = ensure_snapshot_and_movements(
        rows_current, scope=scope)
    rows_view = enrich_view(rows_current, movement_map, pos_map)

    cats = cats_meta_f()
    current_label = next(
        (c["label"] for c in cats if c["value"] == current_cat), current_cat)

    view = {"genero": "F", "cat": current_cat}
    return render_template(
        "ranking.html",
        rows=rows_view,
        groups=groups,
        cats=cats,
        current_cat=current_cat,
        current_label=current_label,
        view=view,
        is_general=False,
        is_masculino=False,
        is_femenino=True
    )


@app.route("/api/sesiones")
def api_sesiones():
    return jsonify(get_rows(force=bool(request.args.get("refresh"))))


# --------------------------------- Main --------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5500, debug=True)
