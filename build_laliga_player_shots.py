# build_laliga_player_shots.py  — Fuente principal: FBref (fallback: Understat)
import os, re, io, time, html, json, requests
import pandas as pd

SEASON_LABEL = "2025-2026"   # etiqueta que aparece en FBref
LEAGUE_NAME  = "La Liga"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": "https://fbref.com/",
    "Connection": "keep-alive",
})

def get(url, retries=2, sleep=0.6, timeout=25):
    last = None
    for _ in range(retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(sleep)
    return None

def parse_fbref_shooting(html_text: str) -> pd.DataFrame:
    if not html_text:
        return pd.DataFrame()
    # FBref mete tablas dentro de comentarios
    clean = html_text.replace("<!--", "").replace("-->", "")
    # Extraer tabla por id
    try:
        tables = pd.read_html(io.StringIO(clean), attrs={"id": "stats_shooting"})
        if not tables:
            return pd.DataFrame()
        df = tables[0]
    except Exception:
        return pd.DataFrame()

    # Aplanar encabezados si vienen como MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[-1] for c in df.columns]

    # Limpiar filas cabecera/total
    for col in ["Player", "Squad"]:
        if col in df.columns:
            df = df[df[col].notna() & (df[col] != col)]
    if "Squad" in df.columns:
        df = df[~df["Squad"].str.fullmatch("Squad Total", case=False, na=False)]

    # Si hay columna 'Comp' (competición), filtra La Liga
    if "Comp" in df.columns:
        df = df[df["Comp"].str.fullmatch(LEAGUE_NAME, case=False, na=False)]

    # Si hay columna 'Season', filtra temporada
    if "Season" in df.columns:
        df = df[df["Season"].astype(str).str.strip() == SEASON_LABEL]

    # Renombrar y convertir
    rename = {
        "Player":"player_name",
        "Squad":"squad",
        "Sh":"shots_total",
        "SoT":"shots_on_target",
        "90s":"ninety",
        "SoT%":"sot_pct",
        "Sh/90":"shots_per90",
        "SoT/90":"sot_per90"
    }
    for k,v in rename.items():
        if k not in df.columns:  # coalesce por si FBref cambia
            # alternativas frecuentes
            alt = {"Sh":"shots", "SoT":"sot"}
            if k in alt and alt[k] in df.columns:
                df[v] = pd.to_numeric(df[alt[k]], errors="coerce")
                continue
        else:
            df[v] = pd.to_numeric(df[k], errors="coerce") if k not in ["Player","Squad"] else df[k]

    keep = ["player_name","squad","shots_total","shots_on_target","ninety","sot_pct","shots_per90","sot_per90"]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA

    # Agregar por jugador (por si aparece con varios equipos)
    out = (df.groupby(["player_name","squad"], dropna=False)
             [["shots_total","shots_on_target","ninety","sot_pct","shots_per90","sot_per90"]]
             .sum(min_count=1)
             .reset_index())

    # Ordenar
    out = out.sort_values(["shots_total","shots_on_target"], ascending=[False, False]).reset_index(drop=True)
    return out

# --------- 1) Intento: página específica de LaLiga 2025-26 ----------
url_specific = "https://fbref.com/en/comps/12/2025-2026/shooting/players/2025-2026-La-Liga-Stats"
html_specific = get(url_specific)

df_fbref = parse_fbref_shooting(html_specific)

# --------- 2) Fallback: Big-5 players shooting (filtrando LaLiga) ----
if df_fbref.empty:
    url_big5 = "https://fbref.com/en/comps/Big5/shooting/players/Big-5-European-Leagues-Stats"
    html_big5 = get(url_big5)
    df_fbref = parse_fbref_shooting(html_big5)

# --------- 3) Si sigue vacío, dejamos CSV con cabeceras (sin romper) ---
if df_fbref.empty:
    df_fbref = pd.DataFrame(columns=[
        "player_name","squad","shots_total","shots_on_target","ninety","sot_pct","shots_per90","sot_per90"
    ])

# Guardar
os.makedirs("out", exist_ok=True)
df_fbref.to_csv("out/laliga_2025_player_shots.csv", index=False)
print(f"✅ filas escritas: {len(df_fbref)} → out/laliga_2025_player_shots.csv")
