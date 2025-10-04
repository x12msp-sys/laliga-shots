# build_laliga_player_shots.py — FBref (mirror) → CSV tiros por jugador
import os, re, time, io, html, json, requests
import pandas as pd
from bs4 import BeautifulSoup

SEASON_LABEL = "2025-2026"   # así aparece en FBref
LEAGUE_NAME  = "La Liga"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": "https://fbref.com/",
    "Connection": "keep-alive",
})

def get_html(url, retries=2, sleep=0.6, timeout=25):
    """Intenta primero con el mirror r.jina.ai y luego directo."""
    variants = [
        "https://r.jina.ai/" + url,
        url
    ]
    last = None
    for u in variants:
        for _ in range(retries + 1):
            try:
                r = session.get(u, timeout=timeout)
                r.raise_for_status()
                t = r.text
                if t and t.strip():
                    return t.replace("<!--", "").replace("-->", "")
            except Exception as e:
                last = e
                time.sleep(sleep)
    return None

def to_num(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None

def parse_shooting_table(html_text):
    """Devuelve lista de dicts con los campos de la tabla #stats_shooting."""
    if not html_text:
        return []
    soup = BeautifulSoup(html_text, "lxml")
    table = soup.find("table", {"id": "stats_shooting"})
    if not table or not table.tbody:
        return []
    rows = []
    for tr in table.tbody.find_all("tr"):
        if tr.get("class") and "thead" in tr.get("class"):
            continue
        td = lambda stat: tr.find("td", {"data-stat": stat})

        player = td("player").get_text(strip=True) if td("player") else None
        squad  = td("team").get_text(strip=True)   if td("team") else None
        comp   = td("comp").get_text(strip=True)   if td("comp") else None
        season = td("season").get_text(strip=True) if td("season") else None

        if not player or player == "Player":
            continue
        if squad and squad.lower() == "squad total":
            continue

        shots_total  = to_num((td("shots_total") or td("shots")).get_text(strip=True) if (td("shots_total") or td("shots")) else None)
        shots_on     = to_num(td("shots_on_target").get_text(strip=True) if td("shots_on_target") else None)
        n90          = to_num((td("minutes_90s") or td("90s")).get_text(strip=True) if (td("minutes_90s") or td("90s")) else None)
        sot_pct      = to_num((td("shots_on_target_pct") or td("sot%")).get_text(strip=True) if (td("shots_on_target_pct") or td("sot%")) else None)
        sh90         = to_num((td("shots_total_per90") or td("sh/90")).get_text(strip=True) if (td("shots_total_per90") or td("sh/90")) else None)
        sot90        = to_num((td("shots_on_target_per90") or td("sot/90")).get_text(strip=True) if (td("shots_on_target_per90") or td("sot/90")) else None)

        rows.append({
            "player_name": player,
            "squad": squad,
            "comp": comp,
            "season": season,
            "shots_total": shots_total,
            "shots_on_target": shots_on,
            "ninety": n90,
            "sot_pct": sot_pct,
            "shots_per90": sh90,
            "sot_per90": sot90,
        })
    return rows

def build_df(rows):
    if not rows:
        return pd.DataFrame(columns=[
            "player_name","squad","shots_total","shots_on_target","ninety","sot_pct","shots_per90","sot_per90"
        ])
    df = pd.DataFrame(rows)

    # filtros (si las columnas existen)
    if "comp" in df.columns:
        df = df[df["comp"].fillna("").str.lower() == LEAGUE_NAME.lower()]
    if "season" in df.columns:
        df = df[df["season"].fillna("").str.strip() == SEASON_LABEL]

    if df.empty:
        return pd.DataFrame(columns=[
            "player_name","squad","shots_total","shots_on_target","ninety","sot_pct","shots_per90","sot_per90"
        ])

    # agregar por jugador + equipo
    num_cols = ["shots_total","shots_on_target","ninety","sot_pct","shots_per90","sot_per90"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    out = (df.groupby(["player_name","squad"], dropna=False)[num_cols]
             .sum(min_count=1)
             .reset_index())

    out = out.sort_values(["shots_total","shots_on_target"], ascending=[False, False]).reset_index(drop=True)
    return out

# URLs: específica y Big-5 (fallback)
URL_SPEC = "https://fbref.com/en/comps/12/2025-2026/shooting/players/2025-2026-La-Liga-Stats"
URL_BIG5 = "https://fbref.com/en/comps/Big5/shooting/players/Big-5-European-Leagues-Stats"

print("▶️ Descargando tabla específica LaLiga 2025-26…")
h1 = get_html(URL_SPEC)
rows = parse_shooting_table(h1)
print(f"   Filas leídas (spec): {len(rows)}")

if not rows:
    print("▶️ Fallback: Big-5 players shooting…")
    h2 = get_html(URL_BIG5)
    rows = parse_shooting_table(h2)
    print(f"   Filas leídas (big5): {len(rows)}")

df = build_df(rows)
print(f"✅ Filas LaLiga post-filtro: {len(df)}")

os.makedirs("out", exist_ok=True)
df.to_csv("out/laliga_2025_player_shots.csv", index=False)
print("📝 Escrito: out/laliga_2025_player_shots.csv")
