# build_laliga_player_shots.py
import re, json, html, os, time, requests
import pandas as pd

LEAGUE = "La_Liga"   # Spain
SEASON = "2025"      # 2025-26

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.8",
    "Referer": "https://understat.com/",
    "Connection": "keep-alive",
})

def url_variants(path: str):
    p = path.lstrip("/")
    return [
        f"https://understat.com/{p}",
        f"https://cors.isomorphic-git.org/https://understat.com/{p}",
        f"https://r.jina.ai/https://understat.com/{p}",
    ]

def get_page(path: str, retries=2, timeout=25, sleep=0.5) -> str:
    for u in url_variants(path):
        for _ in range(retries + 1):
            try:
                r = session.get(u, timeout=timeout)
                r.raise_for_status()
                t = r.text or ""
                if t.strip():
                    return t
            except Exception:
                time.sleep(sleep)
    return ""

def extract_json_var(html_text: str, var_name: str):
    if not html_text:
        return None
    m = re.search(
        rf"{re.escape(var_name)}\s*=\s*JSON\.parse\('(?P<enc>(?:\\.|[^\\'])*?)'\)\s*;",
        html_text, flags=re.DOTALL
    )
    if m:
        enc = m.group("enc")
        if enc.strip():
            try:
                s1 = json.loads(enc)
                if isinstance(s1, str) and s1.strip() and s1.strip()[0] in "[{":
                    try:
                        return json.loads(s1)
                    except json.JSONDecodeError:
                        pass
            except Exception:
                pass
            try:
                s2 = bytes(enc, "utf-8").decode("unicode_escape")
                s2 = html.unescape(s2)
                if s2.strip() and s2.strip()[0] in "[{":
                    return json.loads(s2)
            except Exception:
                pass
    m2 = re.search(
        rf"{re.escape(var_name)}\s*=\s*(?P<raw>\[.*?\]|\{{.*?\}})\s*;",
        html_text, flags=re.DOTALL
    )
    if m2:
        try:
            return json.loads(m2.group("raw"))
        except Exception:
            return None
    return None

# 1) lista de partidos
league_html = get_page(f"league/{LEAGUE}/{SEASON}")
matches = extract_json_var(league_html, "matchesData") or []
match_ids = [str(m.get("id")) for m in matches if m.get("id") is not None]

# 2) acumular tiros por jugador
agg = {}  # pid -> dict
for mid in match_ids:
    h = get_page(f"match/{mid}")
    shots = extract_json_var(h, "shotsData")
    if not isinstance(shots, list):
        continue
    for s in shots:
        pid = s.get("player_id")
        pname = s.get("player")
        if not pid or not pname:
            continue
        rec = agg.get(pid)
        if rec is None:
            rec = {"player_id": int(pid), "player_name": pname, "shots_total": 0, "shots_on_target": 0}
        rec["shots_total"] += 1
        if s.get("result") in ("Goal", "SavedShot"):
            rec["shots_on_target"] += 1
        agg[pid] = rec

df = pd.DataFrame(list(agg.values())).sort_values(
    ["shots_total", "shots_on_target"], ascending=[False, False]
).reset_index(drop=True)

os.makedirs("out", exist_ok=True)
df.to_csv("out/laliga_2025_player_shots.csv", index=False)
print(f"✅ rows: {len(df)}  → out/laliga_2025_player_shots.csv")
