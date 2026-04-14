"""
Hungary 2026 – Download Election Results
"""

import os
import json
import time
import argparse
import requests
import geopandas as gpd
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

TIMESTAMPS = [
#    "04131930",
    "04130115",
    "04122225",
    "04122200",
    "04122130",
    "04122100",
    "04122030",
    "04122000",
    "04121930",
    "04121900",
]

BASE_VER        = "https://vtr.valasztas.hu/ogy2026/data/04112100/ver"
SETTLEMENTS_URL = f"{BASE_VER}/Telepulesek.json"

WORKERS    = 8
DELAY      = 0.05
RETRIES    = 3
SHP_PATH   = "polling_stations/polling_stations.shp"
OUT_PATH   = "results.geojson"
COLORS_PATH = "party_colors.json"

DEFAULT_PARTY_COLORS = {
    "FIDESZ-KDNP":  "#FF8000",
    "DK":           "#0033CC",
    "Mi Hazánk":    "#006400",
    "Jobbik":       "#228B22",
    "MKKP":         "#FF69B4",
    "TISZA":        "#00BFFF",
    "MSZP":         "#CC0000",
    "Momentum":     "#9400D3",
    "LMP":          "#32CD32",
    "NÉP":          "#8B4513",
    "OTHER":        "#888888",
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (election-research/1.0)"})

def get(url):
    r = SESSION.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def find_timestamp():
    for ts in TIMESTAMPS:
        url = f"https://vtr.valasztas.hu/ogy2026/data/{ts}/szavossz/01/SzavkorJkv-01-001.json"
        try:
            r = SESSION.get(url, timeout=10)
            if r.status_code == 200 and r.content:
                print(f"  Using timestamp: {ts}")
                return ts
        except Exception:
            pass
    return None

def get_settlements():
    data = get(SETTLEMENTS_URL)
    return [(item["leiro"]["maz"], item["leiro"]["taz"]) for item in data["list"]]

def fetch_results(maz, taz, timestamp):
    url = f"https://vtr.valasztas.hu/ogy2026/data/{timestamp}/szavossz/{maz}/SzavkorJkv-{maz}-{taz}.json"
    data = None
    for attempt in range(RETRIES):
        try:
            data = get(url)
            if data.get("list"):
                break
        except Exception:
            pass
        time.sleep(0.2 * (attempt + 1))

    if not data or not data.get("list"):
        return []

    records = []
    for item in data.get("list", []):
        rec = {
            "maz":    item.get("maz", maz),
            "taz":    item.get("taz", taz),
            "szk":    item.get("sorsz", ""),
            "feldar": item.get("feldar", 0.0),
        }

        ej = item.get("egyeni_jkv", {})
        rec["e_turnout"]     = ej.get("szavazott_osszesen_szaz", None)
        rec["e_valid"]       = ej.get("szl_ervenyes", 0)
        rec["e_invalid"]     = ej.get("szl_ervenytelen", 0)
        rec["e_total_votes"] = ej.get("szavazott_osszesen", 0)
        rec["e_state"]       = ej.get("allapot", "")

        winner_votes, winner_ej = 0, None
        for t in ej.get("tetelek", []):
            votes = t.get("szavazat", 0)
            ej_id = str(t["ej_id"])
            if votes > winner_votes:
                winner_votes, winner_ej = votes, ej_id
        rec["e_winner_ej"]  = winner_ej
        rec["e_winner_pct"] = round(winner_votes / rec["e_valid"] * 100, 2) if rec["e_valid"] else None

        lj = item.get("listas_jkv", {})
        rec["l_turnout"]     = lj.get("szavazott_osszesen_szaz", None)
        rec["l_valid"]       = lj.get("szl_ervenyes", 0)
        rec["l_invalid"]     = lj.get("szl_ervenytelen", 0)
        rec["l_total_votes"] = lj.get("szavazott_osszesen", 0)
        rec["l_state"]       = lj.get("allapot", "")

        winner_votes, winner_tl = 0, None
        for t in lj.get("tetelek", []):
            votes = t.get("szavazat", 0)
            tl_id = str(t["tl_id"])
            if votes > winner_votes:
                winner_votes, winner_tl = votes, tl_id
        rec["l_winner_tl"]  = winner_tl
        rec["l_winner_pct"] = round(winner_votes / rec["l_valid"] * 100, 2) if rec["l_valid"] else None

        records.append(rec)
    return records

def build_party_lookup():
    ej_to_party, tl_to_party = {}, {}
    try:
        cands = get("https://vtr.valasztas.hu/ogy2026/data/04112100/ver/EgyeniJeloltek.json")
        for c in cands.get("list", []):
            ej_to_party[str(c["ej_id"])] = c.get("jlcs_nev", "OTHER")
        print(f"  {len(ej_to_party)} individual candidates loaded")
    except Exception as e:
        print(f"  WARN EgyeniJeloltek.json: {e}")

    try:
        lists = get("https://vtr.valasztas.hu/ogy2026/data/04112100/ver/ListakEsJeloltek.json")
        for l in lists.get("list", []):
            tl_to_party[str(l["tl_id"])] = l.get("jlcs_nev", "OTHER")
        print(f"  {len(tl_to_party)} party lists loaded")
    except Exception as e:
        print(f"  WARN ListakEsJeloltek.json: {e}")

    all_parties = set(ej_to_party.values()) | set(tl_to_party.values())
    color_map = {p: DEFAULT_PARTY_COLORS.get(p, DEFAULT_PARTY_COLORS["OTHER"]) for p in all_parties}
    return ej_to_party, tl_to_party, color_map

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timestamp")
    parser.add_argument("--shp", default=SHP_PATH)
    args = parser.parse_args()

    print("=== Hungary 2026 – Download Results ===\n")

    print(f"Loading polling stations from {args.shp} ...")
    gdf = gpd.read_file(args.shp)
    print(f"  {len(gdf)} polling stations loaded")

    print("\nFinding latest result snapshot ...")
    ts = args.timestamp or find_timestamp()
    if not ts:
        print("ERROR: Could not find any result snapshot.")
        return
    print(f"  Timestamp: {ts}")

    print("\nFetching settlement list ...")
    settlements = get_settlements()
    print(f"  {len(settlements)} settlements")

    print("\nFetching results ...")
    all_records = []

    def worker(pair):
        recs = fetch_results(pair[0], pair[1], ts)
        time.sleep(DELAY)
        return recs

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(worker, p): p for p in settlements}
        done = 0
        for future in as_completed(futures):
            all_records.extend(future.result())
            done += 1
            if done % 100 == 0 or done == len(settlements):
                print(f"  {done}/{len(settlements)} settlements, {len(all_records)} station results ...")

    print(f"\n  Total result records: {len(all_records)}")

    print("\nBuilding party lookup ...")
    ej_to_party, tl_to_party, color_map = build_party_lookup()

    with open(COLORS_PATH, "w") as f:
        json.dump({"ej_to_party": ej_to_party, "tl_to_party": tl_to_party, "color_map": color_map},
                  f, ensure_ascii=False, indent=2)
    print(f"  Party colors saved to {COLORS_PATH}")

    print("\nMerging results with polling station geometries ...")
    results_df = pd.DataFrame(all_records)

    for col in ["maz", "taz", "szk"]:
        gdf[col]        = gdf[col].astype(str).str.zfill({"maz":2,"taz":3,"szk":3}[col])
        results_df[col] = results_df[col].astype(str).str.zfill({"maz":2,"taz":3,"szk":3}[col])

    merged = gdf.merge(results_df, on=["maz","taz","szk"], how="left")
    print(f"  Merged: {len(merged)} rows  "
          f"({merged['e_winner_ej'].notna().sum()} have individual results, "
          f"{merged['l_winner_tl'].notna().sum()} have list results)")

    merged["e_winner_party"] = merged["e_winner_ej"].map(ej_to_party)
    merged["l_winner_party"] = merged["l_winner_tl"].map(tl_to_party)

    merged.to_file(OUT_PATH, driver="GeoJSON")
    print(f"\nSaved: {OUT_PATH}")

if __name__ == "__main__":
    main()
