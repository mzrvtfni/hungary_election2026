"""
Hungary 2026 Parliamentary Election – Polling Station Boundary Downloader
=========================================================================

Data flow (all JSON, no scraping):
  1. Telepulesek.json          → list of every (maz, taz) settlement
  2. Szavazokorok-{maz}-{taz}.json → polling stations (szk) per settlement,
                                     with address, constituency (evk), etc.
  3. Szavkor-Topo-{maz}-{taz}.json → polygon + centroid per szk

Output: polling_stations/polling_stations.shp (WGS84)

Fields in the shapefile
-----------------------
maz        county code                   e.g. "01"
taz        settlement code               e.g. "001"
szk        polling station number        e.g. "042"
evk        constituency number           e.g. "16"
tel_nev    settlement name (HU)
szk_nev    polling station name (HU)
cim        address
kozter     street / venue
akadaly    wheelchair accessible (I/N)
atjKijel   designated transfer station (I/N)
telepSzint settlement-level station (I/N)
honos      resident voters
atjel      transferred-in voters
osszesen   total registered voters
cent_lat   centroid latitude
cent_lon   centroid longitude

Join key for results later: (maz, taz, szk)
Results URL pattern:
  https://vtr.valasztas.hu/ogy2026/data/{timestamp}/szavossz/{maz}/SzavkorJkv-{maz}-{taz}.json
"""

import os
import time
import shapefile
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config ────────────────────────────────────────────────────────────────────

BASE = "https://vtr.valasztas.hu/ogy2026/data/04112100/ver"
WORKERS    = 6
DELAY      = 0.05
OUTPUT_DIR = "polling_stations"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (election-research/1.0)"})

def get(url):
    r = SESSION.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

# ── Step 1: Get all (maz, taz) settlements ────────────────────────────────────

def get_settlements():
    data = get(f"{BASE}/Telepulesek.json")
    settlements = []
    for item in data["list"]:
        l = item["leiro"]
        settlements.append({
            "maz":      l["maz"],
            "taz":      l["taz"],
            "megnev":   l.get("megnev", ""),
            "megnev_en":l.get("megnev_en", ""),
            "szk_db":   l.get("szk_db", 0),
        })
    print(f"  {len(settlements)} settlements found")
    return settlements

# ── Step 2: Get polling stations per settlement ───────────────────────────────

def get_stations(settlement):
    maz, taz = settlement["maz"], settlement["taz"]
    url = f"{BASE}/{maz}/Szavazokorok-{maz}-{taz}.json"
    try:
        data = get(url)
    except Exception as e:
        print(f"  WARN Szavazokorok {maz}-{taz}: {e}")
        return []

    stations = []
    for szk in data["data"]["szavazokorok"]:
        l = szk["leiro"]
        lt = szk.get("letszam", {})
        stations.append({
            "maz":       maz,
            "taz":       taz,
            "szk":       l["sorszam"],
            "evk":       l.get("evk", ""),
            "tel_nev":   settlement["megnev"],
            "szk_nev":   l.get("szk_nev", ""),
            "cim":       l.get("cim", ""),
            "kozter":    l.get("kozter", ""),
            "akadaly":   l.get("akadaly", ""),
            "atjKijel":  l.get("atjKijelolt", ""),
            "telepSzint":l.get("telepSzintu", ""),
            "honos":     lt.get("honos", 0),
            "atjel":     lt.get("atjel", 0),
            "osszesen":  lt.get("osszesen", 0),
            # polygon fields filled in step 3
            "cent_lat":  None,
            "cent_lon":  None,
            "polygon":   [],
        })
    return stations

# ── Step 3: Fetch topology and attach polygons ────────────────────────────────

def attach_topo(maz, taz, stations_by_szk):
    """Fetch topo JSON for one settlement and fill polygon/centroid into stations."""
    url = f"{BASE}/{maz}/Szavkor-Topo-{maz}-{taz}.json"
    try:
        data = get(url)
    except Exception as e:
        print(f"  WARN Topo {maz}-{taz}: {e}")
        return

    for item in data.get("list", []):
        szk = item.get("szk", "")
        if szk not in stations_by_szk:
            continue

        cent_lat = cent_lon = None
        try:
            cent_lat, cent_lon = [float(x) for x in item["centrum"].split()]
        except Exception:
            pass

        polygon = []
        for point in item.get("poligon", "").split(","):
            point = point.strip()
            if point:
                try:
                    lat, lon = [float(x) for x in point.split()]
                    polygon.append((lon, lat))   # shapefile: x=lon, y=lat
                except Exception:
                    pass

        stations_by_szk[szk]["cent_lat"] = cent_lat
        stations_by_szk[szk]["cent_lon"] = cent_lon
        stations_by_szk[szk]["polygon"]  = polygon

# ── Step 4: Write Shapefile ───────────────────────────────────────────────────

def write_shapefile(all_stations, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "polling_stations")

    w = shapefile.Writer(path, shapeType=shapefile.POLYGON)
    w.field("maz",       "C",  2)
    w.field("taz",       "C",  3)
    w.field("szk",       "C",  3)
    w.field("evk",       "C",  3)
    w.field("tel_nev",   "C", 80)
    w.field("szk_nev",   "C", 120)
    w.field("cim",       "C", 80)
    w.field("kozter",    "C", 120)
    w.field("akadaly",   "C",  1)
    w.field("atjKijel",  "C",  1)
    w.field("telepSzint","C",  1)
    w.field("honos",     "N", 10)
    w.field("atjel",     "N", 10)
    w.field("osszesen",  "N", 10)
    w.field("cent_lat",  "N", decimal=8)
    w.field("cent_lon",  "N", decimal=8)

    written = no_poly = 0
    for s in all_stations:
        poly = s["polygon"]
        if len(poly) < 3:
            no_poly += 1
            continue
        if poly[0] != poly[-1]:
            poly = poly + [poly[0]]
        w.poly([poly])
        w.record(
            s["maz"], s["taz"], s["szk"], s["evk"],
            s["tel_nev"], s["szk_nev"], s["cim"], s["kozter"],
            s["akadaly"], s["atjKijel"], s["telepSzint"],
            s["honos"], s["atjel"], s["osszesen"],
            s["cent_lat"] or 0.0, s["cent_lon"] or 0.0,
        )
        written += 1

    w.close()

    with open(path + ".prj", "w") as f:
        f.write('GEOGCS["WGS 84",DATUM["WGS_1984",'
                'SPHEROID["WGS 84",6378137,298.257223563]],'
                'PRIMEM["Greenwich",0],'
                'UNIT["degree",0.0174532925199433]]')

    print(f"\nShapefile written: {path}.shp")
    print(f"  With polygon : {written}")
    print(f"  No polygon   : {no_poly}  (station exists but topo missing)")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Hungary 2026 – Polling Station Boundary Downloader ===\n")

    # Step 1
    print("Step 1: Fetching settlement list from Telepulesek.json ...")
    settlements = get_settlements()

    # Step 2: fetch Szavazokorok for all settlements in parallel
    print("\nStep 2: Fetching polling station lists ...")
    all_stations = []   # flat list of station dicts

    def fetch_stations(s):
        stations = get_stations(s)
        time.sleep(DELAY)
        return stations

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_stations, s): s for s in settlements}
        done = 0
        for future in as_completed(futures):
            all_stations.extend(future.result())
            done += 1
            if done % 100 == 0 or done == len(settlements):
                print(f"  {done}/{len(settlements)} settlements, "
                      f"{len(all_stations)} stations so far ...")

    print(f"  Total polling stations: {len(all_stations)}")

    # Step 3: attach topology polygons — group by (maz, taz) first
    print("\nStep 3: Fetching topology polygons ...")
    from collections import defaultdict
    by_maz_taz = defaultdict(dict)
    for s in all_stations:
        by_maz_taz[(s["maz"], s["taz"])][s["szk"]] = s

    def fetch_topo(pair):
        maz, taz = pair
        attach_topo(maz, taz, by_maz_taz[(maz, taz)])
        time.sleep(DELAY)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_topo, pair): pair for pair in by_maz_taz}
        done = 0
        for future in as_completed(futures):
            future.result()
            done += 1
            if done % 100 == 0 or done == len(by_maz_taz):
                print(f"  {done}/{len(by_maz_taz)} settlements topology done ...")

    # Step 4
    print("\nStep 4: Writing shapefile ...")
    write_shapefile(all_stations, OUTPUT_DIR)

    print("\nDone. Join election results using (maz, taz, szk).")
    print("Results URL: https://vtr.valasztas.hu/ogy2026/data/{timestamp}/szavossz/{maz}/SzavkorJkv-{maz}-{taz}.json")

if __name__ == "__main__":
    main()
