"""
Hungary 2026 – Voronoi Election Maps
"""

import json
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
from shapely.ops import voronoi_diagram
from shapely.geometry import MultiPoint
import argparse

warnings.filterwarnings("ignore")

RESULTS_PATH = "results.geojson"
COLORS_PATH  = "party_colors.json"
ADMIN_PATH   = "hu3/hu3.shp"
OEVK_PATH    = "oevk_polygons.shp"

OUT_INDIVIDUAL          = "map_individual.svg"
OUT_LIST                = "map_list.svg"
OUT_BUDAPEST_INDIVIDUAL = "map_budapest_individual.svg"
OUT_BUDAPEST_LIST       = "map_budapest_list.svg"

FALLBACK_COLOR = "#CCCCCC"
NO_DATA_COLOR  = "#ffffff"
FIGURE_SIZE    = (18, 12)
BG_COLOR       = "#1a1a2e"

PARTY_GRADIENTS = {
    "TISZA": {
        40: "#99afff", 45: "#7894ff", 50: "#5679ff", 55: "#345fff",
        60: "#1244ff", 65: "#103cdf", 70: "#0e33bf", 75: "#0b2b9f",
        80: "#092280", 85: "#071a60", 90: "#051240", 95: "#030a20",
    },
    "FIDESZ-KDNP": {
        40: "#ffbf92", 45: "#ffaa6d", 50: "#ff9549", 55: "#ff7f24",
        60: "#ff6a00", 65: "#df5d00", 70: "#bf5000", 75: "#9f4200",
        80: "#803500", 85: "#602800", 90: "#4a1f00", 95: "#331300",
    },
    "Mi Hazánk": {
        40: "#688d1b",
    },
}

def pct_to_color(party, pct, color_map):
    if party in PARTY_GRADIENTS:
        grad = PARTY_GRADIENTS[party]
        stops = sorted(grad.keys())
        pct = max(40, min(95, pct or 40))
        for i in range(len(stops) - 1):
            if stops[i] <= pct <= stops[i + 1]:
                t = (pct - stops[i]) / (stops[i + 1] - stops[i])
                c1 = np.array(mcolors.to_rgb(grad[stops[i]]))
                c2 = np.array(mcolors.to_rgb(grad[stops[i + 1]]))
                return mcolors.to_hex(c1 + t * (c2 - c1))
        return grad[stops[-1]]
    return color_map.get(party, FALLBACK_COLOR)

def load_data(admin_path):
    print("Loading data ...")
    results = gpd.read_file(RESULTS_PATH)
    admin = gpd.read_file(admin_path)
    admin = admin[admin.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    if admin.crs != results.crs:
        admin = admin.to_crs(results.crs)
    with open(COLORS_PATH) as f:
        colors_data = json.load(f)
    color_map   = colors_data["color_map"]
    ej_to_party = colors_data["ej_to_party"]
    tl_to_party = colors_data["tl_to_party"]
    print(f"  {len(results)} polling stations, {len(admin)} municipalities")
    return results, admin, color_map, ej_to_party, tl_to_party

def build_voronoi_for_region(points_gdf, region_geom):
    if len(points_gdf) == 0:
        return gpd.GeoDataFrame()
    if len(points_gdf) == 1:
        idx = points_gdf.index[0]
        return gpd.GeoDataFrame({"src_idx": [idx], "geometry": [region_geom]}, crs=points_gdf.crs)

    pts = MultiPoint(list(points_gdf.geometry))
    regions = voronoi_diagram(pts, envelope=region_geom)
    cells = []
    for region in regions.geoms:
        clipped = region.intersection(region_geom)
        if clipped.is_empty:
            continue
        best_idx = points_gdf.geometry.distance(clipped.centroid).idxmin()
        cells.append({"src_idx": best_idx, "geometry": clipped})
    if not cells:
        return gpd.GeoDataFrame()
    return gpd.GeoDataFrame(cells, crs=points_gdf.crs)

def build_all_voronoi(results, admin, oevk):
    print("Building Voronoi cells ...")
    crs_metric = "EPSG:23700"
    admin_m = admin.to_crs(crs_metric).copy()
    oevk_m  = oevk.to_crs(crs_metric).copy()

    budapest_mask = admin_m["name"].str.contains("Budapest", na=False)
    budapest_constituencies = oevk_m[oevk_m["maz"] == "01"].copy()
    budapest_constituencies["_region_name"] = "Budapest_" + budapest_constituencies["evk"].astype(str)

    non_budapest = admin_m[~budapest_mask][["geometry", "name"]].copy()
    non_budapest = non_budapest.rename(columns={"name": "_region_name"})

    regions_m = pd.concat([non_budapest, budapest_constituencies[["geometry", "_region_name"]]],
                          ignore_index=True)
    regions_m = gpd.GeoDataFrame(regions_m, geometry="geometry", crs=crs_metric)

    valid = results[results["cent_lat"].notna() & results["cent_lon"].notna()].copy()
    pts_gdf = gpd.GeoDataFrame(
        valid,
        geometry=gpd.points_from_xy(valid["cent_lon"], valid["cent_lat"]),
        crs="EPSG:4326"
    ).to_crs(crs_metric)
    print(f"  {len(pts_gdf)} stations with valid centroids")

    joined = gpd.sjoin(pts_gdf, regions_m[["geometry"]], how="left", predicate="within")
    joined = joined.rename(columns={"index_right": "region_idx"})

    # Pre-compute the attribute table once, without geometry
    joined_attrs = joined.drop(columns="geometry").rename(columns={"region_idx": "_reg"})

    all_cells = []
    region_ids = joined["region_idx"].dropna().unique()

    for i, region_idx in enumerate(region_ids):
        region_geom = regions_m.loc[region_idx, "geometry"]
        pts = joined[joined["region_idx"] == region_idx].copy()
        pts = pts[pts.geometry.notna()]

        cells = build_voronoi_for_region(pts, region_geom)
        if len(cells) == 0:
            continue

        cells = cells.merge(joined_attrs, left_on="src_idx", right_index=True, how="left")
        all_cells.append(cells)

        if (i + 1) % 200 == 0 or (i + 1) == len(region_ids):
            print(f"  {i+1}/{len(region_ids)} regions done ...")

    voronoi_gdf = gpd.GeoDataFrame(
        pd.concat(all_cells, ignore_index=True), crs=crs_metric
    ).to_crs("EPSG:4326")
    print(f"  {len(voronoi_gdf)} Voronoi cells total")
    return voronoi_gdf

def make_map(voronoi_gdf, oevk, color_map,
             winner_party_col, winner_pct_col,
             title, out_path, bbox=None):
    print(f"Drawing {title} ...")

    fig, ax = plt.subplots(1, 1, figsize=FIGURE_SIZE)
    ax.set_aspect("equal")
    ax.axis("off")
    fig.patch.set_facecolor(BG_COLOR)
    ax.set_facecolor(BG_COLOR)

    if bbox is not None:
        ax.set_xlim(bbox[0], bbox[2])
        ax.set_ylim(bbox[1], bbox[3])

    # No-data cells
    no_data = voronoi_gdf[voronoi_gdf[winner_party_col].isna()]
    if len(no_data):
        no_data.plot(ax=ax, color=NO_DATA_COLOR, linewidth=0, zorder=1)

    # Voronoi cells — compute color per row, then plot all rows of same color together
    has_data = voronoi_gdf[voronoi_gdf[winner_party_col].notna()].copy()
    has_data[winner_pct_col] = pd.to_numeric(has_data[winner_pct_col], errors="coerce")
    has_data["_color"] = has_data.apply(
        lambda r: pct_to_color(r[winner_party_col], r[winner_pct_col], color_map), axis=1
    )

    for color, group in has_data.groupby("_color"):
        group.plot(ax=ax, color=color, linewidth=0.05, edgecolor="#00000033", zorder=1)

    # Constituency boundaries in white
    oevk_wgs = oevk.to_crs("EPSG:4326")
    oevk_wgs.boundary.plot(ax=ax, linewidth=0.4, color="white", zorder=3)

    # Budapest outer boundary — thick white
    budapest = oevk_wgs[oevk_wgs["maz"] == "01"].dissolve()
    budapest.boundary.plot(ax=ax, linewidth=1.8, color="white", zorder=4)

    # Legend
    parties_present = has_data[winner_party_col].dropna().unique()
    legend_patches = [
        mpatches.Patch(color=pct_to_color(p, 60, color_map), label=p)
        for p in sorted(parties_present)
    ]
    if legend_patches:
        ax.legend(handles=legend_patches, loc="lower left", fontsize=7,
                  framealpha=0.85, facecolor="#f0f0f0", title="Party", title_fontsize=8)

    ax.set_title(title, fontsize=16, color="white", pad=12, fontweight="bold")
    fig.savefig(out_path, format="svg", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  Saved: {out_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--admin",   default=ADMIN_PATH)
    parser.add_argument("--results", default=RESULTS_PATH)
    args = parser.parse_args()

    results, admin, color_map, ej_to_party, tl_to_party = load_data(args.admin)
    results = results[results.geometry.notna()].copy()

    oevk = gpd.read_file(OEVK_PATH)
    budapest_bbox = oevk[oevk["maz"] == "01"].to_crs("EPSG:4326").total_bounds
    budapest_geom = oevk[oevk["maz"] == "01"].to_crs("EPSG:4326").dissolve().geometry.iloc[0]

    voronoi = build_all_voronoi(results, admin, oevk)
    voronoi_bp = gpd.clip(voronoi, budapest_geom)
    fidesz_bp = voronoi_bp[voronoi_bp["l_winner_party"] == "FIDESZ-KDNP"]

    print("\nFidesz-winning polling places in Budapest (list vote):")
    for _, row in fidesz_bp.iterrows():
        print(row)

    make_map(voronoi, oevk, color_map,
             winner_party_col="e_winner_party", winner_pct_col="e_winner_pct",
             title="Hungary 2026 – Individual Race (Voronoi)",
             out_path=OUT_INDIVIDUAL)

    make_map(voronoi, oevk, color_map,
             winner_party_col="l_winner_party", winner_pct_col="l_winner_pct",
             title="Hungary 2026 – Party List Race (Voronoi)",
             out_path=OUT_LIST)

    make_map(voronoi_bp, oevk, color_map,
             winner_party_col="e_winner_party", winner_pct_col="e_winner_pct",
             title="Budapest 2026 – Individual Race (Voronoi)",
             out_path=OUT_BUDAPEST_INDIVIDUAL, bbox=budapest_bbox)

    make_map(voronoi_bp, oevk, color_map,
             winner_party_col="l_winner_party", winner_pct_col="l_winner_pct",
             title="Budapest 2026 – Party List Race (Voronoi)",
             out_path=OUT_BUDAPEST_LIST, bbox=budapest_bbox)

    print(f"\nDone. Outputs: {OUT_INDIVIDUAL}, {OUT_LIST}, "
          f"{OUT_BUDAPEST_INDIVIDUAL}, {OUT_BUDAPEST_LIST}")

if __name__ == "__main__":
    main()
