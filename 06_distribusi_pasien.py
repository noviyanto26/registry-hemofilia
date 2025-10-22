import os
import pandas as pd
import streamlit as st
import pydeck as pdk
import requests
from typing import Callable, Optional
from sqlalchemy import create_engine, text

# =========================
# KONFIGURASI HALAMAN
# =========================
st.set_page_config(
    page_title="Peta Jumlah Pasien per Kota",
    page_icon="🗺️",
    layout="wide"
)
st.title("🗺️ Peta Jumlah Pasien per Kota (Hemofilia)")

# =========================
# UTIL KONEKSI (dual-mode)
# =========================
def _build_query_runner() -> Callable[[str], pd.DataFrame]:
    try:
        conn = st.connection("postgresql", type="sql")
        def _run_query_streamlit(sql: str) -> pd.DataFrame:
            return conn.query(sql)
        _ = _run_query_streamlit("SELECT 1 as ok;")
        return _run_query_streamlit
    except Exception:
        pass

    db_url = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL", ""))
    if not db_url:
        st.error("❌ Koneksi DB tidak dikonfigurasi. Set 'connections.postgresql' di secrets.toml atau 'DATABASE_URL' di secrets.")
        st.stop()
    engine = create_engine(db_url, pool_pre_ping=True)

    def _run_query_engine(sql: str) -> pd.DataFrame:
        with engine.connect() as con:
            return pd.read_sql(text(sql), con)
    _ = _run_query_engine("SELECT 1 as ok;")
    return _run_query_engine

run_query = _build_query_runner()

# =========================
# DATA REKAP
# =========================
def load_rekap() -> pd.DataFrame:
    sql = """
        SELECT
            "Nama Rumah Sakit",
            "Jumlah Pasien",
            "Kota",
            "Propinsi"
        FROM pwh.v_hospital_summary
        ORDER BY "Jumlah Pasien" DESC, "Nama Rumah Sakit" ASC;
    """
    df = run_query(sql)
    for c in ["Kota", "Propinsi"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

# =========================
# SUMBER KOORDINAT LOKAL (Fallback statis)
# =========================
STATIC_CITY_COORDS = {
    ("jakarta", "dki jakarta"): (-6.1754, 106.8272),
    ("jakarta pusat", "dki jakarta"): (-6.1857, 106.8410),
    ("bekasi", "jawa barat"): (-6.2383, 106.9756),
    ("depok", "jawa barat"): (-6.4025, 106.7942),
    ("bogor", "jawa barat"): (-6.5971, 106.8060),
    ("bandung", "jawa barat"): (-6.9147, 107.6098),
    ("semarang", "jawa tengah"): (-6.9667, 110.4167),
    ("surabaya", "jawa timur"): (-7.2575, 112.7521),
}

# =========================
# OPSI GEOCODING
# =========================
st.sidebar.header("⚙️ Opsi Geocoding & Tampilan")
use_online_geocoding = st.sidebar.toggle(
    "Aktifkan geocoding online (Nominatim/OSM)", value=False,
    help="Jika dinyalakan, kota yang tidak ditemukan di referensi lokal akan dicari via OSM (butuh internet)."
)
heatmap_radius = st.sidebar.slider("Radius Heatmap", min_value=10, max_value=80, value=40, step=5)
min_count = st.sidebar.number_input("Filter minimum jumlah pasien per kota", min_value=0, value=0, step=1)

# =========================
# UTIL GEOCODING
# =========================
def load_kota_geo_from_db() -> pd.DataFrame:
    try:
        q = "SELECT kota, propinsi, lat, lon FROM public.kota_geo;"
        df_geo = run_query(q)
        for c in ["kota", "propinsi"]:
            df_geo[c] = df_geo[c].astype(str).str.strip()
        return df_geo
    except Exception:
        return pd.DataFrame(columns=["kota", "propinsi", "lat", "lon"])

def nominatim_geocode(city: str, province: str) -> Optional[tuple]:
    base = "https://nominatim.openstreetmap.org/search"
    params = {"q": f"{city}, {province}, Indonesia", "format": "json", "limit": 1}
    headers = {"User-Agent": "hemofilia-geo/1.0"}
    try:
        r = requests.get(base, params=params, headers=headers, timeout=10)
        r.raise_for_status()
        j = r.json()
        if isinstance(j, list) and j:
            return float(j[0]["lat"]), float(j[0]["lon"])
    except Exception:
        pass
    return None

def lookup_coord(city: str, province: str, df_ref: pd.DataFrame) -> Optional[tuple]:
    c = (city or "").strip().lower()
    if c.startswith("kota "):
        c = c.replace("kota ", "", 1)
    p = (province or "").strip().lower()

    df_norm = df_ref.copy()
    if not df_norm.empty:
        df_norm["kota"] = df_norm["kota"].str.lower().str.replace("^kota\\s+", "", regex=True)
        df_norm["propinsi"] = df_norm["propinsi"].str.lower()
        hit = df_norm[(df_norm["kota"] == c) & (df_norm["propinsi"] == p)]
        if not hit.empty:
            r = hit.iloc[0]
            return float(r["lat"]), float(r["lon"])

    if (c, p) in STATIC_CITY_COORDS:
        return STATIC_CITY_COORDS[(c, p)]

    if use_online_geocoding:
        return nominatim_geocode(city, province)

    return None

def _is_valid_coord(v) -> bool:
    return (
        isinstance(v, (list, tuple))
        and len(v) == 2
        and pd.notna(v[0])
        and pd.notna(v[1])
    )

# =========================
# PROSES DATA & PETA
# =========================
df = load_rekap()
if df.empty:
    st.warning("Data rekap tidak ditemukan. Pastikan view pwh.v_hospital_summary tersedia.")
    st.stop()

grouped = df.groupby(["Kota", "Propinsi"], dropna=False)["Jumlah Pasien"].sum().reset_index()
if min_count > 0:
    grouped = grouped[grouped["Jumlah Pasien"] >= min_count].copy()

geo_ref = load_kota_geo_from_db()
grouped["coord"] = grouped.apply(lambda r: lookup_coord(r["Kota"], r["Propinsi"], geo_ref), axis=1)

valid_mask = grouped["coord"].apply(_is_valid_coord)
grouped_valid = grouped[valid_mask].copy()
latlon = grouped_valid["coord"].apply(pd.Series)
latlon.columns = ["lat", "lon"]
grouped_valid = pd.concat([grouped_valid.drop(columns=["coord"]), latlon], axis=1)
grouped_valid = grouped_valid.dropna(subset=["lat", "lon"])

if not grouped_valid.empty:
    grouped_valid["radius"] = (grouped_valid["Jumlah Pasien"] ** 0.5) * 2000
    grouped_valid["label"] = grouped_valid.apply(lambda r: f"{r['Kota']} : {int(r['Jumlah Pasien'])}", axis=1)

st.subheader(f"📋 Rekap Per Kota (koordinat valid: {len(grouped_valid)}/{len(grouped)})")
st.dataframe(grouped_valid[["Kota", "Propinsi", "Jumlah Pasien", "lat", "lon"]].sort_values("Jumlah Pasien", ascending=False), use_container_width=True, hide_index=True)

def_view = pdk.ViewState(latitude=-2.5, longitude=118.0, zoom=4.2, pitch=0)
heatmap_layer = pdk.Layer("HeatmapLayer", data=grouped_valid, get_position='[lon, lat]', get_weight="Jumlah Pasien", radius_pixels=int(heatmap_radius))
scatter_layer = pdk.Layer(
    "ScatterplotLayer",
    data=grouped_valid,
    get_position='[lon, lat]',
    get_radius='radius',
    get_fill_color='[255, 0, 0, 160]',
    pickable=True,
    auto_highlight=True
)
text_layer = pdk.Layer(
    "TextLayer",
    data=grouped_valid,
    get_position='[lon, lat]',
    get_text="label",
    get_size=16,
    get_color=[0, 0, 0],
    get_angle=0,
    billboard=True
)

tooltip = {"html": "<b>{Kota}, {Propinsi}</b><br/>Jumlah Pasien: {Jumlah Pasien}", "style": {"backgroundColor": "white", "color": "black"}}

def get_map_style():
    token = st.secrets.get("MAPBOX_TOKEN", os.getenv("MAPBOX_TOKEN"))
    if token:
        pdk.settings.mapbox_api_key = token
        return "mapbox://styles/mapbox/light-v9"
    return None

st.subheader("🗺️ Peta Persebaran")
if grouped_valid.empty:
    st.info("Belum ada koordinat kota yang valid. Pastikan tabel public.kota_geo terisi atau aktifkan geocoding online.")
else:
    st.pydeck_chart(pdk.Deck(map_style=get_map_style(), initial_view_state=def_view, layers=[heatmap_layer, scatter_layer, text_layer], tooltip=tooltip))

if not grouped_valid.empty:
    st.download_button("📥 Download Data Per Kota (CSV)", data=grouped_valid[["Kota", "Propinsi", "Jumlah Pasien", "lat", "lon"]].to_csv(index=False).encode("utf-8"), file_name="rekap_pasien_per_kota.csv", mime="text/csv")

st.caption("Sumber: view **pwh.v_hospital_summary**. Koordinat diambil dari tabel lokal `public.kota_geo` (jika ada), fallback kamus statis, dan *opsional* geocoding online Nominatim/OSM. Jika tidak ada MAPBOX_TOKEN, otomatis memakai OSM default. Label jumlah pasien ditampilkan di titik kota.")
