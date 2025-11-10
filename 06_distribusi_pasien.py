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
    page_title="Peta Jumlah Pasien per Propinsi",
    page_icon="🗺️",
    layout="wide"
)
st.title("🗺️ Peta Jumlah Pasien per Propinsi (Hemofilia)")

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
# DATA REKAP (Hanya berdasarkan Cabang/Propinsi)
# =========================
def load_rekap() -> pd.DataFrame:
    # MODIFIKASI: Query hanya mengelompokkan berdasarkan 'cabang'.
    # Asumsi: nilai di kolom 'cabang' adalah nama Propinsi.
    sql = """
        SELECT
            cabang AS propinsi,        -- Cabang kita anggap sebagai Propinsi
            COUNT(*) AS jumlah_pasien
        FROM
            pwh.patients
        WHERE
            cabang IS NOT NULL
        GROUP BY
            cabang
        HAVING
            COUNT(*) > 0
        ORDER BY
            jumlah_pasien DESC, cabang ASC;
    """
    df = run_query(sql)
    
    # Rename agar sesuai dengan standar internal skrip
    df = df.rename(columns={
        "propinsi": "Propinsi",
        "jumlah_pasien": "Jumlah Pasien"
    })
    
    # Bersihkan whitespace
    if "Propinsi" in df.columns:
        df["Propinsi"] = df["Propinsi"].astype(str).str.strip()
            
    return df

# =========================
# OPSI TAMPILAN
# =========================
st.sidebar.header("⚙️ Opsi Tampilan Peta")
heatmap_radius = st.sidebar.slider("Radius Heatmap", min_value=10, max_value=100, value=50, step=5)
min_count = st.sidebar.number_input("Filter minimum jumlah pasien", min_value=0, value=0, step=1)

# =========================
# UTIL GEOCODING (Sederhana: Hanya Propinsi)
# =========================
def load_propinsi_geo_from_db() -> pd.DataFrame:
    # MODIFIKASI: Mengambil data dari tabel referensi BARU 'public.kota_geo_new'
    # Hanya mengambil kolom 'propinsi', 'lat', 'lon'
    try:
        q = "SELECT propinsi, lat, lon FROM public.kota_geo_new;"
        df_geo = run_query(q)
        if "propinsi" in df_geo.columns:
            df_geo["propinsi"] = df_geo["propinsi"].astype(str).str.strip()
        return df_geo
    except Exception as e:
        # Jika tabel baru belum ada, return dataframe kosong agar tidak crash
        # st.error(f"Gagal memuat referensi geo: {e}") # Optional debug
        return pd.DataFrame(columns=["propinsi", "lat", "lon"])

def lookup_coord_propinsi(prov_name: str, df_ref: pd.DataFrame) -> Optional[tuple]:
    # MODIFIKASI: Fungsi lookup yang jauh lebih sederhana.
    # Hanya mencocokkan nama propinsi.
    p = (prov_name or "").strip().lower()

    if not df_ref.empty:
        # Normalisasi data referensi untuk pencarian
        df_norm = df_ref.copy()
        df_norm["propinsi_norm"] = df_norm["propinsi"].str.lower()
        
        # Cari yang cocok persis
        hit = df_norm[df_norm["propinsi_norm"] == p]
        if not hit.empty:
            r = hit.iloc[0]
            return float(r["lat"]), float(r["lon"])

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
    st.warning("Data tidak ditemukan. Pastikan tabel pwh.patients berisi data dengan kolom 'cabang' terisi.")
    st.stop()

grouped = df.copy()
if min_count > 0:
    grouped = grouped[grouped["Jumlah Pasien"] >= min_count].copy()

# Muat referensi geo yang baru
geo_ref = load_propinsi_geo_from_db()

if geo_ref.empty:
     st.error("⚠️ Tabel referensi `public.kota_geo_new` kosong atau tidak ditemukan. Peta tidak dapat ditampilkan.")
     st.stop()

# Lakukan lookup hanya berdasarkan propinsi
grouped["coord"] = grouped["Propinsi"].apply(lambda p: lookup_coord_propinsi(p, geo_ref))

valid_mask = grouped["coord"].apply(_is_valid_coord)
grouped_valid = grouped[valid_mask].copy()

# Pisahkan lat/lon dengan aman
if not grouped_valid.empty:
    latlon = grouped_valid["coord"].apply(pd.Series)
    if latlon.shape[1] >= 2:
        latlon = latlon.iloc[:, :2]
        latlon.columns = ["lat", "lon"]
        grouped_valid = pd.concat([grouped_valid.drop(columns=["coord"]), latlon], axis=1)
    else:
        grouped_valid = grouped_valid.iloc[0:0] # Kosongkan jika gagal parsing
else:
     # Siapkan kolom kosong jika tidak ada data valid
    grouped_valid["lat"] = pd.Series(dtype=float)
    grouped_valid["lon"] = pd.Series(dtype=float)

# Finalisasi data untuk peta
if not grouped_valid.empty:
    grouped_valid["radius"] = (grouped_valid["Jumlah Pasien"] ** 0.5) * 2500 # Radius sedikit diperbesar untuk propinsi
    grouped_valid["label"] = grouped_valid.apply(lambda r: f"{r['Propinsi']} : {int(r['Jumlah Pasien'])}", axis=1)

# =========================
# TAMPILAN STREAMLIT
# =========================
st.subheader(f"📋 Rekap Per Propinsi (valid: {len(grouped_valid)}/{len(grouped)})")

# Tampilkan tabel data
display_cols = ["Propinsi", "Jumlah Pasien"]
if not grouped_valid.empty:
    st.dataframe(grouped_valid[display_cols + ["lat", "lon"]].sort_values("Jumlah Pasien", ascending=False), use_container_width=True, hide_index=True)
else:
    st.dataframe(grouped[display_cols], use_container_width=True, hide_index=True) # Tampilkan data mentah jika tidak ada yg valid

# Konfigurasi Peta Pydeck
def_view = pdk.ViewState(latitude=-2.5, longitude=118.0, zoom=4.5, pitch=0)

heatmap_layer = pdk.Layer(
    "HeatmapLayer",
    data=grouped_valid,
    get_position='[lon, lat]',
    get_weight="Jumlah Pasien",
    radius_pixels=int(heatmap_radius)
)

scatter_layer = pdk.Layer(
    "ScatterplotLayer",
    data=grouped_valid,
    get_position='[lon, lat]',
    get_radius='radius',
    get_fill_color='[200, 30, 0, 160]',
    pickable=True,
    auto_highlight=True
)

text_layer = pdk.Layer(
    "TextLayer",
    data=grouped_valid,
    get_position='[lon, lat]',
    get_text="label",
    get_size=14,
    get_color=[0, 0, 0],
    get_angle=0,
    billboard=True,
    get_alignment_baseline="'bottom'"
)

tooltip = {
    "html": "<b>Propinsi: {Propinsi}</b><br/>Jumlah Pasien: {Jumlah Pasien}",
    "style": {"backgroundColor": "white", "color": "black", "zIndex": "999"}
}

def get_map_style():
    token = st.secrets.get("MAPBOX_TOKEN", os.getenv("MAPBOX_TOKEN"))
    return "mapbox://styles/mapbox/light-v9" if token else None

st.subheader("🗺️ Peta Persebaran")
if grouped_valid.empty:
    st.warning("Belum ada data cabang yang cocok dengan referensi propinsi di `public.kota_geo_new`. Periksa kesesuaian penulisan nama propinsi.")
else:
    st.pydeck_chart(pdk.Deck(
        map_style=get_map_style(),
        initial_view_state=def_view,
        layers=[heatmap_layer, scatter_layer, text_layer],
        tooltip=tooltip
    ))

# Tombol Download
if not grouped_valid.empty:
    csv_data = grouped_valid.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Data (CSV)",
        data=csv_data,
        file_name="rekap_pasien_per_propinsi.csv",
        mime="text/csv"
    )

st.caption("Sumber: Agregasi dari tabel **pwh.patients** (kolom `cabang`). Koordinat diambil dari tabel referensi **`public.kota_geo_new`** berdasarkan kesamaan nama Propinsi.")
