import os
import io
import pandas as pd
import streamlit as st
import pydeck as pdk
from typing import Callable, Optional
from sqlalchemy import create_engine, text

# =========================
# KONFIGURASI HALAMAN
# =========================
st.set_page_config(
    page_title="Peta Jumlah Pasien per Cabang HMHI",
    page_icon="🗺️",
    layout="wide"
)
st.title("🗺️ Peta Jumlah Pasien per Cabang HMHI")

# =========================
# UTIL KONEKSI (dual-mode)
# =========================
def _build_query_runner() -> Callable[[str], pd.DataFrame]:
    """
    Mencoba koneksi via st.connection (Streamlit native) terlebih dahulu.
    Jika gagal, fallback ke SQLAlchemy engine standar menggunakan DATABASE_URL.
    """
    try:
        # Percobaan 1: Native Streamlit Connection
        conn = st.connection("postgresql", type="sql")
        def _run_query_streamlit(sql: str) -> pd.DataFrame:
            return conn.query(sql, ttl=0)
        # Test koneksi
        _ = _run_query_streamlit("SELECT 1 as ok;")
        return _run_query_streamlit
    except Exception:
        pass

    # Percobaan 2: SQLAlchemy Engine
    db_url = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL", ""))
    if not db_url:
        st.error("❌ Koneksi DB tidak dikonfigurasi. Set 'connections.postgresql' di secrets.toml atau 'DATABASE_URL' di secrets.")
        st.stop()
    
    engine = create_engine(db_url, pool_pre_ping=True)

    def _run_query_engine(sql: str) -> pd.DataFrame:
        with engine.connect() as con:
            return pd.read_sql(text(sql), con)
    
    # Test koneksi
    try:
        _ = _run_query_engine("SELECT 1 as ok;")
    except Exception as e:
        st.error(f"Gagal koneksi ke Database: {e}")
        st.stop()
        
    return _run_query_engine

run_query = _build_query_runner()

# =========================
# DATA REKAP
# =========================
def load_rekap() -> pd.DataFrame:
    """
    Mengambil jumlah pasien per cabang.
    Menghitung data NULL/Kosong sebagai 'Tanpa Cabang'.
    """
    sql = """
        SELECT
            COALESCE(cabang, 'Tanpa Cabang') AS propinsi,
            COUNT(*) AS jumlah_pasien
        FROM
            pwh.patients
        GROUP BY
            1
        ORDER BY
            jumlah_pasien DESC;
    """
    df = run_query(sql)
    
    df = df.rename(columns={
        "propinsi": "Cabang HMHI",
        "jumlah_pasien": "Jumlah Pasien"
    })
    
    if "Cabang HMHI" in df.columns:
        df["Cabang HMHI"] = df["Cabang HMHI"].astype(str).str.strip()
            
    return df

# =========================
# UTIL GEOCODING
# =========================
def load_propinsi_geo_from_db() -> pd.DataFrame:
    """Mengambil referensi koordinat dari tabel public.kota_geo_new"""
    try:
        q = "SELECT propinsi, lat, lon FROM public.kota_geo_new;"
        df_geo = run_query(q)
        if "propinsi" in df_geo.columns:
            df_geo["propinsi"] = df_geo["propinsi"].astype(str).str.strip()
        return df_geo
    except Exception as e:
        return pd.DataFrame(columns=["propinsi", "lat", "lon"])

def lookup_coord_propinsi(prov_name: str, df_ref: pd.DataFrame) -> Optional[tuple]:
    """Mencari lat/lon berdasarkan nama cabang/propinsi (case-insensitive)"""
    if not prov_name:
        return None
        
    p = prov_name.strip().lower()

    if not df_ref.empty:
        df_norm = df_ref.copy()
        df_norm["propinsi_norm"] = df_norm["propinsi"].str.lower()
        
        hit = df_norm[df_norm["propinsi_norm"] == p]
        if not hit.empty:
            r = hit.iloc[0]
            return float(r["lat"]), float(r["lon"])

    return None

# =========================
# SIDEBAR OPSI
# =========================
st.sidebar.header("⚙️ Opsi Tampilan Peta")
heatmap_radius = st.sidebar.slider("Radius Heatmap", min_value=10, max_value=100, value=50, step=5)
min_count = st.sidebar.number_input("Filter minimum jumlah pasien", min_value=0, value=0, step=1)

# =========================
# PROSES DATA UTAMA
# =========================
df = load_rekap()
if df.empty:
    st.warning("Data pasien tidak ditemukan di database.")
    st.stop()

# 1. Filter Min Count
grouped = df.copy()
if min_count > 0:
    grouped = grouped[grouped["Jumlah Pasien"] >= min_count].copy()

# 2. Load Referensi Geografis
geo_ref = load_propinsi_geo_from_db()

# 3. Lookup Koordinat (Ke semua data)
grouped["coord"] = grouped["Cabang HMHI"].apply(lambda p: lookup_coord_propinsi(p, geo_ref))

# 4. Pisahkan Lat/Lon
latlon = grouped["coord"].apply(pd.Series)
if latlon.shape[1] >= 2:
    latlon = latlon.iloc[:, :2]
    latlon.columns = ["lat", "lon"]
    # Gabung lat/lon ke grouped
    grouped = pd.concat([grouped.drop(columns=["coord"]), latlon], axis=1)
else:
    grouped["lat"] = None
    grouped["lon"] = None

# =========================
# PEMISAHAN DATA: TABEL vs PETA
# =========================

# A. Data Tabel: Semua data (termasuk yang lat/lon nya None)
df_table = grouped.copy()

# B. Data Peta: Hanya yang punya lat/lon valid
valid_mask = (pd.notna(grouped["lat"])) & (pd.notna(grouped["lon"]))
grouped_valid_for_map = grouped[valid_mask].copy()

# Tambahkan properti visual map
if not grouped_valid_for_map.empty:
    grouped_valid_for_map["radius"] = (grouped_valid_for_map["Jumlah Pasien"] ** 0.5) * 2500 
    grouped_valid_for_map["label"] = grouped_valid_for_map.apply(lambda r: f"{r['Cabang HMHI']} : {int(r['Jumlah Pasien'])}", axis=1)

# =========================
# TAMPILAN: TABEL
# =========================
total_pasien_real = df_table["Jumlah Pasien"].sum()

st.subheader(f"📋 Rekap Per Cabang HMHI (Total Pasien: {total_pasien_real})")

# --- PERUBAHAN: Menambahkan 'lat' dan 'lon' ke display_cols ---
display_cols = ["Cabang HMHI", "Jumlah Pasien", "lat", "lon"]
df_to_show = df_table[display_cols].sort_values("Jumlah Pasien", ascending=False).copy()

# --- PERUBAHAN: Menambahkan lat/lon kosong untuk baris TOTAL ---
row_total = pd.DataFrame([{
    "Cabang HMHI": "TOTAL", 
    "Jumlah Pasien": total_pasien_real,
    "lat": None,
    "lon": None
}])
df_to_show = pd.concat([df_to_show, row_total], ignore_index=True)

# Tampilkan dataframe (lat/lon akan muncul)
st.dataframe(df_to_show, use_container_width=True, hide_index=True)

# Info jika ada data tanpa koordinat
count_no_geo = len(df_table) - len(grouped_valid_for_map)
if count_no_geo > 0:
    st.info(f"ℹ️ Ada **{count_no_geo} area/cabang** yang koordinatnya kosong (lihat kolom lat/lon bernilai None/NaN di tabel atas). Data ini tetap dihitung, namun tidak muncul di Peta.")

# =========================
# TAMPILAN: PETA
# =========================
st.subheader("🗺️ Peta Persebaran")

if grouped_valid_for_map.empty:
    st.warning("Tidak ada data cabang yang memiliki koordinat valid untuk ditampilkan di peta.")
else:
    # Konfigurasi Pydeck
    def_view = pdk.ViewState(latitude=-2.5, longitude=118.0, zoom=4.5, pitch=0)

    heatmap_layer = pdk.Layer(
        "HeatmapLayer",
        data=grouped_valid_for_map,
        get_position='[lon, lat]',
        get_weight="Jumlah Pasien",
        radius_pixels=int(heatmap_radius)
    )

    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=grouped_valid_for_map,
        get_position='[lon, lat]',
        get_radius='radius',
        get_fill_color='[200, 30, 0, 160]',
        pickable=True,
        auto_highlight=True
    )

    text_layer = pdk.Layer(
        "TextLayer",
        data=grouped_valid_for_map,
        get_position='[lon, lat]',
        get_text="label",
        get_size=14,
        get_color=[0, 0, 0],
        get_angle=0,
        billboard=True,
        get_alignment_baseline="'bottom'"
    )

    tooltip = {
        "html": "<b>Cabang HMHI: {Cabang HMHI}</b><br/>Jumlah Pasien: {Jumlah Pasien}",
        "style": {"backgroundColor": "white", "color": "black", "zIndex": "999"}
    }

    def get_map_style():
        token = st.secrets.get("MAPBOX_TOKEN", os.getenv("MAPBOX_TOKEN"))
        return "mapbox://styles/mapbox/light-v9" if token else None

    st.pydeck_chart(pdk.Deck(
        map_style=get_map_style(),
        initial_view_state=def_view,
        layers=[heatmap_layer, scatter_layer, text_layer],
        tooltip=tooltip
    ))

# =========================
# DOWNLOAD EXCEL
# =========================
if not df_to_show.empty:
    buffer = io.BytesIO()
    
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        # Download tabel persis seperti tampilan (termasuk lat/lon)
        df_to_show.to_excel(writer, index=False, sheet_name='Rekap Pasien')
            
    buffer.seek(0)
    
    st.download_button(
        label="📥 Download Rekap Data (Excel)",
        data=buffer,
        file_name="rekap_pasien_per_cabang_lengkap.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

st.caption("Sumber: Database PWH (Tabel pwh.patients). Koordinat Peta berdasarkan referensi tabel `public.kota_geo_new`.")
