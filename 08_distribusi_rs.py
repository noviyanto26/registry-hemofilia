import os
import pandas as pd
import streamlit as st
import pydeck as pdk
import requests
from typing import Callable, Optional
from sqlalchemy import create_engine, text

# =========================
# 1. KONFIGURASI HALAMAN
# =========================
st.set_page_config(
    page_title="Peta Jumlah Pasien per Kota",
    page_icon="🗺️",
    layout="wide"
)
st.title("🗺️ Peta Jumlah Pasien per Kota (Hemofilia)")

# =========================
# 2. UTIL KONEKSI DATABASE
# =========================
def _build_query_runner() -> Callable[[str], pd.DataFrame]:
    """
    Membangun fungsi query yang kompatibel dengan st.connection (lokal)
    atau sqlalchemy engine (jika st.connection belum dikonfigurasi).
    """
    # Cara 1: Coba pakai st.connection (Streamlit native)
    try:
        conn = st.connection("postgresql", type="sql")
        def _run_query_streamlit(sql: str) -> pd.DataFrame:
            # MODIFIKASI 1: Menambahkan ttl=0 agar data realtime (tidak di-cache)
            return conn.query(sql, ttl=0)
        _ = _run_query_streamlit("SELECT 1 as ok;")
        return _run_query_streamlit
    except Exception:
        pass

    # Cara 2: Fallback ke SQLAlchemy engine manual (baca dari secrets/env)
    db_url = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL", ""))
    if not db_url:
        st.error("❌ Koneksi DB tidak dikonfigurasi. Set 'connections.postgresql' di secrets.toml atau 'DATABASE_URL' di secrets.")
        st.stop()
    
    engine = create_engine(db_url, pool_pre_ping=True)

    def _run_query_engine(sql: str) -> pd.DataFrame:
        with engine.connect() as con:
            return pd.read_sql(text(sql), con)
    
    return _run_query_engine

run_query = _build_query_runner()

# =========================
# 3. LOAD DATA PASIEN
# =========================
def load_rekap() -> pd.DataFrame:
    """Mengambil data rekap pasien dari view pwh.v_hospital_summary"""
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
    # Bersihkan whitespace
    for c in ["Kota", "Propinsi"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

# =========================
# 4. CONFIG & UTIL GEOCODING
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

st.sidebar.header("⚙️ Opsi Geocoding & Tampilan")
use_online_geocoding = st.sidebar.toggle(
    "Aktifkan geocoding online (Nominatim/OSM)", value=False,
    help="Jika dinyalakan, kota yang tidak ditemukan di referensi lokal akan dicari via OSM (butuh internet & agak lambat)."
)
heatmap_radius = st.sidebar.slider("Radius Heatmap", min_value=10, max_value=80, value=40, step=5)
min_count = st.sidebar.number_input("Filter minimum jumlah pasien per kota", min_value=0, value=0, step=1)

def load_kota_geo_from_db() -> pd.DataFrame:
    """Mengambil referensi koordinat dari tabel public.kota_geo"""
    try:
        q = "SELECT kota, propinsi, lat, lon FROM public.kota_geo;"
        df_geo = run_query(q)
        for c in ["kota", "propinsi"]:
            df_geo[c] = df_geo[c].astype(str)
        return df_geo
    except Exception as e:
        st.warning(f"Gagal membaca tabel public.kota_geo: {e}")
        return pd.DataFrame(columns=["kota", "propinsi", "lat", "lon"])

def normalize_name(name: str) -> str:
    """
    Versi perbaikan: Menangani 'Kab.', 'Kab', 'Kabupaten' secara konsisten.
    Contoh hasil: 
      - 'Kab. Bandung' -> 'kab bandung'
      - 'Kabupaten Bandung' -> 'kab bandung'
      - 'Kota Bandung' -> 'kota bandung'
    """
    if not isinstance(name, str): return ""
    name = name.lower().strip()
    
    # 1. Ganti titik dengan spasi (KRUSIAL untuk menangani 'Kab. Bandung')
    name = name.replace(".", " ") 
    
    # 2. Hapus kata 'propinsi' atau 'provinsi' jika ada
    name = name.replace("propinsi ", "").replace("provinsi ", "")

    # 3. Standarisasi variasi 'kota'
    if name.startswith("kota administrasi "):
        name = name.replace("kota administrasi ", "kota ", 1)
        
    # 4. Standarisasi variasi 'kabupaten'
    # Ubah 'kabupaten ' menjadi 'kab ' standar
    if name.startswith("kabupaten "):
        name = name.replace("kabupaten ", "kab ", 1)
    # Handle 'kab ' (termasuk yang asalnya 'kab.' lalu titiknya dihapus di step 1)
    elif name.startswith("kab ") and not name.startswith("kabupaten"):
        pass # Sudah format 'kab ...', biarkan

    # 5. Bersihkan spasi ganda (double space) akibat replace
    return " ".join(name.split())

def create_geo_lookup(df_geo: pd.DataFrame) -> dict:
    """Membuat Dictionary Lookup Key=(kota_norm, prov_norm) -> Value=(lat, lon)"""
    lookup = {}
    for _, row in df_geo.iterrows():
        c_norm = normalize_name(row["kota"])
        p_norm = normalize_name(row["propinsi"])
        lookup[(c_norm, p_norm)] = (float(row["lat"]), float(row["lon"]))
    return lookup

def nominatim_geocode(city: str, province: str) -> Optional[tuple]:
    """Fallback ke API OpenStreetMap jika data lokal tidak ada"""
    base = "https://nominatim.openstreetmap.org/search"
    params = {"q": f"{city}, {province}, Indonesia", "format": "json", "limit": 1}
    headers = {"User-Agent": "hemofilia-geo-app/1.0"}
    try:
        r = requests.get(base, params=params, headers=headers, timeout=5)
        r.raise_for_status()
        j = r.json()
        if isinstance(j, list) and j:
            return float(j[0]["lat"]), float(j[0]["lon"])
    except Exception:
        pass
    return None

def get_coordinates(city_raw: str, prov_raw: str, lookup_dict: dict) -> Optional[tuple]:
    """Logika utama pencarian koordinat"""
    c_norm = normalize_name(city_raw)
    p_norm = normalize_name(prov_raw)
    
    # 1. Cek Lookup DB (Normalized)
    if (c_norm, p_norm) in lookup_dict:
        return lookup_dict[(c_norm, p_norm)]
    
    # 2. Cek Static Hardcoded
    if (c_norm, p_norm) in STATIC_CITY_COORDS:
        return STATIC_CITY_COORDS[(c_norm, p_norm)]
    
    # 3. (Opsional) Cek jika Provinsi typo
    for (k_city, k_prov), val in lookup_dict.items():
        if k_city == c_norm:
            return val
            
    return None

# =========================
# 5. LOGIKA UTAMA (MAIN PROCESS)
# =========================

# A. Load Data Transaksi
df_pasien = load_rekap()
if df_pasien.empty:
    st.warning("Data rekap pasien kosong atau tabel tidak ditemukan.")
    st.stop()

# B. Agregasi Data (Group By Kota & Propinsi)
grouped = df_pasien.groupby(["Kota", "Propinsi"], dropna=False).agg(
    **{
        "Jumlah Pasien": ("Jumlah Pasien", "sum"),
        "Rumah Sakit Penangan": ("Nama Rumah Sakit", lambda s: ", ".join(s.unique()))
    }
).reset_index()

# Filter Jumlah Minimum
if min_count > 0:
    grouped = grouped[grouped["Jumlah Pasien"] >= min_count].copy()

# C. Load Data Geo & Buat Lookup
df_geo_ref = load_kota_geo_from_db()
geo_lookup_dict = create_geo_lookup(df_geo_ref)

# D. Proses Mapping Koordinat
def apply_mapping(row):
    city = row["Kota"]
    prov = row["Propinsi"]
    
    # Cek Lokal
    coords = get_coordinates(city, prov, geo_lookup_dict)
    
    if coords:
        return pd.Series([coords[0], coords[1], True])
    
    # Cek Online (Jika user mengaktifkan)
    if use_online_geocoding:
        online_coords = nominatim_geocode(city, prov)
        if online_coords:
             return pd.Series([online_coords[0], online_coords[1], True])
    
    return pd.Series([None, None, False])

# Terapkan function ke dataframe
grouped[["lat", "lon", "found"]] = grouped.apply(apply_mapping, axis=1)

# E. Pisahkan Valid dan Invalid
grouped_valid = grouped[grouped["found"] == True].copy()
grouped_missing = grouped[grouped["found"] == False].copy()

# =========================
# 6. VISUALISASI
# =========================

# Tampilkan Debugging jika ada data yang hilang
if not grouped_missing.empty:
    with st.expander(f"⚠️ Ada {len(grouped_missing)} Kota Tanpa Koordinat (Klik untuk Detail)", expanded=False):
        st.warning("Kota berikut ada di data pasien tapi tidak ditemukan di `public.kota_geo`. Silakan insert manual atau aktifkan geocoding online.")
        st.dataframe(grouped_missing[["Kota", "Propinsi", "Jumlah Pasien"]])

# Tabel Data Valid
st.subheader(f"📋 Data Terpetakan ({len(grouped_valid)} Kota)")
if not grouped_valid.empty:
    # Tambahkan kolom radius dan label untuk visualisasi Map
    grouped_valid["radius"] = 3000 
    grouped_valid["label"] = grouped_valid.apply(lambda r: f"{r['Kota']} : {int(r['Jumlah Pasien'])}", axis=1)
    
    # MODIFIKASI 2: Membuat Tabel Display dengan Baris TOTAL
    # 1. Ambil kolom yang akan ditampilkan dan urutkan
    df_display = grouped_valid[["Kota", "Propinsi", "Jumlah Pasien", "Rumah Sakit Penangan", "lat", "lon"]].sort_values("Jumlah Pasien", ascending=False).copy()
    
    # 2. Hitung Total
    total_pasien = df_display["Jumlah Pasien"].sum()
    
    # 3. Buat Baris Total
    row_total = pd.DataFrame([{
        "Kota": "TOTAL",
        "Propinsi": "",
        "Jumlah Pasien": total_pasien,
        "Rumah Sakit Penangan": "",
        "lat": None,
        "lon": None
    }])
    
    # 4. Gabungkan (Concatenate) ke bagian bawah tabel
    df_display = pd.concat([df_display, row_total], ignore_index=True)

    # Tampilkan Tabel
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("Belum ada data yang terpetakan.")

# Peta Pydeck
def get_map_style():
    token = st.secrets.get("MAPBOX_TOKEN", os.getenv("MAPBOX_TOKEN"))
    if token:
        pdk.settings.mapbox_api_key = token
        return "mapbox://styles/mapbox/light-v9"
    return None # Default ke Carto/OSM style jika token kosong

st.subheader("🗺️ Peta Persebaran")
if not grouped_valid.empty:
    # View State Awal (Indonesia Tengah)
    view_state = pdk.ViewState(latitude=-2.5, longitude=118.0, zoom=4.2, pitch=0)

    # Layer 1: Heatmap
    heatmap_layer = pdk.Layer(
        "HeatmapLayer",
        data=grouped_valid,
        get_position='[lon, lat]',
        get_weight="[Jumlah Pasien]", # Mengacu ke kolom DataFrame
        radius_pixels=int(heatmap_radius)
    )

    # Layer 2: Scatterplot (Titik Merah)
    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=grouped_valid,
        get_position='[lon, lat]',
        get_radius="radius",
        get_fill_color='[255, 0, 0, 160]',
        pickable=True,
        auto_highlight=True
    )

    # Layer 3: Text Label
    text_layer = pdk.Layer(
        "TextLayer",
        data=grouped_valid,
        get_position='[lon, lat]',
        get_text="label",
        get_size=14,
        get_color=[0, 0, 0],
        get_alignment_baseline="'bottom'",
        billboard=True
    )
    
    tooltip = {
        "html": "<b>{Kota}, {Propinsi}</b><br/>Jumlah Pasien: {Jumlah Pasien}<br/><i>{Rumah Sakit Penangan}</i>",
        "style": {"backgroundColor": "white", "color": "black"}
    }

    deck = pdk.Deck(
        map_style=get_map_style(),
        initial_view_state=view_state,
        layers=[heatmap_layer, scatter_layer, text_layer],
        tooltip=tooltip
    )
    
    st.pydeck_chart(deck)

    # Tombol Download
    csv_data = grouped_valid[["Kota", "Propinsi", "Jumlah Pasien", "Rumah Sakit Penangan", "lat", "lon"]].to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download Data CSV",
        data=csv_data,
        file_name="rekap_sebaran_hemofilia.csv",
        mime="text/csv"
    )
else:
    st.write("Tidak ada data valid untuk ditampilkan di peta.")
