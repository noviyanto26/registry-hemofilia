import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="Data Rumah Sakit Perawatan Hemofilia",
    page_icon="🏥",
    layout="wide"
)

# --- KONEKSI DATABASE ---
def _resolve_db_url() -> str:
    """Mencari DATABASE_URL dari st.secrets atau environment variables."""
    try:
        sec = st.secrets.get("DATABASE_URL", "")
        if sec:
            return sec
    except Exception:
        pass
    env = os.environ.get("DATABASE_URL")
    if env:
        return env
    st.error('DATABASE_URL tidak ditemukan. Mohon atur di `.streamlit/secrets.toml`.')
    return None

@st.cache_resource(show_spinner="Menghubungkan ke database...")
def get_engine(dsn: str) -> Engine:
    """Membuat dan menyimpan koneksi database engine."""
    if not dsn:
        st.stop()
    try:
        engine = create_engine(dsn, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"Gagal terhubung ke database: {e}")
        st.stop()

# --- FUNGSI PENGAMBILAN DATA DASHBOARD ---
@st.cache_data(ttl="10m")
def load_data_dashboard(_engine: Engine) -> pd.DataFrame:
    """
    Menjalankan query ke database untuk data dashboard utama.
    Data ini di-cache karena tidak sering berubah.
    """
    query = text("SELECT * FROM pwh.rumah_sakit_perawatan_hemofilia ORDER BY no;")
    with _engine.connect() as conn:
        df = pd.read_sql(query, conn)

    # Pastikan kolom boolean bertipe benar (True/False/NA)
    for col in ["terdapat_dokter_hematologi", "terdapat_tim_terpadu_hemofilia"]:
        if col in df.columns:
            df[col] = df[col].astype("boolean")
    return df

# --- ALIAS KOLOM UNTUK TAMPILAN ---
COL_ALIAS = {
    "no": "No",
    "provinsi": "Propinsi",
    "nama_rumah_sakit": "Nama Rumah Sakit",
    "tipe_rs": "Tipe RS",
    "terdapat_dokter_hematologi": "Terdapat Dokter Hematologi",
    "terdapat_tim_terpadu_hemofilia": "Terdapat Tim Terpadu Hemofilia",
}
DISPLAY_COL_ORDER = [
    "no",
    "provinsi",
    "nama_rumah_sakit",
    "tipe_rs",
    "terdapat_dokter_hematologi",
    "terdapat_tim_terpadu_hemofilia",
]

def alias_for_display(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in DISPLAY_COL_ORDER if c in df.columns]
    view = df[cols].copy() if cols else df.copy()
    return view.rename(columns={k: v for k, v in COL_ALIAS.items() if k in view.columns})

# --- TAMPILAN APLIKASI ---
st.title("🏥 Dashboard Rumah Sakit Hemofilia")

db_url = _resolve_db_url()
if db_url:
    engine = get_engine(db_url)

    # Load Data Utama
    df = load_data_dashboard(engine)

    st.markdown(
        "Gunakan **Filter Data** di bawah untuk menyaring tampilan. "
        "Secara default, semua rumah sakit ditampilkan."
    )
    st.subheader("🔎 Filter Data")

    c1, c2, c3 = st.columns([1.2, 1, 1])

    with c1:
        provinsi_list = sorted([p for p in df["provinsi"].dropna().unique()])
        provinsi_options = ["Semua Propinsi"] + provinsi_list
        provinsi_pilihan = st.selectbox("Pilih Propinsi", options=provinsi_options, index=0)

    with c2:
        dokter_option = st.selectbox(
            "Ketersediaan Dokter Hematologi",
            options=["Semua", "Ada", "Tidak Ada", "Data Kosong"],
            index=0,
        )

    with c3:
        tim_option = st.selectbox(
            "Ketersediaan Tim Terpadu Hemofilia",
            options=["Semua", "Ada", "Tidak Ada", "Data Kosong"],
            index=0,
        )

    # Proses Filter
    df_filtered = df.copy()
    
    # Filter Propinsi
    if provinsi_pilihan != "Semua Propinsi":
        df_filtered = df_filtered[df_filtered["provinsi"] == provinsi_pilihan]
    
    # Filter Dokter
    if dokter_option == "Ada":
        df_filtered = df_filtered[df_filtered["terdapat_dokter_hematologi"] == True]
    elif dokter_option == "Tidak Ada":
        df_filtered = df_filtered[df_filtered["terdapat_dokter_hematologi"] == False]
    elif dokter_option == "Data Kosong":
        df_filtered = df_filtered[df_filtered["terdapat_dokter_hematologi"].isna()]
    
    # Filter Tim
    if tim_option == "Ada":
        df_filtered = df_filtered[df_filtered["terdapat_tim_terpadu_hemofilia"] == True]
    elif tim_option == "Tidak Ada":
        df_filtered = df_filtered[df_filtered["terdapat_tim_terpadu_hemofilia"] == False]
    elif tim_option == "Data Kosong":
        df_filtered = df_filtered[df_filtered["terdapat_tim_terpadu_hemofilia"].isna()]

    # Tampilan Tabel & Statistik
    st.header(f"Tabel Data Rumah Sakit ({len(df_filtered)} data ditemukan)")
    st.dataframe(alias_for_display(df_filtered), use_container_width=True, hide_index=True)

    st.header("Statistik Singkat (dari keseluruhan data)")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total RS Tercatat", len(df))
    with col2:
        # Menghitung jumlah True, mengabaikan NA
        rs_dokter = int((df["terdapat_dokter_hematologi"] == True).sum())
        st.metric("RS Dengan Dokter Hematologi", rs_dokter)
    with col3:
        # Menghitung jumlah True, mengabaikan NA
        rs_tim = int((df["terdapat_tim_terpadu_hemofilia"] == True).sum())
        st.metric("RS Dengan Tim Terpadu", rs_tim)