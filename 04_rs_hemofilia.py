# 04_rs_hemofilia.py (patched to follow view schema: Nama Rumah Sakit, Jumlah Pasien, Kota, Propinsi)
import os
import io
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
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

# --- FUNGSI PENGAMBILAN DATA REKAP (SESUAI SCHEMA VIEW) ---
def _select_from_view(engine: Engine) -> pd.DataFrame:
    """
    Coba ambil langsung dari view pwh.v_hospital_summary
    dengan kolom: Nama Rumah Sakit, Jumlah Pasien, Kota, Propinsi
    """
    sql = text("""
        SELECT
            "Nama Rumah Sakit",
            "Jumlah Pasien",
            "Kota",
            "Propinsi"
        FROM pwh.v_hospital_summary
        ORDER BY "Jumlah Pasien" DESC, "Nama Rumah Sakit" ASC;
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

def _select_fallback(engine: Engine) -> pd.DataFrame:
    """
    Jika view belum ada, fallback ke query ekuivalen:
    Ambil dari pwh.treatment_hospital (group by) + info Kota/Propinsi dari public.rumah_sakit.
    Sesuaikan kolom ON jika nama kolom RS berbeda (rs.nama / rs.name_hospital).
    """
    sql = text("""
        SELECT
            COALESCE(NULLIF(TRIM(th.name_hospital), ''), 'Data Tidak Disediakan') AS "Nama Rumah Sakit",
            COUNT(*)::bigint AS "Jumlah Pasien",
            rs.kota AS "Kota",
            rs.propinsi AS "Propinsi"
        FROM pwh.treatment_hospital th
        LEFT JOIN public.rumah_sakit rs
               ON th.name_hospital = rs.nama
        GROUP BY 1, 3, 4
        ORDER BY "Jumlah Pasien" DESC, "Nama Rumah Sakit" ASC;
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

def fetch_view_rs(engine: Engine) -> pd.DataFrame:
    """
    Ambil data sesuai schema Excel:
    Kolom: Nama Rumah Sakit, Jumlah Pasien, Kota, Propinsi
    1) Coba dari view pwh.v_hospital_summary
    2) Jika gagal, fallback ke query builder ekuivalen
    """
    st.info("🔄 Mengambil data rekap dari database...")
    try:
        df = _select_from_view(engine)
        if not set(["Nama Rumah Sakit", "Jumlah Pasien", "Kota", "Propinsi"]).issubset(df.columns):
            raise ValueError("Kolom view tidak sesuai schema yang diharapkan.")
        return df
    except Exception:
        # fallback otomatis
        try:
            return _select_fallback(engine)
        except Exception as e:
            st.error(f"Gagal mengambil data rekapitulasi: {e}")
            return pd.DataFrame(columns=["Nama Rumah Sakit", "Jumlah Pasien", "Kota", "Propinsi"])

# --- FUNGSI UTILITAS ---
def _to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Data") -> bytes:
    """Mengubah DataFrame ke file Excel (bytes) dengan fallback engine."""
    output = io.BytesIO()
    engine_name = None
    for eng in ("openpyxl", "xlsxwriter"):
        try:
            with pd.ExcelWriter(output, engine=eng) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            engine_name = eng
            break
        except Exception:
            output = io.BytesIO()  # reset buffer dan coba engine lain
            continue
    if not engine_name:
        raise RuntimeError("Tidak ada engine Excel yang tersedia. Install 'openpyxl' atau 'xlsxwriter'.")
    return output.getvalue()

def plot_bar(df: pd.DataFrame, label_col: str, value_col: str, title: str, xlabel_text: str) -> plt.Figure:
    """Membuat grafik batang dari DataFrame."""
    fig, ax = plt.subplots(figsize=(14, 8))
    df_sorted = df.sort_values(by=value_col, ascending=True)
    ax.barh(df_sorted[label_col].astype(str), df_sorted[value_col])
    ax.set_title(title, fontsize=16)
    ax.set_xlabel("Jumlah Pasien", fontsize=12)
    ax.set_ylabel(xlabel_text, fontsize=12)
    fig.tight_layout()
    return fig

# --- ALIAS KOLOM UNTUK TAMPILAN DASHBOARD (TAB 1) ---
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

    # Buat dua tab
    tab1, tab2 = st.tabs([
        "📊 Dashboard Interaktif",
        "📈 Rekapitulasi RS Penanganan Pasien"
    ])

    # ================== TAB 1: DASHBOARD INTERAKTIF ==================
    with tab1:
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
        if provinsi_pilihan != "Semua Propinsi":
            df_filtered = df_filtered[df_filtered["provinsi"] == provinsi_pilihan]
        if dokter_option == "Ada":
            df_filtered = df_filtered[df_filtered["terdapat_dokter_hematologi"] == True]
        elif dokter_option == "Tidak Ada":
            df_filtered = df_filtered[df_filtered["terdapat_dokter_hematologi"] == False]
        elif dokter_option == "Data Kosong":
            df_filtered = df_filtered[df_filtered["terdapat_dokter_hematologi"].isna()]
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
            st.metric("RS Dengan Dokter Hematologi", int((df["terdapat_dokter_hematologi"] == True).sum()))
        with col3:
            st.metric("RS Dengan Tim Terpadu", int((df["terdapat_tim_terpadu_hemofilia"] == True).sum()))

    # ================== TAB 2: REKAPITULASI (SCHEMA VIEW) ==================
    with tab2:
        st.subheader("📈 Rekapitulasi Jumlah Pasien per RS")

        df_view = fetch_view_rs(engine)  # kolom: Nama Rumah Sakit, Jumlah Pasien, Kota, Propinsi

        if df_view.empty:
            st.warning("Tidak ada data rekap yang dapat ditampilkan.")
        else:
            # Hitung persentase untuk tampilan saja (tidak disimpan ke file unduhan)
            total = int(df_view["Jumlah Pasien"].sum()) if not df_view.empty else 0
            if total > 0:
                df_show = df_view.copy()
                df_show["Persentase (%)"] = (df_show["Jumlah Pasien"] / total * 100).round(2)
            else:
                df_show = df_view.copy()
                df_show["Persentase (%)"] = 0.0

            # Tampilkan
            st.dataframe(
                df_show.style.format({"Persentase (%)": "{:.2f}"}),
                use_container_width=True,
                hide_index=True
            )

            # Unduh persis sesuai schema (4 kolom)
            st.download_button(
                "📥 Download Rekap",
                data=_to_excel_bytes(df_view, sheet_name="Rekap_RS"),
                file_name="rekap_rs_schema.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            st.markdown("---")
            st.subheader("Visualisasi Data")
            # Untuk label grafik, tampilkan 'Nama Rumah Sakit' (opsional: tambahkan Kota)
            top_20 = df_view.nlargest(20, "Jumlah Pasien").copy()
            # Buat label gabungan agar informatif (RS [Kota])
            top_20["Label RS"] = top_20.apply(
                lambda r: f"{r['Nama Rumah Sakit']} [{r['Kota']}]" if pd.notna(r["Kota"]) and str(r["Kota"]).strip() else str(r["Nama Rumah Sakit"]),
                axis=1
            )
            fig_rekap = plot_bar(
                df=top_20.rename(columns={"Label RS": "label"}),
                label_col="label",
                value_col="Jumlah Pasien",
                title="Distribusi Pasien per Rumah Sakit (Top 20)",
                xlabel_text="Nama Rumah Sakit [Kota]"
            )
            st.pyplot(fig_rekap)

        st.caption(
            "Sumber data: **pwh.v_hospital_summary** (jika tersedia) atau fallback dari "
            "`pwh.treatment_hospital` (kolom `name_hospital`) yang di-*join* dengan `public.rumah_sakit`."
        )
