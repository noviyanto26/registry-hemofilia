# 03_rekap_pendidikan_pekerjaan.py
import os
import io
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import matplotlib.pyplot as plt

# ========================= Konfigurasi Halaman =========================
st.set_page_config(
    page_title="Rekap Pendidikan & Pekerjaan",
    page_icon="📚",
    layout="wide"
)
st.title("📚 Rekap Pendidikan & 💼 Pekerjaan")
st.markdown(
    "Halaman ini menampilkan **rekapitulasi** dan **grafik** berdasarkan "
    "`occupation` (pekerjaan) dan `education` (pendidikan terakhir) dari tabel **pwh.patients**."
)

# ========================= Koneksi Database =========================
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
    st.error("DATABASE_URL tidak ditemukan. Atur di `.streamlit/secrets.toml` atau sebagai environment variable.")
    st.code('DATABASE_URL = "postgresql://USER:PASSWORD@HOST:PORT/DATABASE"')
    return None

@st.cache_resource(show_spinner="🔌 Menghubungkan ke database...")
def get_engine(dsn: str) -> Engine:
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

# ========================= Query Data =========================
def _fetch_count_by_column(engine: Engine, column: str, alias: str) -> pd.DataFrame:
    """
    Mengambil rekap jumlah per nilai kolom pada pwh.patients.
    - ENUM/string di-cast ke text agar TRIM/NULLIF/COALESCE aman
    - Nilai NULL/blank dinormalisasi jadi 'Unknown'
    Return: [alias(asli), jumlah, persentase]
    """
    st.info(f"🔄 Mengambil rekap '{alias}' dari database...")
    q = text(f"""
        SELECT
            COALESCE(NULLIF(TRIM({column}::text), ''), 'Unknown') AS {alias},
            COUNT(*)::int AS jumlah
        FROM pwh.patients
        GROUP BY 1
        ORDER BY jumlah DESC, {alias} ASC;
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn)
        total = int(df["jumlah"].sum()) if not df.empty else 0
        df["persentase"] = (df["jumlah"] / total * 100).round(2) if total > 0 else 0.0
        return df
    except Exception as e:
        st.error(f"Gagal mengambil rekap '{alias}' dari pwh.patients: {e}")
        return pd.DataFrame(columns=[alias, "jumlah", "persentase"])

# ========================= Util Aliasing & Export =========================
def _localized(df: pd.DataFrame, domain: str) -> pd.DataFrame:
    """
    Kembalikan DataFrame dengan alias kolom ID:
    - domain='occupation' -> Pekerjaan, Jumlah, Persentase
    - domain='education'  -> Pendidikan Terakhir, Jumlah, Persentase
    """
    if df.empty:
        return df
    if domain == "occupation":
        return df.rename(columns={
            "occupation": "Pekerjaan",
            "jumlah": "Jumlah",
            "persentase": "Persentase"
        })
    elif domain == "education":
        return df.rename(columns={
            "education": "Pendidikan Terakhir",
            "jumlah": "Jumlah",
            "persentase": "Persentase"
        })
    return df

def _to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Data") -> bytes:
    """Mengubah DataFrame ke file Excel (bytes)."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

# ========================= Plotting =========================
def plot_bar(df: pd.DataFrame, label_col: str, value_col: str, title: str, xlabel_text: str) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(14, 7))
    df_sorted = df.sort_values(by=value_col, ascending=False)
    ax.bar(df_sorted[label_col].astype(str), df_sorted[value_col])
    ax.set_title(title, fontsize=16)
    ax.set_xlabel(xlabel_text, fontsize=12)
    ax.set_ylabel("Jumlah", fontsize=12)
    ax.tick_params(axis='x', labelrotation=45)
    for lbl in ax.get_xticklabels():
        lbl.set_ha('right')
    fig.tight_layout()
    return fig

# ========================= Main =========================
db_url = _resolve_db_url()
engine = get_engine(db_url)

col_occ, col_edu = st.columns(2)

with col_occ:
    st.subheader("💼 Rekapitulasi Pekerjaan")
    df_occ_raw = _fetch_count_by_column(engine, "occupation", "occupation")
    if df_occ_raw.empty:
        st.warning("Tidak ada data pekerjaan yang dapat ditampilkan.")
    else:
        # Tabel & unduh Excel dengan alias Indonesia
        df_occ_view = _localized(df_occ_raw, domain="occupation")
        st.dataframe(
            df_occ_view.style.format({"Persentase": "{:.2f}%"}),
            use_container_width=True,
            hide_index=True  # <-- PERUBAHAN DI SINI
        )
        st.download_button(
            "📥 Download Rekap Pekerjaan (Excel)",
            data=_to_excel_bytes(df_occ_view, sheet_name="Rekap_Pekerjaan"),
            file_name="rekap_pekerjaan.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.markdown(" ")
        # Grafik tetap pakai kolom asli untuk kemudahan pemrosesan
        fig_occ = plot_bar(df_occ_raw, "occupation", "jumlah", "Distribusi Pekerjaan", "Pekerjaan")
        st.pyplot(fig_occ)

with col_edu:
    st.subheader("🎓 Rekapitulasi Pendidikan Terakhir")
    df_edu_raw = _fetch_count_by_column(engine, "education", "education")
    if df_edu_raw.empty:
        st.warning("Tidak ada data pendidikan yang dapat ditampilkan.")
    else:
        df_edu_view = _localized(df_edu_raw, domain="education")
        st.dataframe(
            df_edu_view.style.format({"Persentase": "{:.2f}%"}),
            use_container_width=True,
            hide_index=True  # <-- PERUBAHAN DI SINI
        )
        st.download_button(
            "📥 Download Rekap Pendidikan (Excel)",
            data=_to_excel_bytes(df_edu_view, sheet_name="Rekap_Pendidikan"),
            file_name="rekap_pendidikan.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.markdown(" ")
        fig_edu = plot_bar(df_edu_raw, "education", "jumlah", "Distribusi Pendidikan Terakhir", "Pendidikan Terakhir")
        st.pyplot(fig_edu)

st.markdown("---")
st.caption(
    "Sumber data: **pwh.patients** (kolom `occupation` dan `education`). "
    "Nilai kosong/NULL dipetakan ke **'Unknown'** agar tetap terhitung."
)
