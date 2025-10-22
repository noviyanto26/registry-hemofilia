# 06_rekap_provinsi.py
import os
import io
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import matplotlib.pyplot as plt

# ========================= KONFIGURASI HALAMAN =========================
st.set_page_config(
    page_title="Rekapitulasi berdasarkan Provinsi",
    page_icon="🗺️",
    layout="wide"
)
st.title("🗺️ Rekapitulasi berdasarkan Provinsi")
st.markdown(
    "Halaman ini menampilkan **rekapitulasi jumlah pasien per provinsi** "
    "berdasarkan kolom `pwh.province` pada tabel **pwh.patients**."
)

# ========================= KONEKSI DATABASE =========================
def _resolve_db_url() -> str:
    try:
        sec = st.secrets.get("DATABASE_URL", "")
        if sec:
            return sec
    except Exception:
        pass
    env = os.environ.get("DATABASE_URL")
    if env:
        return env
    st.error("DATABASE_URL tidak ditemukan. Atur di `.streamlit/secrets.toml` atau environment variable.")
    st.stop()

@st.cache_resource(show_spinner="🔌 Menghubungkan ke database...")
def get_engine(dsn: str) -> Engine:
    try:
        engine = create_engine(dsn, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"Gagal terhubung ke database: {e}")
        st.stop()

# ========================= QUERY DATA =========================
def _fetch_count_by_column(engine: Engine, column: str) -> pd.DataFrame:
    q = text(f"""
        SELECT
            COALESCE(NULLIF(TRIM({column}::text), ''), 'Unknown') AS province,
            COUNT(*)::int AS jumlah
        FROM pwh.patients
        GROUP BY 1
        ORDER BY jumlah DESC, province ASC;
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn)
        total = int(df["jumlah"].sum()) if not df.empty else 0
        df["persentase"] = (df["jumlah"] / total * 100).round(2) if total > 0 else 0.0
        return df
    except Exception as e:
        st.error(f"Gagal mengambil data: {e}")
        return pd.DataFrame(columns=["province", "jumlah", "persentase"])

def _to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Rekap_Provinsi") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

# ========================= PLOTTING =========================
def plot_bar_with_labels(df: pd.DataFrame) -> plt.Figure:
    """
    Membuat bar chart dengan label jumlah di atas setiap batang.
    """
    fig, ax = plt.subplots(figsize=(14, 7))
    df_sorted = df.sort_values(by="jumlah", ascending=False).reset_index(drop=True)

    # Gambar bar
    bars = ax.bar(df_sorted["province"].astype(str), df_sorted["jumlah"])

    # Judul & axis
    ax.set_title("Distribusi Pasien per Provinsi", fontsize=16)
    ax.set_xlabel("Provinsi", fontsize=12)
    ax.set_ylabel("Jumlah", fontsize=12)

    # Label sumbu-X miring agar muat
    ax.tick_params(axis='x', labelrotation=45)
    for lbl in ax.get_xticklabels():
        lbl.set_ha('right')

    # Tambahkan label jumlah di atas setiap batang
    # (matplotlib >= 3.4 memiliki bar_label)
    try:
        ax.bar_label(bars, labels=[str(v) for v in df_sorted["jumlah"]], padding=3)
    except Exception:
        # fallback manual jika bar_label tidak tersedia
        for rect, val in zip(bars, df_sorted["jumlah"]):
            ax.annotate(
                f"{val}", 
                xy=(rect.get_x() + rect.get_width() / 2, rect.get_height()),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center", va="bottom"
            )

    fig.tight_layout()
    return fig

# ========================= MAIN =========================
db_url = _resolve_db_url()
engine = get_engine(db_url)

df_prov = _fetch_count_by_column(engine, "province")

if df_prov.empty:
    st.warning("Tidak ada data yang dapat ditampilkan.")
    st.stop()

# Kontrol di area utama (hanya jumlah Top-N)
top_n = st.number_input(
    "Tampilkan Top-N Provinsi (berdasarkan jumlah pasien)",
    min_value=1,
    max_value=50,
    value=20,
    step=1
)

df_top = df_prov.head(top_n)

st.subheader("📊 Tabel Rekap Provinsi")
st.dataframe(
    df_top.rename(columns={"province": "Provinsi", "jumlah": "Jumlah", "persentase": "Persentase"})
          .style.format({"Persentase": "{:.2f}%"}),
    use_container_width=True,
    hide_index=True
)

st.download_button(
    "📥 Download Rekap Provinsi (Excel)",
    data=_to_excel_bytes(df_top.rename(columns={
        "province": "Provinsi", "jumlah": "Jumlah", "persentase": "Persentase"})),
    file_name="rekap_provinsi.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)

st.markdown(" ")
st.pyplot(plot_bar_with_labels(df_top))

st.markdown("---")
st.caption(
    "Sumber data: **pwh.patients** (kolom `province`). Nilai kosong dipetakan otomatis ke **'Unknown'**."
)
