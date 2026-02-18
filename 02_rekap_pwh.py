# 02_rekap_pwh.py (Perbaikan Cache, Download Excel, dan Filter Cabang)
import os
import io
import pandas as pd
import streamlit as st
from pandas import ExcelWriter
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import matplotlib.pyplot as plt

# --- Konfigurasi Halaman Streamlit ---
st.set_page_config(page_title="Rekapitulasi Berdasarkan Kelompok Usia", page_icon="📊", layout="wide")
st.title("📊 Rekapitulasi Berdasarkan Kelompok Usia")
st.markdown("Dashboard ini menampilkan rekapitulasi dan grafik pasien berdasarkan jenis hemofilia dan kelompok usia.")

# --- KONEKSI DATABASE ---
def _resolve_db_url() -> str:
    """Mencari DATABASE_URL dari st.secrets atau environment variables."""
    try:
        sec = st.secrets.get("DATABASE_URL", "")
        if sec: return sec
    except Exception:
        pass
    env = os.environ.get("DATABASE_URL")
    if env: return env
    
    st.error('DATABASE_URL tidak ditemukan. Mohon atur di `.streamlit/secrets.toml` atau sebagai environment variable.')
    st.code('DATABASE_URL = "postgresql://USER:PASSWORD@HOST:PORT/DATABASE"')
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

# --- FUNGSI PENGOLAHAN DATA ---

def fetch_data_from_view(_engine: Engine) -> pd.DataFrame:
    """
    Mengambil data pasien, usia, diagnosis, DAN CABANG.
    Karena view 'pwh.patients_with_age' tidak memiliki kolom cabang,
    kita melakukan JOIN ke tabel induk 'pwh.patients'.
    """
    st.info("🔄 Mengambil data terbaru dari database...") 
    
    # PERBAIKAN SQL: Menambahkan JOIN ke pwh.patients (alias p) untuk mengambil cabang
    query = text("""
        SELECT
            v.id,
            v.full_name,
            v.usia_tahun as usia,
            p.cabang, 
            d.hemo_type,
            d.severity
        FROM pwh.patients_with_age v
        JOIN pwh.patients p ON v.id = p.id  -- JOIN TABEL INDUK
        JOIN pwh.hemo_diagnoses d ON v.id = d.patient_id;
    """)
    try:
        with _engine.connect() as connection:
            df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        st.error(f"Gagal mengambil data: {e}")
        st.info("Pastikan tabel 'pwh.patients' memiliki kolom 'cabang'.")
        return pd.DataFrame()

def get_age_group(age):
    """Mengelompokkan usia ke dalam kategori."""
    if pd.isna(age): return "Unknown"
    age = int(age)
    if 0 <= age <= 4: return "0-4"
    if 5 <= age <= 13: return "5-13"
    if 14 <= age <= 18: return "14-18"
    if 19 <= age <= 44: return "19-44"
    return ">45"

def create_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """Membuat tabel rekapitulasi dengan mapping kolom yang benar."""
    df['hemo_category'] = df.apply(
        lambda row: f"{row['hemo_type']} - {row['severity']}" if pd.notna(row['severity']) else row['hemo_type'],
        axis=1
    )
    
    summary = pd.pivot_table(
        df, index='kelompok_usia', columns='hemo_category', aggfunc='size', fill_value=0
    )

    column_mapping = {
        'A - Berat': 'Hemofilia A - Berat',
        'A - Sedang': 'Hemofilia A - Sedang',
        'A - Ringan': 'Hemofilia A - Ringan',
        'B - Berat': 'Hemofilia B - Berat',
        'B - Sedang': 'Hemofilia B - Sedang',
        'B - Ringan': 'Hemofilia B - Ringan',
        'Other': 'Hemofilia Tipe Lain',
        'vWD': 'vWD'
    }
    
    summary.rename(columns=column_mapping, inplace=True)
    
    desired_columns = [
        'Hemofilia A - Ringan', 'Hemofilia A - Sedang', 'Hemofilia A - Berat',
        'Hemofilia B - Ringan', 'Hemofilia B - Sedang', 'Hemofilia B - Berat',
        'Hemofilia Tipe Lain', 'vWD'
    ]
    for col in desired_columns:
        if col not in summary.columns:
            summary[col] = 0
            
    summary['Total'] = summary.sum(axis=1)
    summary.loc['Total'] = summary.sum()
    age_order = ['>45', '19-44', '14-18', '5-13', '0-4', 'Total']
    summary = summary.reindex(age_order).fillna(0).astype(int)

    return summary[desired_columns + ['Total']]

def plot_graph(summary_df: pd.DataFrame) -> plt.Figure:
    """Membuat grafik batang dari data rekapitulasi."""
    plot_df = summary_df.drop(index='Total', errors='ignore')

    fig, ax = plt.subplots(figsize=(14, 8))
    plot_df.plot(kind='bar', stacked=True, colormap='viridis', ax=ax)
    
    ax.set_title('Rekapitulasi Pasien Hemofilia berdasarkan Jenis dan Kelompok Usia', fontsize=16)
    ax.set_xlabel('Kelompok Usia', fontsize=12)
    ax.set_ylabel('Jumlah Pasien', fontsize=12)
    plt.xticks(rotation=45)
    ax.legend(title='Jenis Hemofilia', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    return fig

def convert_df_to_excel(df: pd.DataFrame) -> bytes:
    """Mengonversi DataFrame ke format Excel (xlsx) untuk diunduh."""
    output = io.BytesIO()
    with ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Rekapitulasi", index=True)
        ws = writer.sheets["Rekapitulasi"]
        max_len_idx = max(df.index.astype(str).map(len).max(), len(df.index.name or "")) + 2
        ws.set_column(0, 0, max_len_idx)
        for col_idx, col_name in enumerate(df.columns, 1):
            max_len = max(
                (df[col_name].astype(str).map(len).max() if not df.empty else 0),
                len(str(col_name))
            ) + 2
            ws.set_column(col_idx, col_idx, min(max_len, 50))
            
    return output.getvalue()

# --- MAIN APP LOGIC ---
db_url = _resolve_db_url()
engine = get_engine(db_url)
data_df = fetch_data_from_view(engine)

if data_df.empty:
    st.warning("Tidak ada data yang dapat ditampilkan dari database.")
else:
    if 'usia' in data_df.columns:
        # --- LOGIKA FILTER CABANG ---
        if 'cabang' in data_df.columns:
            # Ambil daftar cabang unik
            list_cabang = ['Semua Cabang'] + sorted(data_df['cabang'].dropna().astype(str).unique().tolist())
            
            # Buat Selectbox
            selected_cabang = st.selectbox("🏥 Filter Berdasarkan Cabang:", list_cabang)
            
            # Terapkan Filter
            if selected_cabang != 'Semua Cabang':
                data_df = data_df[data_df['cabang'] == selected_cabang]
        else:
            st.warning("Kolom 'cabang' tidak ditemukan dalam data.")
        # --- END LOGIKA FILTER ---

        data_df['kelompok_usia'] = data_df['usia'].apply(get_age_group)
        rekap_table = create_summary_table(data_df)
        
        st.subheader(f"Tabel Rekapitulasi{' - ' + selected_cabang if 'cabang' in data_df.columns and selected_cabang != 'Semua Cabang' else ''}")
        st.dataframe(rekap_table.style.apply(lambda x: ['background-color: #e8f4f8' if x.name == 'Total' else '' for i in x], axis=1)
                                    .apply(lambda x: ['background-color: #e8f4f8' if x.name == 'Total' else '' for i in x], axis=0))

        excel_data = convert_df_to_excel(rekap_table)
        st.download_button(
            label="📥 Download Rekapitulasi (Excel)",
            data=excel_data,
            file_name='rekapitulasi_hemofilia.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
        
        st.markdown("---")

        st.subheader("Grafik Visualisasi")
        fig = plot_graph(rekap_table.drop(columns='Total', errors='ignore'))
        st.pyplot(fig)
    else:
        st.error("Kolom 'usia' (diharapkan dari 'usia_tahun') tidak ditemukan di view 'pwh.patients_with_age'.")
