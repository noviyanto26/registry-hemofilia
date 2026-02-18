# 03_rekap_gender.py
import os
import io
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import matplotlib.pyplot as plt

# --- Konfigurasi Halaman Streamlit ---
st.set_page_config(page_title="Rekapitulasi per Jenis Kelamin", page_icon="🚻", layout="wide")
st.title("🚻 Rekapitulasi Pasien berdasarkan Kategori dan Jenis Kelamin")
st.markdown("Dashboard ini menampilkan rekapitulasi dan grafik pasien berdasarkan jenis hemofilia dan jenis kelamin (Laki-laki/Perempuan).")

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

# --- FUNGSI PENGOLAHAN DATA ---

def fetch_data_for_gender(_engine: Engine) -> pd.DataFrame:
    """
    Mengambil data jenis kelamin pasien, cabang, dan diagnosis hemofilia.
    """
    st.info("🔄 Mengambil data terbaru dari database...")
    # MODIFIKASI: Menambahkan p.cabang
    query = text("""
        SELECT
            p.gender AS jenis_kelamin,
            p.cabang,
            d.hemo_type
        FROM pwh.patients p
        JOIN pwh.hemo_diagnoses d ON p.id = d.patient_id
        WHERE p.gender IS NOT NULL AND d.hemo_type IS NOT NULL;
    """)
    try:
        with _engine.connect() as connection:
            df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        st.error(f"Gagal mengambil data: {e}")
        st.info("Pastikan tabel 'pwh.patients' memiliki kolom 'gender', 'cabang' dan 'pwh.hemo_diagnoses' memiliki kolom 'hemo_type'.")
        return pd.DataFrame()

def map_hemo_type_to_category(hemo_type):
    """Mengelompokkan hemo_type ke kategori yang sesuai."""
    if hemo_type == 'A':
        return 'Hemofilia A'
    if hemo_type == 'B':
        return 'Hemofilia B'
    if hemo_type == 'Other':
        return 'Hemofilia tipe lain'
    if hemo_type == 'vWD':
        return 'VWD'
    return 'Lainnya'

def create_gender_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """Membuat tabel rekapitulasi berdasarkan kategori dan jenis kelamin."""
    # Pastikan data tidak kosong sebelum diproses
    if df.empty:
        return pd.DataFrame(columns=['Laki-laki', 'Perempuan', 'Total'])

    df = df.copy() # Hindari SettingWithCopyWarning
    df['Kategori'] = df['hemo_type'].apply(map_hemo_type_to_category)
    
    # Buat pivot table
    summary = pd.pivot_table(
        df, 
        index='Kategori', 
        columns='jenis_kelamin', 
        aggfunc='size', 
        fill_value=0
    )
    
    # Pastikan kolom Laki-laki dan Perempuan ada
    if 'Laki-laki' not in summary.columns:
        summary['Laki-laki'] = 0
    if 'Perempuan' not in summary.columns:
        summary['Perempuan'] = 0
        
    # Atur urutan baris sesuai contoh excel
    category_order = ['Hemofilia A', 'Hemofilia B', 'Hemofilia tipe lain', 'VWD']
    summary = summary.reindex(category_order).fillna(0).astype(int)
    
    # Ambil kolom utama dan hitung total kolom
    final_summary = summary[['Laki-laki', 'Perempuan']]
    final_summary['Total'] = final_summary.sum(axis=1)
    
    # Tambahkan baris Total di bagian bawah
    final_summary.loc['Total'] = final_summary.sum()
    
    return final_summary.astype(int)


def plot_gender_graph(summary_df: pd.DataFrame) -> plt.Figure:
    """Membuat grafik batang dari data rekapitulasi jenis kelamin."""
    # Hapus baris dan kolom 'Total' sebelum plotting
    plot_df = summary_df.drop(index='Total', columns='Total', errors='ignore')

    fig, ax = plt.subplots(figsize=(12, 7))
    plot_df.plot(kind='bar', ax=ax, color=['#1f77b4', '#ff7f0e']) # Memberi warna berbeda
    
    ax.set_title('Jumlah Pasien berdasarkan Kategori dan Jenis Kelamin', fontsize=16, pad=20)
    ax.set_xlabel('Kategori Hemofilia', fontsize=12)
    ax.set_ylabel('Jumlah Pasien', fontsize=12)
    plt.xticks(rotation=0, ha='center')
    ax.legend(title='Jenis Kelamin')
    
    # Menambahkan label angka di atas setiap bar
    for container in ax.containers:
        ax.bar_label(container, label_type='edge', fontsize=10, padding=3)

    plt.tight_layout()
    return fig

# --- MAIN APP LOGIC ---
db_url = _resolve_db_url()
if db_url:
    engine = get_engine(db_url)
    data_df = fetch_data_for_gender(engine)

    if data_df.empty:
        st.warning("Tidak ada data yang dapat ditampilkan dari database.")
    else:
        # --- MODIFIKASI: Filter Berdasarkan Cabang ---
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
            selected_cabang = "Semua Data"
        # --- END MODIFIKASI ---

        rekap_table = create_gender_summary_table(data_df)

        st.subheader(f"Tabel Rekapitulasi{' - ' + selected_cabang if 'cabang' in data_df.columns and selected_cabang != 'Semua Cabang' else ''}")
        st.dataframe(rekap_table, use_container_width=True)

        # --- FUNGSI DOWNLOAD EXCEL ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            rekap_table.to_excel(writer, index=True, sheet_name='Rekapitulasi Gender')
        excel_data = output.getvalue()

        st.download_button(
           label="📥 Download Rekapitulasi (Excel)",
           data=excel_data,
           file_name='rekapitulasi_jenis_kelamin.xlsx',
           mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        )
        
        st.markdown("---")

        st.subheader("Grafik Visualisasi")
        if not rekap_table.empty:
             fig = plot_gender_graph(rekap_table)
             st.pyplot(fig)
        else:
             st.info("Tidak ada data untuk ditampilkan pada grafik setelah filter diterapkan.")
