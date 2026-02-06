import os
import re
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ==============================================================================
# 1. KONFIGURASI HALAMAN & KONEKSI DATABASE
#    (Diambil dan disesuaikan dari contoh file 01_pwh_input (23).py)
# ==============================================================================

st.set_page_config(page_title="Data Penyandang Hemofilia", page_icon="📋", layout="wide")

def _resolve_db_url() -> str:
    """Mengambil URL database dari secrets atau environment variable."""
    try:
        sec = st.secrets.get("DATABASE_URL", "")
        if sec: return sec
    except Exception: pass
    env = os.environ.get("DATABASE_URL") or os.getenv("DATABASE_URL")
    if env: return env
    st.error('DATABASE_URL tidak ditemukan. Harap konfigurasi .streamlit/secrets.toml atau Environment Variable.')
    st.stop()

@st.cache_resource(show_spinner=False)
def get_engine(dsn: str) -> Engine:
    return create_engine(dsn, pool_pre_ping=True)

# Inisialisasi Engine
DSN = _resolve_db_url()
try:
    engine = get_engine(DSN)
    # Test koneksi
    with engine.connect() as _c:
        _c.exec_driver_sql("SELECT 1")
except Exception as e:
    st.error(f"Gagal konek ke Postgres: {e}")
    st.stop()

def run_df_branch(query: str, params: dict | None = None) -> pd.DataFrame:
    """
    Menjalankan query ke database. 
    Secara otomatis menyuntikkan filter 'cabang' jika user_branch diset di session_state
    (Mengikuti logika keamanan dari file contoh).
    """
    current_user_branch = st.session_state.get("user_branch", None)
    
    query_filtered = query.strip()
    query_upper = query_filtered.upper()
    
    # Filter otomatis berdasarkan cabang jika user bukan ADMIN ('ALL')
    if current_user_branch and current_user_branch != "ALL" and "PWH.PATIENTS" in query_upper:
        params = params or {}
        params["branch"] = current_user_branch
        
        # Tentukan alias tabel patients untuk penyuntikan WHERE clause
        alias_prefix = None
        if "JOIN PWH.PATIENTS P" in query_upper or "FROM PWH.PATIENTS P" in query_upper:
            alias_prefix = "p."
        elif "FROM PWH.PATIENTS T" in query_upper:
            alias_prefix = "t."
        elif "FROM PWH.PATIENTS" in query_upper:
            # Simple check untuk kasus tanpa alias
            from_index = query_upper.find("FROM PWH.PATIENTS")
            join_index = query_upper.find("JOIN PWH.PATIENTS")
            if from_index != -1 and (from_index < join_index or join_index == -1):
                alias_prefix = "" 
        
        # Jika alias ditemukan, inject filter cabang
        if alias_prefix is not None:
            filter_string = f"{alias_prefix}cabang = :branch"
            
            # Cek apakah sudah ada WHERE
            if "WHERE" in query_upper:
                query_filtered = re.sub(r"\bWHERE\b", f"WHERE {filter_string} AND ", query_filtered, count=1, flags=re.IGNORECASE)
            else:
                # Sisipkan WHERE sebelum klausa akhir seperti ORDER BY, LIMIT, dll
                append_points_upper = ["ORDER BY", "GROUP BY", "LIMIT", ";"]
                injected = False
                for point in append_points_upper:
                    point_idx = query_upper.find(point)
                    if point_idx != -1:
                        point_case_sensitive = query_filtered[point_idx : point_idx + len(point)]
                        query_filtered = query_filtered.replace(point_case_sensitive, f" WHERE {filter_string} {point_case_sensitive}", 1)
                        injected = True
                        break
                if not injected:
                    query_filtered += f" WHERE {filter_string}"

    # Eksekusi query
    with engine.begin() as conn:
        return pd.read_sql(text(query_filtered), conn, params=params or {})

# ==============================================================================
# 2. HALAMAN UTAMA (UI & LOGIC)
# ==============================================================================

st.title("Data Lengkap Penyandang Hemofilia")

# --- Fitur Search Data ---
st.markdown("### Filter Data")
col_search, col_spacer = st.columns([1, 2])
with col_search:
    search_term = st.text_input("Search Data", placeholder="Cari Nama, NIK, Kota, atau Cabang...")

# --- Konstruksi Query ---
# Query ini menggabungkan (LEFT JOIN) tabel patients dengan tabel detail lainnya
# untuk memastikan semua data pasien tampil meskipun data detailnya kosong.
base_query = """
    SELECT
        -- Schema pwh.patients
        p.full_name,
        p.nik,
        p.birth_place,
        p.birth_date,
        -- Schema pwh.patient_age (Calculated)
        EXTRACT(YEAR FROM age(CURRENT_DATE, p.birth_date)) AS age_years,
        p.blood_group,
        p.rhesus,
        p.occupation,
        p.education,
        p.address,
        p.phone,
        p.village,
        p.district,
        p.city,
        p.province,
        p.gender,
        p.cabang,
        
        -- Schema pwh.hemo_diagnoses
        hd.hemo_type,
        hd.severity,
        hd.diagnosed_on,
        
        -- Schema pwh.hemo_inhibitors
        hi.factor,
        hi.titer_bu,
        hi.measured_on,
        
        -- Schema pwh.treatment_hospital
        th.name_hospital,
        th.doctor_in_charge,
        th.city_hospital,
        th.province_hospital,
        th.treatment_type,
        th.frequency,
        th.dose,
        th.product,
        
        -- Schema pwh.contacts
        c.relation,
        c.name AS contact_name,
        c.phone AS contact_phone

    FROM pwh.patients p
    LEFT JOIN pwh.hemo_diagnoses hd ON p.id = hd.patient_id
    LEFT JOIN pwh.hemo_inhibitors hi ON p.id = hi.patient_id
    LEFT JOIN pwh.treatment_hospital th ON p.id = th.patient_id
    LEFT JOIN pwh.contacts c ON p.id = c.patient_id
"""

# Tambahkan logika pencarian jika ada input search
where_clauses = []
params = {}

if search_term:
    search_pattern = f"%{search_term}%"
    # Mencari di beberapa field utama
    where_clauses.append("""(
        p.full_name ILIKE :search OR 
        p.nik ILIKE :search OR 
        p.city ILIKE :search OR
        p.cabang ILIKE :search OR
        th.name_hospital ILIKE :search
    )""")
    params["search"] = search_pattern

# Gabungkan WHERE clauses ke query
full_query = base_query
if where_clauses:
    full_query += " WHERE " + " AND ".join(where_clauses)

full_query += " ORDER BY p.id"

# --- Eksekusi Query ---
try:
    df = run_df_branch(full_query, params)
    
    # --- Mapping Nama Kolom Sesuai Permintaan ---
    column_mapping = {
        "full_name": "Nama Lengkap",
        "nik": "NIK",
        "birth_place": "Tempat Lahir",
        "birth_date": "Tanggal Lahir",
        "age_years": "Usia",
        "blood_group": "Gol Darah",
        "rhesus": "Rhesus",
        "occupation": "Pekerjaan",
        "education": "Pendidikan Terakhir",
        "address": "Alamat",
        "phone": "No Telp",
        "village": "Desa/Kelurahan",
        "district": "Kecamatan",
        "city": "Kota/Kabupaten",
        "province": "Propinsi",
        "gender": "Jenis Kelamin",
        "cabang": "HMHI Cabang",
        "hemo_type": "Jenis Hemofilia",
        "severity": "Kategori Hemofilia",
        "diagnosed_on": "Tanggal Diagnosis",
        "factor": "Inhibitor",
        "titer_bu": "Titer BU",
        "measured_on": "Tanggal Pengukuran Inhibitor",
        "name_hospital": "Rumah Sakit Perawatan",
        "doctor_in_charge": "DPJP",
        "city_hospital": "Kota/Kabupaten RS",
        "province_hospital": "Propinsi RS",
        "treatment_type": "Jenis Treatment",
        "frequency": "Frekuensi Perawatan",
        "dose": "Dosis Perawatan",
        "product": "Produk Perawatan",
        "relation": "Kontak Darurat",
        "contact_name": "Nama Kontak Darurat",
        "contact_phone": "No Telp Kontak Darurat"
    }
    
    # Rename kolom
    df_display = df.rename(columns=column_mapping)
    
    # Tampilkan Data
    st.write(f"Menampilkan **{len(df_display)}** baris data.")
    st.dataframe(df_display, use_container_width=True)

except Exception as e:
    st.error(f"Terjadi kesalahan saat memuat data: {e}")