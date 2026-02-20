import os
import re
import io
import math
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from fpdf import FPDF

# ==============================================================================
# 1. KONFIGURASI HALAMAN & KONEKSI DATABASE
# ==============================================================================

def _resolve_db_url() -> str:
    try:
        sec = st.secrets.get("DATABASE_URL") or st.secrets.get("secrets", {}).get("DATABASE_URL", "")
        if sec: return sec
    except Exception: pass
    env = os.environ.get("DATABASE_URL") or os.getenv("DATABASE_URL")
    if env: return env
    st.error('DATABASE_URL tidak ditemukan.')
    st.stop()

@st.cache_resource(show_spinner=False)
def get_engine(dsn: str) -> Engine:
    return create_engine(dsn, pool_pre_ping=True)

DSN = _resolve_db_url()
try:
    engine = get_engine(DSN)
    with engine.connect() as _c:
        _c.exec_driver_sql("SELECT 1")
except Exception as e:
    st.error(f"Gagal konek ke Postgres: {e}")
    st.stop()

def run_df_branch(query: str, params: dict | None = None) -> pd.DataFrame:
    current_user_branch = st.session_state.get("user_branch", None)
    query_filtered = query.strip()
    query_upper = query_filtered.upper()
    
    if current_user_branch and current_user_branch != "ALL" and "PWH.PATIENTS" in query_upper:
        params = params or {}
        params["branch"] = current_user_branch
        alias_prefix = None
        if "JOIN PWH.PATIENTS P" in query_upper or "FROM PWH.PATIENTS P" in query_upper:
            alias_prefix = "p."
        elif "FROM PWH.PATIENTS T" in query_upper:
            alias_prefix = "t."
        elif "FROM PWH.PATIENTS" in query_upper:
            from_index = query_upper.find("FROM PWH.PATIENTS")
            join_index = query_upper.find("JOIN PWH.PATIENTS")
            if from_index != -1 and (from_index < join_index or join_index == -1):
                alias_prefix = "" 
        
        if alias_prefix is not None:
            filter_string = f"{alias_prefix}cabang = :branch"
            if "WHERE" in query_upper:
                query_filtered = re.sub(r"\bWHERE\b", f"WHERE {filter_string} AND ", query_filtered, count=1, flags=re.IGNORECASE)
            else:
                append_points_upper = ["ORDER BY", "GROUP BY", "LIMIT", ";"]
                injected = False
                for point in append_points_upper:
                    point_idx = query_upper.find(point)
                    if point_idx != -1:
                        point_case_sensitive = query_filtered[point_idx : point_idx + len(point)]
                        query_filtered = query_filtered.replace(point_case_sensitive, f" WHERE {filter_string} {point_case_sensitive}", 1)
                        injected = True
                        break
                if not injected: query_filtered += f" WHERE {filter_string}"

    with engine.begin() as conn:
        return pd.read_sql(text(query_filtered), conn, params=params or {})

# ==============================================================================
# 2. DATA PROCESSING (FLATTENING / MERGING)
# ==============================================================================

def process_patient_data(df_raw):
    """
    Mengubah DataFrame raw menjadi list of dicts.
    Diagnosis dan Inhibitor hanya diambil data pertamanya (STATIS).
    Kontak dan Treatment diambil semua datanya (DINAMIS).
    """
    if df_raw.empty:
        return []

    # 1. Mapping Kolom Statis (Data Diri + Diagnosis + Inhibitor)
    # Data ini hanya akan muncul SEKALI (ambil baris pertama)
    static_mapping = {
        # Data Diri
        "full_name": "Nama Lengkap", "nik": "NIK", "birth_place": "Tempat Lahir",
        "birth_date": "Tanggal Lahir", "age_years": "Usia", "blood_group": "Gol Darah",
        "address": "Alamat", "rhesus": "Rhesus", "occupation": "Pekerjaan",
        "education": "Pendidikan Terakhir", "phone": "No Telp", "village": "Desa/Kelurahan",
        "district": "Kecamatan", "city": "Kota/Kabupaten", "province": "Propinsi",
        "gender": "Jenis Kelamin", "cabang": "HMHI Cabang",
        
        # Diagnosis (Dipindah ke sini agar cuma 1 data)
        "hemo_type": "Jenis Hemofilia",
        "severity": "Kategori Hemofilia",
        "diagnosed_on": "Tanggal Diagnosis",
        
        # Inhibitor (Dipindah ke sini agar cuma 1 data)
        "factor": "Inhibitor",
        "titer_bu": "Titer BU",
        "measured_on": "Tanggal Ukur Inhibitor"
    }

    # 2. Definisi Grup Data Berulang (Hanya Treatment dan Kontak)
    dynamic_groups = [
        {
            "name": "Treatment",
            "cols": ["name_hospital", "doctor_in_charge", "city_hospital", "province_hospital", "treatment_type", "frequency", "dose", "product"],
            "labels": {
                "name_hospital": "RS Perawatan", "doctor_in_charge": "DPJP", 
                "city_hospital": "Kota RS", "province_hospital": "Propinsi RS",
                "treatment_type": "Jenis Treatment", "frequency": "Frekuensi", 
                "dose": "Dosis", "product": "Produk"
            }
        },
        {
            "name": "Kontak",
            "cols": ["relation", "contact_name", "contact_phone"],
            "labels": {"relation": "Kontak Darurat", "contact_name": "Nama Kontak Darurat", "contact_phone": "No Telp Kontak Darurat"}
        }
    ]

    processed_list = []
    
    # Group by ID Pasien
    grouped = df_raw.groupby('patient_id', sort=False)

    for pid, group in grouped:
        row_dict = {}
        
        # A. Ambil Data Statis (Data Diri, Diagnosis, Inhibitor)
        # Menggunakan iloc[0] menjamin hanya data PERTAMA yang diambil
        first_row = group.iloc[0]
        
        for db_col, label in static_mapping.items():
            val = first_row.get(db_col)
            
            # Formatting khusus Usia
            if db_col == "age_years" and pd.notna(val):
                try: val = f"{int(float(val))} tahun"
                except: pass
                
            row_dict[label] = val if pd.notna(val) else "-"

        # B. Proses Data Dinamis (Treatment & Kontak)
        for grp in dynamic_groups:
            # Ambil hanya kolom terkait grup ini
            subset = group[grp["cols"]].drop_duplicates()
            subset = subset.dropna(how='all')
            
            for idx, ( _, sub_row) in enumerate(subset.iterrows()):
                # Tambahkan suffix " ke 1", " ke 2", dst
                suffix = f" ke {idx + 1}"
                
                for col_name in grp["cols"]:
                    label_base = grp["labels"][col_name]
                    final_key = f"{label_base}{suffix}"
                    val = sub_row[col_name]
                    row_dict[final_key] = val if pd.notna(val) else "-"
        
        processed_list.append(row_dict)
        
    return processed_list

# ==============================================================================
# 3. HELPER GENERATE PDF
# ==============================================================================

def generate_pdf(row_dict):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # Header
    pdf.set_font("helvetica", 'B', 16)
    pdf.set_text_color(180, 0, 0)
    pdf.cell(0, 10, "KARTU DATA PENYANDANG HEMOFILIA", ln=True, align='C')
    pdf.set_font("helvetica", 'I', 9)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 5, f"Dicetak pada: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align='C')
    pdf.ln(10)
    
    pdf.set_text_color(0, 0, 0)
    
    for key, val in row_dict.items():
        pdf.set_font("helvetica", 'B', 10)
        # Nama Field
        pdf.cell(75, 8, f"{key}", border=0)
        
        pdf.set_font("helvetica", size=10)
        # Nilai Field
        x_start = pdf.get_x()
        y_start = pdf.get_y()
        
        pdf.multi_cell(0, 8, f": {val}", border=0)
        
        pdf.set_draw_color(230, 230, 230)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(1)
        
    return bytes(pdf.output())

# ==============================================================================
# 4. UI & MAIN LOGIC
# ==============================================================================

if 'page_number' not in st.session_state:
    st.session_state.page_number = 0

def reset_page():
    st.session_state.page_number = 0

st.title("📋 Data Lengkap Penyandang Hemofilia")

col_search, _ = st.columns([1, 2])
with col_search:
    search_term = st.text_input("Cari Data", placeholder="Nama, NIK, atau Kota...", on_change=reset_page)

# QUERY SQL
base_query = """
    SELECT
        p.id AS patient_id,
        p.full_name, p.nik, p.birth_place, p.birth_date,
        EXTRACT(YEAR FROM age(CURRENT_DATE, p.birth_date)) AS age_years,
        p.blood_group, p.address, p.rhesus, p.occupation, p.education,
        p.phone, p.village, p.district, p.city, p.province, p.gender, p.cabang,
        hd.hemo_type, hd.severity, hd.diagnosed_on,
        hi.factor, hi.titer_bu, hi.measured_on,
        th.name_hospital, th.doctor_in_charge, th.city_hospital, th.province_hospital,
        th.treatment_type, th.frequency, th.dose, th.product,
        c.relation, c.name AS contact_name, c.phone AS contact_phone
    FROM pwh.patients p
    LEFT JOIN pwh.hemo_diagnoses hd ON p.id = hd.patient_id
    LEFT JOIN pwh.hemo_inhibitors hi ON p.id = hi.patient_id
    LEFT JOIN pwh.treatment_hospital th ON p.id = th.patient_id
    LEFT JOIN pwh.contacts c ON p.id = c.patient_id
"""

params = {}
if search_term:
    params["search"] = f"%{search_term}%"
    base_query += " WHERE (p.full_name ILIKE :search OR p.nik ILIKE :search OR p.city ILIKE :search)"

base_query += " ORDER BY p.full_name ASC"

try:
    # 1. Ambil Raw Data
    df_raw = run_df_branch(base_query, params)
    
    # 2. Proses Flattening
    data_list = process_patient_data(df_raw)
    
    total_data = len(data_list)
    ITEMS_PER_PAGE = 20
    total_pages = math.ceil(total_data / ITEMS_PER_PAGE) if total_data > 0 else 1

    if st.session_state.page_number >= total_pages:
        st.session_state.page_number = 0
    
    start_idx = st.session_state.page_number * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    page_data = data_list[start_idx:end_idx]

    st.info(f"Ditemukan **{total_data}** pasien unik. Menampilkan halaman **{st.session_state.page_number + 1}** dari **{total_pages}**.")

    col_prev, col_spacer, col_next = st.columns([1, 4, 1])
    with col_prev:
        if st.button("⬅️ Sebelumnya", disabled=(st.session_state.page_number == 0), key="top_prev"):
            st.session_state.page_number -= 1
            st.rerun()
    with col_next:
        if st.button("Selanjutnya ➡️", disabled=(end_idx >= total_data), key="top_next"):
            st.session_state.page_number += 1
            st.rerun()

    st.markdown("---")

    if not page_data:
        st.warning("Data tidak ditemukan.")
    else:
        for idx, row_dict in enumerate(page_data):
            nama = row_dict.get("Nama Lengkap", "Tanpa Nama")
            nik = row_dict.get("NIK", "-")
            cabang = row_dict.get("HMHI Cabang", "-")
            
            with st.expander(f"👤 {nama} | NIK: {nik} | {cabang}"):
                c1, c2 = st.columns([1, 4])
                with c1:
                    pdf_bytes = generate_pdf(row_dict)
                    st.download_button(
                        label="📥 Download PDF",
                        data=pdf_bytes,
                        file_name=f"PWH_{str(nama).strip().replace(' ', '_')}.pdf",
                        mime="application/pdf",
                        key=f"btn_pdf_{start_idx + idx}"
                    )
                
                st.markdown("---")
                
                for key, val in row_dict.items():
                    L, R = st.columns([1, 2])
                    L.markdown(f"**{key}**")
                    R.markdown(f": {val}")

    if total_pages > 1:
        st.markdown("---")
        cp, cs, cn = st.columns([1, 4, 1])
        with cp:
            if st.button("⬅️ Sebelumnya ", disabled=(st.session_state.page_number == 0), key="btm_prev"):
                st.session_state.page_number -= 1
                st.rerun()
        with cn:
            if st.button("Selanjutnya ➡️ ", disabled=(end_idx >= total_data), key="btm_next"):
                st.session_state.page_number += 1
                st.rerun()

except Exception as e:
    st.error(f"Terjadi kesalahan sistem: {e}")

