import os
import re
import io
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
# 2. HELPER GENERATE PDF (PERBAIKAN ERROR BYTEARRAY)
# ==============================================================================

def generate_pdf(row_data, fields):
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
    for field in fields:
        val = row_data.get(field, "-")
        if field == "Usia" and val != "-":
            try: val = f"{int(float(val))} tahun"
            except: pass
        
        pdf.set_font("helvetica", 'B', 10)
        pdf.cell(65, 8, f"{field}", border=0)
        pdf.set_font("helvetica", size=10)
        pdf.multi_cell(0, 8, f": {val}", border=0)
        pdf.set_draw_color(230, 230, 230)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(1)
        
    # FIX: Konversi bytearray ke bytes murni
    return bytes(pdf.output())

# ==============================================================================
# 3. UI & MAIN LOGIC
# ==============================================================================

st.title("📋 Data Lengkap Pasien")

col_search, _ = st.columns([1, 2])
with col_search:
    search_term = st.text_input("Cari Pasien", placeholder="Nama, NIK, atau Kota...")

base_query = """
    SELECT
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
    df = run_df_branch(base_query, params)
    
    mapping = {
        "full_name": "Nama Lengkap", "nik": "NIK", "birth_place": "Tempat Lahir",
        "birth_date": "Tanggal Lahir", "age_years": "Usia", "blood_group": "Gol Darah",
        "address": "Alamat", "rhesus": "Rhesus", "occupation": "Pekerjaan",
        "education": "Pendidikan Terakhir", "phone": "No Telp", "village": "Desa/Kelurahan",
        "district": "Kecamatan", "city": "Kota/Kabupaten", "province": "Propinsi",
        "gender": "Jenis Kelamin", "cabang": "HMHI Cabang", "hemo_type": "Jenis Hemofilia",
        "severity": "Kategori Hemofilia", "diagnosed_on": "Tanggal Diagnosis",
        "factor": "Inhibitor", "titer_bu": "Titer BU", "measured_on": "Tanggal Pengukuran Inhibitor",
        "name_hospital": "Rumah Sakit Perawatan", "doctor_in_charge": "DPJP",
        "city_hospital": "Kota/Kabupaten RS", "province_hospital": "Propinsi RS",
        "treatment_type": "Jenis Treatment", "frequency": "Frekuensi Perawatan",
        "dose": "Dosis Perawatan", "product": "Produk Perawatan", "relation": "Kontak Darurat",
        "contact_name": "Nama Kontak Darurat", "contact_phone": "No Telp Kontak Darurat"
    }
    df_display = df.rename(columns=mapping)
    field_order = list(mapping.values())

    st.write(f"Ditemukan **{len(df_display)}** data.")

    for idx, row in df_display.iterrows():
        with st.expander(f"👤 {row['Nama Lengkap']} | NIK: {row['NIK']} | {row['HMHI Cabang']}"):
            
            c1, c2 = st.columns([1, 4])
            with c1:
                # Memanggil fungsi PDF yang sudah diperbaiki
                pdf_data = generate_pdf(row, field_order)
                st.download_button(
                    label="📥 Download PDF",
                    data=pdf_data,
                    file_name=f"PWH_{row['Nama Lengkap'].replace(' ', '_')}.pdf",
                    mime="application/pdf",
                    key=f"pdf_{idx}"
                )
            
            st.markdown("---")
            for field in field_order:
                val = row.get(field, "-")
                if field == "Usia" and val != "-":
                    try: val = f"{int(float(val))} tahun"
                    except: pass
                
                L, R = st.columns([1, 2])
                L.markdown(f"**{field}**")
                R.markdown(f": {val}")

except Exception as e:
    st.error(f"Gagal memuat data: {e}")
