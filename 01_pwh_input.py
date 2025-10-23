# 01_pwh_input.py (Dengan tambahan kolom NIK, autoload Propinsi, autoload Cabang HMHI, dan layout rapi v3)
# VERSI INI SUDAH DITAMBAHKAN FILTER CABANG SESUAI LOGIN + PERBAIKAN CRASH KEYERROR
import os
import io
from datetime import date
import pandas as pd
import streamlit as st
from pandas import ExcelWriter
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

st.set_page_config(page_title="PWH Input", page_icon="🩸", layout="wide")


# Builder file Excel (multi-sheet) untuk semua tab
# ------------------------------------------------------------------------------
def build_excel_bytes() -> bytes:
    
    # --- KONTROL AKSES BERDASARKAN CABANG ---
    user_branch = st.session_state.get("user_branch", "ALL")
    is_admin = (user_branch == "ALL")
    
    filter_p = ""
    params_p = {}
    filter_join_p = ""
    params_join_p = {}

    if not is_admin:
        filter_p = " WHERE p.cabang = :user_branch "
        params_p = {"user_branch": user_branch}
        # Filter untuk query yang sudah ada join ke pwh.patients p
        filter_join_p = " WHERE p.cabang = :user_branch "
        params_join_p = {"user_branch": user_branch}
    # --- END KONTROL ---

    # Ambil semua dataset
    df_patients = run_df(f"""
        SELECT
    p.id,
    p.full_name,
    p.birth_place,
    p.birth_date,
    p.nik,
    COALESCE(pa.age_years, EXTRACT(YEAR FROM age(CURRENT_DATE, p.birth_date))) AS age_years,
    p.blood_group,
    p.rhesus,
    p.gender,
    p.occupation,
    p.education,
    p.address,
    p.village,
    p.district,
    p.phone,
    p.province,
    p.city,
    p.cabang, 
    p.kota_cakupan,
    p.note,
    p.created_at
FROM pwh.patients p
LEFT JOIN pwh.patient_age pa ON pa.id = p.id
{filter_p}
ORDER BY p.id

    """, params_p)
    
    df_diag = run_df(f"""
        SELECT d.id, d.patient_id, p.full_name, d.hemo_type, d.severity, d.diagnosed_on, d.source
        FROM pwh.hemo_diagnoses d JOIN pwh.patients p ON p.id = d.patient_id
        {filter_join_p}
    """, params_join_p)
    
    df_contacts = run_df(f"""
        SELECT c.id, c.patient_id, p.full_name, c.relation, c.name, c.phone, c.is_primary
        FROM pwh.contacts c JOIN pwh.patients p ON p.id = c.patient_id
        {filter_join_p}
    """, params_join_p)
    
    df_inhibitors = run_df(f"""
        SELECT i.id, i.patient_id, p.full_name, i.factor, i.titer_bu, i.measured_on, i.lab
        FROM pwh.inhibitors i JOIN pwh.patients p ON p.id = i.patient_id
        {filter_join_p}
    """, params_join_p)
    
    df_virus = run_df(f"""
        SELECT v.id, v.patient_id, p.full_name, v.test_type, v.result, v.tested_on, v.lab
        FROM pwh.virus_tests v JOIN pwh.patients p ON p.id = v.patient_id
        {filter_join_p}
    """, params_join_p)
    
    df_hospitals = run_df(f"""
        SELECT 
            t.id, t.patient_id, p.full_name, 
            h.name AS hospital_name, h.city AS hospital_city, h.province AS hospital_province,
            t.date_of_visit, t.doctor_in_charge, t.treatment_type, t.care_services,
            t.frequency, t.dose, t.product, t.merk
        FROM pwh.treatment_hospitals t
        JOIN pwh.hospitals h ON t.hospital_id = h.id
        JOIN pwh.patients p ON p.id = t.patient_id
        {filter_join_p}
    """, params_join_p)
    
    df_deaths = run_df(f"""
        SELECT m.id, m.patient_id, p.full_name, m.cause_of_death, m.year_of_death
        FROM pwh.deaths m JOIN pwh.patients p ON p.id = m.patient_id
        {filter_join_p}
    """, params_join_p)
    
    # Buat Excel
    output = io.BytesIO()
    with ExcelWriter(output, engine="xlsxwriter") as writer:
        df_patients.to_excel(writer, sheet_name="Pasien", index=False)
        df_diag.to_excel(writer, sheet_name="Diagnosa", index=False)
        df_contacts.to_excel(writer, sheet_name="Kontak", index=False)
        df_inhibitors.to_excel(writer, sheet_name="Inhibitor", index=False)
        df_virus.to_excel(writer, sheet_name="Virus Tes", index=False)
        df_hospitals.to_excel(writer, sheet_name="RS Penangan", index=False)
        df_deaths.to_excel(writer, sheet_name="Kematian", index=False)
    
    return output.getvalue()


# Builder template bulk import
# ------------------------------------------------------------------------------
def build_bulk_template_bytes() -> bytes:
    # --- KONTROL AKSES ---
    user_branch = st.session_state.get("user_branch", "ALL")
    is_admin = (user_branch == "ALL")
    
    filter_p = ""
    params_p = {}
    if not is_admin:
        filter_p = " WHERE p.cabang = :user_branch "
        params_p = {"user_branch": user_branch}
    # --- END KONTROL ---

    # Ambil data pasien (hanya ID dan nama) untuk validasi data di sheet lain
    df_patients = run_df(f"SELECT id, full_name FROM pwh.patients p {filter_p} ORDER BY full_name", params_p)
    
    # Ambil data RS (ID dan nama) untuk validasi
    df_hospitals = fetch_hospitals() # Ini sudah di-wrap try...except

    # Ambil data lookups dari helper tables
    df_blood_groups = pd.DataFrame(run_df("SELECT blood_group FROM pwh.helper_blood_groups").get('blood_group', pd.Series(dtype=str)))
    df_rhesus = pd.DataFrame(run_df("SELECT rhesus FROM pwh.helper_rhesus").get('rhesus', pd.Series(dtype=str)))
    df_genders = pd.DataFrame(run_df("SELECT gender FROM pwh.helper_genders").get('gender', pd.Series(dtype=str)))
    df_hemo_types = pd.DataFrame(run_df("SELECT hemo_type FROM pwh.helper_hemo_types").get('hemo_type', pd.Series(dtype=str)))
    df_severities = pd.DataFrame(run_df("SELECT severity FROM pwh.helper_severities").get('severity', pd.Series(dtype=str)))
    df_educations = pd.DataFrame(run_df("SELECT education FROM pwh.helper_education_levels").get('education', pd.Series(dtype=str)))
    df_inhibitor_factors = pd.DataFrame(run_df("SELECT factor FROM pwh.helper_inhibitor_factors").get('factor', pd.Series(dtype=str)))
    df_virus_tests = pd.DataFrame(run_df("SELECT test_type FROM pwh.helper_virus_tests").get('test_type', pd.Series(dtype=str)))
    df_test_results = pd.DataFrame(run_df("SELECT result FROM pwh.helper_test_results").get('result', pd.Series(dtype=str)))
    df_relations = pd.DataFrame(run_df("SELECT relation FROM pwh.helper_relations").get('relation', pd.Series(dtype=str)))
    df_occupations = pd.DataFrame(fetch_occupations_list().get('occupation', pd.Series(dtype=str)))
    df_treatment_types = pd.DataFrame(run_df("SELECT treatment_type FROM pwh.helper_treatment_types").get('treatment_type', pd.Series(dtype=str)))
    df_care_services = pd.DataFrame(run_df("SELECT care_service FROM pwh.helper_care_services").get('care_service', pd.Series(dtype=str)))
    df_products = pd.DataFrame(run_df("SELECT product FROM pwh.helper_products").get('product', pd.Series(dtype=str)))

    # Gabungkan semua lookups untuk sheet 'lookups'
    df_lookups = pd.concat([
        df_blood_groups.reset_index(drop=True),
        df_rhesus.reset_index(drop=True),
        df_genders.reset_index(drop=True),
        df_hemo_types.reset_index(drop=True),
        df_severities.reset_index(drop=True),
        df_educations.reset_index(drop=True),
        df_inhibitor_factors.reset_index(drop=True),
        df_virus_tests.reset_index(drop=True),
        df_test_results.reset_index(drop=True),
        df_relations.reset_index(drop=True),
        df_occupations.reset_index(drop=True),
        df_treatment_types.reset_index(drop=True),
        df_care_services.reset_index(drop=True),
        df_products.reset_index(drop=True)
    ], axis=1)
    df_lookups.columns = [
        "blood_groups", "rhesus", "genders", "hemo_types", "severities", 
        "education_levels", "inhibitor_factors", "virus_tests", "test_results", 
        "relations", "occupations", "treatment_types", "care_services", "products"
    ]

    # Buat sheet README
    readme_content = [
        ["Template Bulk Insert PWH (v2)"],
        ["Cara pakai:"],
        ["1) Isi setiap sheet sesuai kolom. 'patients' harus diisi lebih dulu jika menambah pasien baru."],
        ["2) Gunakan format tanggal yyyy-mm-dd (contoh: 2023-01-30)."],
        ["3) Kolom dropdown sudah dibatasi ke pilihan valid (lihat sheet 'lookups')."],
        ["4) Untuk sheet selain 'patients', WAJIB isi 'patient_id' (ambil dari tabel 'Data Pasien Terbaru' di aplikasi)."],
        ["5) 'is_primary' (contacts) gunakan TRUE/FALSE."],
        ["6) 'hospital_name' (treatment_hospitals) harus persis sama dengan nama di tabel RS."],
        ["7) Hapus baris contoh (jika ada) sebelum mengunggah."]
    ]
    df_readme = pd.DataFrame(readme_content)


    # --- Buat sheet template ---
    df_patients_tpl = pd.DataFrame(columns=[
        "full_name", "birth_place", "birth_date", "nik", "blood_group", "rhesus", "gender", 
        "occupation", "education", "address", "village", "district", "phone", 
        "province", "city", "cabang", "kota_cakupan", "note"
    ])
    df_diagnoses_tpl = pd.DataFrame(columns=[
        "patient_id", "hemo_type", "severity", "diagnosed_on", "source"
    ])
    df_contacts_tpl = pd.DataFrame(columns=[
        "patient_id", "relation", "name", "phone", "is_primary"
    ])
    df_inhibitors_tpl = pd.DataFrame(columns=[
        "patient_id", "factor", "titer_bu", "measured_on", "lab"
    ])
    df_virus_tests_tpl = pd.DataFrame(columns=[
        "patient_id", "test_type", "result", "tested_on", "lab"
    ])
    df_treatment_hospitals_tpl = pd.DataFrame(columns=[
        "patient_id", "hospital_name", "date_of_visit", "doctor_in_charge", 
        "treatment_type", "care_services", "frequency", "dose", "product", "merk"
    ])
    df_deaths_tpl = pd.DataFrame(columns=[
        "patient_id", "cause_of_death", "year_of_death"
    ])

    # Buat file Excel
    output = io.BytesIO()
    with ExcelWriter(output, engine="xlsxwriter") as writer:
        df_readme.to_excel(writer, sheet_name="README", index=False, header=False)
        df_patients_tpl.to_excel(writer, sheet_name="patients", index=False)
        df_diagnoses_tpl.to_excel(writer, sheet_name="diagnoses", index=False)
        df_contacts_tpl.to_excel(writer, sheet_name="contacts", index=False)
        df_inhibitors_tpl.to_excel(writer, sheet_name="inhibitors", index=False)
        df_virus_tests_tpl.to_excel(writer, sheet_name="virus_tests", index=False)
        df_treatment_hospitals_tpl.to_excel(writer, sheet_name="treatment_hospitals", index=False)
        df_deaths_tpl.to_excel(writer, sheet_name="kematian", index=False)
        
        # Sembunyikan sheet lookups dan data validasi
        df_lookups.to_excel(writer, sheet_name="lookups", index=False)
        df_patients.to_excel(writer, sheet_name="data_patients_lookup", index=False)
        df_hospitals.to_excel(writer, sheet_name="data_hospitals_lookup", index=False)
        
        workbook = writer.book
        worksheet_lookups = writer.sheets["lookups"]
        worksheet_patients_lookup = writer.sheets["data_patients_lookup"]
        worksheet_hospitals_lookup = writer.sheets["data_hospitals_lookup"]
        
        worksheet_lookups.protect()
        worksheet_lookups.hide()
        worksheet_patients_lookup.protect()
        worksheet_patients_lookup.hide()
        worksheet_hospitals_lookup.protect()
        worksheet_hospitals_lookup.hide()

        # --- Tambahkan Data Validation ---
        def add_validation(sheet_name, col, col_idx, lookup_df, lookup_col_name):
            try:
                worksheet = writer.sheets[sheet_name]
                # Pastikan lookup_df tidak kosong dan kolom ada
                if lookup_df.empty or lookup_col_name not in lookup_df.columns:
                    return
                lookup_len = len(lookup_df[lookup_col_name].dropna())
                if lookup_len == 0: return # Jangan lakukan jika lookup kosong
                
                # Buat named range
                range_name = f"lookup_{lookup_col_name}"
                worksheet_lookups.define_name(range_name, f"=lookups!${col_idx}$2:${col_idx}${lookup_len + 1}")
                
                # Terapkan validasi ke kolom
                worksheet.data_validation(f"{col}2:{col}1048576", {"validate": "list", "source": f"={range_name}"})
            except Exception as e:
                st.warning(f"Gagal menambah validasi untuk {sheet_name} col {col}: {e}")

        # Validasi sheet 'patients'
        add_validation("patients", "E", "A", df_lookups, "blood_groups")
        add_validation("patients", "F", "B", df_lookups, "rhesus")
        add_validation("patients", "G", "C", df_lookups, "genders")
        add_validation("patients", "H", "K", df_lookups, "occupations")
        add_validation("patients", "I", "F", df_lookups, "education_levels")
        
        # Validasi sheet 'diagnoses'
        add_validation("diagnoses", "B", "D", df_lookups, "hemo_types")
        add_validation("diagnoses", "C", "E", df_lookups, "severities")
        
        # Validasi sheet 'contacts'
        add_validation("contacts", "B", "J", df_lookups, "relations")
        
        # Validasi sheet 'inhibitors'
        add_validation("inhibitors", "B", "G", df_lookups, "inhibitor_factors")

        # Validasi sheet 'virus_tests'
        add_validation("virus_tests", "B", "H", df_lookups, "virus_tests")
        add_validation("virus_tests", "C", "I", df_lookups, "test_results")

        # Validasi sheet 'treatment_hospitals'
        add_validation("treatment_hospitals", "E", "L", df_lookups, "treatment_types")
        add_validation("treatment_hospitals", "F", "M", df_lookups, "care_services")
        add_validation("treatment_hospitals", "I", "N", df_lookups, "products")

        # Validasi data 'patient_id' dan 'hospital_name'
        try:
            # Validasi Patient ID
            patient_lookup_len = len(df_patients)
            if patient_lookup_len > 0 and 'id' in df_patients.columns:
                worksheet_patients_lookup.define_name("lookup_patient_ids", f"=data_patients_lookup!$A$2:$A${patient_lookup_len + 1}")
                id_val_opts = {"validate": "list", "source": "=lookup_patient_ids"}
                
                writer.sheets["diagnoses"].data_validation("A2:A1048576", id_val_opts)
                writer.sheets["contacts"].data_validation("A2:A1048576", id_val_opts)
                writer.sheets["inhibitors"].data_validation("A2:A1048576", id_val_opts)
                writer.sheets["virus_tests"].data_validation("A2:A1048576", id_val_opts)
                writer.sheets["treatment_hospitals"].data_validation("A2:A1048576", id_val_opts)
                writer.sheets["kematian"].data_validation("A2:A1048576", id_val_opts)

            # Validasi Hospital Name
            hospital_lookup_len = len(df_hospitals)
            if hospital_lookup_len > 0 and 'name' in df_hospitals.columns:
                worksheet_hospitals_lookup.define_name("lookup_hospital_names", f"=data_hospitals_lookup!$B$2:$B${hospital_lookup_len + 1}")
                writer.sheets["treatment_hospitals"].data_validation("B2:B1048576", {"validate": "list", "source": "=lookup_hospital_names"})

        except Exception as e:
            st.warning(f"Gagal menambah validasi data lookup: {e}")

    return output.getvalue()


# Fungsi import bulk
# ------------------------------------------------------------------------------
def import_bulk_excel(file) -> dict:
    xls = pd.ExcelFile(file)
    sheets_to_import = [
        "patients", "diagnoses", "contacts", "inhibitors", 
        "virus_tests", "treatment_hospitals", "kematian"
    ]
    results = {"dilewati": 0, "diimpor": 0, "error": 0}
    
    # Ambil data RS untuk mapping nama ke ID
    try:
        hospital_lookup_df = fetch_hospitals() # Ini sudah di-wrap
        if hospital_lookup_df.empty:
            raise Exception("Tabel 'pwh.hospitals' kosong atau tidak ditemukan.")
        hospital_lookup = hospital_lookup_df.set_index('name')['id'].to_dict()
    except Exception as e:
        raise Exception(f"Gagal memuat data RS untuk mapping: {e}")

    # --- KONTROL AKSES: Hanya admin atau user cabang yg bisa import ---
    user_branch = st.session_state.get("user_branch", "ALL")
    is_admin = (user_branch == "ALL")

    # Mapping patient_id yang baru dibuat di sheet 'patients'
    new_patient_id_map = {} # Map 'full_name' -> 'new_id'

    with engine.begin() as conn: # Mulai transaksi
        
        # 1. Proses 'patients'
        if "patients" in xls.sheet_names:
            df_pat = pd.read_excel(xls, sheet_name="patients").dropna(how='all')
            df_pat = df_pat.astype(str).replace('nan', None) # Ganti nan
            
            for _, row in df_pat.iterrows():
                try:
                    # Cek apakah cabang user = cabang di row, atau user adalah admin
                    row_cabang = row.get('cabang')
                    if not is_admin and row_cabang != user_branch:
                        st.warning(f"Dilewati (pasien): {row.get('full_name')}. Cabang '{row_cabang}' tidak sesuai dengan user '{user_branch}'.")
                        results["dilewati"] += 1
                        continue

                    # Query insert
                    q_insert_pat = text("""
                        INSERT INTO pwh.patients 
                        (full_name, birth_place, birth_date, nik, blood_group, rhesus, gender, 
                         occupation, education, address, village, district, phone, 
                         province, city, cabang, kota_cakupan, note, created_at)
                        VALUES 
                        (:full_name, :birth_place, :birth_date, :nik, :blood_group, :rhesus, :gender, 
                         :occupation, :education, :address, :village, :district, :phone, 
                         :province, :city, :cabang, :kota_cakupan, :note, CURRENT_TIMESTAMP)
                        RETURNING id;
                    """)
                    
                    params = row.to_dict()
                    # Pastikan kolom date valid
                    params['birth_date'] = pd.to_datetime(params.get('birth_date'), errors='coerce').date() or None
                    
                    res = conn.execute(q_insert_pat, params)
                    new_id = res.fetchone()[0]
                    new_patient_id_map[row['full_name']] = new_id # Simpan ID baru
                    results["diimpor"] += 1
                    
                except Exception as e:
                    st.error(f"Gagal import (pasien) {row.get('full_name')}: {e}")
                    results["error"] += 1

        # 2. Ambil semua patient ID yang ada (termasuk yang baru)
        # Ini penting agar user cabang tidak bisa import data ke pasien cabang lain
        all_patients_df = get_all_patients_for_selection() # Ini sudah difilter by cabang
        existing_patient_id_map = all_patients_df.set_index('full_name')['id'].to_dict() if 'full_name' in all_patients_df.columns else {}
        allowed_patient_ids = set(all_patients_df['id']) if 'id' in all_patients_df.columns else set()
        
        # Gabungkan map
        patient_id_map = {**existing_patient_id_map, **new_patient_id_map}


        # 3. Proses sheet lainnya
        for sheet_name in sheets_to_import:
            if sheet_name == "patients" or sheet_name not in xls.sheet_names:
                continue
            
            df_sheet = pd.read_excel(xls, sheet_name=sheet_name).dropna(how='all')
            df_sheet = df_sheet.astype(str).replace('nan', None)

            for _, row in df_sheet.iterrows():
                try:
                    params = row.to_dict()
                    
                    # Dapatkan patient_id
                    pid_str = params.get('patient_id')
                    pid = None
                    
                    if pid_str and pid_str != 'None':
                        pid = int(float(pid_str)) # Konversi dari string, bisa jadi float "123.0"
                    else:
                        # Coba map dari full_name jika ada
                        full_name = params.get('full_name')
                        if full_name and full_name in patient_id_map:
                            pid = patient_id_map[full_name]
                        else:
                            st.warning(f"Dilewati ({sheet_name}): 'patient_id' kosong/tidak valid dan nama '{full_name}' tidak ditemukan.")
                            results["dilewati"] += 1
                            continue
                    
                    params['patient_id'] = pid
                    
                    # --- VALIDASI KONTROL AKSES ---
                    if pid not in allowed_patient_ids:
                        st.warning(f"Dilewati ({sheet_name}): Data untuk patient_id {pid} tidak termasuk dalam cabang Anda.")
                        results["dilewati"] += 1
                        continue
                    
                    # Tentukan query berdasarkan sheet
                    query = None
                    if sheet_name == "diagnoses":
                        params['diagnosed_on'] = pd.to_datetime(params.get('diagnosed_on'), errors='coerce').date() or None
                        query = text("""
                            INSERT INTO pwh.hemo_diagnoses (patient_id, hemo_type, severity, diagnosed_on, source)
                            VALUES (:patient_id, :hemo_type, :severity, :diagnosed_on, :source)
                        """)
                    
                    elif sheet_name == "contacts":
                        params['is_primary'] = str(params.get('is_primary')).strip().upper() == 'TRUE'
                        query = text("""
                            INSERT INTO pwh.contacts (patient_id, relation, name, phone, is_primary)
                            VALUES (:patient_id, :relation, :name, :phone, :is_primary)
                        """)
                        
                    elif sheet_name == "inhibitors":
                        params['titer_bu'] = pd.to_numeric(params.get('titer_bu'), errors='coerce')
                        params['measured_on'] = pd.to_datetime(params.get('measured_on'), errors='coerce').date() or None
                        query = text("""
                            INSERT INTO pwh.inhibitors (patient_id, factor, titer_bu, measured_on, lab)
                            VALUES (:patient_id, :factor, :titer_bu, :measured_on, :lab)
                        """)
                        
                    elif sheet_name == "virus_tests":
                        params['tested_on'] = pd.to_datetime(params.get('tested_on'), errors='coerce').date() or None
                        query = text("""
                            INSERT INTO pwh.virus_tests (patient_id, test_type, result, tested_on, lab)
                            VALUES (:patient_id, :test_type, :result, :tested_on, :lab)
                        """)
                        
                    elif sheet_name == "kematian":
                        params['year_of_death'] = int(pd.to_numeric(params.get('year_of_death'), errors='coerce'))
                        query = text("""
                            INSERT INTO pwh.deaths (patient_id, cause_of_death, year_of_death)
                            VALUES (:patient_id, :cause_of_death, :year_of_death)
                        """)
                        
                    elif sheet_name == "treatment_hospitals":
                        # Mapping hospital_name ke hospital_id
                        hospital_name = params.get('hospital_name')
                        if hospital_name not in hospital_lookup:
                            st.warning(f"Dilewati (treatment): Nama RS '{hospital_name}' tidak ditemukan di database.")
                            results["dilewati"] += 1
                            continue
                        
                        params['hospital_id'] = hospital_lookup[hospital_name]
                        params['date_of_visit'] = pd.to_datetime(params.get('date_of_visit'), errors='coerce').date() or None
                        query = text("""
                            INSERT INTO pwh.treatment_hospitals 
                            (patient_id, hospital_id, date_of_visit, doctor_in_charge, treatment_type, 
                             care_services, frequency, dose, product, merk)
                            VALUES 
                            (:patient_id, :hospital_id, :date_of_visit, :doctor_in_charge, :treatment_type, 
                             :care_services, :frequency, :dose, :product, :merk)
                        """)
                    
                    if query is not None:
                        conn.execute(query, params)
                        results["diimpor"] += 1

                except Exception as e:
                    st.error(f"Gagal import (sheet {sheet_name}) data: {row.to_dict()}: {e}")
                    results["error"] += 1

    return results


# ------------------------------------------------------------------------------
# KONEKSI DATABASE
# ------------------------------------------------------------------------------
def _resolve_db_url() -> str:
    # --- PERBAIKAN: Membaca dari [secrets] ---
    try:
        sec = st.secrets.get("secrets", {}).get("DATABASE_URL", "")
        if sec: 
            return sec
    except Exception:
        pass
    env = os.environ.get("DATABASE_URL")
    if env: 
        return env
    
    st.error('DATABASE_URL tidak ditemukan di Streamlit Secrets atau env var.')
    st.caption("Pastikan Anda sudah menambahkan `DATABASE_URL` ke dalam blok `[secrets]` di Streamlit Cloud.")
    return None
    # --- AKHIR PERBAIKAN ---

@st.cache_resource(show_spinner="Menghubungkan ke database...")
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

# --- Inisialisasi Engine ---
db_url = _resolve_db_url()
if db_url:
    engine = get_engine(db_url)
else:
    st.stop()

# Helper untuk eksekusi query
def run_df(query: str, params: dict = None) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn, params=params)
    except Exception as e:
        st.error(f"Gagal eksekusi query: {e}")
        # --- PERBAIKAN: Kembalikan DataFrame kosong agar tidak crash ---
        return pd.DataFrame()

# Helper untuk eksekusi DML (Insert, Update, Delete)
def run_dml(query: str, params: dict, success_msg: str):
    try:
        with engine.begin() as conn:
            conn.execute(text(query), params)
        st.success(success_msg)
        # Clear cache yang relevan
        get_all_patients_for_selection.clear()
        st.rerun()
    except Exception as e:
        st.error(f"Gagal: {e}")


# ------------------------------------------------------------------------------
# FUNGSI CACHE DATA
# ------------------------------------------------------------------------------

# Cache data pasien untuk dropdown
@st.cache_data(show_spinner="Memuat daftar pasien...")
def get_all_patients_for_selection() -> pd.DataFrame:
    # --- KONTROL AKSES BERDASARKAN CABANG ---
    user_branch = st.session_state.get("user_branch", "ALL")
    is_admin = (user_branch == "ALL")
    
    filter_p = ""
    params_p = {}
    if not is_admin:
        filter_p = " WHERE p.cabang = :user_branch "
        params_p = {"user_branch": user_branch}
    # --- END KONTROL ---

    query = f"SELECT id, full_name FROM pwh.patients p {filter_p} ORDER BY full_name"
    return run_df(query, params_p)

# Cache data pekerjaan
@st.cache_data(show_spinner="Memuat daftar pekerjaan...")
def fetch_occupations_list() -> pd.DataFrame:
    # --- PERBAIKAN: Tambahkan try...except untuk mencegah crash ---
    try:
        return run_df("SELECT occupation FROM pwh.helper_occupations ORDER BY occupation")
    except Exception as e:
        st.error(f"Gagal memuat daftar pekerjaan: {e}")
        return pd.DataFrame(columns=['occupation']) # Kembalikan DataFrame kosong
    # --- AKHIR PERBAIKAN ---

# Cache data RS
@st.cache_data(show_spinner="Memuat daftar RS...")
def fetch_hospitals() -> pd.DataFrame:
    # --- PERBAIKAN: Tambahkan try...except untuk mencegah crash ---
    try:
        return run_df("SELECT id, name, city, province FROM pwh.hospitals ORDER BY name")
    except Exception as e:
        st.error(f"Gagal memuat daftar RS: {e}")
        return pd.DataFrame(columns=['id', 'name', 'city', 'province']) # Kembalikan DataFrame kosong
    # --- AKHIR PERBAIKAN ---

# Cache data wilayah (Provinsi, Kota)
@st.cache_data(show_spinner="Memuat data wilayah...")
def fetch_all_wilayah_details() -> pd.DataFrame:
    # --- PERBAIKAN: Tambahkan try...except untuk mencegah crash ---
    try:
        # --- PERUBAHAN NAMA TABEL ---
        return run_df("SELECT province, city, district, village FROM public.wilayah ORDER BY province, city, district, village")
    except Exception as e:
        st.error(f"Gagal memuat data wilayah: {e}")
        return pd.DataFrame(columns=["province", "city", "district", "village"]) # Kembalikan DataFrame kosong
    # --- AKHIR PERBAIKAN ---

# Cache data Cabang HMHI
@st.cache_data(show_spinner="Memuat data cabang HMHI...")
def fetch_hmhi_branches() -> list:
    # --- PERBAIKAN: Tambahkan try...except dan penanganan DataFrame kosong ---
    try:
        df = run_df("SELECT DISTINCT cabang FROM pwh.hmhi_cabang WHERE cabang IS NOT NULL ORDER BY cabang")
        if not df.empty and 'cabang' in df.columns:
            return df['cabang'].tolist()
        return [] # Kembalikan list kosong jika query gagal atau tabel kosong
    except Exception as e:
        st.error(f"Gagal memuat daftar cabang HMHI: {e}")
        return []
    # --- AKHIR PERBAIKAN ---

# ------------------------------------------------------------------------------
# FUNGSI HELPER
# ------------------------------------------------------------------------------

def get_safe_index(item_list: list, item_value: str, default_index: int = 0) -> int:
    """Mencari index dari item di list, return default jika tidak ketemu."""
    try:
        return item_list.index(item_value)
    except ValueError:
        return default_index

def load_patient_data_by_id(pat_id: int) -> (dict, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """Memuat semua data terkait untuk satu pasien."""
    if not pat_id:
        return {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # --- KONTROL AKSES: Pastikan user hanya bisa load data dari cabangnya ---
    user_branch = st.session_state.get("user_branch", "ALL")
    is_admin = (user_branch == "ALL")
    
    filter_p = ""
    params_p = {"id": pat_id}
    if not is_admin:
        filter_p = " AND cabang = :user_branch "
        params_p["user_branch"] = user_branch
    # --- END KONTROL ---

    # 1. Data Pasien
    q_pat = f"SELECT * FROM pwh.patients WHERE id = :id {filter_p} LIMIT 1"
    df_pat = run_df(q_pat, params_p)
    pat_data = df_pat.to_dict('records')[0] if not df_pat.empty else {}
    
    if not pat_data:
        # Jika data pasien kosong (karena ID tidak ada ATAU tidak sesuai cabang)
        st.warning(f"Data pasien ID {pat_id} tidak ditemukan atau tidak sesuai dengan cabang Anda.")
        return {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 2. Data Terkait (hanya jika pasien ditemukan)
    params_related = {"pat_id": pat_id}
    q_diag = "SELECT * FROM pwh.hemo_diagnoses WHERE patient_id = :pat_id"
    q_contacts = "SELECT * FROM pwh.contacts WHERE patient_id = :pat_id"
    q_inhibitors = "SELECT * FROM pwh.inhibitors WHERE patient_id = :pat_id"
    q_virus = "SELECT * FROM pwh.virus_tests WHERE patient_id = :pat_id"
    q_deaths = "SELECT * FROM pwh.deaths WHERE patient_id = :pat_id"
    q_hospitals = """
        SELECT 
            t.id, t.patient_id, t.hospital_id, h.name AS hospital_name,
            h.city AS hospital_city, h.province AS hospital_province,
            t.date_of_visit, t.doctor_in_charge, t.treatment_type, t.care_services,
            t.frequency, t.dose, t.product, t.merk
        FROM pwh.treatment_hospitals t
        JOIN pwh.hospitals h ON t.hospital_id = h.id
        WHERE t.patient_id = :pat_id
    """
    
    df_diag = run_df(q_diag, params_related)
    df_contacts = run_df(q_contacts, params_related)
    df_inhibitors = run_df(q_inhibitors, params_related)
    df_virus = run_df(q_virus, params_related)
    df_hospitals = run_df(q_hospitals, params_related)
    df_deaths = run_df(q_deaths, params_related)
    
    return pat_data, df_diag, df_contacts, df_inhibitors, df_virus, df_hospitals, df_deaths

def safe_date(val):
    """Konversi ke date, return None jika gagal."""
    if not val or val == 'NaT':
        return None
    try:
        return pd.to_datetime(val).date()
    except:
        return None

# ------------------------------------------------------------------------------
# MAIN UI
# ------------------------------------------------------------------------------
st.title("📝 Input Data Pasien Hemofilia")

# --- KONTROL AKSES: Ambil info user di awal ---
user_branch = st.session_state.get("user_branch", "ALL")
is_admin = (user_branch == "ALL")

# Ambil data cache
df_patients_list = get_all_patients_for_selection() # Sudah difilter
df_wilayah = fetch_all_wilayah_details()
df_occupations = fetch_occupations_list()
df_hospitals = fetch_hospitals()

# Helper list
list_patients = df_patients_list.to_dict('records') # List of {'id': 1, 'full_name': '...'}
# --- PERBAIKAN: Cek jika df_wilayah kosong ---
list_provinsi = [""]
if not df_wilayah.empty and 'province' in df_wilayah.columns:
    list_provinsi = [""] + df_wilayah['province'].unique().tolist()

list_occupations = [""]
if not df_occupations.empty and 'occupation' in df_occupations.columns:
    list_occupations = [""] + df_occupations['occupation'].unique().tolist()
    
list_hospitals = []
if not df_hospitals.empty:
    list_hospitals = df_hospitals.to_dict('records') # List of {'id': 1, 'name': '...', 'city': '...'}
# --- AKHIR PERBAIKAN ---


# --- STATE: ID Pasien yang dipilih ---
if 'selected_patient_id' not in st.session_state:
    st.session_state.selected_patient_id = None

def select_patient():
    if st.session_state.patient_selector:
        st.session_state.selected_patient_id = st.session_state.patient_selector.id

def clear_selection():
    st.session_state.selected_patient_id = None
    st.session_state.patient_selector = None # Hapus juga selector

# --- UI: Kolom Seleksi Pasien ---
st.markdown("Pilih pasien untuk **Edit Data** atau kosongkan untuk **Tambah Pasien Baru**.")
col_sel, col_btn = st.columns([3, 1])
with col_sel:
    # --- PERBAIKAN: Ambil index default ---
    default_patient_index = None
    if st.session_state.selected_patient_id:
        try:
            # Cari index dari pasien yang dipilih
            default_patient_index = df_patients_list[df_patients_list['id'] == st.session_state.selected_patient_id].index[0]
        except IndexError:
            default_patient_index = None # Tidak ditemukan (mungkin baru dibuat)
    # --- AKHIR PERBAIKAN ---

    st.selectbox(
        "Pilih Pasien (Cari berdasarkan Nama atau ID)",
        options=df_patients_list.itertuples(),
        format_func=lambda x: f"{x.full_name} (ID: {x.id})",
        key="patient_selector",
        on_change=select_patient,
        index=default_patient_index, # Set index agar selector update
        placeholder="Ketik untuk mencari...",
    )
with col_btn:
    st.button("➕ Tambah Pasien Baru", on_click=clear_selection, use_container_width=True)

st.markdown("---")


# --- Muat Data Pasien (jika dipilih) ---
pat_id = st.session_state.selected_patient_id
pat_data, df_diag, df_contacts, df_inhibitors, df_virus, df_hospitals, df_deaths = load_patient_data_by_id(pat_id)


# === UI: TABS ===
tab_pat, tab_diag, tab_contact, tab_inhib, tab_virus, tab_rs, tab_death, tab_export = st.tabs([
    "Identitas Pasien", "Diagnosa", "Kontak Darurat", "Inhibitor", 
    "Tes Virus", "RS Penangan", "Kematian", "Export/Import"
])

# ------------------------------------------------------------------------------
# --- TAB 1: IDENTITAS PASIEN ---
# ------------------------------------------------------------------------------
with tab_pat:
    st.header(f"Identitas Pasien (ID: {pat_id or 'BARU'})")
    
    with st.form("patient_form"):
        # Baris 1: Nama, Tempat/Tgl Lahir
        c1, c2, c3 = st.columns([2, 1, 1])
        full_name = c1.text_input("Nama Lengkap", value=pat_data.get('full_name', ''))
        birth_place = c2.text_input("Tempat Lahir", value=pat_data.get('birth_place', ''))
        birth_date = c3.date_input("Tanggal Lahir", value=safe_date(pat_data.get('birth_date')), min_value=date(1920, 1, 1), max_value=date.today())

        # Baris 2: NIK, Gol. Darah, Rhesus, Jenis Kelamin
        c1, c2, c3, c4 = st.columns(4)
        nik = c1.text_input("NIK (16 digit)", value=pat_data.get('nik', ''), max_chars=16)
        blood_group = c2.selectbox("Gol. Darah", ["", "A", "B", "AB", "O"], index=get_safe_index(["", "A", "B", "AB", "O"], pat_data.get('blood_group')))
        rhesus = c3.selectbox("Rhesus", ["", "+", "-"], index=get_safe_index(["", "+", "-"], pat_data.get('rhesus')))
        gender = c4.selectbox("Jenis Kelamin", ["", "Laki-laki", "Perempuan"], index=get_safe_index(["", "Laki-laki", "Perempuan"], pat_data.get('gender')))
        
        # Baris 3: Pekerjaan, Pendidikan
        c1, c2 = st.columns(2)
        occupation = c1.selectbox(
            "Pekerjaan", 
            list_occupations, 
            index=get_safe_index(list_occupations, pat_data.get('occupation'))
        )
        education = c2.selectbox(
            "Pendidikan Terakhir",
            ["", "SD", "SMP", "SMA/SMK/MA", "Diploma I", "Diploma II", "Diploma III", "Diploma IV", 
             "Pendidikan Profesi", "S1", "S2", "S3", "Spesialis", "Subspesialis"],
            index=get_safe_index(["", "SD", "SMP", "SMA/SMK/MA", "Diploma I", "Diploma II", "Diploma III", "Diploma IV", 
             "Pendidikan Profesi", "S1", "S2", "S3", "Spesialis", "Subspesialis"], pat_data.get('education'))
        )

        # Baris 4: Alamat Lengkap
        address = st.text_area("Alamat Lengkap (Sesuai KTP)", value=pat_data.get('address', ''), height=100)
        
        # Baris 5: Wilayah (dependen)
        c1, c2, c3, c4 = st.columns(4)
        
        # Logika Wilayah Dependen
        selected_prov = c1.selectbox(
            "Propinsi (Domisili)", 
            list_provinsi, 
            index=get_safe_index(list_provinsi, pat_data.get('province'))
        )
        
        list_kota = [""]
        if selected_prov and not df_wilayah.empty:
            list_kota = [""] + df_wilayah[df_wilayah['province'] == selected_prov]['city'].unique().tolist()
        selected_kota = c2.selectbox(
            "Kabupaten/Kota (Domisili)", 
            list_kota, 
            index=get_safe_index(list_kota, pat_data.get('city'))
        )
        
        list_kecamatan = [""]
        if selected_kota and not df_wilayah.empty:
            list_kecamatan = [""] + df_wilayah[(df_wilayah['city'] == selected_kota) & (df_wilayah['province'] == selected_prov)]['district'].unique().tolist()
        selected_kecamatan = c3.selectbox(
            "Kecamatan (Domisili)", 
            list_kecamatan,
            index=get_safe_index(list_kecamatan, pat_data.get('district'))
        )
        
        list_kelurahan = [""]
        if selected_kecamatan and not df_wilayah.empty:
            list_kelurahan = [""] + df_wilayah[(df_wilayah['district'] == selected_kecamatan) & (df_wilayah['city'] == selected_kota)]['village'].unique().tolist()
        selected_kelurahan = c4.selectbox(
            "Kelurahan/Desa (Domisili)",
            list_kelurahan,
            index=get_safe_index(list_kelurahan, pat_data.get('village'))
        )
        
        # Baris 6: Telepon
        phone = st.text_input("No. Ponsel Aktif (Contoh: 0812...)", value=pat_data.get('phone', ''))


        # --- START: Logika HMHI Cabang Autofill (BARU) ---
        st.markdown("---") # Pemisah visual
        
        # Ambil daftar cabang (ini tidak difilter, semua admin/user bisa lihat)
        cabang_list = [""] + fetch_hmhi_branches()
        kota_cakupan_val = ""
        
        # Cek data yang ada (jika mode edit)
        default_cabang = ""
        if pat_data:
            default_cabang = pat_data.get('cabang') or ""
        elif not is_admin:
            # --- PERUBAHAN DI SINI ---
            # Jika user BUKAN ADMIN dan ini form BARU,
            # otomatis isi cabang sesuai login user
            default_cabang = user_branch 

        cabang_idx = get_safe_index(cabang_list, default_cabang)

        # Baris 7: HMHI Cabang (Input) dan Kota Cakupan (Display)
        col_cabang, col_cakupan = st.columns(2)
        with col_cabang:
            selected_cabang = st.selectbox(
                "HMHI Cabang",
                cabang_list,
                index=cabang_idx,
                # --- PERUBAHAN DI SINI ---
                # Nonaktifkan pilihan jika bukan admin
                disabled=(not is_admin),
                key="selected_cabang_hmhi"
            )
        
        # Ambil data kota cakupan (dari cache)
        # Kita tidak perlu query lagi, cukup load df_hmhi_cabang
        try:
            df_hmhi_cabang = run_df("SELECT cabang, kota_cakupan FROM pwh.hmhi_cabang")
            if not df_hmhi_cabang.empty:
                kota_cakupan_dict = df_hmhi_cabang.set_index('cabang')['kota_cakupan'].to_dict()
                kota_cakupan_val = kota_cakupan_dict.get(selected_cabang, "N/A")
            else:
                kota_cakupan_val = "N/A (Tabel hmhi_cabang kosong)"
        except Exception as e:
            st.error(f"Gagal memuat kota cakupan: {e}")
            kota_cakupan_val = "Error"

        with col_cakupan:
            st.text_input(
                "Kota Cakupan (Otomatis)",
                value=kota_cakupan_val,
                disabled=True,
                key="kota_cakupan_display"
            )
        
        # --- END: Logika HMHI Cabang Autofill ---

        # Baris 8: Catatan
        note = st.text_area("Catatan Tambahan (Riwayat medis lain, dll)", value=pat_data.get('note', ''), height=100)

        # Tombol Submit
        submitted = st.form_submit_button("Simpan Data Pasien", type="primary")

        if submitted:
            # Validasi NIK jika diisi
            if nik and (not nik.isdigit() or len(nik) != 16):
                st.error("NIK harus 16 digit angka.")
            # Validasi nama
            elif not full_name:
                st.error("Nama Lengkap wajib diisi.")
            else:
                params = {
                    "full_name": full_name, "birth_place": birth_place, "birth_date": birth_date,
                    "nik": nik or None, "blood_group": blood_group or None, "rhesus": rhesus or None,
                    "gender": gender or None, "occupation": occupation or None, "education": education or None,
                    "address": address or None, "village": selected_kelurahan or None, "district": selected_kecamatan or None,
                    "phone": phone or None, "province": selected_prov or None, "city": selected_kota or None,
                    "cabang": selected_cabang or None, "kota_cakupan": kota_cakupan_val or None, "note": note or None
                }
                
                if pat_id: # UPDATE
                    params["id"] = pat_id
                    query = """
                        UPDATE pwh.patients SET
                        full_name = :full_name, birth_place = :birth_place, birth_date = :birth_date, nik = :nik,
                        blood_group = :blood_group, rhesus = :rhesus, gender = :gender, occupation = :occupation,
                        education = :education, address = :address, village = :village, district = :district,
                        phone = :phone, province = :province, city = :city, cabang = :cabang,
                        kota_cakupan = :kota_cakupan, note = :note
                        WHERE id = :id;
                    """
                    run_dml(query, params, f"Sukses: Data pasien {full_name} (ID: {pat_id}) berhasil diperbarui.")
                
                else: # INSERT
                    query = """
                        INSERT INTO pwh.patients 
                        (full_name, birth_place, birth_date, nik, blood_group, rhesus, gender, 
                         occupation, education, address, village, district, phone, 
                         province, city, cabang, kota_cakupan, note, created_at)
                        VALUES 
                        (:full_name, :birth_place, :birth_date, :nik, :blood_group, :rhesus, :gender, 
                         :occupation, :education, :address, :village, :district, :phone, 
                         :province, :city, :cabang, :kota_cakupan, :note, CURRENT_TIMESTAMP)
                        RETURNING id;
                    """
                    # Jalankan DML dengan cara sedikit berbeda untuk mendapatkan ID baru
                    try:
                        with engine.begin() as conn:
                            result = conn.execute(text(query), params)
                            new_id = result.fetchone()[0]
                        st.success(f"Sukses: Pasien baru {full_name} berhasil dibuat dengan ID: {new_id}.")
                        st.session_state.selected_patient_id = new_id # Otomatis pilih pasien baru
                        # Clear cache
                        get_all_patients_for_selection.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal menyimpan pasien baru: {e}")


# ------------------------------------------------------------------------------
# --- TAB 2: DIAGNOSA ---
# ------------------------------------------------------------------------------
with tab_diag:
    st.header(f"Diagnosa (Pasien: {pat_data.get('full_name', '...')})")
    
    if not pat_id:
        st.info("Simpan data pasien baru terlebih dahulu untuk menambah data diagnosa.")
    else:
        # Form Tambah/Edit Diagnosa
        with st.form("diag_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns(4)
            hemo_type = c1.selectbox("Jenis Hemofilia", ["", "A", "B", "vWD", "Other"], key="diag_type")
            severity = c2.selectbox("Kategori", ["", "Ringan", "Sedang", "Berat", "Tidak diketahui"], key="diag_sev")
            diagnosed_on = c3.date_input("Tanggal Diagnosis", value=None, min_value=date(1920, 1, 1), max_value=date.today(), key="diag_date")
            source = c4.text_input("Sumber Diagnosis (Opsional)", key="diag_source")
            
            submitted = st.form_submit_button("Tambah Diagnosa", type="primary")
            if submitted:
                if not hemo_type or not severity:
                    st.error("Jenis Hemofilia dan Kategori wajib diisi.")
                else:
                    params = {
                        "pat_id": pat_id,
                        "hemo_type": hemo_type,
                        "severity": severity,
                        "diagnosed_on": diagnosed_on,
                        "source": source or None
                    }
                    query = """
                        INSERT INTO pwh.hemo_diagnoses 
                        (patient_id, hemo_type, severity, diagnosed_on, source)
                        VALUES (:pat_id, :hemo_type, :severity, :diagnosed_on, :source);
                    """
                    run_dml(query, params, "Sukses: Data diagnosa berhasil ditambahkan.")

        st.markdown("---")
        # Tampilkan data diagnosa yang ada
        st.subheader("Data Diagnosa Tersimpan")
        if df_diag.empty:
            st.info("Belum ada data diagnosa untuk pasien ini.")
        else:
            st.dataframe(df_diag)


# ------------------------------------------------------------------------------
# --- TAB 3: KONTAK DARURAT ---
# ------------------------------------------------------------------------------
with tab_contact:
    st.header(f"Kontak Darurat (Pasien: {pat_data.get('full_name', '...')})")

    if not pat_id:
        st.info("Simpan data pasien baru terlebih dahulu untuk menambah data kontak.")
    else:
        # Form Tambah Kontak
        with st.form("contact_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns([1, 2, 1, 1])
            relation = c1.selectbox("Relasi", ["", "pasien", "ayah", "ibu", "wali", "lainnya"])
            name = c2.text_input("Nama Kontak")
            phone = c3.text_input("No. Telepon Kontak")
            is_primary = c4.checkbox("Kontak Utama?", value=False)
            
            submitted = st.form_submit_button("Tambah Kontak", type="primary")
            if submitted:
                if not relation or not name:
                    st.error("Relasi dan Nama Kontak wajib diisi.")
                else:
                    params = {
                        "pat_id": pat_id,
                        "relation": relation,
                        "name": name,
                        "phone": phone or None,
                        "is_primary": is_primary
                    }
                    query = """
                        INSERT INTO pwh.contacts (patient_id, relation, name, phone, is_primary)
                        VALUES (:pat_id, :relation, :name, :phone, :is_primary);
                    """
                    run_dml(query, params, "Sukses: Data kontak berhasil ditambahkan.")

        st.markdown("---")
        st.subheader("Data Kontak Tersimpan")
        if df_contacts.empty:
            st.info("Belum ada data kontak untuk pasien ini.")
        else:
            st.dataframe(df_contacts)


# ------------------------------------------------------------------------------
# --- TAB 4: INHIBITOR ---
# ------------------------------------------------------------------------------
with tab_inhib:
    st.header(f"Riwayat Inhibitor (Pasien: {pat_data.get('full_name', '...')})")

    if not pat_id:
        st.info("Simpan data pasien baru terlebih dahulu untuk menambah data inhibitor.")
    else:
        with st.form("inhibitor_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns(4)
            factor = c1.selectbox("Faktor", ["", "FVIII", "FIX"])
            titer_bu = c2.number_input("Titer (BU)", min_value=0.0, step=0.1, format="%.2f")
            measured_on = c3.date_input("Tanggal Pemeriksaan", value=None, min_value=date(1920, 1, 1), max_value=date.today())
            lab = c4.text_input("Nama Lab (Opsional)")
            
            submitted = st.form_submit_button("Tambah Riwayat Inhibitor", type="primary")
            if submitted:
                if not factor:
                    st.error("Faktor wajib diisi.")
                else:
                    params = {
                        "pat_id": pat_id,
                        "factor": factor,
                        "titer_bu": titer_bu,
                        "measured_on": measured_on,
                        "lab": lab or None
                    }
                    query = """
                        INSERT INTO pwh.inhibitors (patient_id, factor, titer_bu, measured_on, lab)
                        VALUES (:pat_id, :factor, :titer_bu, :measured_on, :lab);
                    """
                    run_dml(query, params, "Sukses: Data inhibitor berhasil ditambahkan.")

        st.markdown("---")
        st.subheader("Data Inhibitor Tersimpan")
        if df_inhibitors.empty:
            st.info("Belum ada data inhibitor untuk pasien ini.")
        else:
            st.dataframe(df_inhibitors)

# ------------------------------------------------------------------------------
# --- TAB 5: TES VIRUS ---
# ------------------------------------------------------------------------------
with tab_virus:
    st.header(f"Riwayat Tes Virus (Pasien: {pat_data.get('full_name', '...')})")

    if not pat_id:
        st.info("Simpan data pasien baru terlebih dahulu untuk menambah data tes virus.")
    else:
        with st.form("virus_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns(4)
            test_type = c1.selectbox("Jenis Tes", ["", "HBsAg", "Anti-HCV", "HIV"])
            result = c2.selectbox("Hasil Tes", ["", "positive", "negative", "indeterminate", "unknown"])
            tested_on = c3.date_input("Tanggal Tes", value=None, min_value=date(1920, 1, 1), max_value=date.today())
            lab = c4.text_input("Nama Lab (Opsional)")
            
            submitted = st.form_submit_button("Tambah Riwayat Tes Virus", type="primary")
            if submitted:
                if not test_type or not result:
                    st.error("Jenis Tes dan Hasil Tes wajib diisi.")
                else:
                    params = {
                        "pat_id": pat_id,
                        "test_type": test_type,
                        "result": result,
                        "tested_on": tested_on,
                        "lab": lab or None
                    }
                    query = """
                        INSERT INTO pwh.virus_tests (patient_id, test_type, result, tested_on, lab)
                        VALUES (:pat_id, :test_type, :result, :tested_on, :lab);
                    """
                    run_dml(query, params, "Sukses: Data tes virus berhasil ditambahkan.")

        st.markdown("---")
        st.subheader("Data Tes Virus Tersimpan")
        if df_virus.empty:
            st.info("Belum ada data tes virus untuk pasien ini.")
        else:
            st.dataframe(df_virus)


# ------------------------------------------------------------------------------
# --- TAB 6: RS PENANGAN ---
# ------------------------------------------------------------------------------
with tab_rs:
    st.header(f"Riwayat RS Penangan (Pasien: {pat_data.get('full_name', '...')})")
    
    if not pat_id:
        st.info("Simpan data pasien baru terlebih dahulu untuk menambah data RS penangan.")
    else:
        with st.form("hospital_form", clear_on_submit=True):
            # Baris 1: RS dan Tanggal
            c1, c2 = st.columns(2)
            selected_hospital = c1.selectbox(
                "Nama RS",
                options=df_hospitals.itertuples(), # df_hospitals sudah di-cache
                format_func=lambda x: f"{x.name} (ID: {x.id}, Kota: {x.city})",
                index=None,
                placeholder="Ketik untuk mencari RS..."
            )
            date_of_visit = c2.date_input("Tanggal Kunjungan/Perawatan", value=None, min_value=date(1920, 1, 1), max_value=date.today())
            
            # Baris 2: DPJP, Jenis Penanganan, Layanan Rawat
            c1, c2, c3 = st.columns(3)
            doctor_in_charge = c1.text_input("DPJP (Dokter Penanggung Jawab)")
            treatment_type = c2.selectbox("Jenis Penanganan", ["", "Prophylaxis", "On Demand"])
            care_services = c3.selectbox("Layanan Rawat", ["", "Rawat Jalan", "Rawat Inap"])
            
            # Baris 3: Frekuensi, Dosis, Produk, Merk
            c1, c2, c3, c4 = st.columns(4)
            frequency = c1.text_input("Frekuensi")
            dose = c2.text_input("Dosis")
            product = c3.selectbox("Produk", ["", "Plasma (FFP)", "Cryoprecipitate", "Konsentrat (plasma derived)", "Konsentrat (rekombinan)", "Konsentrat (prolonged half life)", "Prothrombin Complex", "DDAVP", "Emicizumab (Hemlibra)", "Konsentrat Bypassing Agent"])
            merk = c4.text_input("Merk (Opsional)")
            
            submitted = st.form_submit_button("Tambah Riwayat RS", type="primary")
            if submitted:
                if not selected_hospital:
                    st.error("Nama RS wajib diisi.")
                elif df_hospitals.empty:
                    st.error("Gagal menambah RS: Daftar RS kosong. Periksa koneksi/nama tabel pwh.hospitals.")
                else:
                    params = {
                        "pat_id": pat_id,
                        "hospital_id": selected_hospital.id,
                        "date_of_visit": date_of_visit,
                        "doctor_in_charge": doctor_in_charge or None,
                        "treatment_type": treatment_type or None,
                        "care_services": care_services or None,
                        "frequency": frequency or None,
                        "dose": dose or None,
                        "product": product or None,
                        "merk": merk or None
                    }
                    query = """
                        INSERT INTO pwh.treatment_hospitals 
                        (patient_id, hospital_id, date_of_visit, doctor_in_charge, treatment_type, 
                         care_services, frequency, dose, product, merk)
                        VALUES 
                        (:pat_id, :hospital_id, :date_of_visit, :doctor_in_charge, :treatment_type, 
                         :care_services, :frequency, :dose, :product, :merk);
                    """
                    run_dml(query, params, "Sukses: Data riwayat RS berhasil ditambahkan.")

        st.markdown("---")
        st.subheader("Data Riwayat RS Tersimpan")
        if df_hospitals.empty:
            st.info("Belum ada data riwayat RS untuk pasien ini.")
        else:
            # Tampilkan dengan nama, bukan ID
            st.dataframe(df_hospitals.drop(columns=['id', 'patient_id', 'hospital_id']))


# ------------------------------------------------------------------------------
# --- TAB 7: KEMATIAN ---
# ------------------------------------------------------------------------------
with tab_death:
    st.header(f"Data Kematian (Pasien: {pat_data.get('full_name', '...')})")
    
    if not pat_id:
        st.info("Simpan data pasien baru terlebih dahulu untuk menambah data kematian.")
    else:
        # Cek apakah data kematian sudah ada
        if not df_deaths.empty:
            st.error("Data kematian untuk pasien ini sudah tercatat.")
            st.dataframe(df_deaths)
        else:
            with st.form("death_form", clear_on_submit=True):
                c1, c2 = st.columns(2)
                year_of_death = c1.number_input("Tahun Kematian", min_value=1920, max_value=date.today().year, step=1, format="%d")
                cause_of_death = c2.text_input("Penyebab Kematian")
                
                submitted = st.form_submit_button("Simpan Data Kematian", type="primary")
                if submitted:
                    if not year_of_death or not cause_of_death:
                        st.error("Tahun dan Penyebab Kematian wajib diisi.")
                    else:
                        params = {
                            "pat_id": pat_id,
                            "year_of_death": year_of_death,
                            "cause_of_death": cause_of_death
                        }
                        query = """
                            INSERT INTO pwh.deaths (patient_id, cause_of_death, year_of_death)
                            VALUES (:pat_id, :cause_of_death, :year_of_death);
                        """
                        run_dml(query, params, "Sukses: Data kematian berhasil disimpan.")

# ------------------------------------------------------------------------------
# --- TAB 8: EXPORT / IMPORT ---
# ------------------------------------------------------------------------------
with tab_export:
    st.subheader("⬇️ Export Excel (semua tab)")
    st.write(f"Klik tombol di bawah untuk membuat file Excel dengan semua data pasien **(Hanya untuk cabang: {user_branch if not is_admin else 'SEMUA'})**.")
    
    if st.button("Generate file Excel"):
        try:
            excel_bytes = build_excel_bytes()
            st.download_button(
                label="💾 Download data_pwh.xlsx", 
                data=excel_bytes, 
                file_name=f"data_pwh_{user_branch.lower().replace(' ','_') if not is_admin else 'all'}.xlsx", 
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.success("File siap diunduh.")
        except Exception as e: 
            st.error(f"Gagal membuat file Excel: {e}")

    st.markdown("---")
    st.subheader("📥 Template Bulk & ⬆️ Import")
    st.warning(f"**PERHATIAN:** Anda hanya dapat mengimpor data untuk pasien yang terdaftar di cabang Anda **({user_branch if not is_admin else 'SEMUA'})**.")
    
    c1, c2 = st.columns([1,2])
    with c1:
        try:
            tpl = build_bulk_template_bytes()
            st.download_button(
                label="📄 Download Template Bulk (.xlsx)", 
                data=tpl, 
                file_name="pwh_bulk_template.xlsx", 
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.success("Template bulk (Bahasa Indonesia) siap diunduh.")
        except Exception as e: 
            st.error(f"Gagal membuat template: {e}")

    with c2:
        up = st.file_uploader("Unggah file Template Bulk (.xlsx) untuk di-import", type=["xlsx"])
        if up and st.button("🚀 Import Bulk ke Database", type="primary"):
            try:
                result = import_bulk_excel(up)
                msg = "Import selesai — " + ", ".join(f"{k}: {v}" for k, v in result.items())
                st.success(msg)
                # Clear cache setelah import bulk berhasil
                fetch_all_wilayah_details.clear()
                get_all_patients_for_selection.clear()
                fetch_occupations_list.clear()
                fetch_hospitals.clear()
                fetch_hmhi_branches.clear() # <-- Clear cache baru
                st.rerun() # Refresh data di tabel tampilan
            except Exception as e:
                st.error(f"Gagal import bulk: {e}")
                st.exception(e) # Tampilkan detail error

# ------------------------------------------------------------------------------
# --- TAMPILAN DATA PASIEN (DI LUAR TABS) ---
# ------------------------------------------------------------------------------

# --- KONTROL AKSES (Sudah didefinisikan di atas) ---
# user_branch = st.session_state.get("user_branch", "ALL")
# is_admin = (user_branch == "ALL")

filter_p = ""
params_p = {}
if not is_admin:
    filter_p = " WHERE p.cabang = :user_branch "
    params_p = {"user_branch": user_branch}
# --- END KONTROL ---

st.subheader(f"Data Pasien Terbaru (Cabang: {user_branch if not is_admin else 'SEMUA'})")
st.write("Menampilkan 200 data pasien terbaru yang terdaftar.")

# Query untuk mengambil data pasien (dfp)
dfp_query = f"""
    SELECT
        p.id,
        p.full_name,
        p.birth_place,
        p.birth_date,
        p.nik,
        COALESCE(pa.age_years, EXTRACT(YEAR FROM age(CURRENT_DATE, p.birth_date))) AS age_years,
        p.blood_group,
        p.rhesus,
        p.gender,
        p.occupation,
        p.education,
        p.address,
        p.village,
        p.district,
        p.phone,
        p.province,
        p.city,
        p.cabang,
        p.kota_cakupan,
        p.note,
        p.created_at
    FROM pwh.patients p
    LEFT JOIN pwh.patient_age pa ON pa.id = p.id
    {filter_p}
    ORDER BY p.id DESC
    LIMIT 200;
"""
dfp = run_df(dfp_query, params_p)

# --- PERBAIKAN: Cek jika dfp kosong sebelum memilih kolom ---
if not dfp.empty:
    # Kolom yang ingin ditampilkan
    dfp_display = dfp[[
        "id", "full_name", "nik", "age_years", "gender", 
        "cabang", "kota_cakupan", "province", "city", 
        "phone", "created_at"
    ]]
    # Ubah nama kolom untuk tampilan
    dfp_display.columns = [
        "ID", "Nama Lengkap", "NIK", "Usia (Thn)", "Gender",
        "Cabang HMHI", "Kota Cakupan", "Propinsi", "Kota/Kab",
        "Telepon", "Tgl Dibuat"
    ]
    st.dataframe(dfp_display, use_container_width=True)
else:
    st.info("Tidak ada data pasien untuk ditampilkan (sesuai filter cabang Anda).")
# --- AKHIR PERBAIKAN ---


