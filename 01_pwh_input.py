# 01_pwh_input.py (Dengan tambahan kolom NIK, autoload Propinsi, autoload Cabang HMHI, dan layout rapi v3)
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
    # Ambil semua dataset
    df_patients = run_df("""
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
WHERE (:branch = 'ALL' OR p.cabang = :branch)
ORDER BY p.id

    """)
    df_diag = run_df("""
        SELECT d.id, d.patient_id, p.full_name, d.hemo_type, d.severity, d.diagnosed_on, d.source
        FROM pwh.hemo_diagnoses d JOIN pwh.patients p ON p.id = d.patient_id
        ORDER BY d.patient_id, d.id
    """)
    df_inh = run_df("""
        SELECT i.id, i.patient_id, p.full_name, i.factor, i.titer_bu, i.measured_on, i.lab
        FROM pwh.hemo_inhibitors i JOIN pwh.patients p ON p.id = i.patient_id
        ORDER BY i.patient_id, i.measured_on NULLS LAST, i.id
    """)
    df_virus = run_df("""
        SELECT v.id, v.patient_id, p.full_name, v.test_type, v.result, v.tested_on, v.lab
        FROM pwh.virus_tests v JOIN pwh.patients p ON p.id = v.patient_id
        ORDER BY v.patient_id, v.tested_on NULLS LAST, v.id
    """)
    df_hospital = run_df("""
        SELECT th.id, th.patient_id, p.full_name, th.name_hospital, th.city_hospital, th.province_hospital,
               th.date_of_visit, th.doctor_in_charge, th.treatment_type, th.care_services, th.frequency, th.dose, th.product, th.merk
        FROM pwh.treatment_hospital th JOIN pwh.patients p ON p.id = th.patient_id
        ORDER BY th.patient_id, th.id
    """)
    df_death = run_df("""
        SELECT d.id, d.patient_id, p.full_name, d.cause_of_death, d.year_of_death
        FROM pwh.death d JOIN pwh.patients p ON p.id = d.patient_id
        ORDER BY d.patient_id, d.id
    """)
    df_contacts = run_df("""
        SELECT c.id, c.patient_id, p.full_name, c.relation, c.name, c.phone, c.is_primary
        FROM pwh.contacts c JOIN pwh.patients p ON p.id = c.patient_id
        ORDER BY c.patient_id, c.id
    """)
    df_summary = run_df("""SELECT * FROM pwh.patient_summary ORDER BY id""")
    
    # --- FIX: Hapus Timezone dari Datetime Columns ---
    # Excel (via xlsxwriter) tidak mendukung datetime yang 'timezone-aware' (misal: UTC)
    # Kita harus membuatnya 'naive' (menghapus info timezone) sebelum di-write.
    
    # 1. Kolom spesifik di df_patients (created_at)
    if 'created_at' in df_patients.columns and pd.api.types.is_datetime64_any_dtype(df_patients['created_at']):
        try:
            if df_patients['created_at'].dt.tz is not None:
                df_patients['created_at'] = df_patients['created_at'].dt.tz_localize(None)
        except AttributeError:
            pass # Lanjut jika kolom kosong/NaT

    # 2. Sanitasi umum untuk df_summary (karena kita tidak tahu kolomnya dari SELECT *)
    for col in df_summary.columns:
        if pd.api.types.is_datetime64_any_dtype(df_summary[col]):
            try:
                if df_summary[col].dt.tz is not None:
                    df_summary[col] = df_summary[col].dt.tz_localize(None)
            except AttributeError:
                pass # Lanjut jika kolom kosong/NaT
    # --- END FIX ---


    # Alias sheet agar ramah dilihat
    a_patients  = _alias_df(df_patients, ALIAS_PATIENTS)
    a_diag      = _alias_df(df_diag, ALIAS_DIAG)
    a_inh       = _alias_df(df_inh, ALIAS_INH)
    a_virus     = _alias_df(df_virus, ALIAS_VIRUS)
    a_hospital  = _alias_df(df_hospital, ALIAS_HOSPITAL)
    a_death     = _alias_df(df_death, ALIAS_DEATH)
    a_contacts  = _alias_df(df_contacts, ALIAS_CONTACTS)
    a_summary   = _alias_df(df_summary, ALIAS_SUMMARY)

    output = io.BytesIO()
    with ExcelWriter(output, engine="xlsxwriter", datetime_format="yyyy-mm-dd", date_format="yyyy-mm-dd") as writer:
        # --- Gunakan nama sheet Bahasa Indonesia untuk EXPORT ---
        a_patients.to_excel(writer, sheet_name="Pasien", index=False)
        a_diag.to_excel(writer, sheet_name="Diagnosa", index=False)
        a_inh.to_excel(writer, sheet_name="Inhibitor", index=False)
        a_virus.to_excel(writer, sheet_name="Virus Tes", index=False)
        a_hospital.to_excel(writer, sheet_name="RS Penangan", index=False)
        a_death.to_excel(writer, sheet_name="Kematian", index=False)
        a_contacts.to_excel(writer, sheet_name="Kontak", index=False)
        a_summary.to_excel(writer, sheet_name="Ringkasan Pasien", index=False)
        # --- END PERUBAHAN ---

        # Auto-fit kolom sederhana
        sheet_map = {
            "Pasien": a_patients,
            "Diagnosa": a_diag,
            "Inhibitor": a_inh,
            "Virus Tes": a_virus,
            "RS Penangan": a_hospital,
            "Kematian": a_death,
            "Kontak": a_contacts,
            "Ringkasan Pasien": a_summary,
        }
        for sheet, df_sheet in sheet_map.items():
            ws = writer.sheets[sheet]
            for col_idx, col_name in enumerate(df_sheet.columns):
                max_len = max((df_sheet[col_name].astype(str).map(len).max() if not df_sheet.empty else 0),
                                      len(str(col_name)))
                ws.set_column(col_idx, col_idx, min(max_len + 2, 50))
    return output.getvalue()


# ------------------------------------------------------------------------------
# Builder Template Excel (bulk) untuk insert data ke semua tabel
# ------------------------------------------------------------------------------
def build_bulk_template_bytes() -> bytes:
    # Lookup dari DB/enum (fallback ke nilai yang sudah ditentukan di program)
    blood_groups = BLOOD_GROUPS or ["A","B","AB","O"]
    rhesus = RHESUS or ["+","-"]
    genders = GENDERS or ["Laki-laki", "Perempuan"]
    education_levels = EDUCATION_LEVELS[1:] if EDUCATION_LEVELS and EDUCATION_LEVELS[0] == "" else EDUCATION_LEVELS
    hemo_types = HEMO_TYPES or ["A","B","vWD","Other"]
    severities = SEVERITY_CHOICES or ["Ringan","Sedang","Berat","Tidak diketahui"]
    inhibitor_factors = INHIB_FACTORS or ["FVIII","FIX"]
    virus_tests = VIRUS_TESTS or ["HBsAg","Anti-HCV","HIV"]
    test_results = TEST_RESULTS or ["positive","negative","indeterminate","unknown"]
    relations = RELATIONS or ["ayah","ibu","wali","pasien","lainnya"]
    occupations = fetch_occupations_list()
    
    # <-- TAMBAHAN BARU -->
    df_hmhi_branches = fetch_hmhi_branches() 
    hmhi_branches = [""] + df_hmhi_branches['cabang'].unique().tolist()
    # <-- END TAMBAHAN BARU -->

    treatment_types = ["", "Prophylaxis", "On Demand"] # Add blank option
    care_services = ["", "Rawat Jalan", "Rawat Inap"] # Add blank option
    products = ["", "Plasma (FFP)","Cryoprecipitate","Konsentrat (plasma derived)","Konsentrat (rekombinan)","Konsentrat (prolonged half life)","Prothrombin Complex","DDAVP","Emicizumab (Hemlibra)","Konsentrat Bypassing Agent"] # Add blank option
    primary_bools = ["TRUE", "FALSE"] # Define list for Kontak Primary

    # Gunakan nama sheet dan kolom Bahasa Indonesia
    template_sheets = {
        "Pasien": [
            ("Nama Lengkap", "text"), ("Tempat Lahir", "text"), ("Tanggal Lahir", "date"),
            ("NIK", "text"),
            ("Gol. Darah", ("list", "blood_groups")), ("Rhesus", ("list", "rhesus")), # Use named range
            ("Jenis Kelamin", ("list", "genders")), # Use named range
            ("Pekerjaan", ("list", "occupations")), # Use named range
            ("Pendidikan Terakhir", ("list", "education_levels")), # Use named range
            ("Alamat", "text"),
            ("No. Ponsel", "text"), ("Propinsi", "text"), ("Kabupaten/Kota", "text"),
            ("Kecamatan", "text"), ("Kelurahan/Desa", "text"),
            ("HMHI Cabang", ("list", "hmhi_cabang_vals")), ("Kota Cakupan Cabang", "text"), # <-- TAMBAHAN BARU
            ("Catatan", "text"),
        ],
        "Diagnosa": [
            ("patient_id", "int"), ("Nama Lengkap", "text"),
            ("Jenis Hemofilia", ("list", "hemo_types")), ("Kategori", ("list", "severities")), # Use named ranges
            ("Tgl Diagnosis", "date"), ("Sumber", "text"),
        ],
        "Inhibitor": [
            ("patient_id", "int"), ("Nama Lengkap", "text"),
            ("Faktor", ("list", "inhibitor_factors")), ("Titer (BU)", "number"), # Use named range
            ("Tgl Ukur", "date"), ("Lab", "text"),
        ],
        "Virus Tes": [
            ("patient_id", "int"), ("Nama Lengkap", "text"),
            ("Jenis Tes", ("list", "virus_tests")), ("Hasil", ("list", "test_results")), # Use named ranges
            ("Tgl Tes", "date"), ("Lab", "text"),
        ],
        "RS Penangan": [
            ("patient_id", "int"), ("Nama Lengkap", "text"),
            ("Nama RS", "text"), ("Kota RS", "text"), ("Provinsi RS", "text"),
            ("Tanggal Kunjungan", "date"), ("DPJP", "text"),
            ("Jenis Penanganan", ("list", "treatment_types_vals")), # Use new named range
            ("Layanan Rawat", ("list", "care_services_vals")),    # Use new named range
            ("Frekuensi", "text"), ("Dosis", "text"),
            ("Produk", ("list", "products_vals")),                # Use new named range
            ("Merk", "text"),
        ],
        "Kematian": [
            ("patient_id", "int"), ("Nama Lengkap", "text"),
            ("Penyebab Kematian", "text"), ("Tahun Kematian", "int"),
        ],
        "Kontak": [
            ("patient_id", "int"), ("Nama Lengkap", "text"),
            ("Relasi", ("list", "relations")), ("Nama Kontak", "text"), # Use named range
            ("No. Telp", "text"),
            ("Primary", ("list", "primary_vals")), # Use new named range for TRUE/FALSE
        ],
    }

    import io
    from pandas import ExcelWriter

    def _col_letter(n: int) -> str:
        s = ""
        while True:
            n, r = divmod(n, 26)
            s = chr(65 + r) + s
            if n == 0: break
            n -= 1
        return s

    bio = io.BytesIO()
    with ExcelWriter(bio, engine="xlsxwriter", datetime_format="yyyy-mm-dd", date_format="yyyy-mm-dd") as writer:
        wb = writer.book
        fmt_header = wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border": 1})
        fmt_date = wb.add_format({"num_format": "yyyy-mm-dd"})
        fmt_note = wb.add_format({"italic": True, "font_color": "#555555"})
        fmt_h = wb.add_format({"bold": True, "font_size": 14})

        ws_readme = wb.add_worksheet("README")
        readme = [("Template Bulk Insert PWH", fmt_h),("Cara pakai:", None),("1) Isi setiap sheet sesuai kolom.", None),("2) Gunakan format tanggal yyyy-mm-dd.", None),("3) Kolom dropdown sudah dibatasi ke pilihan valid.", None),("4) Jika mengisi pasien baru, sheet lain boleh pakai kolom 'Nama Lengkap' untuk mapping.", None),("5) Jika sudah tahu patient_id, isi langsung untuk akurasi.", None),("6) 'Primary' (Kontak) gunakan TRUE/FALSE.", None)]
        for r, (txt2, sty) in enumerate(readme): ws_readme.write(r, 0, txt2, sty)

        # Definisikan SEMUA list di sheet 'lookups'
        ws_lk = wb.add_worksheet("lookups")
        # Tambahkan list lokal ke look_cols
        look_cols = [
            ("blood_groups", blood_groups), ("rhesus", rhesus), ("genders", genders),
            ("hemo_types", hemo_types), ("severities", severities), ("education_levels", education_levels),
            ("inhibitor_factors", inhibitor_factors), ("virus_tests", virus_tests), ("test_results", test_results),
            ("relations", relations), ("occupations", occupations),
            ("treatment_types_vals", treatment_types), # Nama baru untuk named range
            ("care_services_vals", care_services),     # Nama baru
            ("products_vals", products),               # Nama baru
            ("primary_vals", primary_bools),           # Nama baru
            ("hmhi_cabang_vals", hmhi_branches)        # <-- TAMBAHAN BARU
        ]
        for j, (name, items) in enumerate(look_cols):
            ws_lk.write(0, j, name, fmt_header)
            # Pastikan items adalah list sebelum iterasi
            if isinstance(items, list):
                for i, v in enumerate(items, start=1):
                    ws_lk.write(i, j, v)
                col_letter = _col_letter(j)
                last_row = len(items) + 1
                # Define name globaly
                wb.define_name(name, f"=lookups!${col_letter}$2:${col_letter}${last_row}")
            else:
                st.warning(f"Lookup '{name}' expected a list, but got {type(items)}. Skipping definition.") # Tambah warning jika bukan list

        max_rows = 1000 # Excel row limit for validation range (adjust if needed)
        for sheet, cols in template_sheets.items():
            ws = wb.add_worksheet(sheet)
            ws.freeze_panes(1, 0)
            for idx, (col_name, col_type) in enumerate(cols):
                ws.write(0, idx, col_name, fmt_header)
                ws.set_column(idx, idx, max(15, len(col_name) + 2))

                if isinstance(col_type, tuple) and col_type[0] == "list":
                    # Semua list sekarang pakai named range dari 'lookups'
                    named_range = col_type[1] # Ini *harus* string nama range (e.g., "blood_groups")
                    if isinstance(named_range, str):
                        source = f"={named_range}"
                        ws.data_validation(1, idx, max_rows, idx, {"validate": "list", "source": source})
                    else:
                         st.warning(f"Invalid list source for '{col_name}' in sheet '{sheet}': Expected named range string.") # Warning jika format salah
                    # Hapus logika pembuatan range lokal

                elif col_type == "date":
                    ws.set_column(idx, idx, 14, fmt_date)
                # Tambah kriteria untuk validasi angka
                elif col_type == "int":
                    ws.data_validation(1, idx, max_rows, idx, {
                        "validate": "integer",
                        "criteria": ">=",
                        "value": 0, # Tahun kematian minimal 0 (atau sesuaikan jika perlu)
                        "input_title": "Angka Bulat",
                        "input_message": "Masukkan angka bulat positif.",
                        "error_title": "Input Salah",
                        "error_message": "Hanya angka bulat positif yang diizinkan."
                    })
                elif col_type == "number":
                     ws.data_validation(1, idx, max_rows, idx, {
                        "validate": "decimal",
                        "criteria": ">=",
                        "value": 0.0, # Titer BU minimal 0.0
                        "input_title": "Angka Desimal",
                        "input_message": "Masukkan angka desimal positif.",
                        "error_title": "Input Salah",
                        "error_message": "Hanya angka desimal positif yang diizinkan."
                    })

            ws.write(max_rows + 2, 0, "Catatan: baris kosong akan diabaikan saat import.", fmt_note)
    return bio.getvalue()

st.title("🩸 Form Input Penyandang Hemofilia")

# ------------------------------------------------------------------------------
# KONEKSI DATABASE
# ------------------------------------------------------------------------------
def _resolve_db_url() -> str:
    try:
        sec = st.secrets.get("DATABASE_URL", "")
        if sec: return sec
    except Exception: pass
    env = os.environ.get("DATABASE_URL") or os.getenv("DATABASE_URL")
    if env: return env
    st.error('DATABASE_URL tidak ditemukan. Isi `.streamlit/secrets.toml` dengan format:\n''DATABASE_URL = "postgresql+psycopg2://USER:PASSWORD@127.0.0.1:5432/pwhdb"')
    st.stop()

@st.cache_resource(show_spinner=False)
def get_engine(dsn: str) -> Engine:
    return create_engine(dsn, pool_pre_ping=True)

DSN = _resolve_db_url()
try:
    engine = get_engine(DSN)
# ============================================================
# FILTER CABANG LOGIN
# ============================================================
if "user_branch" not in st.session_state:
    st.error("Anda belum login melalui halaman utama.")
    st.stop()
USER_BRANCH = st.session_state.get("user_branch", "ALL")

    with engine.connect() as _c:
        _c.exec_driver_sql("SELECT 1")
except Exception as e:
    st.error(f"Gagal konek ke Postgres: {e}")
    st.stop()

# ------------------------------------------------------------------------------
# Helper eksekusi
# ------------------------------------------------------------------------------
def run_df(query: str, params: dict | None = None) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(text(query), conn, params=params or {})

def run_exec(sql: str, params: dict | None = None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

# ------------------------------------------------------------------------------
# Ambil data referensi dari DB
# ------------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def fetch_enum_vals(enum_typename: str) -> list[str]:
    q = "SELECT e.enumlabel FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = 'pwh' AND t.typname = :typ ORDER BY e.enumsortorder;"
    try:
        df = run_df(q, {"typ": enum_typename})
        return df["enumlabel"].tolist()
    except Exception:
        return []

@st.cache_data(show_spinner=False)
def fetch_occupations_list() -> list[str]:
    try:
        df = run_df("SELECT name FROM pwh.occupations ORDER BY name;")
        if not df.empty: return [""] + df["name"].dropna().astype(str).tolist()
    except Exception: pass
    return ["","Tidak bekerja","Nelayan","Petani","PNS/TNI/Polri","Karyawan Swasta","Wiraswasta","Pensiunan"]

@st.cache_data(show_spinner="Memuat data wilayah...")
def fetch_all_wilayah_details() -> pd.DataFrame:
    """Mengambil data lengkap wilayah (Kelurahan, Kecamatan, Kota, Propinsi) dari DB."""
    try:
        q = """
        SELECT
            kel.nama AS village_name,
            kec.nama AS district_name,
            kota.nama AS city_name,
            prov.nama AS province_name,
            CONCAT_WS(' - ', kel.nama, kec.nama, kota.nama, prov.nama) AS full_display
        FROM
            public.wilayah AS kel
        JOIN
            public.wilayah AS kec ON kec.kode = LEFT(kel.kode, 8) -- e.g., 32.75.01
        JOIN
            public.wilayah AS kota ON kota.kode = LEFT(kel.kode, 5) -- e.g., 32.75
        JOIN
            public.wilayah AS prov ON prov.kode = LEFT(kel.kode, 2) -- e.g., 32
        WHERE
            LENGTH(kel.kode) = 13  -- Standard code length for village
        ORDER BY
            full_display;
        """
        df = run_df(q)
        if not df.empty:
            return df
    except Exception as e:
        st.warning(f"Gagal memuat data wilayah: {e}")
        pass
    # Fallback data jika query gagal
    return pd.DataFrame({
        'village_name': ['MUSTIKA JAYA'],
        'district_name': ['MUSTIKA JAYA'],
        'city_name': ['KOTA BEKASI'],
        'province_name': ['JAWA BARAT'],
        'full_display': ['MUSTIKA JAYA - MUSTIKA JAYA - KOTA BEKASI - JAWA BARAT']
    })

# --- FUNGSI BARU UNTUK CABANG HMHI ---
@st.cache_data(show_spinner="Memuat data cabang HMHI...")
def fetch_hmhi_branches() -> pd.DataFrame:
    """Mengambil data cabang HMHI dan kota cakupannya."""
    try:
        # Menggunakan schema pwh.hmhi_cabang sesuai permintaan
        q = "SELECT DISTINCT cabang, kota_cakupan FROM pwh.hmhi_cabang WHERE cabang IS NOT NULL ORDER BY cabang;"
        df = run_df(q)
        if not df.empty:
            return df
    except Exception as e:
        st.warning(f"Gagal memuat data Cabang HMHI: {e}")
        pass
    # Fallback data jika query gagal
    return pd.DataFrame({
        'cabang': ['BEKASI'],
        'kota_cakupan': ['KOTA BEKASI, KAB. BEKASI']
    })
# --- END FUNGSI BARU ---

@st.cache_data(show_spinner=False)
def fetch_hospitals() -> list[str]:
    try:
        q = "SELECT CONCAT_WS(' - ', nama_rs, kota, provinsi) as hospital_display FROM public.rumah_sakit ORDER BY hospital_display;"
        df = run_df(q)
        if not df.empty:
            return [""] + df["hospital_display"].tolist()
    except Exception as e:
        st.warning(f"Gagal mengambil daftar RS: {e}")
        pass
    return ["", "RSUPN Dr. Cipto Mangunkusumo - Jakarta Pusat - DKI Jakarta", "RS Kanker Dharmais - Jakarta Barat - DKI Jakarta"]

# --- FUNGSI BARU UNTUK MENGAMBIL DATA PASIEN ---
@st.cache_data(show_spinner="Memuat daftar pasien...")
def get_all_patients_for_selection():
    """Mengambil daftar pasien dari DB untuk digunakan di selectbox."""
    return run_df("SELECT id, full_name FROM pwh.patients ORDER BY full_name;")

# ------------------------------------------------------------------------------
# Definisi Pilihan Statis & Dinamis
# ------------------------------------------------------------------------------
BLOOD_GROUPS = [""] + (fetch_enum_vals("blood_group_enum") or ["A","B","AB","O"])
RHESUS       = [""] + (fetch_enum_vals("rhesus_enum")        or ["+","-"])
GENDERS      = ["", "Laki-laki", "Perempuan"]
EDUCATION_LEVELS = [""] + (fetch_enum_vals("education_enum") or ["Tidak sekolah", "SD", "SMP", "SMA/SMK", "Diploma", "S1", "S2", "S3"])
HEMO_TYPES   = fetch_enum_vals("hemo_type_enum")      or ["A","B","vWD","Other"]
SEVERITIES   = fetch_enum_vals("severity_enum")         or ["Ringan","Sedang","Berat","Tidak diketahui"]
INHIB_FACTORS= fetch_enum_vals("inhibitor_factor_enum") or ["FVIII","FIX"]
VIRUS_TESTS  = fetch_enum_vals("virus_test_enum")       or ["HBsAg","Anti-HCV","HIV"]
TEST_RESULTS = fetch_enum_vals("test_result_enum")    or ["positive","negative","indeterminate","unknown"]
RELATIONS    = fetch_enum_vals("relation_enum")         or ["ayah","ibu","wali","pasien","lainnya"]
PREFERRED_SEVERITY_ORDER = ["Ringan", "Sedang", "Berat", "Tidak diketahui"]
SEVERITY_CHOICES = PREFERRED_SEVERITY_ORDER if all(x in SEVERITIES for x in PREFERRED_SEVERITY_ORDER) else SEVERITIES
TREATMENT_TYPES = ["", "Prophylaxis", "On Demand"]
CARE_SERVICES = ["", "Rawat Jalan", "Rawat Inap"]
PRODUCTS = ["", "Plasma (FFP)","Cryoprecipitate","Konsentrat (plasma derived)","Konsentrat (rekombinan)","Konsentrat (prolonged half life)","Prothrombin Complex","DDAVP","Emicizumab (Hemlibra)","Konsentrat Bypassing Agent"]

def _severity_default_index(choices: list[str]) -> int:
    try: return choices.index("Tidak diketahui")
    except ValueError: return 0

# ------------------------------------------------------------------------------
# Alias kolom (header) untuk tampilan
# ------------------------------------------------------------------------------
ALIAS_PATIENTS = {"full_name": "Nama Lengkap","birth_place": "Tempat Lahir","birth_date": "Tanggal Lahir", "nik": "NIK", "age_years": "Umur (tahun)", "blood_group": "Gol. Darah","rhesus": "Rhesus", "gender": "Jenis Kelamin", "occupation": "Pekerjaan", "education": "Pendidikan Terakhir", "address": "Alamat", "village": "Kelurahan/Desa", "district": "Kecamatan", "phone": "No. Ponsel","province": "Propinsi","city": "Kabupaten/Kota", "cabang": "HMHI Cabang", "kota_cakupan": "Kota Cakupan Cabang", "note": "Catatan", "created_at": "Dibuat"}
ALIAS_DIAG = {"full_name": "Nama Lengkap","hemo_type": "Jenis Hemofilia","severity": "Kategori","diagnosed_on": "Tgl Diagnosis","source": "Sumber"}
ALIAS_INH = {"full_name": "Nama Lengkap","factor": "Faktor","titer_bu": "Titer (BU)","measured_on": "Tgl Ukur","lab": "Lab"}
ALIAS_VIRUS = {"full_name": "Nama Lengkap","test_type": "Jenis Tes","result": "Hasil","tested_on": "Tgl Tes","lab": "Lab"}
ALIAS_HOSPITAL = {"full_name": "Nama Lengkap","name_hospital": "Nama RS","city_hospital": "Kota RS","province_hospital": "Provinsi RS", "date_of_visit": "Tanggal Kunjungan", "doctor_in_charge": "DPJP", "treatment_type": "Jenis Penanganan","care_services": "Layanan Rawat","frequency": "Frekuensi","dose": "Dosis","product": "Produk","merk": "Merk"}
ALIAS_DEATH = {"full_name": "Nama Lengkap", "cause_of_death": "Penyebab Kematian", "year_of_death": "Tahun Kematian"}
ALIAS_CONTACTS = {"full_name": "Nama Lengkap","relation": "Relasi","name": "Nama Kontak","phone": "No. Telp","is_primary": "Primary"}
ALIAS_SUMMARY = {"Nama Lengkap": "Nama Lengkap","Lahir: Tempat": "Tempat Lahir","Lahir: Tanggal": "Tanggal Lahir","Gol. Darah": "Gol. Darah","Rhesus": "Rhesus","Pekerjaan": "Pekerjaan","vWD": "vWD","Kategori Hemofilia A": "Kategori A","Kategori Hemofilia B": "Kategori B","Inhibitor FVIII (BU)": "FVIII (BU)","Inhibitor FIX (BU)": "FIX (BU)","HBsAg": "HBsAg","Anti HCV": "Anti-HCV","HIV": "HIV","Alamat": "Alamat","No. Telp": "No. Telp","Org Tua: Ayah": "Ayah","Org Tua: Ibu": "Ibu","Umur (tahun)": "Umur"}

def _alias_df(df: pd.DataFrame, alias_map: dict) -> pd.DataFrame:
    if df is None or df.empty: return df
    return df.rename(columns={c: alias_map.get(c, c) for c in df.columns})

# ------------------------------------------------------------------------------
# Helper Functions (INSERT, UPDATE)
# ------------------------------------------------------------------------------
def insert_patient(payload: dict) -> int:
    sql = "INSERT INTO pwh.patients (full_name, birth_place, birth_date, nik, blood_group, rhesus, gender, occupation, education, address, phone, province, city, note, village, district, cabang, kota_cakupan) VALUES (:full_name, :birth_place, :birth_date, :nik, :blood_group, :rhesus, :gender, :occupation, :education, :address, :phone, :province, :city, :note, :village, :district, :cabang, :kota_cakupan) RETURNING id;"
    with engine.begin() as conn:
        return int(conn.execute(text(sql), payload).scalar())

def update_patient(id: int, payload: dict):
    payload['id'] = id
    sql = "UPDATE pwh.patients SET full_name=:full_name, birth_place=:birth_place, birth_date=:birth_date, nik=:nik, blood_group=:blood_group, rhesus=:rhesus, gender=:gender, occupation=:occupation, education=:education, address=:address, phone=:phone, province=:province, city=:city, note=:note, village=:village, district=:district, cabang=:cabang, kota_cakupan=:kota_cakupan WHERE id=:id;"
    run_exec(sql, payload)

def insert_diagnosis(patient_id: int, hemo_type: str, severity: str, diagnosed_on: date | None, source: str | None):
    sql = "INSERT INTO pwh.hemo_diagnoses (patient_id, hemo_type, severity, diagnosed_on, source) VALUES (:pid, :hemo_type, :severity, :diagnosed_on, :source) ON CONFLICT (patient_id, hemo_type) DO UPDATE SET severity = EXCLUDED.severity, diagnosed_on= COALESCE(EXCLUDED.diagnosed_on, pwh.hemo_diagnoses.diagnosed_on), source = COALESCE(EXCLUDED.source, pwh.hemo_diagnoses.source);"
    run_exec(sql, {"pid": patient_id, "hemo_type": hemo_type, "severity": severity, "diagnosed_on": diagnosed_on, "source": (source or "").strip() or None})

def update_diagnosis(id: int, payload: dict):
    payload['id'] = id
    sql = "UPDATE pwh.hemo_diagnoses SET hemo_type=:hemo_type, severity=:severity, diagnosed_on=:diagnosed_on, source=:source WHERE id=:id;"
    run_exec(sql, payload)

def insert_inhibitor(patient_id: int, factor: str, titer_bu: float | None, measured_on: date | None, lab: str | None):
    sql = "INSERT INTO pwh.hemo_inhibitors (patient_id, factor, titer_bu, measured_on, lab) VALUES (:pid, :factor, :titer_bu, :measured_on, :lab);"
    run_exec(sql, {"pid": patient_id, "factor": factor, "titer_bu": titer_bu, "measured_on": measured_on, "lab": (lab or "").strip() or None})

def update_inhibitor(id: int, payload: dict):
    payload['id'] = id
    sql = "UPDATE pwh.hemo_inhibitors SET factor=:factor, titer_bu=:titer_bu, measured_on=:measured_on, lab=:lab WHERE id=:id;"
    run_exec(sql, payload)

def insert_virus_test(patient_id: int, test_type: str, result: str, tested_on: date | None, lab: str | None):
    sql = "INSERT INTO pwh.virus_tests (patient_id, test_type, result, tested_on, lab) VALUES (:pid, :test_type, :result, :tested_on, :lab) ON CONFLICT (patient_id, test_type, tested_on) DO NOTHING;"
    run_exec(sql, {"pid": patient_id, "test_type": test_type, "result": result, "tested_on": tested_on, "lab": (lab or "").strip() or None})

def update_virus_test(id: int, payload: dict):
    payload['id'] = id
    sql = "UPDATE pwh.virus_tests SET test_type=:test_type, result=:result, tested_on=:tested_on, lab=:lab WHERE id=:id;"
    run_exec(sql, payload)

def insert_treatment_hospital(payload: dict):
    sql = """
        INSERT INTO pwh.treatment_hospital 
        (patient_id, name_hospital, city_hospital, province_hospital, date_of_visit, doctor_in_charge, treatment_type, care_services, frequency, dose, product, merk) 
        VALUES (:patient_id, :name_hospital, :city_hospital, :province_hospital, :date_of_visit, :doctor_in_charge, :treatment_type, :care_services, :frequency, :dose, :product, :merk);
    """
    run_exec(sql, payload)

def update_treatment_hospital(id: int, payload: dict):
    payload['id'] = id
    sql = """
        UPDATE pwh.treatment_hospital SET 
        name_hospital=:name_hospital, city_hospital=:city_hospital, province_hospital=:province_hospital, 
        date_of_visit=:date_of_visit, doctor_in_charge=:doctor_in_charge,
        treatment_type=:treatment_type, care_services=:care_services, frequency=:frequency, dose=:dose, product=:product, merk=:merk 
        WHERE id=:id;
    """
    run_exec(sql, payload)

def insert_death_record(payload: dict):
    # Hanya boleh ada 1 record kematian per pasien
    sql = "INSERT INTO pwh.death (patient_id, cause_of_death, year_of_death) VALUES (:patient_id, :cause_of_death, :year_of_death) ON CONFLICT (patient_id) DO UPDATE SET cause_of_death = EXCLUDED.cause_of_death, year_of_death = EXCLUDED.year_of_death;"
    run_exec(sql, payload)

def update_death_record(id: int, payload: dict):
    payload['id'] = id
    sql = "UPDATE pwh.death SET cause_of_death=:cause_of_death, year_of_death=:year_of_death WHERE id=:id;"
    run_exec(sql, payload)

def insert_contact(patient_id: int, relation: str, name: str, phone: str | None, is_primary: bool):
    sql = "INSERT INTO pwh.contacts (patient_id, relation, name, phone, is_primary) VALUES (:pid, :relation, :name, :phone, :is_primary);"
    run_exec(sql, {"pid": patient_id, "relation": relation, "name": name.strip(), "phone": (phone or "").strip() or None, "is_primary": bool(is_primary)})

def update_contact(id: int, payload: dict):
    payload['id'] = id
    sql = "UPDATE pwh.contacts SET relation=:relation, name=:name, phone=:phone, is_primary=:is_primary WHERE id=:id;"
    run_exec(sql, payload)

# ------------------------------------------------------------------------------
# Import Bulk Excel
# ------------------------------------------------------------------------------
def _to_bool(x):
    s = str(x).strip().lower()
    return s in ("true", "1", "yes", "ya", "y")

def _to_date(x):
    if x is None: return None
    try:
        dt = pd.to_datetime(x)
        return None if pd.isna(dt) else dt.date()
    except Exception: return None

def _safe_str(x):
    if pd.isna(x): return None
    s = str(x).strip()
    return s if s else None

def import_bulk_excel(file) -> dict:
    xl = pd.ExcelFile(file)
    sheets = {name.lower(): name for name in xl.sheet_names}

    # Modifikasi df_or_empty untuk menerima rename_map
    def df_or_empty(key, cols, rename_map=None):
        if key in sheets:
            df = xl.parse(sheets[key])
            df.columns = [c.strip() for c in df.columns]
            if rename_map:
                df = df.rename(columns=rename_map) # Rename dari B. Indo -> B. Inggris (internal)
            for c in cols:
                if c not in df.columns: df[c] = None
            return df.fillna(value=None).dropna(how="all")
        return pd.DataFrame(columns=cols)

    # Definisikan Peta Pembalikan (Header B. Indo -> Nama Internal)
    MAP_PAT = {
        "Nama Lengkap": "full_name", "Tempat Lahir": "birth_place", "Tanggal Lahir": "birth_date", "NIK": "nik",
        "Gol. Darah": "blood_group", "Rhesus": "rhesus", "Jenis Kelamin": "gender", "Pekerjaan": "occupation",
        "Pendidikan Terakhir": "education", "Alamat": "address", "No. Ponsel": "phone", "Propinsi": "province",
        "Kabupaten/Kota": "city", "Kecamatan": "district", "Kelurahan/Desa": "village",
        "HMHI Cabang": "cabang", "Kota Cakupan Cabang": "kota_cakupan", # <-- TAMBAHAN BARU
        "Catatan": "note"
    }
    MAP_DIAG = {
        "Nama Lengkap": "full_name", "Jenis Hemofilia": "hemo_type", "Kategori": "severity",
        "Tgl Diagnosis": "diagnosed_on", "Sumber": "source", "patient_id": "patient_id"
    }
    MAP_INH = {
        "Nama Lengkap": "full_name", "Faktor": "factor", "Titer (BU)": "titer_bu",
        "Tgl Ukur": "measured_on", "Lab": "lab", "patient_id": "patient_id"
    }
    MAP_VIRUS = {
        "Nama Lengkap": "full_name", "Jenis Tes": "test_type", "Hasil": "result",
        "Tgl Tes": "tested_on", "Lab": "lab", "patient_id": "patient_id"
    }
    MAP_HOSP = {
        "Nama Lengkap": "full_name", "Nama RS": "name_hospital", "Kota RS": "city_hospital", "Provinsi RS": "province_hospital",
        "Tanggal Kunjungan": "date_of_visit", "DPJP": "doctor_in_charge", "Jenis Penanganan": "treatment_type",
        "Layanan Rawat": "care_services", "Frekuensi": "frequency", "Dosis": "dose", "Produk": "product",
        "Merk": "merk", "patient_id": "patient_id"
    }
    MAP_DEATH = {
        "Nama Lengkap": "full_name", "Penyebab Kematian": "cause_of_death", "Tahun Kematian": "year_of_death",
        "patient_id": "patient_id"
    }
    MAP_CONTACT = {
        "Nama Lengkap": "full_name", "Relasi": "relation", "Nama Kontak": "name", "No. Telp": "phone",
        "Primary": "is_primary", "patient_id": "patient_id"
    }

    # Gunakan nama internal (B. Inggris) untuk pat_cols
    pat_cols_internal = ["full_name","birth_place","birth_date","nik","blood_group","rhesus","gender","occupation", "education", "address","phone","province","city","note", "village", "district", "cabang", "kota_cakupan"]
    # Baca sheet "pasien" (lowercase dari "Pasien") dan terapkan map
    df_pat = df_or_empty("pasien", pat_cols_internal, MAP_PAT)
    inserted_patients = []
    
    for _, r in df_pat[df_pat["full_name"].notna()].iterrows():
        payload = {
            "full_name": _safe_str(r.get("full_name")), "birth_place": _safe_str(r.get("birth_place")),
            "birth_date": _to_date(r.get("birth_date")), "nik": _safe_str(r.get("nik")),
            "blood_group": _safe_str(r.get("blood_group")),
            "rhesus": _safe_str(r.get("rhesus")), "gender": _safe_str(r.get("gender")),
            "occupation": _safe_str(r.get("occupation")), "education": _safe_str(r.get("education")),
            "address": _safe_str(r.get("address")), 
            "phone": _safe_str(r.get("phone")), "province": _safe_str(r.get("province")), 
            "city": _safe_str(r.get("city")), "note": _safe_str(r.get("note")),
            "village": _safe_str(r.get("village")), "district": _safe_str(r.get("district")),
            "cabang": _safe_str(r.get("cabang")), "kota_cakupan": _safe_str(r.get("kota_cakupan")) # <-- TAMBAHAN BARU
        }
        pid = insert_patient(payload)
        inserted_patients.append((pid, payload["full_name"]))

    map_new = {name.lower(): pid for pid, name in inserted_patients}
    df_all_pat = run_df("SELECT id, full_name FROM pwh.patients")
    map_db = {str(n).lower(): int(i) for i, n in zip(df_all_pat["id"], df_all_pat["full_name"])}

    def _resolve_pid(row):
        pid = row.get("patient_id")
        if pd.notna(pid) and str(pid).strip():
            try: return int(pid)
            except Exception: pass
        nm = _safe_str(row.get("full_name")).lower() # 'full_name' sudah di-rename dari 'Nama Lengkap'
        if nm: return map_new.get(nm) or map_db.get(nm)
        return None

    # Gunakan nama B. Indo di hasil dan konfigurasi
    results = {"Pasien": len(inserted_patients)}
    sheet_configs = {
        "Diagnosa": ("diagnosa", # nama sheet lowercase
                       ["patient_id","full_name","hemo_type","severity","diagnosed_on","source"], # nama kolom internal
                       MAP_DIAG, # Peta pembalikan
                       lambda r, pid: insert_diagnosis(pid, _safe_str(r.get("hemo_type")), _safe_str(r.get("severity")), _to_date(r.get("diagnosed_on")), _safe_str(r.get("source")))),
        "Inhibitor": ("inhibitor",
                        ["patient_id","full_name","factor","titer_bu","measured_on","lab"],
                        MAP_INH,
                        lambda r, pid: insert_inhibitor(pid, _safe_str(r.get("factor")), pd.to_numeric(r.get("titer_bu"), errors='coerce'), _to_date(r.get("measured_on")), _safe_str(r.get("lab")))),
        "Virus Tes": ("virus tes",
                        ["patient_id","full_name","test_type","result","tested_on","lab"],
                        MAP_VIRUS,
                        lambda r, pid: insert_virus_test(pid, _safe_str(r.get("test_type")), _safe_str(r.get("result")), _to_date(r.get("tested_on")), _safe_str(r.get("lab")))),
        "RS Penangan": ("rs penangan",
                          ["patient_id", "full_name", "name_hospital", "city_hospital", "province_hospital", "date_of_visit", "doctor_in_charge", "treatment_type", "care_services", "frequency", "dose", "product", "merk"],
                          MAP_HOSP,
                          lambda r, pid: insert_treatment_hospital({
                              "patient_id": pid, "name_hospital": _safe_str(r.get("name_hospital")), "city_hospital": _safe_str(r.get("city_hospital")), "province_hospital": _safe_str(r.get("province_hospital")),
                              "date_of_visit": _to_date(r.get("date_of_visit")), "doctor_in_charge": _safe_str(r.get("doctor_in_charge")),
                              "treatment_type": _safe_str(r.get("treatment_type")), "care_services": _safe_str(r.get("care_services")), "frequency": _safe_str(r.get("frequency")), "dose": _safe_str(r.get("dose")),
                              "product": _safe_str(r.get("product")), "merk": _safe_str(r.get("merk"))
                          })
                         ),
        "Kematian": ("kematian",
                       ["patient_id", "full_name", "cause_of_death", "year_of_death"],
                       MAP_DEATH,
                       lambda r, pid: insert_death_record({
                           "patient_id": pid,
                           "cause_of_death": _safe_str(r.get("cause_of_death")),
                           "year_of_death": pd.to_numeric(r.get("year_of_death"), errors='coerce')
                       })
                      ),
        "Kontak": ("kontak",
                       ["patient_id","full_name","relation","name","phone","is_primary"],
                       MAP_CONTACT,
                       lambda r, pid: insert_contact(pid, _safe_str(r.get("relation")), _safe_str(r.get("name")), _safe_str(r.get("phone")), _to_bool(r.get("is_primary")))),
    }

    for key, (sheet_name_lowercase, internal_cols, reverse_map, insert_func) in sheet_configs.items():
        df_sheet = df_or_empty(sheet_name_lowercase, internal_cols, reverse_map)
        count = 0
        for _, r in df_sheet.iterrows():
            pid = _resolve_pid(r)
            if pid:
                try:
                    insert_func(r, pid)
                    count += 1
                except Exception as e:
                    # Optional: Log the error for debugging
                    # st.warning(f"Gagal impor baris di sheet {key} untuk PID {pid}: {e}")
                    pass
        results[key] = count # 'key' sekarang "Diagnosa", "Inhibitor", dst.

    return results

# ------------------------------------------------------------------------------
# Fungsi Helper untuk UI
# ------------------------------------------------------------------------------
def get_safe_index(options, value):
    """Mencari index dari value di options, return 0 jika tidak ada."""
    try:
        return options.index(value)
    except (ValueError, TypeError):
        return 0

def clear_session_state(prefix):
    """Menghapus semua key di session state yang berawalan `prefix`."""
    keys_to_del = [k for k in st.session_state if k.startswith(prefix)]
    for k in keys_to_del:
        del st.session_state[k]

def auto_pick_latest_for_edit(df, state_key: str, table_fullname: str,
                                id_col: str = "id", order_cols: list[str] | None = None):
    # Abaikan jika tidak ada data
    if df is None or getattr(df, "empty", True):
        return
    # Jangan override jika sudah ada record sedang diedit
    if st.session_state.get(state_key):
        return
    # Tentukan kolom urut (DESC)
    order_cols = order_cols or [id_col]
    valid_cols = [c for c in order_cols if c in df.columns] or [id_col]
    df_sorted = df.sort_values(valid_cols, ascending=[False] * len(valid_cols))
    pick_id = int(df_sorted.iloc[0][id_col])
    # Set state edit seperti pola tab Pasien
    set_editing_state(state_key, pick_id, table_fullname)

def set_editing_state(state_key, data_id, table_name):
    """Memuat data untuk diedit ke dalam session state."""
    if not data_id:
        return
    query = f"SELECT * FROM {table_name} WHERE id = {int(data_id)};"
    data = run_df(query)
    if not data.empty:
        st.session_state[state_key] = data.to_dict('records')[0]
    else:
        st.error(f"ID {data_id} tidak ditemukan di tabel {table_name}.")
        if state_key in st.session_state:
            del st.session_state[state_key]

# ------------------------------------------------------------------------------
# TABS (Form Input)
# ------------------------------------------------------------------------------
tab_pat, tab_diag, tab_inh, tab_virus, tab_hospital, tab_death, tab_contacts, tab_view, tab_export = st.tabs(
    ["🧑‍⚕️ Pasien", "🧬 Diagnosis", "🧪 Inhibitor", "🧫 Virus Tests", "🏥 Rumah Sakit Penangan", "⚰️ Kematian", "👨‍👩‍👧 Kontak", "📄 Ringkasan", "⬇️ Export"]
)

# Patient
with tab_pat:
    st.subheader("🧑‍⚕️ Tambah Data Pasien")

    pat_data = st.session_state.get('patient_to_edit', {})

    if pat_data:
        st.info(f"Mode Edit untuk Pasien: {pat_data.get('full_name')} (ID: {pat_data.get('id')})")
        if st.button("❌ Batal Edit", key="cancel_pat_edit"):
            clear_session_state('patient_to_edit')
            clear_session_state('patient_matches') # Hapus juga hasil pencarian
            st.rerun()

    df_wilayah_all = fetch_all_wilayah_details()
    df_hmhi = fetch_hmhi_branches() # <-- PANGGIL FUNGSI BARU
    occupations_list = fetch_occupations_list()

    with st.container(border=True):
        full_name = st.text_input("Nama Lengkap*", value=pat_data.get('full_name', ''))

        c1, c2, c3 = st.columns(3)
        with c1: birth_place = st.text_input("Tempat Lahir", value=pat_data.get('birth_place', ''))
        with c2:
            birth_date_val = pd.to_datetime(pat_data.get('birth_date')).date() if pd.notna(pat_data.get('birth_date')) else None
            birth_date = st.date_input("Tanggal Lahir", value=birth_date_val, format="YYYY-MM-DD", min_value=date(1920, 1, 1), max_value=date.today())
        with c3:
            nik = st.text_input("NIK*", value=pat_data.get('nik', ''), max_chars=16)

        c_pekerjaan, c_pendidikan = st.columns(2)
        with c_pekerjaan:
            occupation_idx = get_safe_index(occupations_list, pat_data.get('occupation'))
            occupation = st.selectbox("Pekerjaan", occupations_list, index=occupation_idx)
        with c_pendidikan:
            education_idx = get_safe_index(EDUCATION_LEVELS, pat_data.get('education'))
            education = st.selectbox("Pendidikan Terakhir", EDUCATION_LEVELS, index=education_idx)

        c5, c6, c7, c8 = st.columns(4)
        with c5:
            blood_group_idx = get_safe_index(BLOOD_GROUPS, pat_data.get('blood_group'))
            blood_group = st.selectbox("Golongan Darah", BLOOD_GROUPS, index=blood_group_idx)
        with c6:
            rhesus_idx = get_safe_index(RHESUS, pat_data.get('rhesus'))
            rhesus = st.selectbox("Rhesus", RHESUS, index=rhesus_idx)
        with c7:
            gender_idx = get_safe_index(GENDERS, pat_data.get('gender'))
            gender = st.selectbox("Jenis Kelamin", GENDERS, index=gender_idx)
        with c8:
            phone = st.text_input("No. Ponsel", max_chars=50, value=pat_data.get('phone', ''))

        address = st.text_area("Alamat", value=pat_data.get('address', ''))

        # --- START: Logika Wilayah Autofill ---
        village_list = [""] + df_wilayah_all['full_display'].tolist()
        village_name, district_name, city_name, province_name = "", "", "", ""
        village_display_val = ""
        if pat_data:
            v = pat_data.get('village')
            d = pat_data.get('district')
            c = pat_data.get('city')
            p = pat_data.get('province')
            if v and d and c and p:
                village_display_val = f"{v} - {d} - {c} - {p}"
            if not village_display_val:
                village_name = v or ""
                district_name = d or ""
                city_name = c or ""
                province_name = p or ""

        village_idx = get_safe_index(village_list, village_display_val)

        # --- PERUBAHAN LAYOUT WILAYAH (START) ---
        # Baris 1: Kelurahan (Input) dan Kecamatan (Display)
        col_vil, col_dis = st.columns(2)
        with col_vil:
            # Ini adalah SATU-SATUNYA input Kelurahan
            selected_village_display = st.selectbox(
                "Kelurahan/Desa (pilih ini untuk autofill)", # Label dipersingkat
                village_list,
                index=village_idx
            )
        
        # Logika autofill dipindahkan ke sini, *setelah* selectbox
        if selected_village_display:
            match = df_wilayah_all[df_wilayah_all['full_display'] == selected_village_display]
            if not match.empty:
                village_name = match.iloc[0]['village_name']
                district_name = match.iloc[0]['district_name']
                city_name = match.iloc[0]['city_name']
                province_name = match.iloc[0]['province_name']

        with col_dis:
            # Tampilkan Kecamatan
            st.text_input("Kecamatan (otomatis)", value=district_name, disabled=True)
        
        # Baris 2: Kota/Kab dan Propinsi (Display)
        col_city, col_prov = st.columns(2)
        with col_city:
            st.text_input("Kabupaten/Kota (otomatis)", value=city_name, disabled=True)
        with col_prov:
            st.text_input("Propinsi (otomatis)", value=province_name, disabled=True)
        # --- PERUBAHAN LAYOUT WILAYAH (END) ---
        # --- END: Logika Wilayah Autofill ---

        
        # --- START: Logika HMHI Cabang Autofill (BARU) ---
        st.markdown("---") # Pemisah visual
        
        cabang_list = [""] + df_hmhi['cabang'].unique().tolist()
        kota_cakupan_val = ""

        # Cek data yang ada (jika mode edit)
        default_cabang = ""
        if pat_data:
            default_cabang = pat_data.get('cabang') or ""

        cabang_idx = get_safe_index(cabang_list, default_cabang)

        # Baris 3: HMHI Cabang (Input) dan Kota Cakupan (Display)
        col_cabang, col_cakupan = st.columns(2)
        with col_cabang:
            selected_cabang = st.selectbox(
                "HMHI Cabang",
                cabang_list,
                index=cabang_idx
            )

        # Logika Autoload Kota Cakupan
        if selected_cabang:
            match_cabang = df_hmhi[df_hmhi['cabang'] == selected_cabang]
            if not match_cabang.empty:
                # Ambil kota cakupan, pastikan tidak null
                kota_cakupan_val = match_cabang.iloc[0]['kota_cakupan'] or ""
        elif pat_data and not selected_cabang: # Jika tidak ada yg dipilih, tapi mode edit, isi data lama
             kota_cakupan_val = pat_data.get('kota_cakupan', '')
        
        with col_cakupan:
            st.text_input("Kota Cakupan Cabang (otomatis)", value=kota_cakupan_val, disabled=True)
        # --- END: Logika HMHI Cabang Autofill (BARU) ---


        note = st.text_area("Catatan (opsional)", value=pat_data.get('note', ''))

        form_label = "💾 Perbarui Pasien" if pat_data else "💾 Simpan Pasien Baru"
        submitted = st.button(form_label, type="primary")

    if submitted:
        if not (full_name or "").strip():
            st.error("Nama Lengkap wajib diisi.")
        elif not (nik or "").strip():
            st.error("NIK wajib diisi.")
        else:
            payload = {
                "full_name": full_name.strip(), "birth_place": (birth_place or "").strip() or None,
                "birth_date": birth_date, "nik": (nik or "").strip(),
                "blood_group": blood_group or None, "rhesus": rhesus or None,
                "gender": gender or None,
                "occupation": occupation or None, "education": education or None,
                "address": (address or "").strip() or None,
                "phone": (phone or "").strip() or None, 
                # -- Data dari Autofill Wilayah --
                "province": (province_name or "").strip() or None,
                "city": (city_name or "").strip() or None,
                "district": (district_name or "").strip() or None,
                "village": (village_name or "").strip() or None,
                # -- Data dari Autofill HMHI Cabang (BARU) --
                "cabang": (selected_cabang or "").strip() or None,
                "kota_cakupan": (kota_cakupan_val or "").strip() or None,
                # ------------------------
                "note": (note or "").strip() or None
            }
            if pat_data:
                # Logika update
                q_check_nik = "SELECT id FROM pwh.patients WHERE nik = :nik AND id != :current_id"
                existing_nik = run_df(q_check_nik, {"nik": payload["nik"], "current_id": pat_data['id']})
                q_check_name = "SELECT id FROM pwh.patients WHERE lower(full_name) = lower(:name) AND id != :current_id"
                existing_name = run_df(q_check_name, {"name": payload["full_name"], "current_id": pat_data['id']})
                if not existing_nik.empty:
                    st.error(f"NIK '{payload['nik']}' sudah digunakan oleh pasien lain (ID: {existing_nik.iloc[0]['id']}).")
                elif not existing_name.empty:
                    st.error(f"Nama '{payload['full_name']}' sudah digunakan oleh pasien lain (ID: {existing_name.iloc[0]['id']}). Gunakan nama yang unik.")
                else:
                    update_patient(pat_data['id'], payload)
                    st.success(f"Pasien dengan ID {pat_data['id']} berhasil diperbarui.")
                    fetch_all_wilayah_details.clear() 
                    fetch_hmhi_branches.clear() # <-- Clear cache baru
                    get_all_patients_for_selection.clear()
                    clear_session_state('patient_to_edit')
                    clear_session_state('patient_matches')
                    st.rerun()
            else:
                # Logika insert
                q_check_nik = "SELECT id FROM pwh.patients WHERE nik = :nik"
                existing_nik = run_df(q_check_nik, {"nik": payload["nik"]})
                q_check_name = "SELECT id FROM pwh.patients WHERE lower(full_name) = lower(:name)"
                existing_name = run_df(q_check_name, {"name": payload["full_name"]})
                if not existing_nik.empty:
                    st.error(f"NIK '{payload['nik']}' sudah ada di database dengan ID: {existing_nik.iloc[0]['id']}. Gunakan NIK lain.")
                elif not existing_name.empty:
                    st.error(f"Nama '{payload['full_name']}' sudah ada di database dengan ID: {existing_name.iloc[0]['id']}. Gunakan nama lain.")
                else:
                    pid = insert_patient(payload)
                    st.success(f"Pasien baru berhasil disimpan dengan ID: {pid}")
                    fetch_all_wilayah_details.clear()
                    fetch_hmhi_branches.clear() # <-- Clear cache baru
                    get_all_patients_for_selection.clear()
                    st.rerun()

    st.markdown("---")
    st.markdown("### 📋 Data Pasien Terbaru")

    st.write("**Edit Data Pasien**")
    search_name_pat = st.text_input("Ketik nama pasien untuk diedit", key="search_name_pat")
    if st.button("Cari Pasien", key="search_pat_button"):
        clear_session_state('patient_to_edit') 
        if search_name_pat:
            results_df = run_df("SELECT id, full_name, birth_date FROM pwh.patients WHERE full_name ILIKE :name", {"name": f"%{search_name_pat}%"})
            if results_df.empty:
                st.warning("Pasien tidak ditemukan.")
                clear_session_state('patient_matches')
            elif len(results_df) == 1:
                set_editing_state('patient_to_edit', results_df.iloc[0]['id'], 'pwh.patients')
                clear_session_state('patient_matches')
                st.rerun()
            else:
                st.info(f"Ditemukan {len(results_df)} pasien dengan nama serupa. Silakan pilih satu.")
                st.session_state.patient_matches = results_df
        else:
            st.warning("Silakan masukkan nama untuk dicari.")
            clear_session_state('patient_matches')

    if 'patient_matches' in st.session_state and not st.session_state.patient_matches.empty:
        df_matches = st.session_state.patient_matches
        options = {f"ID: {row['id']} - {row['full_name']} (Lahir: {row['birth_date']})": row['id'] for index, row in df_matches.iterrows()}
        
        selected_option = st.selectbox("Pilih pasien yang benar:", options.keys())
        if st.button("Pilih Pasien Ini", key="select_patient_button"):
            selected_id = options[selected_option]
            set_editing_state('patient_to_edit', selected_id, 'pwh.patients')
            clear_session_state('patient_matches')
            st.rerun()

    dfp = run_df("""
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
    p.created_at
FROM pwh.patients p
LEFT JOIN pwh.patient_age pa ON pa.id = p.id
WHERE (:branch = 'ALL' OR p.cabang = :branch)
ORDER BY p.id DESC
LIMIT 200;

""")
    
    if not dfp.empty:
        dfp_display = dfp.copy()
        dfp_display['birth_place'] = dfp_display['birth_place'].apply(lambda x: '*****' if pd.notna(x) and str(x).strip() else x)
        dfp_display['birth_date'] = dfp_display['birth_date'].apply(lambda x: '*****' if pd.notna(x) else x)
        dfp_display['nik'] = dfp_display['nik'].apply(lambda x: '*****' if pd.notna(x) and str(x).strip() else x)
        dfp_display['phone'] = dfp_display['phone'].apply(lambda x: '*****' if pd.notna(x) and str(x).strip() else x)
        # Sembunyikan data alamat lengkap di tampilan tabel utama
        # dfp_display['address'] = dfp_display['address'].apply(lambda x: '*****' if pd.notna(x) and str(x).strip() else x)
        # dfp_display['village'] = dfp_display['village'].apply(lambda x: '*****' if pd.notna(x) and str(x).strip() else x)
        # dfp_display['district'] = dfp_display['district'].apply(lambda x: '*****' if pd.notna(x) and str(x).strip() else x)
        
        # --- PERUBAHAN DI SINI ---
        # Data cabang dan kota cakupan TIDAK disembunyikan lagi
        # dfp_display['cabang'] = dfp_display['cabang'].apply(lambda x: '*****' if pd.notna(x) and str(x).strip() else x)
        # dfp_display['kota_cakupan'] = dfp_display['kota_cakupan'].apply(lambda x: '*****' if pd.notna(x) and str(x).strip() else x)
        # --- END PERUBAHAN ---
        
        dfp_display = dfp_display.drop(columns=['id'], errors='ignore')
        dfp_display.index = range(1, len(dfp_display) + 1)
        dfp_display.index.name = "No."
        
        st.write(f"Total Data Pasien: **{len(dfp_display)}**")
        st.dataframe(_alias_df(dfp_display, ALIAS_PATIENTS), use_container_width=True)
    else:
        st.info("Belum ada data pasien.")

# ==============================================================================
# --- Blok Kode Umum untuk Semua Tab Lainnya ---
# ==============================================================================

# Ambil data pasien sekali saja untuk semua tab
df_all_patients = get_all_patients_for_selection()
patient_id_map = df_all_patients.set_index('id')['full_name'].to_dict()

def format_patient_name(patient_id):
    """Fungsi untuk menampilkan nama pasien di selectbox."""
    if pd.isna(patient_id):
        return "Pilih pasien..."
    return patient_id_map.get(patient_id, "ID tidak ditemukan")

# Siapkan daftar ID untuk opsi, tambahkan None di awal untuk placeholder
patient_id_options = [None] + df_all_patients['id'].tolist()

# ==============================================================================
# Diagnosis
with tab_diag:
    st.subheader("🧬 Tambah Data Diagnosis Pasien")

    diag_data = st.session_state.get('diag_to_edit', {})
    
    if diag_data:
        st.info(f"Mode Edit untuk Diagnosis ID: {diag_data.get('id')}")
        if st.button("❌ Batal Edit", key="cancel_diag_edit"):
            clear_session_state('diag_to_edit')
            clear_session_state('diag_matches')
            st.rerun()

    # Tentukan pilihan default jika dalam mode edit
    default_patient_id = diag_data.get('patient_id') if diag_data else None
    
    pid_diag = st.selectbox(
        "Pilih Pasien (untuk data baru)",
        options=patient_id_options,
        index=patient_id_options.index(default_patient_id) if default_patient_id in patient_id_options else 0,
        format_func=format_patient_name,
        key="diag_patient_selector",
        disabled=bool(diag_data)
    )

    with st.form("diag::form", clear_on_submit=False):
        hemo_type_idx = get_safe_index(HEMO_TYPES, diag_data.get('hemo_type'))
        hemo_type = st.selectbox("Tipe Hemofilia", HEMO_TYPES, index=hemo_type_idx)
        severity_idx = get_safe_index(SEVERITY_CHOICES, diag_data.get('severity'))
        severity = st.selectbox("Kategori", SEVERITY_CHOICES, index=severity_idx)
        diagnosed_on_val = pd.to_datetime(diag_data.get('diagnosed_on')).date() if pd.notna(diag_data.get('diagnosed_on')) else None
        diagnosed_on = st.date_input("Tanggal Diagnosis", value=diagnosed_on_val, format="YYYY-MM-DD", min_value=date(1920, 1, 1))
        source = st.text_input("Sumber (opsional)", value=diag_data.get('source', ''))
        
        sdiag_label = "Perbarui Diagnosis" if diag_data else "Simpan Diagnosis Baru"
        sdiag = st.form_submit_button(f"💾 {sdiag_label}", type="primary")

    if sdiag:
        if diag_data:
            payload = {"hemo_type": hemo_type, "severity": severity, "diagnosed_on": diagnosed_on, "source": (source or "").strip() or None}
            update_diagnosis(diag_data['id'], payload)
            st.success("Diagnosis diperbarui.")
            clear_session_state('diag_to_edit')
            st.rerun()
        elif pid_diag:
            insert_diagnosis(int(pid_diag), hemo_type, severity, diagnosed_on, source)
            st.success("Diagnosis disimpan/diperbarui.")
            st.rerun()
        else:
            if not diag_data: st.warning("Silakan pilih pasien terlebih dahulu.")

    st.markdown("---")
    st.markdown("### 📋 Data Diagnosis Terbaru")
    st.write("**Edit Data Diagnosis**")
    search_name_diag = st.text_input("Ketik nama pasien untuk mencari riwayat dan mengedit", key="search_name_diag")
    if st.button("Cari Riwayat Diagnosis", key="search_diag_button"):
        clear_session_state('diag_to_edit')
        clear_session_state('diag_matches')
        st.session_state.diag_selected_patient_name = search_name_diag

        if search_name_diag:
            q = """
                SELECT d.id, p.full_name, d.hemo_type, d.diagnosed_on
                FROM pwh.hemo_diagnoses d
                JOIN pwh.patients p ON p.id = d.patient_id
                WHERE p.full_name ILIKE :name ORDER BY d.id DESC
            """
            results_df = run_df(q, {"name": f"%{search_name_diag}%"})

            if results_df.empty:
                st.warning("Riwayat diagnosis tidak ditemukan untuk pasien dengan nama tersebut.")
            elif len(results_df) == 1:
                set_editing_state('diag_to_edit', results_df.iloc[0]['id'], 'pwh.hemo_diagnoses')
                st.rerun()
            else:
                st.info(f"Ditemukan {len(results_df)} riwayat diagnosis. Silakan pilih satu untuk diedit.")
                st.session_state.diag_matches = results_df
        else:
            st.warning("Silakan masukkan nama untuk dicari.")
            st.session_state.diag_selected_patient_name = ""

    if 'diag_matches' in st.session_state and not st.session_state.diag_matches.empty:
        df_matches = st.session_state.diag_matches
        options = {
            f"ID: {row['id']} - {row['hemo_type']} (Tgl: {row['diagnosed_on']})": row['id']
            for _, row in df_matches.iterrows()
        }
        selected_option = st.selectbox("Pilih riwayat diagnosis yang akan diedit:", options.keys(), key="select_diag_box")
        if st.button("Pilih Riwayat Ini", key="select_diag_button"):
            selected_id = options[selected_option]
            set_editing_state('diag_to_edit', selected_id, 'pwh.hemo_diagnoses')
            clear_session_state('diag_matches')
            st.rerun()
    
    query_diag = "SELECT d.id, d.patient_id, p.full_name, d.hemo_type, d.severity, d.diagnosed_on, d.source FROM pwh.hemo_diagnoses d JOIN pwh.patients p ON p.id = d.patient_id"
    params = {}
    if 'diag_selected_patient_name' in st.session_state and st.session_state.diag_selected_patient_name:
        query_diag += " WHERE p.full_name ILIKE :name"
        params['name'] = f"%{st.session_state.diag_selected_patient_name}%"
    query_diag += " ORDER BY d.id DESC LIMIT 300;"
    
    df_diag = run_df(query_diag, params)

    if not df_diag.empty:
        df_diag_display = df_diag.drop(columns=['id', 'patient_id'], errors='ignore')
        df_diag_display.index = range(1, len(df_diag_display) + 1)
        df_diag_display.index.name = "No."
        st.write(f"Total Data Diagnosis: **{len(df_diag_display)}**")
        st.dataframe(_alias_df(df_diag_display, ALIAS_DIAG), use_container_width=True)
    else:
        st.info("Tidak ada data diagnosis untuk ditampilkan. Cari nama pasien di atas untuk memfilter.")

# ==================== Inhibitor ====================
with tab_inh:
    st.subheader("🧪 Tambah Data Inhibitor (BU)")

    inh_data = st.session_state.get('inh_to_edit', {})
    if inh_data:
        st.info(f"Mode Edit untuk Inhibitor ID: {inh_data.get('id')}")
        if st.button("❌ Batal Edit", key="cancel_inh_edit"):
            clear_session_state('inh_to_edit')
            clear_session_state('inh_matches')
            st.rerun()

    default_patient_id_inh = inh_data.get('patient_id') if inh_data else None
    
    pid_inh = st.selectbox(
        "Pilih Pasien (untuk data baru)",
        options=patient_id_options,
        index=patient_id_options.index(default_patient_id_inh) if default_patient_id_inh in patient_id_options else 0,
        format_func=format_patient_name,
        key="inh_patient_selector",
        disabled=bool(inh_data)
    )

    with st.form("inh::form", clear_on_submit=False):
        factor_idx = get_safe_index(INHIB_FACTORS, inh_data.get('factor'))
        factor = st.selectbox("Faktor", INHIB_FACTORS, index=factor_idx)
        titer_bu = st.number_input("Titer (BU)", min_value=0.0, step=0.1, value=float(inh_data.get('titer_bu', 0.0)))
        measured_on_val = pd.to_datetime(inh_data.get('measured_on')).date() if pd.notna(inh_data.get('measured_on')) else None
        measured_on = st.date_input("Tanggal Ukur", value=measured_on_val, format="YYYY-MM-DD", min_value=date(1920, 1, 1))
        lab = st.text_input("Lab (opsional)", value=inh_data.get('lab', ''))
        sinh_label = "Perbarui Riwayat" if inh_data else "Simpan Riwayat Baru"
        sinh = st.form_submit_button(f"💾 {sinh_label}", type="primary")

    if sinh:
        if inh_data:
            payload = { "factor": factor, "titer_bu": float(titer_bu), "measured_on": measured_on, "lab": (lab or "").strip() or None }
            update_inhibitor(inh_data['id'], payload)
            st.success("Riwayat inhibitor diperbarui.")
            clear_session_state('inh_to_edit')
            st.rerun()
        elif pid_inh:
            insert_inhibitor(int(pid_inh), factor, float(titer_bu), measured_on, lab)
            st.success("Riwayat inhibitor ditambahkan.")
            st.rerun()
        else:
                if not inh_data: st.warning("Silakan pilih pasien terlebih dahulu.")

    st.markdown("---")
    st.markdown("### 📋 Data Inhibitor Terbaru")
    
    st.write("**Edit Data Inhibitor**")
    search_name_inh = st.text_input("Ketik nama pasien untuk mencari riwayat dan mengedit", key="search_name_inh")
    if st.button("Cari Riwayat Inhibitor", key="search_inh_button"):
        clear_session_state('inh_to_edit')
        clear_session_state('inh_matches')
        st.session_state.inh_selected_patient_name = search_name_inh

        if search_name_inh:
            q = """
                SELECT i.id, p.full_name, i.factor, i.measured_on
                FROM pwh.hemo_inhibitors i
                JOIN pwh.patients p ON p.id = i.patient_id
                WHERE p.full_name ILIKE :name ORDER BY i.id DESC
            """
            results_df = run_df(q, {"name": f"%{search_name_inh}%"})

            if results_df.empty:
                st.warning("Riwayat inhibitor tidak ditemukan.")
            elif len(results_df) == 1:
                set_editing_state('inh_to_edit', results_df.iloc[0]['id'], 'pwh.hemo_inhibitors')
                st.rerun()
            else:
                st.info(f"Ditemukan {len(results_df)} riwayat. Silakan pilih satu.")
                st.session_state.inh_matches = results_df
        else:
            st.warning("Silakan masukkan nama untuk dicari.")
            st.session_state.inh_selected_patient_name = ""

    if 'inh_matches' in st.session_state and not st.session_state.inh_matches.empty:
        df_matches = st.session_state.inh_matches
        options = {
            f"ID: {row['id']} - {row['factor']} (Tgl: {row['measured_on']})": row['id']
            for _, row in df_matches.iterrows()
        }
        selected_option = st.selectbox("Pilih riwayat inhibitor:", options.keys(), key="select_inh_box")
        if st.button("Pilih Riwayat Ini", key="select_inh_button"):
            selected_id = options[selected_option]
            set_editing_state('inh_to_edit', selected_id, 'pwh.hemo_inhibitors')
            clear_session_state('inh_matches')
            st.rerun()

    query_inh = "SELECT i.id, i.patient_id, p.full_name, i.factor, i.titer_bu, i.measured_on, i.lab FROM pwh.hemo_inhibitors i JOIN pwh.patients p ON p.id = i.patient_id"
    params_inh = {}
    if 'inh_selected_patient_name' in st.session_state and st.session_state.inh_selected_patient_name:
        query_inh += " WHERE p.full_name ILIKE :name"
        params_inh['name'] = f"%{st.session_state.inh_selected_patient_name}%"
    query_inh += " ORDER BY i.id DESC LIMIT 500;"
    df_inh = run_df(query_inh, params_inh)

    if not df_inh.empty:
        df_inh_display = df_inh.drop(columns=['id', 'patient_id'], errors='ignore')
        df_inh_display.index = range(1, len(df_inh_display) + 1)
        df_inh_display.index.name = "No."
        st.write(f"Total Data Inhibitor: **{len(df_inh_display)}**")
        st.dataframe(_alias_df(df_inh_display, ALIAS_INH), use_container_width=True)
    else:
        st.info("Tidak ada data inhibitor untuk ditampilkan.")

# Virus Tests
with tab_virus:
    st.subheader("🧫 Tambah Data Virus Tests")
        
    virus_data = st.session_state.get('virus_to_edit', {})
    if virus_data:
        st.info(f"Mode Edit untuk Tes Virus ID: {virus_data.get('id')}")
        if st.button("❌ Batal Edit", key="cancel_virus_edit"):
            clear_session_state('virus_to_edit')
            clear_session_state('virus_matches')
            st.rerun()

    default_patient_id_virus = virus_data.get('patient_id') if virus_data else None
    
    pid_virus = st.selectbox(
        "Pilih Pasien (untuk data baru)",
        options=patient_id_options,
        index=patient_id_options.index(default_patient_id_virus) if default_patient_id_virus in patient_id_options else 0,
        format_func=format_patient_name,
        key="virus_patient_selector",
        disabled=bool(virus_data)
    )

    with st.form("virus::form", clear_on_submit=False):
        test_type_idx = get_safe_index(VIRUS_TESTS, virus_data.get('test_type'))
        test_type = st.selectbox("Jenis Tes", VIRUS_TESTS, index=test_type_idx)
        result_idx = get_safe_index(TEST_RESULTS, virus_data.get('result'))
        result = st.selectbox("Hasil", TEST_RESULTS, index=result_idx)
        tested_on_val = pd.to_datetime(virus_data.get('tested_on')).date() if pd.notna(virus_data.get('tested_on')) else None
        tested_on = st.date_input("Tanggal Tes", value=tested_on_val, format="YYYY-MM-DD", min_value=date(1920, 1, 1))
        lab = st.text_input("Lab (opsional)", value=virus_data.get('lab', ''))
        svirus_label = "Perbarui Hasil Tes" if virus_data else "Simpan Hasil Tes Baru"
        svirus = st.form_submit_button(f"💾 {svirus_label}", type="primary")

    if svirus:
        if virus_data:
            payload = {"test_type": test_type, "result": result, "tested_on": tested_on, "lab": (lab or "").strip() or None}
            update_virus_test(virus_data['id'], payload)
            st.success("Hasil tes diperbarui.")
            clear_session_state('virus_to_edit')
            st.rerun()
        elif pid_virus:
            insert_virus_test(int(pid_virus), test_type, result, tested_on, lab)
            st.success("Hasil tes disimpan.")
            st.rerun()
        else:
            if not virus_data: st.warning("Silakan pilih pasien terlebih dahulu.")

    st.markdown("---")
    st.markdown("### 📋 Data Tes Virus Terbaru")
    
    st.write("**Edit Data Tes Virus**")
    search_name_virus = st.text_input("Ketik nama pasien untuk mencari riwayat dan mengedit", key="search_name_virus")
    if st.button("Cari Riwayat Tes Virus", key="search_virus_button"):
        clear_session_state('virus_to_edit')
        clear_session_state('virus_matches')
        st.session_state.virus_selected_patient_name = search_name_virus

        if search_name_virus:
            q = """
                SELECT v.id, p.full_name, v.test_type, v.result, v.tested_on
                FROM pwh.virus_tests v
                JOIN pwh.patients p ON p.id = v.patient_id
                WHERE p.full_name ILIKE :name ORDER BY v.id DESC
            """
            results_df = run_df(q, {"name": f"%{search_name_virus}%"})

            if results_df.empty:
                st.warning("Riwayat tes virus tidak ditemukan.")
            elif len(results_df) == 1:
                set_editing_state('virus_to_edit', results_df.iloc[0]['id'], 'pwh.virus_tests')
                st.rerun()
            else:
                st.info(f"Ditemukan {len(results_df)} riwayat. Silakan pilih satu.")
                st.session_state.virus_matches = results_df
        else:
            st.warning("Silakan masukkan nama untuk dicari.")
            st.session_state.virus_selected_patient_name = ""

    if 'virus_matches' in st.session_state and not st.session_state.virus_matches.empty:
        df_matches = st.session_state.virus_matches
        options = {
            f"ID: {row['id']} - {row['test_type']}: {row['result']} (Tgl: {row['tested_on']})": row['id']
            for _, row in df_matches.iterrows()
        }
        selected_option = st.selectbox("Pilih riwayat tes:", options.keys(), key="select_virus_box")
        if st.button("Pilih Riwayat Ini", key="select_virus_button"):
            selected_id = options[selected_option]
            set_editing_state('virus_to_edit', selected_id, 'pwh.virus_tests')
            clear_session_state('virus_matches')
            st.rerun()

    query_virus = "SELECT v.id, v.patient_id, p.full_name, v.test_type, v.result, v.tested_on, v.lab FROM pwh.virus_tests v JOIN pwh.patients p ON p.id = v.patient_id"
    params_virus = {}
    if 'virus_selected_patient_name' in st.session_state and st.session_state.virus_selected_patient_name:
        query_virus += " WHERE p.full_name ILIKE :name"
        params_virus['name'] = f"%{st.session_state.virus_selected_patient_name}%"
    query_virus += " ORDER BY v.id DESC LIMIT 500;"
    
    df_virus = run_df(query_virus, params_virus)

    if not df_virus.empty:
        df_virus_display = df_virus.copy()
        df_virus_display['result'] = '*****'
        
        df_virus_display = df_virus_display.drop(columns=['id', 'patient_id'], errors='ignore')
        df_virus_display.index = range(1, len(df_virus_display) + 1)
        df_virus_display.index.name = "No."
        st.write(f"Total Data Tes Virus: **{len(df_virus_display)}**")
        st.dataframe(_alias_df(df_virus_display, ALIAS_VIRUS), use_container_width=True)
    else:
        st.info("Tidak ada data tes virus untuk ditampilkan.")


# Rumah Sakit Penangan
with tab_hospital:
    st.subheader("🏥 Tambah Data Rumah Sakit Penangan")
        
    hosp_data = st.session_state.get('hosp_to_edit', {})
    if hosp_data:
        st.info(f"Mode Edit untuk Data RS ID: {hosp_data.get('id')}")
        if st.button("❌ Batal Edit", key="cancel_hosp_edit"):
            clear_session_state('hosp_to_edit')
            clear_session_state('hosp_matches')
            st.rerun()
            
    default_patient_id_hosp = hosp_data.get('patient_id') if hosp_data else None
    
    pid_hosp = st.selectbox(
        "Pilih Pasien (untuk data baru)",
        options=patient_id_options,
        index=patient_id_options.index(default_patient_id_hosp) if default_patient_id_hosp in patient_id_options else 0,
        format_func=format_patient_name,
        key="hosp_patient_selector",
        disabled=bool(hosp_data)
    )
    
    with st.form("hospital::form", clear_on_submit=False):
        hospital_list = fetch_hospitals()
        name_h, city_h, prov_h = hosp_data.get('name_hospital'), hosp_data.get('city_hospital'), hosp_data.get('province_hospital')
        hosp_val = f"{name_h} - {city_h} - {prov_h}" if all([name_h, city_h, prov_h]) else ''
        hosp_idx = get_safe_index(hospital_list, hosp_val)
        hospital_selection = st.selectbox("Nama Rumah Sakit*", hospital_list, index=hosp_idx)
        
        col_date, col_doc = st.columns(2)
        with col_date:
            visit_date_val = pd.to_datetime(hosp_data.get('date_of_visit')).date() if pd.notna(hosp_data.get('date_of_visit')) else None
            date_of_visit = st.date_input("Tanggal Kunjungan", value=visit_date_val, format="YYYY-MM-DD", min_value=date(1920, 1, 1))
        with col_doc:
            doctor_in_charge = st.text_input("DPJP", value=hosp_data.get('doctor_in_charge', ''))

        col1, col2 = st.columns(2)
        with col1:
            ttype_idx = get_safe_index(TREATMENT_TYPES, hosp_data.get('treatment_type'))
            treatment_type = st.selectbox("Jenis Penanganan", TREATMENT_TYPES, index=ttype_idx)
        with col2:
            cserv_idx = get_safe_index(CARE_SERVICES, hosp_data.get('care_services'))
            care_services = st.selectbox("Layanan Rawat", CARE_SERVICES, index=cserv_idx)
        col3, col4 = st.columns(2)
        with col3: frequency = st.text_input("Frekuensi", placeholder="Contoh: 1x Seminggu", value=hosp_data.get('frequency', ''))
        with col4: dose = st.text_input("Dosis", placeholder="Contoh: 1000 IU", value=hosp_data.get('dose', ''))
        prod_idx = get_safe_index(PRODUCTS, hosp_data.get('product'))
        product = st.selectbox("Produk", PRODUCTS, index=prod_idx) # Diubah ke 'Produk'
        merk = st.text_input("Merk", value=hosp_data.get('merk', ''))
        shosp_label = "Perbarui Data" if hosp_data else "Simpan Data Baru"
        shosp = st.form_submit_button(f"💾 {shosp_label}", type="primary")

    if shosp:
        if not hospital_selection: st.error("Nama Rumah Sakit wajib diisi.")
        else:
            parts = hospital_selection.split(' - ')
            name_h, city_h, prov_h = (parts[0].strip(), parts[1].strip(), parts[2].strip()) if len(parts) == 3 else (hospital_selection, None, None)
            payload = { 
                "name_hospital": name_h, "city_hospital": city_h, "province_hospital": prov_h, 
                "date_of_visit": date_of_visit, "doctor_in_charge": (doctor_in_charge or "").strip() or None,
                "treatment_type": treatment_type or None, "care_services": care_services or None, 
                "frequency": (frequency or "").strip() or None, "dose": (dose or "").strip() or None, 
                "product": product or None, "merk": (merk or "").strip() or None, 
            }
            if hosp_data:
                update_treatment_hospital(hosp_data['id'], payload)
                st.success("Data penanganan diperbarui.")
                clear_session_state('hosp_to_edit')
                st.rerun()
            elif pid_hosp:
                payload['patient_id'] = int(pid_hosp)
                insert_treatment_hospital(payload)
                st.success("Data penanganan disimpan.")
                st.rerun()
            else:
                if not hosp_data: st.warning("Silakan pilih pasien terlebih dahulu.")
            
    st.markdown("---")
    st.markdown("### 📋 Data Penanganan RS Terbaru")
    
    st.write("**Edit Data Penanganan RS**")
    search_name_hosp = st.text_input("Ketik nama pasien untuk mencari riwayat dan mengedit", key="search_name_hosp")
    if st.button("Cari Riwayat Penanganan", key="search_hosp_button"):
        clear_session_state('hosp_to_edit')
        clear_session_state('hosp_matches')
        st.session_state.hosp_selected_patient_name = search_name_hosp

        if search_name_hosp:
            q = """
                SELECT th.id, p.full_name, th.name_hospital, th.date_of_visit, th.product
                FROM pwh.treatment_hospital th
                JOIN pwh.patients p ON p.id = th.patient_id
                WHERE p.full_name ILIKE :name ORDER BY th.id DESC
            """
            results_df = run_df(q, {"name": f"%{search_name_hosp}%"})

            if results_df.empty:
                st.warning("Riwayat penanganan RS tidak ditemukan.")
            elif len(results_df) == 1:
                set_editing_state('hosp_to_edit', results_df.iloc[0]['id'], 'pwh.treatment_hospital')
                st.rerun()
            else:
                st.info(f"Ditemukan {len(results_df)} riwayat. Silakan pilih satu.")
                st.session_state.hosp_matches = results_df
        else:
            st.warning("Silakan masukkan nama untuk dicari.")
            st.session_state.hosp_selected_patient_name = ""

    if 'hosp_matches' in st.session_state and not st.session_state.hosp_matches.empty:
        df_matches = st.session_state.hosp_matches
        options = {
            f"ID: {row['id']} - {row['name_hospital']} (Kunjungan: {row['date_of_visit']})": row['id']
            for _, row in df_matches.iterrows()
        }
        selected_option = st.selectbox("Pilih riwayat penanganan:", options.keys(), key="select_hosp_box")
        if st.button("Pilih Riwayat Ini", key="select_hosp_button"):
            selected_id = options[selected_option]
            set_editing_state('hosp_to_edit', selected_id, 'pwh.treatment_hospital')
            clear_session_state('hosp_matches')
            st.rerun()

    query_hosp = "SELECT th.id, th.patient_id, p.full_name, th.name_hospital, th.city_hospital, th.province_hospital, th.date_of_visit, th.doctor_in_charge, th.treatment_type, th.care_services, th.frequency, th.dose, th.product, th.merk FROM pwh.treatment_hospital th JOIN pwh.patients p ON p.id = th.patient_id"
    params_hosp = {}
    if 'hosp_selected_patient_name' in st.session_state and st.session_state.hosp_selected_patient_name:
        query_hosp += " WHERE p.full_name ILIKE :name"
        params_hosp['name'] = f"%{st.session_state.hosp_selected_patient_name}%"
    query_hosp += " ORDER BY th.id DESC LIMIT 300;"

    df_th = run_df(query_hosp, params_hosp)
        
    if not df_th.empty:
        df_th_display = df_th.drop(columns=['id', 'patient_id'], errors='ignore')
        df_th_display.index = range(1, len(df_th_display) + 1)
        df_th_display.index.name = "No."
        st.write(f"Total Data Penanganan: **{len(df_th_display)}**")
        st.dataframe(_alias_df(df_th_display, ALIAS_HOSPITAL), use_container_width=True)
    else:
        st.info("Tidak ada data penanganan RS untuk ditampilkan.")

# Kematian
with tab_death:
    st.subheader("⚰️ Tambah Data Data Kematian")

    death_data = st.session_state.get('death_to_edit', {})
    if death_data:
        st.info(f"Mode Edit untuk Data Kematian ID: {death_data.get('id')}")
        if st.button("❌ Batal Edit", key="cancel_death_edit"):
            clear_session_state('death_to_edit')
            st.rerun()
    
    default_patient_id_death = death_data.get('patient_id') if death_data else None

    pid_death = st.selectbox(
        "Pilih Pasien (untuk data baru)",
        options=patient_id_options,
        index=patient_id_options.index(default_patient_id_death) if default_patient_id_death in patient_id_options else 0,
        format_func=format_patient_name,
        key="death_patient_selector",
        disabled=bool(death_data)
    )

    with st.form("death::form", clear_on_submit=False):
        cause_of_death = st.text_area("Penyebab Kematian", value=death_data.get('cause_of_death', ''))
        current_year = date.today().year
        year_of_death_val = death_data.get('year_of_death')
        if year_of_death_val:
            year_of_death_val = int(year_of_death_val)

        year_of_death = st.number_input("Tahun Kematian", min_value=1900, max_value=current_year, value=year_of_death_val, step=1)
        
        sdeath_label = "Perbarui Data Kematian" if death_data else "Simpan Data Kematian"
        sdeath = st.form_submit_button(f"💾 {sdeath_label}", type="primary")

    if sdeath:
        payload = { "cause_of_death": (cause_of_death or "").strip() or None, "year_of_death": int(year_of_death) if year_of_death else None }
        if death_data:
            update_death_record(death_data['id'], payload)
            st.success("Data kematian diperbarui.")
            clear_session_state('death_to_edit')
            st.rerun()
        elif pid_death:
            payload['patient_id'] = int(pid_death)
            insert_death_record(payload)
            st.success("Data kematian disimpan.")
            st.rerun()
        else:
            if not death_data: st.warning("Silakan pilih pasien terlebih dahulu.")

    st.markdown("---")
    st.markdown("### 📋 Data Kematian Terbaru")

    st.write("**Edit Data Kematian**")
    search_name_death = st.text_input("Ketik nama pasien untuk mencari & mengedit", key="search_name_death")
    if st.button("Cari Data Kematian", key="search_death_button"):
        clear_session_state('death_to_edit')
        st.session_state.death_selected_patient_name = search_name_death

        if search_name_death:
            q = """
                SELECT d.id FROM pwh.death d
                JOIN pwh.patients p ON p.id = d.patient_id
                WHERE p.full_name ILIKE :name
            """
            results_df = run_df(q, {"name": f"%{search_name_death}%"})
            if results_df.empty:
                st.warning("Data kematian tidak ditemukan.")
            else:
                set_editing_state('death_to_edit', results_df.iloc[0]['id'], 'pwh.death')
                st.rerun()
        else:
            st.warning("Silakan masukkan nama untuk dicari.")
            st.session_state.death_selected_patient_name = ""

    query_death = "SELECT d.id, d.patient_id, p.full_name, d.cause_of_death, d.year_of_death FROM pwh.death d JOIN pwh.patients p ON p.id = d.patient_id"
    params_death = {}
    if 'death_selected_patient_name' in st.session_state and st.session_state.death_selected_patient_name:
        query_death += " WHERE p.full_name ILIKE :name"
        params_death['name'] = f"%{st.session_state.death_selected_patient_name}%"
    query_death += " ORDER BY d.id DESC;"
    
    df_death = run_df(query_death, params_death)

    if not df_death.empty:
        df_death_display = df_death.drop(columns=['id', 'patient_id'], errors='ignore')
        df_death_display.index = range(1, len(df_death_display) + 1)
        df_death_display.index.name = "No."
        st.write(f"Total Data Kematian: **{len(df_death_display)}**")
        st.dataframe(_alias_df(df_death_display, ALIAS_DEATH), use_container_width=True)
    else:
        st.info("Tidak ada data kematian untuk ditampilkan.")


# Kontak
with tab_contacts:
    st.subheader("👨‍👩‍👧 Tambah Data Kontak")

    cont_data = st.session_state.get('contact_to_edit', {})
    if cont_data:
        st.info(f"Mode Edit untuk Kontak ID: {cont_data.get('id')}")
        if st.button("❌ Batal Edit", key="cancel_cont_edit"):
            clear_session_state('contact_to_edit')
            clear_session_state('contact_matches')
            st.rerun()
    
    default_patient_id_cont = cont_data.get('patient_id') if cont_data else None
    
    pid_cont = st.selectbox(
        "Pilih Pasien (untuk data baru)",
        options=patient_id_options,
        index=patient_id_options.index(default_patient_id_cont) if default_patient_id_cont in patient_id_options else 0,
        format_func=format_patient_name,
        key="cont_patient_selector",
        disabled=bool(cont_data)
    )
    
    with st.form("contact::form", clear_on_submit=False):
        relation_idx = get_safe_index(RELATIONS, cont_data.get('relation'))
        relation = st.selectbox("Relasi", RELATIONS, index=relation_idx)
        name = st.text_input("Nama Kontak*", value=cont_data.get('name', ''))
        phone = st.text_input("No. Telp", value=cont_data.get('phone', ''))
        is_primary = st.checkbox("Kontak Utama?", value=bool(cont_data.get('is_primary', False)))
        scont_label = "Perbarui Kontak" if cont_data else "Simpan Kontak Baru"
        scont = st.form_submit_button(f"💾 {scont_label}", type="primary")

    if scont:
        if not name.strip():
            st.error("Nama Kontak wajib diisi.")
        elif not relation:
                st.error("Relasi wajib diisi.")
        else:
            payload = {"relation": relation, "name": name, "phone": (phone or "").strip() or None, "is_primary": is_primary}
            if cont_data:
                update_contact(cont_data['id'], payload)
                st.success("Kontak diperbarui.")
                clear_session_state('contact_to_edit')
                st.rerun()
            elif pid_cont:
                insert_contact(int(pid_cont), relation, name, phone, is_primary)
                st.success("Kontak baru ditambahkan.")
                st.rerun()
            else:
                if not cont_data: st.warning("Silakan pilih pasien terlebih dahulu.")

    st.markdown("---")
    st.markdown("### 📋 Data Kontak Terbaru")
    
    st.write("**Edit Data Kontak**")
    search_name_cont = st.text_input("Ketik nama pasien untuk mencari riwayat dan mengedit", key="search_name_cont")
    if st.button("Cari Kontak", key="search_cont_button"):
        clear_session_state('contact_to_edit')
        clear_session_state('contact_matches')
        st.session_state.cont_selected_patient_name = search_name_cont

        if search_name_cont:
            q = """
                SELECT c.id, p.full_name, c.name, c.relation
                FROM pwh.contacts c
                JOIN pwh.patients p ON p.id = c.patient_id
                WHERE p.full_name ILIKE :name ORDER BY c.id DESC
            """
            results_df = run_df(q, {"name": f"%{search_name_cont}%"})

            if results_df.empty:
                st.warning("Kontak tidak ditemukan.")
            elif len(results_df) == 1:
                set_editing_state('contact_to_edit', results_df.iloc[0]['id'], 'pwh.contacts')
                st.rerun()
            else:
                st.info(f"Ditemukan {len(results_df)} kontak. Silakan pilih satu.")
                st.session_state.contact_matches = results_df
        else:
            st.warning("Silakan masukkan nama untuk dicari.")
            st.session_state.cont_selected_patient_name = ""

    if 'contact_matches' in st.session_state and not st.session_state.contact_matches.empty:
        df_matches = st.session_state.contact_matches
        options = {
            f"ID: {row['id']} - {row['name']} ({row['relation']})": row['id']
            for _, row in df_matches.iterrows()
        }
        selected_option = st.selectbox("Pilih kontak:", options.keys(), key="select_cont_box")
        if st.button("Pilih Kontak Ini", key="select_cont_button"):
            selected_id = options[selected_option]
            set_editing_state('contact_to_edit', selected_id, 'pwh.contacts')
            clear_session_state('contact_matches')
            st.rerun()

    query_cont = "SELECT c.id, c.patient_id, p.full_name, c.relation, c.name, c.phone, c.is_primary FROM pwh.contacts c JOIN pwh.patients p ON p.id = c.patient_id"
    params_cont = {}
    if 'cont_selected_patient_name' in st.session_state and st.session_state.cont_selected_patient_name:
        query_cont += " WHERE p.full_name ILIKE :name"
        params_cont['name'] = f"%{st.session_state.cont_selected_patient_name}%"
    query_cont += " ORDER BY c.id DESC LIMIT 500;"
    df_contacts = run_df(query_cont, params_cont)

    if not df_contacts.empty:
        df_contacts_display = df_contacts.drop(columns=['id', 'patient_id'], errors='ignore')
        df_contacts_display.index = range(1, len(df_contacts_display) + 1)
        df_contacts_display.index.name = "No."
        st.write(f"Total Data Kontak: **{len(df_contacts_display)}**")
        st.dataframe(_alias_df(df_contacts_display, ALIAS_CONTACTS), use_container_width=True)
    else:
        st.info("Tidak ada data kontak untuk ditampilkan.")

# Ringkasan
with tab_view:
    st.subheader("📄 Ringkasan Pasien") # Diubah ke 'Ringkasan Pasien'
    df = run_df("SELECT * FROM pwh.patient_summary ORDER BY id DESC LIMIT 300;")
    if df.empty:
        st.info("Belum ada data.")
    else:
        df_summary_display = df.copy()
        # Sembunyikan data sensitif potensial
        sensitive_cols = ['Lahir: Tempat', 'Lahir: Tanggal', 'Alamat', 'No. Telp', 'Org Tua: Ayah', 'Org Tua: Ibu']
        for col in sensitive_cols:
            if col in df_summary_display.columns:
                df_summary_display[col] = '*****'

        df_summary_display = df_summary_display.drop(columns=['id'], errors='ignore')
        df_summary_display.index = range(1, len(df_summary_display) + 1)
        df_summary_display.index.name = "No."
        st.write(f"Total Data Pasien: **{len(df_summary_display)}**")
        st.dataframe(_alias_df(df_summary_display, ALIAS_SUMMARY), use_container_width=True)
        st.caption("View ini mengambil hasil terbaru per pasien (diagnosis A/B/vWD, inhibitor FVIII/FIX, dan tes HBsAg/Anti-HCV/HIV).")

# Export
with tab_export:
    st.subheader("⬇️ Export Excel (semua tab)")
    st.write("Klik tombol di bawah untuk membuat file Excel dengan semua data (nama sheet dan kolom dalam Bahasa Indonesia).")
    if st.button("Generate file Excel"):
        try:
            excel_bytes = build_excel_bytes()
            st.download_button(label="💾 Download data_pwh.xlsx", data=excel_bytes, file_name="data_pwh.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.success("File siap diunduh.")
        except Exception as e: st.error(f"Gagal membuat file Excel: {e}")

    st.markdown("---")
    st.subheader("📥 Template Bulk & ⬆️ Import")
    c1, c2 = st.columns([1,2])
    with c1:
        try:
            tpl = build_bulk_template_bytes()
            st.download_button(label="📄 Download Template Bulk (.xlsx)", data=tpl, file_name="pwh_bulk_template.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.success("Template bulk (Bahasa Indonesia) siap diunduh.")
        except Exception as e: st.error(f"Gagal membuat template: {e}")

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
                st.error(f"Gagal import: {e}")
                st.exception(e) # Tampilkan traceback error untuk debugging
