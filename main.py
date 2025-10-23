# main.py (gabungan - Versi Database Auth - DIPERBAIKI)
import runpy
import os  # <-- Tambahan
import streamlit as st
from streamlit_option_menu import option_menu
from sqlalchemy import create_engine, text, Engine  # <-- Tambahan
from passlib.context import CryptContext  # <-- Tambahan

# -----------------------------
# Konfigurasi halaman
# -----------------------------
st.set_page_config(
    page_title="PWH Dashboard",
    page_icon="🩸",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------
# KONEKSI DATABASE (Diambil dari file rekap)
# -----------------------------
def _resolve_db_url() -> str:
    """Mencari DATABASE_URL dari st.secrets atau environment variables."""
    try:
        # --- PERBAIKAN: Membaca dari dalam blok [secrets] ---
        sec = st.secrets.get("secrets", {}).get("DATABASE_URL", "")
        if sec: 
            return sec
    except Exception:
        pass
    
    env = os.environ.get("DATABASE_URL")
    if env: 
        return env
    
    # Error jika tidak ditemukan
    st.error('DATABASE_URL tidak ditemukan di Streamlit Secrets.')
    st.caption("Pastikan Anda sudah menambahkan `DATABASE_URL` ke dalam blok `[secrets]` di Streamlit Cloud.")
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

# --- Inisialisasi Engine & Hashing ---
DB_URL = _resolve_db_url()
if DB_URL:
    DB_ENGINE = get_engine(DB_URL)
else:
    st.stop() # Hentikan jika DB_URL tidak ditemukan

# Konteks untuk hashing password
pwd_context = CryptContext(schemes=["bcrypt_sha256"], deprecated="auto")

# -----------------------------
# Auth via Database
# -----------------------------
def check_password() -> bool:
    """
    Validasi login user dari tabel pwh.users di database.
    """
    # Sudah login di session?
    if st.session_state.get("auth_ok", False):
        return True

    # Form login
    with st.sidebar:
        st.markdown("### 🔐 Login")
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")
        login = st.button("Masuk")

    if login:
        if not username or not password:
            st.error("Username dan Password wajib diisi.")
            return False

        try:
            # --- PERBAIKAN: Tambahkan .strip() pada username ---
            username_cleaned = username.strip()
            # --- SELESAI PERBAIKAN ---

            with DB_ENGINE.connect() as conn:
                # 1. Cari user di database (gunakan username yang sudah dibersihkan)
                query = text("SELECT username, hashed_password, cabang FROM pwh.users WHERE username = :user")
                result = conn.execute(query, {"user": username_cleaned}) # <-- Variabel diubah
                user_data = result.mappings().fetchone() # Ambil data sbg dictionary

            if not user_data:
                st.error("Username atau password salah.")
                return False

            # --- PERBAIKAN KUNCI DI SINI ---
            # Kita harus memotong password input ke 72 karakter,
            # sama seperti yang kita lakukan saat membuat hash
            password_to_check = password[:72]
            
            # 2. Verifikasi hash password (menggunakan password yang sudah dipotong)
            if pwd_context.verify(password_to_check, user_data['hashed_password']):
            # --- SELESAI PERBAIKAN ---
            
                # 3. Sukses! Simpan data ke session
                st.session_state.auth_ok = True
                st.session_state.username = user_data['username']
                st.session_state.user_branch = user_data['cabang'] # Ini adalah kuncinya
                
                # Kita perlu rerun agar st.success muncul di halaman utama
                st.rerun()
            else:
                st.error("Username atau password salah.")
                return False

        except Exception as e:
            st.error(f"Terjadi error saat login: {e}")
            return False

    # Belum login → hentikan render halaman
    st.stop()
    return False


# -----------------------------
# Daftar halaman (judul → file)
# -----------------------------
MENU_ITEMS = {
    "📝 Input Data Pasien": "01_pwh_input.py",
    "📊 Rekapitulasi per Kelompok Usia": "02_rekap_pwh.py",
    "🚻 Rekapitulasi per Jenis Kelamin": "03_rekap_gender.py",
    "🏥 RS Perawatan Hemofilia": "04_rs_hemofilia.py",
    "📚 Rekap Pendidikan & Pekerjaan": "05_rekap_pend_pekerjaan.py",
    "🗺️ Distribusi Pasien per Kota (Berdasarkan RS Penangan)": "06_distribusi_pasien.py",
    "🗺️ Rekapitulasi per Provinsi (Berdasarkan Domisi)": "07_rekap_propinsi.py",
}

ICONS = [
    "pencil-square",   # 📝
    "bar-chart",       # 📊
    "person-arms-up",  # 🚻
    "hospital",        # 🏥
    "book",            # 📚
    "map",             # 🗺️ Kota
    "geo-alt",         # 🗺️ Provinsi
]

# -----------------------------
# App
# -----------------------------
def main():
    st.title("📊 Pendataan Hemofilia")
    # Cek login
    if not check_password():
        return
        
    # Tampilkan pesan sukses SETELAH login berhasil dan di-rerun
    if "auth_ok" in st.session_state and not st.session_state.get("welcome_message_shown", False):
        st.success(f"Selamat datang, **{st.session_state.username}**!")
        st.session_state.welcome_message_shown = True # Tandai agar tidak muncul lagi

    # Sidebar header + tombol logout
    with st.sidebar:
        st.markdown("### 📁 Menu")
        selection = option_menu(
            menu_title="",  # minimalis
            options=list(MENU_ITEMS.keys()),
            icons=ICONS[:len(MENU_ITEMS)],
            default_index=0,
            orientation="vertical",
        )

        st.divider()
        col1, col2 = st.columns([1, 1])
        with col1:
            # Tampilkan juga cabang user
            branch_info = st.session_state.get('user_branch', 'N/A')
            if branch_info == "ALL":
                branch_info = "Admin (Semua Cabang)"
            st.caption(f"👤 {st.session_state.get('username', '')}\n🏢 {branch_info}")
        with col2:
            if st.button("Logout", use_container_width=True):
                st.session_state.clear()
                st.rerun()

    # Muat halaman sesuai pilihan
    page_path = MENU_ITEMS[selection]
    try:
        runpy.run_path(page_path, run_name="__main__")
    except FileNotFoundError:
        st.error(f"File halaman tidak ditemukan: `{page_path}`")
    except Exception as e:
        st.exception(e)

    # Footer kecil
    st.markdown("---")
    st.caption("© PWH Dashboard — Streamlit")


if __name__ == "__main__":
    main()

