import runpy
import os
import streamlit as st
from streamlit_option_menu import option_menu
from sqlalchemy import create_engine, text, Engine
from passlib.context import CryptContext

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
# KONEKSI DATABASE
# -----------------------------
def _resolve_db_url() -> str:
    try:
        sec = st.secrets.get("secrets", {}).get("DATABASE_URL", "")
        if sec:
            return sec
    except Exception:
        pass
    env = os.environ.get("DATABASE_URL")
    if env:
        return env
    st.error('DATABASE_URL tidak ditemukan di Streamlit Secrets.')
    st.caption("Pastikan Anda sudah menambahkan `DATABASE_URL` ke dalam blok `[secrets]` di Streamlit Cloud.")
    return None

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

DB_URL = _resolve_db_url()
if DB_URL:
    DB_ENGINE = get_engine(DB_URL)
else:
    st.stop()

# -----------------------------
# Gunakan bcrypt_sha256 untuk keamanan & kompatibilitas
# -----------------------------
pwd_context = CryptContext(
    schemes=["bcrypt_sha256", "bcrypt"],
    default="bcrypt_sha256",
    deprecated="auto"
)

# -----------------------------
# Fungsi Login
# -----------------------------
def check_password() -> bool:
    if st.session_state.get("auth_ok", False):
        return True

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
            username_cleaned = username.strip()
            with DB_ENGINE.connect() as conn:
                query = text("SELECT username, hashed_password, cabang FROM pwh.users WHERE username = :user")
                result = conn.execute(query, {"user": username_cleaned})
                user_data = result.mappings().fetchone()

            if not user_data:
                st.error("Username atau password salah.")
                return False

            password_to_check = password[:72]

            # Verifikasi hash dengan bcrypt_sha256 fallback ke bcrypt
            if pwd_context.verify(password_to_check, user_data['hashed_password']):
                st.session_state.auth_ok = True
                st.session_state.username = user_data['username']
                st.session_state.user_branch = user_data['cabang']
                st.rerun()
            else:
                st.error("Username atau password salah.")
                return False

        except Exception as e:
            st.error(f"Terjadi error saat login: {e}")
            return False

    st.stop()
    return False

# -----------------------------
# Menu Aplikasi
# -----------------------------
MENU_ITEMS = {
    "📝 Input Data Pasien": "01_pwh_input.py",
    "📊 Rekapitulasi per Kelompok Usia": "02_rekap_pwh.py",
    "🚻 Rekapitulasi per Jenis Kelamin": "03_rekap_gender.py",
    "🏥 RS Perawatan Hemofilia": "04_rs_hemofilia.py",
    "📚 Rekap Pendidikan & Pekerjaan": "05_rekap_pend_pekerjaan.py",
    "🗺️ Distribusi Pasien per Cabang": "06_distribusi_pasien.py",
    "🗺️ Rekapitulasi per Provinsi (Berdasarkan Domisili)": "07_rekap_propinsi.py",
}

ICONS = [
    "pencil-square", "bar-chart", "person-arms-up", "hospital", "book", "map", "geo-alt",
]

# -----------------------------
# Main App
# -----------------------------
def main():
    st.title("📊 Pendataan Hemofilia")
    if not check_password():
        return

    if "auth_ok" in st.session_state and not st.session_state.get("welcome_message_shown", False):
        st.success(f"Selamat datang, **{st.session_state.username}**!")
        st.session_state.welcome_message_shown = True

    with st.sidebar:
        st.markdown("### 📁 Menu")
        selection = option_menu(
            menu_title="",
            options=list(MENU_ITEMS.keys()),
            icons=ICONS[:len(MENU_ITEMS)],
            default_index=0,
            orientation="vertical",
        )

        st.divider()
        col1, col2 = st.columns([1, 1])
        with col1:
            branch_info = st.session_state.get('user_branch', 'N/A')
            if branch_info == "ALL":
                branch_info = "Admin (Semua Cabang)"
            st.caption(f"👤 {st.session_state.get('username', '')}\n🏢 {branch_info}")
        with col2:
            if st.button("Logout", use_container_width=True):
                st.session_state.clear()
                st.rerun()

    page_path = MENU_ITEMS[selection]
    try:
        runpy.run_path(page_path, run_name="__main__")
    except FileNotFoundError:
        st.error(f"File halaman tidak ditemukan: `{page_path}`")
    except Exception as e:
        st.exception(e)

    st.markdown("---")
    st.caption("© PWH Dashboard — Streamlit")

if __name__ == "__main__":
    main()
