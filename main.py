import runpy
import os
import random
import streamlit as st
import streamlit.components.v1 as components
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
# Google Analytics Injection
# -----------------------------
def inject_ga():
    GA_ID = "G-HM8QDXQFEH"
    ga_code = f"""
    <script async src="https://www.googletagmanager.com/gtag/js?id={GA_ID}"></script>
    <script>
      window.dataLayer = window.dataLayer || [];
      function gtag(){{dataLayer.push(arguments);}}
      gtag('js', new Date());
      gtag('config', '{GA_ID}');
    </script>
    """
    components.html(ga_code, height=0)

inject_ga()

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
# Keamanan Password
# -----------------------------
pwd_context = CryptContext(
    schemes=["bcrypt_sha256", "bcrypt"],
    default="bcrypt_sha256",
    deprecated="auto"
)

# -----------------------------
# Fungsi Helper CAPTCHA
# -----------------------------
def generate_captcha():
    """Membuat soal matematika acak dan menyimpannya di session state."""
    if 'captcha_num1' not in st.session_state:
        st.session_state.captcha_num1 = random.randint(1, 10)
        st.session_state.captcha_num2 = random.randint(1, 10)
        st.session_state.captcha_op = random.choice(['+', '-', '*'])

def reset_captcha():
    """Mereset soal captcha agar berubah setelah percobaan gagal."""
    st.session_state.captcha_num1 = random.randint(1, 10)
    st.session_state.captcha_num2 = random.randint(1, 10)
    st.session_state.captcha_op = random.choice(['+', '-', '*'])

# -----------------------------
# Fungsi Login (FIXED INDENTATION)
# -----------------------------
def check_password() -> bool:
    # Jika sudah login, langsung return True
    if st.session_state.get("auth_ok", False):
        return True

    # --- CSS Untuk menyembunyikan Sidebar saat Login ---
    hide_sidebar_style = """
        <style>
            [data-testid="st
