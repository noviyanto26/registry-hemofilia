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
# Fungsi Login (FIXED WIDTH)
# -----------------------------
def check_password() -> bool:
    # Jika sudah login, langsung return True
    if st.session_state.get("auth_ok", False):
        return True

    # --- CSS Untuk menyembunyikan Sidebar saat Login ---
    hide_sidebar_style = """
        <style>
            [data-testid="stSidebar"] {display: none;}
            [data-testid="stSidebarCollapsedControl"] {display: none;}
        </style>
    """
    st.markdown(hide_sidebar_style, unsafe_allow_html=True)

    # Inisialisasi CAPTCHA jika belum ada
    generate_captcha()
    
    # Hitung jawaban yang benar berdasarkan operator
    num1 = st.session_state.captcha_num1
    num2 = st.session_state.captcha_num2
    op = st.session_state.captcha_op
    
    if op == '+':
        correct_answer = num1 + num2
    elif op == '-':
        correct_answer = num1 - num2
    else:
        correct_answer = num1 * num2

    # --- LAYOUT LOGIN DI TENGAH LAYAR ---
    # PERUBAHAN DISINI: Menggunakan rasio [2, 1, 2] agar kolom tengah lebih sempit
    col1, col2, col3 = st.columns([2, 1, 2])

    with col2:
        with st.container(border=True):
            # --- JUDUL DITENGAHKAN DENGAN HTML DIV ---
            st.markdown(
                """
                <div style="text-align: center; margin-bottom: 10px;">
                    <h2 style="margin-bottom: 0px; padding-bottom: 0px;">Login Dashboard</h2>
                    <p style="font-size: 18px; color: gray; margin-top: 5px; font-weight: 500;">Registry Hemofilia</p>
                </div>
                """,
                unsafe_allow_html=True
            )

            st.info("🔐 Silakan masukkan username dan password Anda.")

            # --- FORM INPUT ---
            with st.form(key="login_form", clear_on_submit=False):
                username = st.text_input("Username", key="login_username")
                password = st.text_input("Password", type="password", key="login_password")
                
                st.markdown("---")
                
                # UI CAPTCHA
                col_cap1, col_cap2 = st.columns([2, 1])
                
                with col_cap1:
                    captcha_label = f"**Keamanan:** Hitung {num1} {op} {num2} = ?"
                    captcha_input = st.text_input(captcha_label, key="captcha_input", help="Jawab pertanyaan matematika ini.")
                
                with col_cap2:
                    st.write("") 
                    st.write("") 
                
                login_submitted = st.form_submit_button("Masuk", type="primary", use_container_width=True)

    # Logika Validasi berjalan HANYA jika tombol submit ditekan
    if login_submitted:
        # 1. Validasi Input Kosong
        if not username or not password or not captcha_input:
            st.error("Username, Password, dan CAPTCHA wajib diisi.")
            return False

        # 2. Validasi CAPTCHA
        try:
            if int(captcha_input) != correct_answer:
                st.error("Jawaban CAPTCHA salah. Silakan coba lagi.")
                reset_captcha() 
                return False
        except ValueError:
            st.error("CAPTCHA harus berupa angka.")
            return False

        # 3. Validasi Database
        try:
            username_cleaned = username.strip()
            with DB_ENGINE.connect() as conn:
                query = text("SELECT username, hashed_password, cabang FROM pwh.users WHERE username = :user")
                result = conn.execute(query, {"user": username_cleaned})
                user_data = result.mappings().fetchone()

            if not user_data:
                st.error("Username atau password salah.")
                reset_captcha() 
                return False

            password_to_check = password[:72]

            # Verifikasi hash
            if pwd_context.verify(password_to_
