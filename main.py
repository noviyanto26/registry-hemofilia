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
    page_title="Registry Hemofilia Dashboard",
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
    schemes=["pbkdf2_sha256", "bcrypt_sha256", "bcrypt"],
    default="pbkdf2_sha256",
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
    """Mereset soal captcha dan mengosongkan input jawaban captcha."""
    st.session_state.captcha_num1 = random.randint(1, 10)
    st.session_state.captcha_num2 = random.randint(1, 10)
    st.session_state.captcha_op = random.choice(['+', '-', '*'])
    st.session_state.captcha_input = ""

def fail_login(message: str, refresh_captcha: bool = True):
    """Menyimpan pesan error, mereset CAPTCHA agar sinkron dengan UI, lalu rerun."""
    st.session_state.login_error = message
    if refresh_captcha:
        reset_captcha()
    st.rerun()

# -----------------------------
# Fungsi Login
# -----------------------------
def check_password() -> bool:
    # Jika sudah login, langsung return True
    if st.session_state.get("auth_ok", False):
        return True

    # --- CSS CUSTOM UNTUK TAMPILAN LOGIN ---
    login_style = """
        <style>
            /* 1. Menyembunyikan Sidebar saat Login */
            [data-testid="stSidebar"] {display: none;}
            [data-testid="stSidebarCollapsedControl"] {display: none;}
            
            /* 2. BACKGROUND HALAMAN UTAMA (ABU-ABU) */
            .stApp, [data-testid="stAppViewContainer"] {
                background-color: #f0f2f6 !important;
            }
            
            /* 3. BACKGROUND CONTAINER / KOTAK LOGIN (PUTIH) */
            [data-testid="stVerticalBlockBorderWrapper"] {
                background-color: #ffffff !important;
                border-radius: 15px !important;
                border: 1px solid #e0e0e0 !important;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1) !important;
                padding: 30px !important;
            }
            
            /* 4. MENGHILANGKAN PADDING BAWAAN */
            .block-container {
                padding-top: 5rem !important;
                max-width: 100% !important;
            }
            
            /* Opsional: Input field styling */
            div[data-baseweb="input"] > div {
                background-color: #f8f9fa !important;
                border-radius: 8px !important;
            }
        </style>
    """
    st.markdown(login_style, unsafe_allow_html=True)

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
    col1, col2, col3 = st.columns([1, 1, 1])

    with col2:
        with st.container(border=True):
            # --- JUDUL DITENGAHKAN ---
            st.markdown(
                """
                <div style="text-align: center; margin-bottom: 20px;">
                    <div style="font-size: 60px;">🩸</div>
                    <h2 style="margin-bottom: 0px; padding-bottom: 0px; color: #333;">Login Dashboard</h2>
                    <p style="font-size: 16px; color: #666; margin-top: 5px; font-weight: 500;">Registry Hemofilia</p>
                </div>
                """,
                unsafe_allow_html=True
            )

            st.info("🔐 Silakan masukkan username dan password Anda.")

            # Tampilkan pesan error dari percobaan login sebelumnya (jika ada)
            if st.session_state.get("login_error"):
                st.error(st.session_state.pop("login_error"))

            # --- FORM INPUT ---
            with st.form(key="login_form", clear_on_submit=False):
                username = st.text_input("Username", key="login_username")
                password = st.text_input("Password", type="password", key="login_password")
                
                st.markdown("---")
                
                # UI CAPTCHA
                captcha_label = f"**Keamanan:** Hitung {num1} {op} {num2} = ?"
                captcha_input = st.text_input(
                    captcha_label,
                    key="captcha_input",
                    help="Verifikasi keamanan."
                )
                
                # Tombol Submit
                login_submitted = st.form_submit_button("Masuk", type="primary", use_container_width=True)

    # Logika Validasi
    if login_submitted:
        # 1. Validasi Input Kosong
        if not username or not password or not captcha_input.strip():
            st.error("Username, Password, dan CAPTCHA wajib diisi.")
            st.stop()

        # 2. Validasi CAPTCHA
        try:
            if int(captcha_input.strip()) != correct_answer:
                fail_login("Jawaban CAPTCHA salah. Silakan coba lagi.", refresh_captcha=True)
        except ValueError:
            fail_login("CAPTCHA harus berupa angka.", refresh_captcha=False)

        # 3. Validasi Database
        try:
            username_cleaned = username.strip()
            with DB_ENGINE.connect() as conn:
                query = text("SELECT username, hashed_password, cabang FROM pwh.users WHERE username = :user")
                result = conn.execute(query, {"user": username_cleaned})
                user_data = result.mappings().fetchone()

            if not user_data:
                fail_login("Username atau password salah.", refresh_captcha=True)

            # Verifikasi hash
            if pwd_context.verify(password, user_data['hashed_password']):
                st.session_state.auth_ok = True
                st.session_state.username = user_data['username']
                st.session_state.user_branch = user_data['cabang']
                
                for key in ('captcha_num1', 'captcha_num2', 'captcha_op', 'captcha_input', 'login_error'):
                    st.session_state.pop(key, None)
                
                st.rerun()
            else:
                fail_login("Username atau password salah.", refresh_captcha=True)

        except Exception as e:
            st.error(f"Terjadi error saat login: {e}")
            st.stop()

    st.stop()
    return False

# -----------------------------
# Definisi Menu & Icon Lengkap (ADMIN)
# -----------------------------
FULL_MENU_ITEMS = {
    "📝 Input Data Hemofilia": "01_pwh_input.py",
    "📋 Tampil Data": "01a_tampil_data.py",
    "📊 Rekapitulasi per Kelompok Usia": "02_rekap_pwh.py",
    "🚻 Rekapitulasi per Jenis Kelamin": "03_rekap_gender.py",
    "🏥 RS Perawatan Hemofilia": "04_rs_hemofilia.py",
    "📚 Rekap Pendidikan & Pekerjaan": "05_rekap_pend_pekerjaan.py",
    "🗺️ Distribusi Hemofilia per Cabang": "06_distribusi_pasien.py",
    "🗺️ Rekapitulasi per Provinsi (Berdasarkan Domisili)": "07_rekap_propinsi.py",
    "🗺️ Distribusi Hemofilia per RS Penangan": "08_distribusi_rs.py",
}

FULL_ICONS = [
    "pencil-square", "table", "bar-chart", "person-arms-up", "hospital", 
    "book", "map", "geo-alt", "building"
]

# -----------------------------
# Main App
# -----------------------------
def main():
    # Cek Login
    if not check_password():
        return

    # --- KODE DI BAWAH HANYA JALAN JIKA SUDAH LOGIN ---
    
    # CSS Reset (Agar background kembali putih saat masuk dashboard)
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] {
            background-color: white; /* Kembali putih di dashboard */
        }
        </style>
        """, 
        unsafe_allow_html=True
    )

    if "auth_ok" in st.session_state and not st.session_state.get("welcome_message_shown", False):
        st.success(f"Selamat datang, **{st.session_state.username}**!")
        st.session_state.welcome_message_shown = True

    # --- LOGIKA HAK AKSES MENU (DIPERBARUI) ---
    user_branch = st.session_state.get('user_branch', 'N/A')
    
    if user_branch == 'ALL':
        # Admin dapat melihat semua menu
        current_menu = FULL_MENU_ITEMS
        current_icons = FULL_ICONS
        role_label = "Admin (Semua Cabang)"
    else:
        # User selain Admin (User Cabang)
        current_menu = {
            "📝 Input Data Hemofilia": "01_pwh_input.py",
            "📋 Tampil Data Hemofilia": "01a_tampil_data.py",
            "🏥 RS Perawatan Hemofilia": "04a_rs_perawatan_hemofilia.py"
        }
        current_icons = ["pencil-square", "table", "hospital"]
        role_label = user_branch

    with st.sidebar:
        st.markdown("### 📁 Menu")
        
        selection = option_menu(
            menu_title="",
            options=list(current_menu.keys()),
            icons=current_icons[:len(current_menu)],
            default_index=0,
            orientation="vertical",
        )

        st.divider()
        col1, col2 = st.columns([1, 1])
        with col1:
            st.caption(f"👤 {st.session_state.get('username', '')}\n🏢 {role_label}")
        with col2:
            if st.button("Logout", use_container_width=True):
                st.session_state.clear()
                st.rerun()

    page_path = current_menu[selection]
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
