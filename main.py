# main.py (gabungan)
import runpy
import streamlit as st
from streamlit_option_menu import option_menu

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
# Auth sederhana via st.secrets
# -----------------------------
def check_password() -> bool:
    # Pastikan secrets tersedia
    if "credentials" not in st.secrets or "usernames" not in st.secrets["credentials"]:
        st.error("❌ Konfigurasi kredensial di secrets.toml tidak ditemukan.")
        st.caption(
            "Tambahkan di Secrets (Streamlit Cloud) / .streamlit/secrets.toml (lokal):\n"
            "[credentials]\n  [credentials.usernames]\n    admin = \"passwordAnda\""
        )
        return False

    users = st.secrets["credentials"]["usernames"]  # dict: {username: password}

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
        expected_pw = users.get(username)
        if expected_pw and password == str(expected_pw):
            st.session_state.auth_ok = True
            st.session_state.username = username
            st.success(f"Selamat datang, **{username}**!")
            return True
        else:
            st.error("Username atau password salah.")
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
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # Tambahan baru: Rekap per Provinsi (mengarah ke file 07_rekap_propinsi.py)
    "🗺️ Rekapitulasi per Provinsi (Berdasarkan Domisili)": "07_rekap_propinsi.py",
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
}

ICONS = [
    "pencil-square",   # 📝
    "bar-chart",       # 📊
    "person-arms-up",  # 🚻
    "hospital",        # 🏥
    "book",            # 📚
    "map",             # 🗺️ Kota
    "geo-alt",         # 🗺️ Provinsi (ikon baru untuk item yang ditambahkan)
]

# -----------------------------
# App
# -----------------------------
def main():
    st.title("📊 Pendataan Hemofilia")
    # Cek login
    if not check_password():
        return

    # Sidebar header + tombol logout
    with st.sidebar:
        st.markdown("### 📁 Menu")
        selection = option_menu(
            menu_title="",  # minimalis
            options=list(MENU_ITEMS.keys()),
            icons=ICONS[:len(MENU_ITEMS)],  # pastikan panjang sesuai
            default_index=0,
            orientation="vertical",
        )

        st.divider()
        col1, col2 = st.columns([1, 1])
        with col1:
            st.caption(f"👤 {st.session_state.get('username', '')}")
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
