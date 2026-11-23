# 03_rekap_gender.py
import io
import os
import matplotlib.pyplot as plt
import pandas as pd
from pandas import ExcelWriter
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import streamlit as st

# --- Konfigurasi Halaman Streamlit ---
st.set_page_config(
    page_title="Rekapitulasi per Jenis Kelamin",
    page_icon="🚻",
    layout="wide",
)
st.title("🚻 Rekapitulasi Pasien berdasarkan Kategori dan Jenis Kelamin")
st.markdown(
    "Dashboard ini menampilkan rekapitulasi dan grafik pasien berdasarkan jenis"
    " hemofilia dan jenis kelamin (Laki-laki, Perempuan, dan Jenis Kelamin"
    " Tidak Diketahui)."
)


# --- KONEKSI DATABASE ---
def _resolve_db_url() -> str:
    """Mencari DATABASE_URL dari st.secrets atau environment variables."""
    try:
        sec = st.secrets.get("DATABASE_URL", "")
        if sec:
            return sec
    except Exception:
        pass
    env = os.environ.get("DATABASE_URL")
    if env:
        return env

    st.error(
        "DATABASE_URL tidak ditemukan. Mohon atur di `.streamlit/secrets.toml`"
        " atau sebagai environment variable."
    )
    st.code('DATABASE_URL = "postgresql://USER:PASSWORD@HOST:PORT/DATABASE"')
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


# --- FUNGSI PENGOLAHAN DATA ---


def fetch_data_for_gender(_engine: Engine) -> pd.DataFrame:
    """Mengambil data jenis kelamin pasien, cabang, dan diagnosis hemofilia (1 baris per pasien)."""
    st.info("🔄 Mengambil data terbaru dari database...")

    query = text("""
        SELECT DISTINCT ON (p.id)
            p.id,
            p.gender AS jenis_kelamin,
            COALESCE(NULLIF(TRIM(p.cabang), ''), 'Tanpa Cabang') AS cabang,
            d.hemo_type
        FROM pwh.patients p
        LEFT JOIN pwh.hemo_diagnoses d ON p.id = d.patient_id
        ORDER BY p.id, d.id DESC NULLS LAST;
    """)
    try:
        with _engine.connect() as connection:
            df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        st.error(f"Gagal mengambil data: {e}")
        st.info(
            "Pastikan tabel 'pwh.patients' memiliki kolom 'gender', 'cabang'"
            " dan 'pwh.hemo_diagnoses' memiliki kolom 'hemo_type'."
        )
        return pd.DataFrame()


def map_hemo_type_to_category(hemo_type) -> str:
    """Mengelompokkan hemo_type ke kategori yang sesuai (selaras dengan 02_rekap_pwh)."""
    htype = "" if pd.isna(hemo_type) else str(hemo_type).strip()

    if htype.upper() == "A" or htype.lower() == "hemofilia a":
        return "Hemofilia A"
    if htype.upper() == "B" or htype.lower() == "hemofilia b":
        return "Hemofilia B"
    if htype.lower() == "vwd":
        return "VWD"
    return "Hemofilia tipe lain"


def normalize_gender(val) -> str:
    """Menormalisasi nilai gender; jika kosong/tidak valid dipetakan ke 'Jenis Kelamin Tidak Diketahui'."""
    g = "" if pd.isna(val) else str(val).strip().lower()

    if g in ("laki-laki", "laki laki", "l", "male", "m"):
        return "Laki-laki"
    if g in ("perempuan", "p", "female", "f"):
        return "Perempuan"
    return "Jenis Kelamin Tidak Diketahui"


def create_gender_summary_table(
    df: pd.DataFrame, include_total_row: bool = True
) -> pd.DataFrame:
    """Membuat tabel rekapitulasi berdasarkan kategori dan jenis kelamin."""
    gender_cols = [
        "Laki-laki",
        "Perempuan",
        "Jenis Kelamin Tidak Diketahui",
    ]
    category_order = [
        "Hemofilia A",
        "Hemofilia B",
        "Hemofilia tipe lain",
        "VWD",
    ]

    # Pastikan data tidak kosong sebelum diproses
    if df.empty:
        cols = gender_cols + ["Total"]
        idx = category_order + (["Total"] if include_total_row else [])
        res = pd.DataFrame(0, index=idx, columns=cols)
        res.index.name = "Kategori"
        return res

    df = df.copy()  # Hindari SettingWithCopyWarning
    df["Kategori"] = df["hemo_type"].apply(map_hemo_type_to_category)
    df["jenis_kelamin_norm"] = df["jenis_kelamin"].apply(normalize_gender)

    # Buat pivot table
    summary = pd.pivot_table(
        df,
        index="Kategori",
        columns="jenis_kelamin_norm",
        aggfunc="size",
        fill_value=0,
    )

    # Pastikan semua kolom jenis kelamin tersedia
    for col in gender_cols:
        if col not in summary.columns:
            summary[col] = 0

    # Atur urutan baris sesuai standar rekapitulasi
    summary = summary.reindex(category_order).fillna(0).astype(int)
    summary.index.name = "Kategori"

    # Ambil kolom utama dan hitung total kolom
    final_summary = summary[gender_cols].copy()
    final_summary["Total"] = final_summary.sum(axis=1)

    # Tambahkan baris Total di bagian bawah hanya jika diminta
    if include_total_row:
        final_summary.loc["Total"] = final_summary.sum()

    return final_summary.astype(int)


def plot_gender_graph(summary_df: pd.DataFrame) -> plt.Figure:
    """Membuat grafik batang dari data rekapitulasi jenis kelamin."""
    # Hapus baris dan kolom 'Total' sebelum plotting
    plot_df = summary_df.drop(index="Total", columns="Total", errors="ignore")

    fig, ax = plt.subplots(figsize=(12, 7))
    plot_df.plot(
        kind="bar", ax=ax, color=["#1f77b4", "#ff7f0e", "#7f7f7f"]
    )

    ax.set_title(
        "Jumlah Pasien berdasarkan Kategori dan Jenis Kelamin",
        fontsize=16,
        pad=20,
    )
    ax.set_xlabel("Kategori Hemofilia", fontsize=12)
    ax.set_ylabel("Jumlah Pasien", fontsize=12)
    plt.xticks(rotation=0, ha="center")
    ax.legend(title="Jenis Kelamin")

    # Menambahkan label angka di atas setiap bar
    for container in ax.containers:
        ax.bar_label(container, label_type="edge", fontsize=10, padding=3)

    plt.tight_layout()
    return fig


def convert_all_branches_to_excel(
    rekap_nasional: pd.DataFrame, df_rincian_cabang: pd.DataFrame
) -> bytes:
    """Mengonversi rekap Semua Cabang ke Excel dengan 2 sheet:
    - Rekap Nasional: Rekap gabungan nasional dengan styling header & auto-fit kolom
    - Rincian Cabang: Rincian per cabang
    """
    output = io.BytesIO()
    df_sheet1 = rekap_nasional.reset_index()

    with ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book

        title_format = workbook.add_format({
            "bold": True,
            "font_size": 14,
            "font_name": "Arial",
            "font_color": "#1F4E78",
        })
        subtitle_format = workbook.add_format({
            "bold": True,
            "font_size": 11,
            "font_name": "Arial",
            "font_color": "#333333",
        })
        header_format = workbook.add_format({
            "bold": True,
            "font_name": "Arial",
            "font_size": 10,
            "font_color": "#FFFFFF",
            "bg_color": "#1F4E78",
            "border": 1,
            "align": "center",
            "valign": "vcenter",
        })
        cell_format = workbook.add_format({
            "font_name": "Arial",
            "font_size": 10,
            "border": 1,
        })
        total_row_format = workbook.add_format({
            "bold": True,
            "font_name": "Arial",
            "font_size": 10,
            "bg_color": "#E8F4F8",
            "border": 1,
        })

        # --- SHEET 1: REKAP GABUNGAN NASIONAL ---
        df_sheet1.to_excel(writer, sheet_name="Rekap Nasional", index=False)
        ws1 = writer.sheets["Rekap Nasional"]
        ws1.set_row(0, 24)

        # Styling header tebal dan warna latar pada baris header
        for col_idx, col_name in enumerate(df_sheet1.columns):
            ws1.write(0, col_idx, col_name, header_format)

        # Styling border dan highlight baris Total pada Rekap Nasional
        for row_idx, row_data in df_sheet1.iterrows():
            excel_row = row_idx + 1
            is_total = str(row_data.get("Kategori", "")).strip() == "Total"
            fmt = total_row_format if is_total else cell_format
            for col_idx, val in enumerate(row_data):
                ws1.write(excel_row, col_idx, "" if pd.isna(val) else val, fmt)

        # Penyesuaian lebar kolom otomatis (auto-fit) untuk Rekap Nasional
        for col_idx, col_name in enumerate(df_sheet1.columns):
            max_len = (
                max(
                    df_sheet1[col_name].fillna("").astype(str).map(len).max()
                    if not df_sheet1.empty
                    else 0,
                    len(str(col_name)),
                )
                + 3
            )
            ws1.set_column(col_idx, col_idx, min(max_len, 50))

        # --- SHEET 2: RINCIAN PER CABANG ---
        df_rincian_cabang.to_excel(
            writer, sheet_name="Rincian Cabang", index=False, startrow=3
        )
        ws2 = writer.sheets["Rincian Cabang"]

        # Tulis teks judul pada baris teratas sheet Rincian Cabang
        ws2.write(
            0,
            0,
            "REKAPITULASI PASIEN HEMOFILIA BERDASARKAN KATEGORI DAN JENIS KELAMIN",
            title_format,
        )
        ws2.write(1, 0, "Cabang: Semua Cabang", subtitle_format)

        # Terapkan highlight pada baris Total (di bagian paling akhir tabel)
        if not df_rincian_cabang.empty:
            for row_idx, row_data in df_rincian_cabang.iterrows():
                if (
                    row_data.get("Cabang") == "Total"
                    or row_data.get("Kategori") == "Total"
                ):
                    excel_row = (
                        row_idx + 4
                    )  # offset 3 baris judul + 1 baris header tabel
                    for col_idx, val in enumerate(row_data):
                        ws2.write(
                            excel_row,
                            col_idx,
                            "" if pd.isna(val) else val,
                            total_row_format,
                        )

        # Penyesuaian lebar kolom otomatis untuk Rincian Cabang
        for col_idx, col_name in enumerate(df_rincian_cabang.columns):
            max_len = (
                max(
                    df_rincian_cabang[col_name]
                    .fillna("")
                    .astype(str)
                    .map(len)
                    .max()
                    if not df_rincian_cabang.empty
                    else 0,
                    len(str(col_name)),
                )
                + 2
            )
            ws2.set_column(col_idx, col_idx, min(max_len, 50))

    return output.getvalue()


def convert_df_to_excel(
    df_excel: pd.DataFrame, cabang: str = "Semua Cabang"
) -> bytes:
    """Mengonversi DataFrame per cabang ke format Excel (xlsx) beserta komponen Headernya."""
    output = io.BytesIO()

    with ExcelWriter(output, engine="xlsxwriter") as writer:
        df_excel.to_excel(
            writer, sheet_name="Rekapitulasi", index=False, startrow=3
        )

        workbook = writer.book
        ws = writer.sheets["Rekapitulasi"]

        title_format = workbook.add_format({
            "bold": True,
            "font_size": 14,
            "font_name": "Arial",
            "font_color": "#1F4E78",
        })
        subtitle_format = workbook.add_format({
            "bold": True,
            "font_size": 11,
            "font_name": "Arial",
            "font_color": "#333333",
        })
        total_row_format = workbook.add_format({
            "bold": True,
            "bg_color": "#E8F4F8",
            "border": 1,
        })

        # Tulis teks judul pada baris teratas file Excel
        ws.write(
            0,
            0,
            "REKAPITULASI PASIEN HEMOFILIA BERDASARKAN KATEGORI DAN JENIS KELAMIN",
            title_format,
        )
        ws.write(1, 0, f"Cabang: {cabang}", subtitle_format)

        # Terapkan highlight pada baris Total (di bagian paling akhir tabel)
        if not df_excel.empty:
            for row_idx, row_data in df_excel.iterrows():
                if (
                    row_data.get("Cabang") == "Total"
                    or row_data.get("Kategori") == "Total"
                ):
                    excel_row = (
                        row_idx + 4
                    )  # offset 3 baris judul + 1 baris header tabel
                    for col_idx, val in enumerate(row_data):
                        ws.write(
                            excel_row,
                            col_idx,
                            "" if pd.isna(val) else val,
                            total_row_format,
                        )

        # Penyesuaian lebar kolom otomatis
        for col_idx, col_name in enumerate(df_excel.columns):
            max_len = (
                max(
                    df_excel[col_name].fillna("").astype(str).map(len).max()
                    if not df_excel.empty
                    else 0,
                    len(str(col_name)),
                )
                + 2
            )
            ws.set_column(col_idx, col_idx, min(max_len, 50))

    return output.getvalue()


# --- MAIN APP LOGIC ---
db_url = _resolve_db_url()
engine = get_engine(db_url)
raw_data_df = fetch_data_for_gender(engine)

if raw_data_df.empty:
    st.warning("Tidak ada data yang dapat ditampilkan dari database.")
else:
    # Salinan data untuk proses filter tampilan web
    data_df = raw_data_df.copy()
    selected_cabang = "Semua Cabang"

    # --- MODIFIKASI: Filter Berdasarkan Cabang ---
    if "cabang" in data_df.columns:
        list_cabang = ["Semua Cabang"] + sorted(
            data_df["cabang"].dropna().astype(str).unique().tolist()
        )
        selected_cabang = st.selectbox(
            "🏢 Filter Berdasarkan Cabang:", list_cabang
        )

        if selected_cabang != "Semua Cabang":
            data_df = data_df[data_df["cabang"] == selected_cabang].copy()
    else:
        st.warning("Kolom 'cabang' tidak ditemukan dalam data.")

    # Tampilan Web: tetap menampilkan baris Total di layar
    rekap_table = create_gender_summary_table(data_df, include_total_row=True)

    st.subheader(
        f"Tabel Rekapitulasi{' - ' + selected_cabang if 'cabang' in data_df.columns and selected_cabang != 'Semua Cabang' else ''}"
    )
    st.dataframe(
        rekap_table.style.apply(
            lambda x: [
                "background-color: #e8f4f8" if x.name == "Total" else ""
                for i in x
            ],
            axis=1,
        ).apply(
            lambda x: [
                "background-color: #e8f4f8" if x.name == "Total" else ""
                for i in x
            ],
            axis=0,
        ),
        use_container_width=True,
    )

    # --- LOGIKA EKSPOR EXCEL ---
    if selected_cabang == "Semua Cabang" and "cabang" in raw_data_df.columns:
        list_rekap_cabang = []
        unique_branches = sorted(
            raw_data_df["cabang"].dropna().astype(str).unique().tolist()
        )

        for cb in unique_branches:
            df_cb = raw_data_df[raw_data_df["cabang"] == cb].copy()
            if not df_cb.empty:
                # Tanpa baris Total di setiap akhir cabang
                rekap_cb = create_gender_summary_table(
                    df_cb, include_total_row=False
                )

                rekap_cb.index.name = "Kategori"
                rekap_cb = rekap_cb.reset_index()
                rekap_cb.insert(0, "Cabang", cb)
                list_rekap_cabang.append(rekap_cb)

        if list_rekap_cabang:
            df_excel_rincian = pd.concat(list_rekap_cabang, ignore_index=True)

            # Hitung & tambahkan satu baris Total keseluruhan di bagian paling akhir tabel
            numeric_cols = df_excel_rincian.select_dtypes(
                include="number"
            ).columns
            grand_total = df_excel_rincian[numeric_cols].sum().to_dict()
            grand_total["Cabang"] = "Total"
            grand_total["Kategori"] = ""

            df_excel_rincian = pd.concat(
                [df_excel_rincian, pd.DataFrame([grand_total])],
                ignore_index=True,
            )
        else:
            df_excel_rincian = pd.DataFrame()

        # Gabungkan sheet Rekap Nasional dan Rincian Cabang
        excel_data = convert_all_branches_to_excel(
            rekap_table, df_excel_rincian
        )
    else:
        # Jika memilih cabang tertentu, gunakan rekap cabang tersebut saja (dengan baris Total)
        df_excel_final = rekap_table.copy()
        df_excel_final.index.name = "Kategori"
        df_excel_final = df_excel_final.reset_index()
        df_excel_final.insert(0, "Cabang", selected_cabang)
        excel_data = convert_df_to_excel(df_excel_final, selected_cabang)

    # Format nama file dinamis dan sesuaikan nama tombol
    cabang_clean = selected_cabang.replace(" ", "_").lower()
    st.download_button(
        label=f"📥 Download Rekapitulasi (Excel) - {selected_cabang}",
        data=excel_data,
        file_name=f"rekapitulasi_jenis_kelamin_{cabang_clean}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.markdown("---")

    st.subheader("Grafik Visualisasi")
    if not rekap_table.empty:
        fig = plot_gender_graph(rekap_table)
        st.pyplot(fig)
    else:
        st.info(
            "Tidak ada data untuk ditampilkan pada grafik setelah filter"
            " diterapkan."
        )
