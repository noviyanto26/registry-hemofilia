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
    page_title="Rekapitulasi Berdasarkan Kelompok Usia",
    page_icon="📊",
    layout="wide",
)
st.title("📊 Rekapitulasi Berdasarkan Kelompok Usia")
st.markdown(
    "Dashboard ini menampilkan rekapitulasi dan grafik pasien berdasarkan jenis"
    " hemofilia dan kelompok usia."
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


def fetch_data_from_view(_engine: Engine) -> pd.DataFrame:
    """Mengambil data pasien, usia, diagnosis, DAN CABANG (1 baris per pasien)."""
    st.info("🔄 Mengambil data terbaru dari database...")

    query = text("""
        SELECT DISTINCT ON (p.id)
            p.id,
            p.full_name,
            v.usia_tahun AS usia,
            COALESCE(NULLIF(TRIM(p.cabang), ''), 'Tanpa Cabang') AS cabang,
            d.hemo_type,
            d.severity
        FROM pwh.patients p
        LEFT JOIN pwh.patients_with_age v ON p.id = v.id
        LEFT JOIN pwh.hemo_diagnoses d ON p.id = d.patient_id
        ORDER BY p.id, d.id DESC NULLS LAST;
    """)
    try:
        with _engine.connect() as connection:
            df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        st.error(f"Gagal mengambil data: {e}")
        st.info("Pastikan tabel 'pwh.patients' memiliki kolom 'cabang'.")
        return pd.DataFrame()


def get_age_group(age):
    """Mengelompokkan usia ke dalam kategori."""
    if pd.isna(age):
        return "Tanpa Data Usia"
    try:
        age = int(age)
    except (ValueError, TypeError):
        return "Tanpa Data Usia"

    if 0 <= age <= 4:
        return "0-4"
    if 5 <= age <= 13:
        return "5-13"
    if 14 <= age <= 18:
        return "14-18"
    if 19 <= age <= 44:
        return "19-44"
    if 45 <= age <= 64:
        return "45-64"
    if age >= 65:
        return ">65"
    return "Tanpa Data Usia"


def create_summary_table(
    df: pd.DataFrame, include_total_row: bool = True
) -> pd.DataFrame:
    """Membuat tabel rekapitulasi dengan mapping dan urutan perhitungan Total yang benar."""
    df = df.copy()

    # Standardisasi format teks (aman dari nilai NULL / NaN)
    df["hemo_type"] = df["hemo_type"].fillna("").astype(str).str.strip()
    df["severity"] = df["severity"].fillna("").astype(str).str.strip().str.title()

    def map_category(row):
        htype = "" if pd.isna(row["hemo_type"]) else str(row["hemo_type"]).strip()
        sev = "" if pd.isna(row["severity"]) else str(row["severity"]).strip()

        if htype.upper() == "A" or htype.lower() == "hemofilia a":
            prefix = "Hemofilia A"
        elif htype.upper() == "B" or htype.lower() == "hemofilia b":
            prefix = "Hemofilia B"
        elif htype.lower() == "vwd":
            return "vWD"
        else:
            return "Hemofilia Tipe Lain"

        if sev in ("Ringan", "Sedang", "Berat"):
            return f"{prefix} - {sev}"
        return f"{prefix} - Tidak Diketahui"

    df["hemo_category"] = df.apply(map_category, axis=1)

    # 1. Buat Pivot Table dasar
    summary = pd.pivot_table(
        df,
        index="kelompok_usia",
        columns="hemo_category",
        aggfunc="size",
        fill_value=0,
    )

    # 2. Paksakan seluruh kolom kategori agar tidak ada pasien yang terbuang
    desired_columns = [
        "Hemofilia A - Ringan",
        "Hemofilia A - Sedang",
        "Hemofilia A - Berat",
        "Hemofilia A - Tidak Diketahui",
        "Hemofilia B - Ringan",
        "Hemofilia B - Sedang",
        "Hemofilia B - Berat",
        "Hemofilia B - Tidak Diketahui",
        "Hemofilia Tipe Lain",
        "vWD",
    ]
    for col in desired_columns:
        if col not in summary.columns:
            summary[col] = 0

    summary = summary[desired_columns]

    # 3. Filter HANYA baris usia yang diinginkan
    age_order = [
        ">65",
        "45-64",
        "19-44",
        "14-18",
        "5-13",
        "0-4",
        "Tanpa Data Usia",
    ]
    summary = summary.reindex(age_order).fillna(0).astype(int)

    # 4. Hitung Total kolom ke kanan
    summary["Total"] = summary.sum(axis=1)

    # 5. Hitung baris Total bawah hanya jika diminta
    if include_total_row:
        summary.loc["Total"] = summary.sum()

    return summary


def plot_graph(summary_df: pd.DataFrame) -> plt.Figure:
    """Membuat grafik batang dari data rekapitulasi."""
    plot_df = summary_df.drop(index="Total", errors="ignore")

    fig, ax = plt.subplots(figsize=(14, 8))
    plot_df.plot(kind="bar", stacked=True, colormap="viridis", ax=ax)

    ax.set_title(
        "Rekapitulasi Pasien Hemofilia berdasarkan Jenis dan Kelompok Usia",
        fontsize=16,
    )
    ax.set_xlabel("Kelompok Usia", fontsize=12)
    ax.set_ylabel("Jumlah Pasien", fontsize=12)
    plt.xticks(rotation=45)
    ax.legend(title="Jenis Hemofilia", bbox_to_anchor=(1.05, 1), loc="upper left")
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
            is_total = str(row_data.get("kelompok_usia", "")).strip() == "Total"
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
            "REKAPITULASI PASIEN HEMOFILIA BERDASARKAN KELOMPOK USIA",
            title_format,
        )
        ws2.write(1, 0, "Cabang: Semua Cabang", subtitle_format)

        # Terapkan highlight pada baris Total (di bagian paling akhir tabel)
        if not df_rincian_cabang.empty:
            for row_idx, row_data in df_rincian_cabang.iterrows():
                if (
                    row_data.get("Cabang") == "Total"
                    or row_data.get("Kelompok Usia") == "Total"
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
            "REKAPITULASI PASIEN HEMOFILIA BERDASARKAN KELOMPOK USIA",
            title_format,
        )
        ws.write(1, 0, f"Cabang: {cabang}", subtitle_format)

        # Terapkan highlight pada baris Total (di bagian paling akhir tabel)
        if not df_excel.empty:
            for row_idx, row_data in df_excel.iterrows():
                if (
                    row_data.get("Cabang") == "Total"
                    or row_data.get("Kelompok Usia") == "Total"
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
raw_data_df = fetch_data_from_view(engine)

if raw_data_df.empty:
    st.warning("Tidak ada data yang dapat ditampilkan dari database.")
else:
    if "usia" in raw_data_df.columns:
        data_df = raw_data_df.copy()
        selected_cabang = "Semua Cabang"

        # --- LOGIKA FILTER CABANG ---
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
        data_df["kelompok_usia"] = data_df["usia"].apply(get_age_group)
        rekap_table = create_summary_table(data_df, include_total_row=True)

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
            )
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
                    df_cb["kelompok_usia"] = df_cb["usia"].apply(get_age_group)
                    # Tanpa baris Total di setiap akhir cabang
                    rekap_cb = create_summary_table(
                        df_cb, include_total_row=False
                    )

                    rekap_cb.index.name = "Kelompok Usia"
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
                grand_total["Kelompok Usia"] = ""

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
            df_excel_final.index.name = "Kelompok Usia"
            df_excel_final = df_excel_final.reset_index()
            df_excel_final.insert(0, "Cabang", selected_cabang)
            excel_data = convert_df_to_excel(df_excel_final, selected_cabang)

        cabang_clean = selected_cabang.replace(" ", "_").lower()
        st.download_button(
            label=f"📥 Download Rekapitulasi (Excel) - {selected_cabang}",
            data=excel_data,
            file_name=f"rekapitulasi_hemofilia_{cabang_clean}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.markdown("---")

        st.subheader("Grafik Visualisasi")
        fig = plot_graph(rekap_table.drop(columns="Total", errors="ignore"))
        st.pyplot(fig)
    else:
        st.error(
            "Kolom 'usia' (diharapkan dari 'usia_tahun') tidak ditemukan di"
            " view 'pwh.patients_with_age'."
        )
