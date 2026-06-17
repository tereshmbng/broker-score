"""
manual_scores_app.py
====================
Form input Sales Service & Corporate Access per kuartal.
Tim SAM isi nilai di sini, data langsung masuk MySQL.

Cara jalankan lokal:
    streamlit run manual_scores_app.py
"""

import math
import streamlit as st
import pandas as pd
import mysql.connector
from datetime import date

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SAM — Manual Scores",
    page_icon="📊",
    layout="wide"
)

# ── BOBOT ─────────────────────────────────────────────────────────────────────
# Manual: Sales Service 20% + Corporate Access 25% = 45%
# Research 55% diambil otomatis dari broker_score.py
WEIGHTS = {
    "communication":    0.10,
    "trading_ideas":    0.10,
    "corporate_access": 0.25,
    "research_quality": 0.55,
}

LABELS = {
    "communication":    "Communication",
    "trading_ideas":    "Trading Ideas",
    "corporate_access": "Corporate Access",
    "research_quality": "Research Quality (otomatis — 55%)",
}

# ── DAFTAR BROKER ─────────────────────────────────────────────────────────────
BROKERS = [
    "Bahana Sekuritas",
    "BCA Sekuritas",
    "BNI Sekuritas",
    "BRI Danareksa Sekuritas",
    "CGSI Sekuritas Indonesia",
    "Indo Premier Sekuritas",
    "Mandiri Sekuritas",
    "Maybank Sekuritas Indonesia",
    "Mirae Asset Sekuritas Indonesia",
    "RHB Sekuritas Indonesia",
    "Sinarmas Sekuritas",
    "Trimegah Sekuritas",
    "Verdhana Sekuritas Indonesia",
]

# ── MAPPING NAMA BROKER ───────────────────────────────────────────────────────
# Nama di manual_scores → nama di broker_scorecard
BROKER_MAPPING = {
    "Bahana Sekuritas":               "Bahana",
    "BCA Sekuritas":                  "BCAS",
    "BNI Sekuritas":                  "BNIS",
    "BRI Danareksa Sekuritas":        "BRI Danareksa",
    "CGSI Sekuritas Indonesia":       "CGS",
    "Indo Premier Sekuritas":         "Indopremier",
    "Mandiri Sekuritas":              "Mandiri",
    "Maybank Sekuritas Indonesia":    "Maybank",
    "Mirae Asset Sekuritas Indonesia":"Mirae",
    "RHB Sekuritas Indonesia":        "RHB",
    "Sinarmas Sekuritas":             "Sinarmas",
    "Trimegah Sekuritas":             "Trimegah",
    "Verdhana Sekuritas Indonesia":   "Verdhana",
}

# ── MySQL CONNECTION ──────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    cfg = st.secrets["mysql"]
    return mysql.connector.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        charset="utf8mb4",
        autocommit=True,
    )

def run_query(sql, params=None, fetch=True):
    conn = get_engine()
    if not conn.is_connected():
        conn.reconnect()
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params or ())
    if fetch:
        result = cur.fetchall()
        cur.close()
        return result
    cur.close()

def init_table():
    run_query("""
        CREATE TABLE IF NOT EXISTS manual_scores (
            id               INT AUTO_INCREMENT PRIMARY KEY,
            broker           VARCHAR(100) NOT NULL,
            quarter          VARCHAR(10)  NOT NULL,
            communication    DECIMAL(4,2),
            trading_ideas    DECIMAL(4,2),
            corporate_access DECIMAL(4,2),
            submitted_by     VARCHAR(100),
            submitted_at     DATETIME DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_broker_quarter (broker, quarter)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """, fetch=False)

# ── HELPERS ───────────────────────────────────────────────────────────────────
def derive_quarter(d=None):
    d = d or date.today()
    q = (d.month - 1) // 3 + 1
    return f"Q{q}-{d.year}"

def quarter_options():
    today = date.today()
    opts = []
    for y in [today.year - 1, today.year, today.year + 1]:
        for q in range(1, 5):
            opts.append(f"Q{q}-{y}")
    return opts

def get_manual_scores(quarter):
    rows = run_query(
        "SELECT * FROM manual_scores WHERE quarter = %s ORDER BY broker",
        (quarter,)
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def get_research_scores(quarter):
    rows = run_query("""
        SELECT broker,
               ROUND(adjusted_score, 4) AS research_quality,
               ROUND(avg_alpha_pct, 2)  AS avg_alpha_pct,
               ROUND(hit_rate_pct, 2)   AS hit_rate_pct,
               scored_calls
        FROM broker_scorecard
        WHERE quarter = %s
    """, (quarter,))
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def upsert_score(data: dict):
    run_query("""
        INSERT INTO manual_scores
            (broker, quarter, communication, trading_ideas,
             corporate_access, submitted_by)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            communication    = VALUES(communication),
            trading_ideas    = VALUES(trading_ideas),
            corporate_access = VALUES(corporate_access),
            submitted_by     = VALUES(submitted_by),
            submitted_at     = NOW()
    """, (
        data["broker"], data["quarter"],
        data["communication"], data["trading_ideas"],
        data["corporate_access"], data["submitted_by"],
    ), fetch=False)

def delete_score(broker, quarter):
    run_query(
        "DELETE FROM manual_scores WHERE broker = %s AND quarter = %s",
        (broker, quarter), fetch=False
    )

def robust_sigmoid(series: pd.Series) -> pd.Series:
    """Robust Scaling + Sigmoid → skala 1–5."""
    if len(series) < 2 or series.nunique() == 1:
        return pd.Series([3.0] * len(series), index=series.index)
    median = series.median()
    iqr = series.quantile(0.75) - series.quantile(0.25)
    if iqr == 0:
        return pd.Series([3.0] * len(series), index=series.index)
    z = (series - median) / iqr
    return 1 + 4 / (1 + z.apply(lambda v: math.exp(-v)))

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    try:
        init_table()
    except Exception as e:
        st.error(f"❌ Koneksi database gagal: {e}")
        st.stop()

    # ── SIDEBAR ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title("SAM Broker Scores")
        st.divider()

        quarters = quarter_options()
        default_q = derive_quarter()
        sel_q = st.selectbox(
            "Kuartal",
            quarters,
            index=quarters.index(default_q) if default_q in quarters else 0
        )

        submitted_by = st.text_input("👤 Nama Pengisi", placeholder="e.g. Tere")

        st.divider()
        st.caption("**Bobot Komponen**")
        st.markdown("*Manual:*")
        for k in ["communication", "trading_ideas", "corporate_access"]:
            st.markdown(f"- {LABELS[k]}: `{int(WEIGHTS[k]*100)}%`")
        st.markdown("*Otomatis (broker_score.py):*")
        st.markdown(f"- {LABELS['research_quality']}: `{int(WEIGHTS['research_quality']*100)}%` ⚙️")

    # Header
    st.title(f"📊 Manual Scores — {sel_q}")

    # Progress
    existing_df = get_manual_scores(sel_q)
    n_filled = len(existing_df)
    n_total  = len(BROKERS)
    if n_total > 0:
        st.progress(
            n_filled / n_total,
            text=f"**{n_filled} / {n_total}** broker sudah diisi untuk {sel_q}"
        )

    st.divider()

    # ── TABS ──────────────────────────────────────────────────────────────────
    tab_input, tab_data, tab_rank = st.tabs([
        "✏️ Input Nilai",
        "📋 Lihat & Edit Data",
        "🏆 Preview Final Ranking"
    ])

    # ─────────────────────────────────────────────────────────────────────────
    # TAB 1 — INPUT
    # ─────────────────────────────────────────────────────────────────────────
    with tab_input:
        st.subheader("Input / Update Nilai Broker")

        sel_broker = st.selectbox("Pilih Broker", BROKERS, key="sel_broker")

        # Prefill jika sudah ada data
        prefill = {}
        if not existing_df.empty and sel_broker in existing_df["broker"].values:
            row = existing_df[existing_df["broker"] == sel_broker].iloc[0]
            prefill = {
                "communication":    float(row["communication"]),
                "trading_ideas":    float(row["trading_ideas"]),
                "corporate_access": float(row["corporate_access"]),
            }
            st.info(
                f"**{sel_broker}** sudah punya data untuk {sel_q}. "
                "Form berisi nilai lama — ubah lalu simpan untuk update.",
                icon="ℹ️"
            )

        st.divider()

        with st.form("score_form", clear_on_submit=False):

            # ── Sales Service ──────────────────────────────────────────
            st.markdown("#### 🤝 Sales Service")
            col1, col2 = st.columns(2)
            with col1:
                communication = st.number_input(
                    "Communication  (10%)",
                    min_value=1.0, max_value=5.0, step=0.05,
                    value=prefill.get("communication", 3.0), format="%.2f"
                )
            with col2:
                trading_ideas = st.number_input(
                    "Trading Ideas  (10%)",
                    min_value=1.0, max_value=5.0, step=0.05,
                    value=prefill.get("trading_ideas", 3.0), format="%.2f"
                )

            # ── Corporate Access ───────────────────────────────────────
            st.markdown("#### 🏢 Corporate Access")
            corporate_access = st.number_input(
                "IPO / NDR / Conferences / Company Visit / Site Visit  (25%)",
                min_value=1.0, max_value=5.0, step=0.05,
                value=prefill.get("corporate_access", 3.0), format="%.2f"
            )

            st.caption("*Research Quality (55%) diambil otomatis dari score_broker.py.*")

            submitted = st.form_submit_button(
                "💾 Simpan", use_container_width=True, type="primary"
            )

            if submitted:
                if not submitted_by.strip():
                    st.error("Isi nama pengisi di sidebar terlebih dahulu.")
                else:
                    try:
                        upsert_score({
                            "broker":           sel_broker,
                            "quarter":          sel_q,
                            "communication":    communication,
                            "trading_ideas":    trading_ideas,
                            "corporate_access": corporate_access,
                            "submitted_by":     submitted_by.strip(),
                        })
                        st.success(f"✅ Skor **{sel_broker}** untuk **{sel_q}** berhasil disimpan!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Gagal menyimpan: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # TAB 2 — LIHAT & EDIT DATA
    # ─────────────────────────────────────────────────────────────────────────
    with tab_data:
        st.subheader(f"Data Manual Scores — {sel_q}")

        df = get_manual_scores(sel_q)

        if df.empty:
            st.info(f"Belum ada data untuk {sel_q}. Isi di tab 'Input Nilai'.")
        else:
            display_df = df[[
                "broker", "communication", "trading_ideas",
                "corporate_access", "submitted_by", "submitted_at"
            ]].rename(columns={
                "broker":           "Broker",
                "communication":    "Comm.",
                "trading_ideas":    "Trading",
                "corporate_access": "Corp. Access",
                "submitted_by":     "Diisi oleh",
                "submitted_at":     "Waktu",
            })

            st.dataframe(display_df, use_container_width=True, hide_index=True)

            col_dl, col_del = st.columns([3, 1])
            with col_dl:
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Download CSV",
                    data=csv,
                    file_name=f"manual_scores_{sel_q}.csv",
                    mime="text/csv"
                )
            with col_del:
                with st.popover("🗑️ Hapus data broker"):
                    del_broker = st.selectbox(
                        "Pilih broker yang akan dihapus",
                        df["broker"].tolist(), key="del_broker"
                    )
                    if st.button("Hapus", type="primary"):
                        delete_score(del_broker, sel_q)
                        st.success(f"Data {del_broker} dihapus.")
                        st.rerun()

    # ─────────────────────────────────────────────────────────────────────────
    # TAB 3 — PREVIEW FINAL RANKING
    # ─────────────────────────────────────────────────────────────────────────
    with tab_rank:
        st.subheader(f"Preview Final Ranking — {sel_q}")

        manual_df   = get_manual_scores(sel_q)
        research_df = get_research_scores(sel_q)

        if manual_df.empty:
            st.info("Belum ada manual scores. Isi di tab 'Input Nilai'.")
            st.stop()

        if research_df.empty:
            st.warning(
                "Research scores belum tersedia. "
                "Jalankan `score_broker.py` terlebih dahulu.",
                icon="⚠️"
            )
            st.stop()

        # Mapping nama sebelum merge
        manual_df["broker_mapped"] = (
            manual_df["broker"].map(BROKER_MAPPING).fillna(manual_df["broker"])
        )

        # Left join untuk deteksi broker tanpa research data
        merged_all = manual_df.merge(
            research_df,
            left_on="broker_mapped", right_on="broker",
            how="left"
        )

        # Info broker yang di-skip
        no_research = merged_all[merged_all["research_quality"].isna()]["broker_x"].tolist()
        if no_research:
            st.info(
                f"**{len(no_research)} broker di-skip** — belum ada research data "
                f"(belum ada call yang terskor): {', '.join(no_research)}",
                icon="ℹ️"
            )

        # Hanya broker yang punya keduanya
        merged = merged_all[merged_all["research_quality"].notna()].copy()
        merged["broker"] = merged["broker_x"]
        merged = merged.drop(
            columns=["broker_x", "broker_y", "broker_mapped"], errors="ignore"
        )

        if merged.empty:
            st.warning(
                "Belum ada broker yang punya manual scores sekaligus research data.",
                icon="⚠️"
            )
            st.stop()

        # Normalisasi (Robust Scaling + Sigmoid) per kolom
        for col in ["communication", "trading_ideas", "corporate_access", "research_quality"]:
            if col in merged.columns:
                merged[f"{col}_norm"] = robust_sigmoid(merged[col].astype(float)).round(4)

        # Final Score
        merged["final_score"] = sum(
            merged[f"{col}_norm"] * w
            for col, w in WEIGHTS.items()
            if f"{col}_norm" in merged.columns
        )

        merged = merged.sort_values("final_score", ascending=False).reset_index(drop=True)
        merged.insert(0, "rank", range(1, len(merged) + 1))

        show_cols = {
            "rank":             "#",
            "broker":           "Broker",
            "final_score":      "Final Score",
            "research_quality": "Research (55%)",
            "communication":    "Comm.",
            "trading_ideas":    "Trading",
            "corporate_access": "Corp. Access",
            "scored_calls":     "Calls",
            "avg_alpha_pct":    "Alpha %",
            "hit_rate_pct":     "Hit Rate",
        }

        display = merged[
            [c for c in show_cols if c in merged.columns]
        ].rename(columns=show_cols)

        for c in ["Final Score", "Research (55%)"]:
            if c in display.columns:
                display[c] = display[c].apply(lambda x: f"{float(x):.2f}")

        st.dataframe(display, use_container_width=True, hide_index=True)
        st.caption(
            "Final Score = Robust Scaling + Sigmoid per kolom × bobot. "
            "Research Quality dari `broker_scorecard.adjusted_score`."
        )

        csv = display.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Download Ranking CSV",
            data=csv,
            file_name=f"final_ranking_{sel_q}.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    main()