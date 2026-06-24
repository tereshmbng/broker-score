"""
score_broker.py
===============
Script untuk menghitung Broker Scorecard dari data broker_research.

Tahapan per run:
  1. Buat tabel broker_scores, alpha_thresholds, broker_scorecard (jika belum ada)
  2. Hitung / perbarui alpha_thresholds dari price_history (berdasarkan periode broker_research)
  3. Untuk setiap call di broker_research:
       - Normalisasi rating → direction collapse
       - Tentukan window evaluasi (mid-cycle atau T+90)
       - Hitung Direction, Progress, Alpha, Total Score
       - INSERT baru atau UPDATE jika is_complete=0
       - Skip jika is_complete=1 (skor sudah final)
  4. Hitung ulang broker_scorecard (adjusted_score, hit_rate, avg_alpha, dll)
  5. Tampilkan preview leaderboard

Cara pakai:
    python score_broker.py

Aman dijalankan kapan saja. Call yang is_complete=1 tidak akan pernah berubah.
"""

import mysql.connector
import math
import calendar
from collections import defaultdict
from datetime import datetime, date, timedelta

# ==============================================================
# CONFIG
# ==============================================================

MYSQL_HOST  = "localhost"
MYSQL_PORT  = 3306
MYSQL_DB    = "broker_research"
MYSQL_USER  = "broker_user"
MYSQL_PASS  = "broker123"

MIN_HOLDING_DAYS   = 21     # minimum hari untuk bisa diskor
MIN_CALLS_GATE     = 10     # minimum scored_calls untuk masuk leaderboard
PROGRESS_SKIP_PCT  = 3.0    # |implied_return| < 3% → progress di-skip (skor netral 3)

RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

# ==============================================================
# RATING NORMALIZATION MAP
# ==============================================================

RATING_MAP = {
    # Skor 5 — Strong Buy
    "BUY": 5, "STRONG BUY": 5, "TRADING BUY": 5,
    # Skor 4 — Buy
    "ADD": 4, "ACCUMULATE": 4, "OVERWEIGHT": 4, "MODERATE BUY": 4,
    # Skor 3 — Hold
    "HOLD": 3, "NEUTRAL": 3, "MARKET PERFORM": 3, "EQUAL WEIGHT": 3,
    # Skor 2 — Reduce
    "REDUCE": 2, "TAKE PROFIT": 2, "UNDERWEIGHT": 2, "UNDERPERFORM": 2,
    # Skor 1 — Sell
    "SELL": 1, "STRONG SELL": 1,
}

# ==============================================================
# DDL — 3 TABEL BARU
# ==============================================================

DDL_BROKER_SCORES = """
CREATE TABLE IF NOT EXISTS broker_scores (
    id                   INT            AUTO_INCREMENT PRIMARY KEY,
    broker_research_id   INT            NOT NULL,
    quarter              VARCHAR(10)    NOT NULL,
    evaluation_quarter   VARCHAR(10)    NOT NULL,
    call_start_date      DATE           NOT NULL,
    call_end_date        DATE,
    holding_days         INT,
    normalized_rating    TINYINT,
    direction_label      VARCHAR(10),
    start_price          DECIMAL(15,2),
    end_price            DECIMAL(15,2),
    start_jci            DECIMAL(15,2),
    end_jci              DECIMAL(15,2),
    stock_return_pct     DECIMAL(10,4),
    jci_return_pct       DECIMAL(10,4),
    alpha_pct            DECIMAL(10,4),
    implied_return_pct   DECIMAL(10,4),
    expected_return_pct  DECIMAL(10,4),
    progress_ratio_pct   DECIMAL(12,4),
    direction_score      TINYINT,
    progress_score       TINYINT,
    alpha_score          TINYINT,
    total_score          DECIMAL(6,4),
    is_complete          TINYINT(1)     DEFAULT 0,
    flag                 VARCHAR(200),
    scored_at            DATETIME       DEFAULT CURRENT_TIMESTAMP
                                        ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_br_eval (broker_research_id, evaluation_quarter),
    INDEX idx_quarter   (quarter),
    INDEX idx_eval_q    (evaluation_quarter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_ALPHA_THRESHOLDS = """
CREATE TABLE IF NOT EXISTS alpha_thresholds (
    id             INT            AUTO_INCREMENT PRIMARY KEY,
    quarter        VARCHAR(10)    NOT NULL,
    p20            DECIMAL(10,4),
    p40            DECIMAL(10,4),
    p60            DECIMAL(10,4),
    p80            DECIMAL(10,4),
    median_alpha   DECIMAL(10,4),
    stddev_alpha   DECIMAL(10,4),
    total_tickers  INT,
    calculated_at  DATETIME       DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_quarter (quarter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_BROKER_SCORECARD = """
CREATE TABLE IF NOT EXISTS broker_scorecard (
    id                   INT            AUTO_INCREMENT PRIMARY KEY,
    broker               VARCHAR(100)   NOT NULL,
    quarter              VARCHAR(10)    NOT NULL,
    total_calls          INT,
    scored_calls         INT,
    excluded_calls       INT,
    avg_score            DECIMAL(6,4),
    std_dev_score        DECIMAL(6,4),
    adjusted_score       DECIMAL(6,4),
    avg_alpha_pct        DECIMAL(10,4),
    hit_rate_pct         DECIMAL(5,2),
    avg_progress_ratio   DECIMAL(10,4),
    meets_minimum        TINYINT(1)     DEFAULT 0,
    `rank`               INT,
    calculated_at        DATETIME       DEFAULT CURRENT_TIMESTAMP
                                        ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_broker_quarter (broker, quarter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ==============================================================
# DATABASE
# ==============================================================

def connect_mysql():
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
        user=MYSQL_USER, password=MYSQL_PASS, charset="utf8mb4"
    )

def init_tables(conn):
    cur = conn.cursor()
    cur.execute(DDL_BROKER_SCORES)
    cur.execute(DDL_ALPHA_THRESHOLDS)
    cur.execute(DDL_BROKER_SCORECARD)
    conn.commit()
    cur.close()

# ==============================================================
# HELPERS — RATING & DIRECTION
# ==============================================================

def normalize_rating(recommendation):
    """Normalisasi rekomendasi broker ke skala 1-5. Return None jika tidak dikenali."""
    if not recommendation:
        return None
    return RATING_MAP.get(recommendation.strip().upper())

def get_direction(normalized_rating):
    """Collapse skor 1-5 ke direction label: BULLISH / NEUTRAL / BEARISH."""
    if normalized_rating is None:
        return None
    if normalized_rating >= 4:
        return "BULLISH"
    elif normalized_rating == 3:
        return "NEUTRAL"
    else:
        return "BEARISH"

# ==============================================================
# HELPERS — HARGA
# ==============================================================

def get_price(conn, ticker, target_date):
    """
    Ambil harga penutupan terdekat pada atau sebelum target_date.
    Return: (close_float, actual_date) atau None jika tidak ada.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT close, date FROM price_history
        WHERE ticker = %s AND date <= %s
        ORDER BY date DESC LIMIT 1
    """, (ticker, target_date))
    row = cur.fetchone()
    cur.close()
    if row and row[0]:
        return (float(row[0]), row[1])
    return None

def get_max_price_date(conn):
    """Ambil tanggal harga terbaru yang tersedia (dari saham non-IHSG)."""
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) FROM price_history WHERE ticker != '^JKSE'")
    result = cur.fetchone()[0]
    cur.close()
    return result

# ==============================================================
# HELPERS — KUARTAL
# ==============================================================

def derive_quarter(dt):
    """Konversi tanggal ke label kuartal. Contoh: 2026-05-15 → Q2-2026."""
    q = (dt.month - 1) // 3 + 1
    return f"Q{q}-{dt.year}"

def get_prev_quarter(quarter):
    """
    Ambil kuartal sebelumnya.
    Contoh: Q3-2026 → Q2-2026, Q1-2026 → Q4-2025.
    """
    q    = int(quarter[1])
    year = int(quarter[3:])
    if q == 1:
        return f"Q4-{year - 1}"
    return f"Q{q - 1}-{year}"

def quarter_end_date(quarter):
    """Return last calendar day of a quarter. Q2-2026 → 2026-06-30."""
    q    = int(quarter[1])
    year = int(quarter[3:])
    month = q * 3  # Q1→3, Q2→6, Q3→9, Q4→12
    _, last_day = calendar.monthrange(year, month)
    return date(year, month, last_day)

def get_next_quarter(quarter):
    """Q3-2026 → Q4-2026, Q4-2026 → Q1-2027."""
    q    = int(quarter[1])
    year = int(quarter[3:])
    if q == 4:
        return f"Q1-{year + 1}"
    return f"Q{q + 1}-{year}"

def get_active_quarters(call, price_end):
    """
    Return list of (evaluation_quarter, eval_end_date) for all quarters
    where this call is still active (1-year horizon).

    A call is active in quarter Q if it started before end of Q
    and hasn't been replaced before start of Q.
    """
    start     = call["report_date"]
    next_call = call["next_call_date"]
    ideal_end = start + timedelta(days=365)

    # Natural end of this call view (capped by available price data)
    if next_call:
        view_end = min(next_call, price_end)
    else:
        view_end = min(ideal_end, price_end)

    if view_end <= start:
        return []

    result = []
    q = derive_quarter(start)

    while True:
        q_end    = quarter_end_date(q)
        eval_end = min(q_end, view_end)
        result.append((q, eval_end))
        if q_end >= view_end:
            break
        q = get_next_quarter(q)

    return result

# ==============================================================
# SCORING FUNCTIONS
# ==============================================================

def score_direction(actual_return_pct, direction):
    """
    Hitung Direction Score (1-5).
    actual_return_pct: float dalam % (contoh: +6.0, -9.2).
    """
    r = actual_return_pct
    if direction == "BULLISH":
        if r >  10:  return 5
        elif r >  3: return 4
        elif r > -3: return 3
        elif r > -10: return 2
        else:         return 1
    elif direction == "BEARISH":
        if r < -10:  return 5
        elif r < -3: return 4
        elif r <  3: return 3
        elif r < 10: return 2
        else:         return 1
    else:  # NEUTRAL
        a = abs(r)
        if a <=  3:  return 5
        elif a <=  5: return 4
        elif a <= 10: return 3
        elif a <= 15: return 2
        else:          return 1

def score_progress(implied_return_pct, actual_return_pct, holding_days):
    """
    Hitung Progress Score (1-5).
    Return: (score, flag_or_None).
    flag = 'progress_skipped' jika |implied_return| < PROGRESS_SKIP_PCT.
    """
    if abs(implied_return_pct) < PROGRESS_SKIP_PCT:
        return 3, "progress_skipped"

    expected = implied_return_pct * (holding_days / 365.0)
    if expected == 0:
        return 3, "progress_skipped"

    ratio = actual_return_pct / expected  # 1.0 = 100% on track

    if ratio >= 1.00:    return 5, None
    elif ratio >= 0.75:  return 4, None
    elif ratio >= 0.50:  return 3, None
    elif ratio >= 0.00:  return 2, None
    else:                return 1, None

def score_alpha(alpha_pct, thresholds):
    """
    Hitung Alpha Score (1-5) berdasarkan dynamic percentile thresholds.
    thresholds: (p20, p40, p60, p80) atau None.
    Return: int atau None jika thresholds tidak tersedia.
    """
    if thresholds is None:
        return None
    p20, p40, p60, p80 = thresholds
    if alpha_pct > p80:    return 5
    elif alpha_pct > p60:  return 4
    elif alpha_pct > p40:  return 3
    elif alpha_pct > p20:  return 2
    else:                   return 1

def calc_total_score(d_score, p_score, a_score):
    """
    Weighted average: Direction 20% + Progress 20% + Alpha 60%.
    Jika Alpha NULL: redistribute ke Direction 50% + Progress 50%.
    """
    if a_score is None:
        return round((d_score * 0.50) + (p_score * 0.50), 4)
    return round((d_score * 0.20) + (p_score * 0.20) + (a_score * 0.60), 4)

# ==============================================================
# ALPHA THRESHOLDS
# ==============================================================

def get_alpha_thresholds(conn, quarter):
    """
    Ambil alpha thresholds untuk kuartal tertentu dari alpha_thresholds.
    Return: (p20, p40, p60, p80) atau None jika tidak ada.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT p20, p40, p60, p80 FROM alpha_thresholds WHERE quarter = %s",
        (quarter,)
    )
    row = cur.fetchone()
    cur.close()
    if row:
        return tuple(float(v) for v in row)
    return None

def calc_and_store_alpha_thresholds(conn, quarter):
    """
    Hitung P20/P40/P60/P80 dari distribusi alpha price_history,
    lalu simpan ke alpha_thresholds. Periode ditentukan dari broker_research.
    Return: (p20, p40, p60, p80) atau None jika data tidak cukup.
    """
    cur = conn.cursor()

    # Tentukan periode dari broker_research
    cur.execute("""
        SELECT MIN(report_date), MAX(report_date)
        FROM broker_research
        WHERE report_date IS NOT NULL
    """)
    br_min, br_max = cur.fetchone()

    if not br_min:
        cur.close()
        return None

    # Batas harga tersedia
    cur.execute("SELECT MAX(date) FROM price_history WHERE ticker != '^JKSE'")
    price_end = cur.fetchone()[0]
    if not price_end:
        cur.close()
        return None

    # Sinkronkan start_date dengan ketersediaan data ^JKSE
    cur.execute("SELECT MIN(date) FROM price_history WHERE ticker = '^JKSE'")
    jkse_min = cur.fetchone()[0]
    if not jkse_min:
        cur.close()
        return None

    start_date = max(br_min, jkse_min)   # pakai yang lebih baru
    end_date   = min(br_max, price_end)

    if start_date >= end_date:
        cur.close()
        return None

    # Return JCI
    jci_s = get_price(conn, "^JKSE", start_date)
    jci_e = get_price(conn, "^JKSE", end_date)
    if not jci_s or not jci_e or jci_s[0] == 0:
        cur.close()
        return None

    jci_return = (jci_e[0] - jci_s[0]) / jci_s[0] * 100

    # Return seluruh ticker dari price_history
    cur.execute("SELECT DISTINCT ticker FROM price_history WHERE ticker != '^JKSE'")
    tickers = [r[0] for r in cur.fetchall()]
    cur.close()

    alphas = []
    for ticker in tickers:
        ps = get_price(conn, ticker, start_date)
        pe = get_price(conn, ticker, end_date)
        if ps and pe and ps[0] > 0:
            stock_ret = (pe[0] - ps[0]) / ps[0] * 100
            alphas.append(stock_ret - jci_return)

    if len(alphas) < 5:
        return None

    alphas.sort()
    n = len(alphas)

    def pct(data, p):
        idx = max(0, min(int(math.ceil(len(data) * p / 100)) - 1, len(data) - 1))
        return data[idx]

    p20    = pct(alphas, 20)
    p40    = pct(alphas, 40)
    p60    = pct(alphas, 60)
    p80    = pct(alphas, 80)
    median = pct(alphas, 50)
    mean   = sum(alphas) / n
    stddev = math.sqrt(sum((x - mean) ** 2 for x in alphas) / n) if n > 1 else 0

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO alpha_thresholds
            (quarter, p20, p40, p60, p80, median_alpha, stddev_alpha, total_tickers, calculated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            p20 = VALUES(p20), p40 = VALUES(p40),
            p60 = VALUES(p60), p80 = VALUES(p80),
            median_alpha  = VALUES(median_alpha),
            stddev_alpha  = VALUES(stddev_alpha),
            total_tickers = VALUES(total_tickers),
            calculated_at = NOW()
    """, (quarter,
          round(p20, 4), round(p40, 4), round(p60, 4), round(p80, 4),
          round(median, 4), round(stddev, 4), n))
    conn.commit()
    cur.close()
    return (p20, p40, p60, p80)

# ==============================================================
# CALL WINDOWS — MID-CYCLE DETECTION
# ==============================================================

def get_calls_with_windows(conn):
    """
    Ambil satu scoring unit per direction sequence dari broker_research.

    Logika:
    - Consecutive calls dengan rec DAN TP sama = SATU view (daily update)
    - Setiap unit diwakili oleh: id & report_date PERTAMA (window start),
      recommendation & target_price TERAKHIR (most recent update)
    - next_call_date = tanggal pertama call dengan direction berbeda (window end)
      atau NULL jika view masih ongoing

    Ini memastikan:
    - Daily update tidak membuat mini windows < 21 hari
    - Setiap genuine direction change menghasilkan scoring unit baru
    Return: list of dict.
    """
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        WITH normalized AS (
            SELECT
                id, ticker, broker, recommendation, target_price, report_date,
                CASE
                    WHEN UPPER(TRIM(recommendation)) IN (
                        'BUY','STRONG BUY','TRADING BUY',
                        'ADD','ACCUMULATE','OVERWEIGHT','MODERATE BUY'
                    ) THEN 'BULLISH'
                    WHEN UPPER(TRIM(recommendation)) IN (
                        'SELL','STRONG SELL','REDUCE','TAKE PROFIT',
                        'UNDERWEIGHT','UNDERPERFORM'
                    ) THEN 'BEARISH'
                    ELSE 'NEUTRAL'
                END AS direction
            FROM broker_research
            WHERE recommendation IS NOT NULL
              AND report_date IS NOT NULL
        ),
        flagged AS (
            SELECT *,
                CASE
                    -- View baru jika rec ATAU TP berubah dari call sebelumnya
                    -- Rec/TP sama persis = daily update, bukan view baru
                    WHEN LAG(recommendation) OVER (PARTITION BY broker, ticker ORDER BY report_date) IS NULL
                      OR LAG(recommendation) OVER (PARTITION BY broker, ticker ORDER BY report_date) != recommendation
                      OR COALESCE(CAST(LAG(target_price) OVER (PARTITION BY broker, ticker ORDER BY report_date) AS CHAR), '')
                         != COALESCE(CAST(target_price AS CHAR), '')
                    THEN 1 ELSE 0
                END AS is_new_group
            FROM normalized
        ),
        grouped AS (
            SELECT *,
                SUM(is_new_group) OVER (
                    PARTITION BY broker, ticker
                    ORDER BY report_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS grp
            FROM flagged
        ),
        with_bounds AS (
            SELECT *,
                FIRST_VALUE(id)            OVER w AS grp_first_id,
                FIRST_VALUE(report_date)   OVER w AS grp_start_date,
                LAST_VALUE(recommendation) OVER w AS grp_last_rec,
                LAST_VALUE(target_price)   OVER w AS grp_last_tp,
                ROW_NUMBER()               OVER w AS rn
            FROM grouped
            WINDOW w AS (
                PARTITION BY broker, ticker, grp
                ORDER BY report_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        ),
        first_per_group AS (
            SELECT
                grp_first_id   AS id,
                ticker, broker, direction,
                grp_last_rec   AS recommendation,
                grp_last_tp    AS target_price,
                grp_start_date AS report_date
            FROM with_bounds
            WHERE rn = 1
        ),
        with_next AS (
            SELECT fp.*,
                LEAD(report_date) OVER (
                    PARTITION BY broker, ticker ORDER BY report_date
                ) AS next_call_date
            FROM first_per_group fp
        )
        SELECT id, ticker, broker, recommendation, target_price, report_date, next_call_date
        FROM with_next
        ORDER BY broker, ticker, report_date
    """)
    calls = cur.fetchall()
    cur.close()
    return calls

# ==============================================================
# SCORING PIPELINE — PER CALL
# ==============================================================

def score_and_upsert_call(conn, call, thresholds, eval_quarter, eval_end):
    """
    Score satu call untuk evaluation_quarter tertentu dan INSERT ke broker_scores.
    eval_quarter : kuartal yang sedang dievaluasi (e.g. 'Q3-2026')
    eval_end     : tanggal akhir evaluasi = min(quarter_end, view_end)
    Return: 'inserted' | 'failed'
    """
    br_id = call["id"]
    cur   = conn.cursor()

    # ── Window evaluasi sudah ditetapkan oleh caller ──────────
    report_date = call["report_date"]
    next_call   = call["next_call_date"]
    quarter     = derive_quarter(report_date)   # kuartal saat call dibuat

    call_end    = eval_end
    holding_days = (call_end - report_date).days

    # view_closed: call view berakhir dalam atau sebelum eval_end ini
    view_closed = (next_call is not None and next_call <= eval_end)

    # Filter minimum holding period:
    # - View tertutup oleh call baru (rec/TP berubah) → selalu diskor
    #   meski < 21 hari (broker officially mengubah pandangan)
    # - View masih ongoing → wajib >= 21 hari
    if holding_days < MIN_HOLDING_DAYS and not view_closed:
        cur.close()
        return "failed"

    # ── Normalisasi rating ───────────────────────────────────
    normalized = normalize_rating(call["recommendation"])
    if normalized is None:
        cur.close()
        return "failed"

    direction = get_direction(normalized)

    # ── Ambil harga saham ────────────────────────────────────
    ticker = call["ticker"]
    ps = get_price(conn, ticker, report_date)
    pe = get_price(conn, ticker, call_end)

    if not ps or not pe or ps[0] == 0:
        cur.close()
        return "failed"

    start_price  = ps[0]
    end_price    = pe[0]
    stock_return = (end_price - start_price) / start_price * 100

    # ── Ambil harga IHSG ─────────────────────────────────────
    jci_s = get_price(conn, "^JKSE", report_date)
    jci_e = get_price(conn, "^JKSE", call_end)

    start_jci   = None
    end_jci     = None
    jci_return  = None
    alpha_pct   = None
    a_score     = None
    flag_parts  = []

    if jci_s and jci_e and jci_s[0] > 0:
        start_jci  = jci_s[0]
        end_jci    = jci_e[0]
        jci_return = (end_jci - start_jci) / start_jci * 100
        alpha_pct  = stock_return - jci_return
        a_score    = score_alpha(alpha_pct, thresholds)
    else:
        flag_parts.append("no_jci_data")

    # ── Implied return & validasi ─────────────────────────────
    implied_return  = None
    expected_return = None

    if call["target_price"] and start_price > 0:
        implied_return = (float(call["target_price"]) - start_price) / start_price * 100

        # Validasi kontradiksi: SELL tapi TP > start_price
        if normalized <= 2 and implied_return > 0:
            flag_parts.append("invalid_sell_tp")
            cur.execute("""
                INSERT INTO broker_scores
                    (broker_research_id, quarter, evaluation_quarter, call_start_date, flag)
                VALUES (%s, %s, %s, %s, %s)
            """, (br_id, quarter, eval_quarter, report_date, ", ".join(flag_parts)))
            conn.commit()
            cur.close()
            return "failed"

        # Validasi kontradiksi: BUY tapi TP < start_price
        if normalized >= 4 and implied_return < 0:
            flag_parts.append("invalid_buy_tp")
            cur.execute("""
                INSERT INTO broker_scores
                    (broker_research_id, quarter, evaluation_quarter, call_start_date, flag)
                VALUES (%s, %s, %s, %s, %s)
            """, (br_id, quarter, eval_quarter, report_date, ", ".join(flag_parts)))
            conn.commit()
            cur.close()
            return "failed"

        expected_return = round(implied_return * (holding_days / 365.0), 4)

    # ── Direction Score ──────────────────────────────────────
    d_score = score_direction(stock_return, direction)

    # ── Progress Score ───────────────────────────────────────
    p_score    = 3      # default netral
    prog_ratio = None

    if implied_return is not None:
        p_score_raw, pflag = score_progress(implied_return, stock_return, holding_days)
        p_score = p_score_raw
        if pflag:
            flag_parts.append(pflag)
        elif abs(implied_return) >= PROGRESS_SKIP_PCT and expected_return:
            if expected_return != 0:
                prog_ratio = round(stock_return / (implied_return * (holding_days / 365.0)) * 100, 4)

    # ── Total Score ──────────────────────────────────────────
    total = calc_total_score(d_score, p_score, a_score)

    flag_str = ", ".join(flag_parts) if flag_parts else None

    # ── Insert ke broker_scores ─────────────────────────────
    cur.execute("""
        INSERT INTO broker_scores (
            broker_research_id, quarter, evaluation_quarter,
            call_start_date, call_end_date, holding_days,
            normalized_rating, direction_label,
            start_price, end_price, start_jci, end_jci,
            stock_return_pct, jci_return_pct, alpha_pct,
            implied_return_pct, expected_return_pct, progress_ratio_pct,
            direction_score, progress_score, alpha_score, total_score,
            flag
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s
        )
    """, (
        br_id, quarter, eval_quarter,
        report_date, call_end, holding_days,
        normalized, direction,
        round(start_price, 2), round(end_price, 2),
        round(start_jci, 2) if start_jci else None,
        round(end_jci, 2) if end_jci else None,
        round(stock_return, 4),
        round(jci_return, 4) if jci_return is not None else None,
        round(alpha_pct, 4) if alpha_pct is not None else None,
        round(implied_return, 4) if implied_return is not None else None,
        expected_return,
        prog_ratio,
        d_score, p_score, a_score, total,
        flag_str
    ))

    cur.close()
    return "inserted"

# ==============================================================
# BROKER SCORECARD — AGREGASI
# ==============================================================

def update_broker_scorecard(conn):
    """
    Hitung ulang broker_scorecard dari broker_scores.

    Logika agregasi (Opsi 1 — 1 data point per ticker):
    - Ticker tanpa direction change → skor langsung
    - Ticker dengan direction change → segmen digabung dengan
      weighted average by holding_days → 1 skor per ticker
    - N di leaderboard = jumlah ticker unik, bukan jumlah segmen

    Formula adjusted_score = avg - (std_dev / sqrt(n)).
    Return: jumlah broker yang diproses.
    """
    cur = conn.cursor(dictionary=True)

    # Ambil semua skor valid beserta ticker (untuk grouping per ticker)
    cur.execute("""
        SELECT
            br.broker,
            br.ticker,
            bs.evaluation_quarter,
            bs.total_score,
            bs.direction_score,
            bs.alpha_pct,
            bs.progress_ratio_pct,
            bs.holding_days
        FROM broker_scores bs
        JOIN broker_research br ON bs.broker_research_id = br.id
        WHERE bs.total_score IS NOT NULL
          AND (bs.flag IS NULL
               OR (bs.flag NOT LIKE '%invalid_sell_tp%'
               AND bs.flag NOT LIKE '%invalid_buy_tp%'))
        ORDER BY br.broker, br.ticker, bs.evaluation_quarter, bs.call_start_date
    """)
    scored_rows = cur.fetchall()

    # Total ticker unik per broker per quarter (dari broker_scores)
    cur.execute("""
        SELECT
            br.broker,
            bs.evaluation_quarter,
            COUNT(DISTINCT br.ticker) AS total_tickers
        FROM broker_research br
        JOIN broker_scores bs ON br.id = bs.broker_research_id
        WHERE bs.evaluation_quarter IS NOT NULL
        GROUP BY br.broker, bs.evaluation_quarter
    """)
    total_map = {(r["broker"], r["evaluation_quarter"]): r["total_tickers"]
                 for r in cur.fetchall()}

    # ── Step 1: Gabungkan segmen per broker+quarter+ticker ────────────────────
    # Setiap ticker menghasilkan 1 skor gabungan (weighted by holding_days)
    from collections import defaultdict as _dd
    ticker_groups = _dd(list)
    for row in scored_rows:
        key = (row["broker"], row["evaluation_quarter"], row["ticker"])
        ticker_groups[key].append(row)

    # ── Step 2: Hitung skor gabungan per ticker ───────────────────────────────
    broker_tickers = _dd(list)  # key: (broker, evaluation_quarter)
    for (broker, quarter, ticker), segs in ticker_groups.items():
        weights = [max(1, s["holding_days"] or 1) for s in segs]
        total_w = sum(weights)

        # Weighted combined total_score
        combined_total = sum(float(s["total_score"]) * w
                             for s, w in zip(segs, weights)) / total_w

        # Weighted combined alpha (None jika semua segmen tidak punya alpha)
        alpha_pairs = [(float(s["alpha_pct"]), w)
                       for s, w in zip(segs, weights)
                       if s["alpha_pct"] is not None]
        combined_alpha = (sum(a * w for a, w in alpha_pairs) /
                          sum(w for _, w in alpha_pairs)) if alpha_pairs else None

        # Weighted combined direction_score (untuk hit_rate per ticker)
        combined_dir = sum(float(s["direction_score"]) * w
                           for s, w in zip(segs, weights)) / total_w

        # Weighted combined progress_ratio
        prog_pairs = [(float(s["progress_ratio_pct"]), w)
                      for s, w in zip(segs, weights)
                      if s["progress_ratio_pct"] is not None]
        combined_prog = (sum(p * w for p, w in prog_pairs) /
                         sum(w for _, w in prog_pairs)) if prog_pairs else None

        broker_tickers[(broker, quarter)].append({  # quarter here = evaluation_quarter
            "total_score":        combined_total,
            "alpha_pct":          combined_alpha,
            "direction_score":    combined_dir,
            "progress_ratio_pct": combined_prog,
        })

    # ── Step 3: Agregasi per broker+quarter ──────────────────────────────────
    scorecard_rows = []
    for (broker, quarter), ticker_list in broker_tickers.items():
        n           = len(ticker_list)           # jumlah ticker unik
        total_calls = total_map.get((broker, quarter), n)
        excluded    = total_calls - n

        # Simple average across tickers (setiap ticker 1 data point)
        vals    = [t["total_score"] for t in ticker_list]
        avg     = sum(vals) / n
        std_dev = math.sqrt(
            sum((v - avg) ** 2 for v in vals) / (n - 1)
        ) if n > 1 else 0.0

        # adjusted_score = avg - (std_dev / sqrt(n))
        adjusted = avg - (std_dev / math.sqrt(n))

        # Avg alpha raw % (simple avg across tickers)
        alphas    = [t["alpha_pct"] for t in ticker_list if t["alpha_pct"] is not None]
        avg_alpha = round(sum(alphas) / len(alphas), 4) if alphas else None

        # Hit rate: % ticker dimana combined total_score >= 3
        hits     = sum(1 for t in ticker_list if t["total_score"] >= 3)
        hit_rate = round(hits / n * 100, 2)

        # Avg progress ratio
        progs    = [t["progress_ratio_pct"] for t in ticker_list
                    if t["progress_ratio_pct"] is not None]
        avg_prog = round(sum(progs) / len(progs), 4) if progs else None

        meets_min = 1 if n >= MIN_CALLS_GATE else 0

        scorecard_rows.append({
            "broker": broker, "quarter": quarter,
            "total_calls": total_calls, "scored_calls": n, "excluded_calls": excluded,
            "avg_score": round(avg, 4), "std_dev_score": round(std_dev, 4),
            "adjusted_score": round(adjusted, 4),
            "avg_alpha_pct": avg_alpha, "hit_rate_pct": hit_rate,
            "avg_progress_ratio": avg_prog, "meets_minimum": meets_min,
            "rank": None,
        })

    # Assign rank per quarter (hanya broker yang meets_minimum=1)
    quarters_seen = set(d["quarter"] for d in scorecard_rows)
    for q in quarters_seen:
        eligible = sorted(
            [d for d in scorecard_rows if d["quarter"] == q and d["meets_minimum"] == 1],
            key=lambda x: x["adjusted_score"], reverse=True
        )
        for i, d in enumerate(eligible, 1):
            d["rank"] = i

    # Hapus dulu semua data kuartal yang akan diproses
    # → mencegah broker lama dengan nama berbeda tetap tersisa
    insert_cur = conn.cursor()
    quarters_to_update = list(set(d["quarter"] for d in scorecard_rows))
    for q in quarters_to_update:
        insert_cur.execute(
            "DELETE FROM broker_scorecard WHERE quarter = %s", (q,)
        )
    conn.commit()

    for d in scorecard_rows:
        insert_cur.execute("""
            INSERT INTO broker_scorecard (
                broker, quarter,
                total_calls, scored_calls, excluded_calls,
                avg_score, std_dev_score, adjusted_score,
                avg_alpha_pct, hit_rate_pct, avg_progress_ratio,
                meets_minimum, `rank`, calculated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, NOW()
            )
            ON DUPLICATE KEY UPDATE
                total_calls        = VALUES(total_calls),
                scored_calls       = VALUES(scored_calls),
                excluded_calls     = VALUES(excluded_calls),
                avg_score          = VALUES(avg_score),
                std_dev_score      = VALUES(std_dev_score),
                adjusted_score     = VALUES(adjusted_score),
                avg_alpha_pct      = VALUES(avg_alpha_pct),
                hit_rate_pct       = VALUES(hit_rate_pct),
                avg_progress_ratio = VALUES(avg_progress_ratio),
                meets_minimum      = VALUES(meets_minimum),
                `rank`             = VALUES(`rank`),
                calculated_at      = NOW()
        """, (
            d["broker"], d["quarter"],
            d["total_calls"], d["scored_calls"], d["excluded_calls"],
            d["avg_score"], d["std_dev_score"], d["adjusted_score"],
            d["avg_alpha_pct"], d["hit_rate_pct"], d["avg_progress_ratio"],
            d["meets_minimum"], d["rank"]
        ))

    conn.commit()
    insert_cur.close()
    cur.close()
    return len(scorecard_rows)

# ==============================================================
# MAIN
# ==============================================================

def main():
    print()
    print("=" * 68)
    print(f"{BOLD}{CYAN}  BROKER SCORECARD — Score Calculator{RESET}")
    print(f"  Dijalankan : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Min holding: {MIN_HOLDING_DAYS} hari  |  Min calls gate: {MIN_CALLS_GATE}")
    print("=" * 68)

    # ── Koneksi ─────────────────────────────────────────────────
    print(f"\n  Menyambungkan ke MySQL ({MYSQL_HOST}/{MYSQL_DB})...")
    try:
        conn = connect_mysql()
    except Exception as e:
        print(f"\n{RED}ERROR koneksi MySQL:{RESET} {e}")
        input("\nTekan Enter untuk keluar...")
        return
    print(f"  {GREEN}✓ Terhubung{RESET}")

    # ── Init tabel ───────────────────────────────────────────────
    init_tables(conn)
    print(f"  {GREEN}✓ Tabel broker_scores, alpha_thresholds, broker_scorecard siap{RESET}")

    # ── Cek periode dari broker_research ─────────────────────────
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(report_date), MAX(report_date)
        FROM broker_research
        WHERE report_date IS NOT NULL
    """)
    br_min, br_max = cur.fetchone()
    cur.close()

    if not br_min:
        print(f"\n{RED}Tidak ada data di broker_research.{RESET}")
        conn.close()
        input("\nTekan Enter untuk keluar...")
        return

    current_quarter = derive_quarter(datetime.now().date())
    prev_quarter    = get_prev_quarter(current_quarter)
    price_end       = get_max_price_date(conn)

    print(f"\n  Periode broker_research : {br_min} → {br_max}")
    print(f"  Kuartal scoring         : {current_quarter}")
    print(f"  Harga tersedia sampai   : {price_end}")

    # ── STEP 1: Alpha Thresholds (selalu dihitung ulang) ─────────
    print(f"\n{'─'*68}")
    print(f"  {BOLD}Step 1 — Alpha Thresholds ({prev_quarter}) — recalculating...{RESET}")

    thresholds = calc_and_store_alpha_thresholds(conn, prev_quarter)
    if thresholds:
        p20, p40, p60, p80 = thresholds
        print(f"  {GREEN}✓ Thresholds dihitung dan disimpan{RESET}")
        print(f"    P20={p20:+.2f}%  P40={p40:+.2f}%  P60={p60:+.2f}%  P80={p80:+.2f}%")
    else:
        print(f"  {YELLOW}  Gagal menghitung thresholds — Alpha Score akan NULL{RESET}")

    # ── STEP 2: Scoring per call ─────────────────────────────────
    print(f"\n{'─'*68}")
    print(f"  {BOLD}Step 2 — Full Rescore (truncate + recalculate){RESET}")

    # Truncate semua tabel derived — full rescore setiap run
    trunc_cur = conn.cursor()
    trunc_cur.execute("TRUNCATE TABLE broker_scores")
    trunc_cur.execute("TRUNCATE TABLE broker_scorecard")
    conn.commit()
    trunc_cur.close()
    print(f"  {DIM}✓ broker_scores dan broker_scorecard di-truncate{RESET}")

    calls = get_calls_with_windows(conn)
    print(f"  Total call groups    : {len(calls)}")
    print()

    n_inserted = 0
    n_failed   = 0
    thresholds_cache = {}  # cache thresholds per prev_quarter

    for i, call in enumerate(calls, 1):
        active_quarters = get_active_quarters(call, price_end)

        if not active_quarters:
            n_failed += 1
            broker = (call["broker"] or "?")[:18]
            ticker = (call["ticker"] or "?")[:8]
            rec    = (call["recommendation"] or "?")[:12]
            print(f"  [{i:>4}/{len(calls)}]  {ticker:<8}  {broker:<18}  {rec:<14}  {YELLOW}✗ SKIP{RESET}")
            continue

        call_scored = False
        for eval_q, eval_end in active_quarters:
            # Load thresholds for this evaluation quarter
            prev_q = get_prev_quarter(eval_q)
            if prev_q not in thresholds_cache:
                thresh = get_alpha_thresholds(conn, prev_q)
                if not thresh:
                    thresh = calc_and_store_alpha_thresholds(conn, prev_q)
                thresholds_cache[prev_q] = thresh
            thresh = thresholds_cache[prev_q]

            result = score_and_upsert_call(conn, call, thresh, eval_q, eval_end)
            if result == "inserted":
                call_scored = True

        if call_scored:
            n_inserted += 1
            status = f"{GREEN}✓ SCORED ({len(active_quarters)} quarter){RESET}"
        else:
            n_failed += 1
            status = f"{YELLOW}✗ SKIP{RESET}"

        broker = (call["broker"] or "?")[:18]
        ticker = (call["ticker"] or "?")[:8]
        rec    = (call["recommendation"] or "?")[:12]
        print(f"  [{i:>4}/{len(calls)}]  {ticker:<8}  {broker:<18}  {rec:<14}  {status}")

    conn.commit()

    # ── STEP 3: Broker Scorecard ─────────────────────────────────
    print(f"\n{'─'*68}")
    print(f"  {BOLD}Step 3 — Update Broker Scorecard{RESET}")

    n_brokers = update_broker_scorecard(conn)
    print(f"  {GREEN}✓ {n_brokers} broker diperbarui di broker_scorecard{RESET}")

    # ── Preview Leaderboard ──────────────────────────────────────
    print(f"\n{'─'*68}")
    # ── Format leaderboard ──────────────────────────────────────
    W        = 72
    date_str = datetime.now().strftime("%d %b %Y")
    lhs      = f"  Broker Scorecard — {current_quarter}"
    rhs      = f"Diperbarui: {date_str}"
    gap      = max(1, W - len(lhs) - len(rhs))

    print(f"\n{'─'*W}")
    print(f"{BOLD}{lhs}{RESET}{' ' * gap}{DIM}{rhs}{RESET}")
    print(f"{'─'*W}\n")
    print(f"  {DIM}  {'':4}  {'Broker':<22}  {'Score':>6}  {'Avg Alpha':>10}  {'Hit Rate':>9}  Calls{RESET}")
    print(f"  {'─'*66}")

    cur = conn.cursor()
    cur.execute("""
        SELECT broker, scored_calls, avg_score, std_dev_score,
               adjusted_score, avg_alpha_pct, hit_rate_pct,
               meets_minimum, `rank`
        FROM broker_scorecard
        WHERE quarter = %s
        ORDER BY COALESCE(`rank`, 9999), adjusted_score DESC
        -- NOTE: broker_scorecard.quarter stores evaluation_quarter
    """, (current_quarter,))
    rows = cur.fetchall()
    cur.close()

    for row in rows:
        broker, n, avg, std, adj, alpha, hitrate, meets, rank = row

        rank_str = f"#{rank}" if rank else "—"
        adj_f    = float(adj)    if adj    is not None else 0.0
        alpha_f  = float(alpha)  if alpha  is not None else None
        hit_f    = float(hitrate) if hitrate is not None else None

        if not meets:
            insuf_plain = "insufficient data"
            insuf_col   = f"{YELLOW}{insuf_plain}{RESET}"
            pad         = " " * max(0, 10 - len(insuf_plain))
            print(f"  {rank_str:<4}  {broker:<22}  {insuf_col}{pad}  {'—':>10}  {'—':>8}  {n} calls")
            continue

        # Score color: top 3 = green, next 3 = cyan, rest = dim
        score_plain = f"{adj_f:.2f}"
        if rank and rank <= 3:
            score_col = f"{GREEN}{BOLD}{score_plain}{RESET}"
        elif rank and rank <= 6:
            score_col = f"{CYAN}{score_plain}{RESET}"
        else:
            score_col = f"{DIM}{score_plain}{RESET}"

        # Alpha color: positive = green, negative = red
        if alpha_f is not None:
            alpha_plain = f"{alpha_f:+.2f}%"
            if alpha_f > 0.5:
                alpha_col = f"{GREEN}{alpha_plain}{RESET}"
            elif alpha_f < -0.5:
                alpha_col = f"{RED}{alpha_plain}{RESET}"
            else:
                alpha_col = alpha_plain
        else:
            alpha_plain = "N/A"
            alpha_col   = alpha_plain

        hit_str = f"{hit_f:.2f}%" if hit_f is not None else "N/A"

        # Manual padding (ANSI codes add invisible chars, can't use f-string width)
        score_pad = " " * max(0, 6 - len(score_plain))
        alpha_pad = " " * max(0, 10 - len(alpha_plain))

        print(f"  {rank_str:<4}  {broker:<22}  {score_pad}{score_col}  {alpha_pad}{alpha_col}  {hit_str:>9}  {n} calls")

    print()
    conn.close()

    # ── Ringkasan ────────────────────────────────────────────────
    print(f"\n{'═'*68}")
    print(f"  {BOLD}{CYAN}SELESAI{RESET}")
    print(f"{'═'*68}")
    print(f"  {GREEN}Berhasil diskor  : {n_inserted}{RESET}")
    print(f"  {YELLOW}Skip (gagal)     : {n_failed}{RESET}")
    print(f"  Broker terskor   : {n_brokers}")
    print(f"{'═'*68}\n")
    input("Tekan Enter untuk keluar...")


if __name__ == "__main__":
    main()