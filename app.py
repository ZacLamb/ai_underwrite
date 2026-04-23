import os
import io
import re
import uuid
import json
import time
import requests
import pdfplumber
import boto3
import psycopg2

from flask import Flask, request, jsonify
from anthropic import Anthropic
from psycopg2.extras import RealDictCursor
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.colors import HexColor
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Image,
    HRFlowable,
    Table,
    TableStyle,
    KeepTogether,
)
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError

app = Flask(__name__)

# ── ENV ─────────────────────────────────────────────────────────────
GHL_API_KEY = os.environ.get("GHL_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GROK_API_KEY = os.environ.get("GROK_API_KEY")

R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID")
R2_PUBLIC_URL = os.environ.get("R2_PUBLIC_URL")
R2_BUCKET = os.environ.get("R2_BUCKET", "fundara-reports")

DATABASE_URL = os.environ.get("DATABASE_URL")
GSHEET_WEBHOOK_URL = os.environ.get("GSHEET_WEBHOOK_URL")
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL")

LOGO_URL = "https://assets.cdn.filesafe.space/HD59NWC1biIA31IHm1y8/media/69bdb2b44865cdd2954821be.png"

DOWNLOAD_RETRIES = int(os.environ.get("DOWNLOAD_RETRIES", "3"))
DOWNLOAD_TIMEOUT = int(os.environ.get("DOWNLOAD_TIMEOUT", "30"))
MODEL_TIMEOUT = int(os.environ.get("MODEL_TIMEOUT", "75"))


def env_float(name, default):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return float(default)
    return float(raw)


OPENAI_INPUT_PER_M = env_float("OPENAI_INPUT_PER_M", 2.00)
OPENAI_OUTPUT_PER_M = env_float("OPENAI_OUTPUT_PER_M", 8.00)

ANTHROPIC_INPUT_PER_M = env_float("ANTHROPIC_INPUT_PER_M", 3.00)
ANTHROPIC_OUTPUT_PER_M = env_float("ANTHROPIC_OUTPUT_PER_M", 15.00)

GROK_INPUT_PER_M = env_float("GROK_INPUT_PER_M", 0.00)
GROK_OUTPUT_PER_M = env_float("GROK_OUTPUT_PER_M", 0.00)

# ── PALETTE ─────────────────────────────────────────────────────────
BG = HexColor("#0D1B2A")
CARD = HexColor("#152030")
ROW_ALT = HexColor("#1C2B3A")
HDR_BG = HexColor("#1A3550")
BORDER = HexColor("#2A3F55")
RED = HexColor("#E53935")
ORANGE = HexColor("#F57C00")
YELLOW = HexColor("#F9A825")
GREEN = HexColor("#43A047")
LBLUE = HexColor("#64B5F6")
TW = HexColor("#ECEFF1")
TG = HexColor("#90A4AE")
TL = HexColor("#B0BEC5")

W_PAGE = 7.0 * inch

# ── STYLES ──────────────────────────────────────────────────────────
def build_styles():
    s = {}

    def ps(name, **kw):
        base = dict(
            fontName="Helvetica",
            fontSize=9,
            textColor=TL,
            leading=13,
            spaceAfter=0,
            spaceBefore=0,
        )
        base.update(kw)
        s[name] = ParagraphStyle(name, **base)

    ps("h1", fontName="Helvetica-Bold", fontSize=13, textColor=TW, spaceBefore=6, spaceAfter=4, leading=17)
    ps("h2", fontName="Helvetica-Bold", fontSize=10.5, textColor=LBLUE, spaceBefore=8, spaceAfter=4, leading=14)
    ps("h3", fontName="Helvetica-Bold", fontSize=9.5, textColor=TW, spaceBefore=5, spaceAfter=3, leading=13)
    ps("body", fontSize=8.5, textColor=TL, leading=12)
    ps("body_b", fontName="Helvetica-Bold", fontSize=8.5, textColor=TW, leading=12)
    ps("bullet", fontSize=8.5, textColor=TL, leading=12, leftIndent=12)
    ps("caption", fontSize=7.5, textColor=TG, leading=10, fontName="Helvetica-Oblique", alignment=TA_CENTER, spaceAfter=4)
    ps("center", fontSize=8.5, textColor=TL, leading=12, alignment=TA_CENTER)
    ps("th", fontName="Helvetica-Bold", fontSize=8, textColor=TW, leading=11, alignment=TA_CENTER)
    ps("th_l", fontName="Helvetica-Bold", fontSize=8, textColor=TW, leading=11)
    ps("td", fontSize=8, textColor=TL, leading=11)
    ps("td_c", fontSize=8, textColor=TL, leading=11, alignment=TA_CENTER)
    ps("td_b", fontName="Helvetica-Bold", fontSize=8, textColor=TW, leading=11)
    ps("td_r", fontName="Helvetica-Bold", fontSize=8, textColor=RED, leading=11)
    ps("td_g", fontName="Helvetica-Bold", fontSize=8, textColor=GREEN, leading=11)
    ps("td_y", fontName="Helvetica-Bold", fontSize=8, textColor=YELLOW, leading=11)
    ps("td_o", fontName="Helvetica-Bold", fontSize=8, textColor=ORANGE, leading=11)
    return s


STY = build_styles()

BASE_TS = TableStyle([
    ("BACKGROUND", (0, 0), (-1, 0), HDR_BG),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [CARD, ROW_ALT]),
    ("GRID", (0, 0), (-1, -1), 0.35, BORDER),
    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ("FONTSIZE", (0, 0), (-1, 0), 8),
    ("TEXTCOLOR", (0, 0), (-1, 0), TW),
    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
    ("FONTSIZE", (0, 1), (-1, -1), 8),
    ("TEXTCOLOR", (0, 1), (-1, -1), TL),
    ("TOPPADDING", (0, 0), (-1, -1), 5),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ("LEFTPADDING", (0, 0), (-1, -1), 8),
    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
])

# ── PDF HELPERS ─────────────────────────────────────────────────────
def _draw_page(canvas, doc, logo_img=None):
    canvas.saveState()
    pw, ph = letter

    canvas.setFillColor(BG)
    canvas.rect(0, 0, pw, ph, fill=1, stroke=0)

    canvas.setFillColor(CARD)
    canvas.rect(0, ph - 42, pw, 42, fill=1, stroke=0)

    if logo_img:
        try:
            logo_img.drawOn(canvas, 0.55 * inch, ph - 36)
        except Exception:
            pass

    canvas.setFillColor(TG)
    canvas.setFont("Helvetica", 7.5)
    canvas.drawRightString(pw - 0.55 * inch, ph - 16, "AI Underwriting Report  |  CONFIDENTIAL")
    canvas.drawRightString(pw - 0.55 * inch, ph - 28, "Powered by Fundara")

    canvas.setStrokeColor(BORDER)
    canvas.setLineWidth(0.4)
    canvas.line(0.55 * inch, ph - 42, pw - 0.55 * inch, ph - 42)

    canvas.setFillColor(CARD)
    canvas.rect(0, 0, pw, 28, fill=1, stroke=0)

    canvas.setFillColor(TG)
    canvas.setFont("Helvetica", 7)
    canvas.drawString(
        0.55 * inch,
        10,
        "This report is generated by Fundara AI and is for internal use only. Not financial advice.",
    )
    canvas.drawRightString(pw - 0.55 * inch, 10, f"Page {doc.page}")
    canvas.restoreState()


def _draw_cover(canvas, doc, logo_img=None):
    canvas.saveState()
    pw, ph = letter
    canvas.setFillColor(BG)
    canvas.rect(0, 0, pw, ph, fill=1, stroke=0)
    canvas.setFillColor(LBLUE)
    canvas.rect(0, ph - 5, pw, 5, fill=1, stroke=0)
    canvas.setFillColor(CARD)
    canvas.rect(0, 0, pw, 32, fill=1, stroke=0)
    canvas.setFillColor(TG)
    canvas.setFont("Helvetica", 7)
    canvas.drawString(0.55 * inch, 11, "CONFIDENTIAL — Authorized Fundara Personnel Only")
    canvas.restoreState()


def _cell_color_style(text):
    t = text.strip().upper()
    if any(k in t for k in ("DECLINE", "CRITICAL", "NEGATIVE", "FAILED")):
        return STY["td_r"]
    if any(k in t for k in ("HIGH RISK", "OVERDRAFT", "UNSUSTAINABLE")):
        return STY["td_o"]
    if any(k in t for k in ("MEDIUM", "CAUTION", "WEAK")):
        return STY["td_y"]
    if any(k in t for k in ("APPROVE", "PASS", "GOOD", "LOW RISK", "STRONG", "CONDITIONAL APPROVAL")):
        return STY["td_g"]
    return STY["td"]


def _parse_md_bold(text):
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)


def _parse_table(table_lines):
    rows = []
    for line in table_lines:
        if re.match(r"^\|[-| :]+\|$", line.strip()):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        rows.append(cells)

    if not rows:
        return None

    ncols = max(len(r) for r in rows)
    col_w = W_PAGE / ncols
    flowable_rows = []

    for ri, row in enumerate(rows):
        while len(row) < ncols:
            row.append("")

        fcells = []
        for ci, cell in enumerate(row):
            clean = _parse_md_bold(cell)
            if ri == 0:
                sty = STY["th_l"] if ci == 0 else STY["th"]
            else:
                sty = _cell_color_style(cell)
                if ci == 0:
                    sty = STY["td_b"] if ri % 2 == 0 else STY["td"]
            fcells.append(Paragraph(clean, sty))
        flowable_rows.append(fcells)

    t = Table(flowable_rows, colWidths=[col_w] * ncols, repeatRows=1)
    t.setStyle(BASE_TS)
    return t


def _section_header(title):
    return [
        Spacer(1, 10),
        HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER),
        Spacer(1, 5),
        Paragraph(title, STY["h1"]),
    ]


def _decision_banner(text):
    u = text.upper()
    is_decline = "DECLINE" in u
    is_conditional = "CONDITIONAL APPROVAL" in u
    c = RED if is_decline else (YELLOW if is_conditional else GREEN)
    bg = HexColor("#150505") if is_decline else (HexColor("#1d1703") if is_conditional else HexColor("#051505"))

    banner = Table(
        [[Paragraph(
            text.strip(),
            ParagraphStyle("dec", fontName="Helvetica-Bold", fontSize=20, textColor=c, alignment=TA_CENTER, leading=24),
        )]],
        colWidths=[W_PAGE],
    )
    banner.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), bg),
        ("BOX", (0, 0), (-1, -1), 2.5, c),
        ("TOPPADDING", (0, 0), (-1, -1), 14),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
    ]))
    return banner


def markdown_to_flowables(markdown_text):
    story = []
    lines = markdown_text.split("\n")
    i = 0
    table_buf = []

    def flush_table():
        if table_buf:
            t = _parse_table(table_buf)
            if t:
                story.append(t)
                story.append(Spacer(1, 8))
            table_buf.clear()

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped:
            flush_table()
            story.append(Spacer(1, 5))
            i += 1
            continue

        if stripped.startswith("|"):
            table_buf.append(stripped)
            i += 1
            continue
        else:
            flush_table()

        sec_match = re.match(r"^##\s+(.+)$", stripped)
        if sec_match:
            story += _section_header(sec_match.group(1).strip())
            i += 1
            continue

        explicit_section_match = re.match(r"^(Section\s+\d+:\s+.+)$", stripped, re.IGNORECASE)
        if explicit_section_match:
            story += _section_header(explicit_section_match.group(1).strip())
            i += 1
            continue

        if stripped.startswith("### "):
            story.append(Paragraph(stripped[4:].strip(), STY["h2"]))
            story.append(Spacer(1, 4))
            i += 1
            continue

        if stripped.startswith("#### "):
            story.append(Paragraph(stripped[5:].strip(), STY["h3"]))
            story.append(Spacer(1, 3))
            i += 1
            continue

        rec_match = re.match(
            r"^\*\*RECOMMENDATION:\s*(APPROVE|CONDITIONAL APPROVAL|DECLINE)\*\*$",
            stripped,
            re.IGNORECASE,
        )
        if rec_match:
            story.append(Spacer(1, 6))
            story.append(_decision_banner(f"RECOMMENDATION: {rec_match.group(1).upper()}"))
            story.append(Spacer(1, 10))
            i += 1
            continue

        text = _parse_md_bold(stripped)
        story.append(Paragraph(text, STY["body"]))
        story.append(Spacer(1, 3))
        i += 1

    flush_table()
    return story


def convert_to_pdf(markdown_text):
    try:
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            leftMargin=0.6 * inch,
            rightMargin=0.6 * inch,
            topMargin=0.75 * inch,
            bottomMargin=0.55 * inch,
        )

        logo_img_draw = None
        logo_img_flow = None

        try:
            logo_resp = requests.get(LOGO_URL, timeout=15)
            if logo_resp.status_code == 200:
                logo_img_draw = Image(io.BytesIO(logo_resp.content), width=2.1 * inch, height=0.5 * inch)
                logo_img_flow = Image(io.BytesIO(logo_resp.content), width=3.0 * inch, height=0.72 * inch)
                logo_img_flow.hAlign = "LEFT"
        except Exception as logo_err:
            print(f"Logo load error: {logo_err}")

        story = []
        story.append(Spacer(1, 0.55 * inch))

        if logo_img_flow:
            story.append(logo_img_flow)
            story.append(Spacer(1, 0.18 * inch))

        story.append(HRFlowable(width=W_PAGE, thickness=2, color=LBLUE, spaceAfter=10))
        story.append(Paragraph(
            "AI Underwriting Report",
            ParagraphStyle("cover_title", fontName="Helvetica-Bold", fontSize=26, textColor=TW, leading=31),
        ))
        story.append(Paragraph(
            "Powered by Fundara  |  Confidential",
            ParagraphStyle("cover_sub", fontName="Helvetica", fontSize=10, textColor=TG, leading=14, spaceAfter=6),
        ))
        story.append(Paragraph(
            "fundara.co",
            ParagraphStyle("cover_brand", fontName="Helvetica-Bold", fontSize=11, textColor=LBLUE, leading=14),
        ))
        story.append(HRFlowable(width=W_PAGE, thickness=0.5, color=BORDER, spaceAfter=16))
        story.append(Spacer(1, 0.1 * inch))
        story += markdown_to_flowables(markdown_text)
        story.append(Spacer(1, 0.25 * inch))
        story.append(HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER))
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            "This report is generated by Fundara AI and is for internal use only. Not financial advice.",
            ParagraphStyle("foot", fontName="Helvetica", fontSize=7, textColor=TG, alignment=TA_CENTER),
        ))

        def on_first(canvas, doc):
            _draw_cover(canvas, doc, logo_img_draw)

        def on_later(canvas, doc):
            _draw_page(canvas, doc, logo_img_draw)

        doc.build(story, onFirstPage=on_first, onLaterPages=on_later)
        buffer.seek(0)
        return buffer.getvalue()

    except Exception as pdf_err:
        print(f"PDF conversion error: {pdf_err}")
        return None

# ── STORAGE / DOWNLOAD / EXTRACTION ────────────────────────────────
def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def upload_to_r2(pdf_bytes, contact_id):
    try:
        client = get_r2_client()
        filename = f"reports/{contact_id}_{uuid.uuid4().hex[:8]}.pdf"
        client.put_object(
            Bucket=R2_BUCKET,
            Key=filename,
            Body=pdf_bytes,
            ContentType="application/pdf"
        )
        url = f"{R2_PUBLIC_URL}/{filename}"
        print(f"PDF uploaded to R2: {url}")
        return url
    except Exception as r2_err:
        print(f"R2 upload error: {r2_err}")
        return None


def is_probably_pdf(pdf_bytes):
    return bool(pdf_bytes and pdf_bytes[:5] == b"%PDF-")


def download_pdf(url, api_key):
    if not url or str(url).strip().lower() in {"undefined", "null", ""}:
        print(f"Skipping invalid statement URL: {url}")
        return None

    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, timeout=DOWNLOAD_TIMEOUT)
            if response.status_code != 200:
                print(f"Failed to download PDF (attempt {attempt}): {response.status_code} | {url}")
                continue

            pdf_bytes = response.content
            if not is_probably_pdf(pdf_bytes):
                print(f"Downloaded file is not a valid PDF header (attempt {attempt}) | {url}")
                return None

            return pdf_bytes

        except requests.exceptions.ReadTimeout as e:
            print(f"Download timeout attempt {attempt}/{DOWNLOAD_RETRIES}: {e}")
            if attempt < DOWNLOAD_RETRIES:
                time.sleep(2 * attempt)
        except Exception as download_err:
            print(f"Download error attempt {attempt}/{DOWNLOAD_RETRIES}: {download_err}")
            if attempt < DOWNLOAD_RETRIES:
                time.sleep(1 * attempt)

    print(f"Giving up on download after {DOWNLOAD_RETRIES} attempts: {url}")
    return None


def extract_text(pdf_bytes):
    try:
        text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text() or ""
                if page_text.strip():
                    text += f"\n--- PAGE {page_num} ---\n{page_text}\n"
        return text
    except Exception as extract_err:
        print(f"Extract error: {extract_err}")
        return ""

# ── DB / COST HELPERS ──────────────────────────────────────────────
def calc_cost(input_tokens, output_tokens, input_rate_per_m, output_rate_per_m):
    return round(
        ((input_tokens / 1_000_000) * input_rate_per_m) +
        ((output_tokens / 1_000_000) * output_rate_per_m),
        6
    )


def get_db_conn():
    if not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL)


def init_db():
    if not DATABASE_URL:
        print("DATABASE_URL not set, skipping DB init")
        return

    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS underwriting_runs (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                location_id TEXT,
                contact_id TEXT,
                report_type TEXT,
                openai_prompt_tokens INTEGER DEFAULT 0,
                openai_completion_tokens INTEGER DEFAULT 0,
                openai_cost NUMERIC(12,6) DEFAULT 0,
                claude_input_tokens INTEGER DEFAULT 0,
                claude_output_tokens INTEGER DEFAULT 0,
                claude_cost NUMERIC(12,6) DEFAULT 0,
                grok_prompt_tokens INTEGER DEFAULT 0,
                grok_completion_tokens INTEGER DEFAULT 0,
                grok_cost NUMERIC(12,6) DEFAULT 0,
                final_revision_input_tokens INTEGER DEFAULT 0,
                final_revision_output_tokens INTEGER DEFAULT 0,
                final_revision_cost NUMERIC(12,6) DEFAULT 0,
                total_cost NUMERIC(12,6) DEFAULT 0,
                pdf_url TEXT
            );
        """)
        cur.execute("""ALTER TABLE underwriting_runs ADD COLUMN IF NOT EXISTS run_status TEXT;""")
        cur.execute("""ALTER TABLE underwriting_runs ADD COLUMN IF NOT EXISTS run_reason TEXT;""")
        cur.execute("""ALTER TABLE underwriting_runs ADD COLUMN IF NOT EXISTS provider_status TEXT;""")
        cur.execute("""ALTER TABLE underwriting_runs ADD COLUMN IF NOT EXISTS provider_reason TEXT;""")
        conn.commit()
        cur.close()
        print("underwriting_runs table ready")
    except Exception as e:
        print(f"DB init error: {e}")
    finally:
        if conn:
            conn.close()


def save_run_cost(location_id, contact_id, report_type, cost_data, pdf_url="", run_status="", run_reason="", provider_status="", provider_reason=""):
    if not DATABASE_URL:
        print("DATABASE_URL not set, skipping save_run_cost")
        return

    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO underwriting_runs (
                location_id,
                contact_id,
                report_type,
                openai_prompt_tokens,
                openai_completion_tokens,
                openai_cost,
                claude_input_tokens,
                claude_output_tokens,
                claude_cost,
                grok_prompt_tokens,
                grok_completion_tokens,
                grok_cost,
                final_revision_input_tokens,
                final_revision_output_tokens,
                final_revision_cost,
                total_cost,
                pdf_url,
                run_status,
                run_reason,
                provider_status,
                provider_reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            location_id,
            contact_id,
            report_type,
            cost_data.get("openai_prompt_tokens", 0),
            cost_data.get("openai_completion_tokens", 0),
            cost_data.get("openai_cost", 0),
            cost_data.get("claude_input_tokens", 0),
            cost_data.get("claude_output_tokens", 0),
            cost_data.get("claude_cost", 0),
            cost_data.get("grok_prompt_tokens", 0),
            cost_data.get("grok_completion_tokens", 0),
            cost_data.get("grok_cost", 0),
            cost_data.get("final_revision_input_tokens", 0),
            cost_data.get("final_revision_output_tokens", 0),
            cost_data.get("final_revision_cost", 0),
            cost_data.get("total_cost", 0),
            pdf_url,
            run_status,
            run_reason,
            provider_status,
            provider_reason
        ))
        conn.commit()
        cur.close()
        print(f"Saved run cost for location_id={location_id} with status={run_status}")
    except Exception as e:
        print(f"save_run_cost error: {e}")
    finally:
        if conn:
            conn.close()

# ── STATUS / ALERT HELPERS ─────────────────────────────────────────
def classify_run(results, final_revision):
    openai_item = results.get("openai") or {}
    claude_item = results.get("claude") or {}
    grok_item = results.get("grok") or {}

    openai_ok = bool(openai_item.get("content"))
    claude_ok = bool(claude_item.get("content"))
    grok_ok = bool(grok_item.get("content"))
    revision_ok = bool(final_revision.get("content")) and final_revision.get("input_tokens", 0) > 0

    if grok_ok and not openai_ok and not claude_ok:
        return "grok_only", "OpenAI and Claude failed; Grok carried the run."
    if grok_ok and openai_ok and not claude_ok:
        return "claude_failed", "Claude failed; OpenAI and Grok still contributed."
    if grok_ok and claude_ok and not openai_ok:
        return "openai_failed", "OpenAI failed; Claude and Grok still contributed."
    if openai_ok and claude_ok and not revision_ok:
        return "final_revision_skipped", "Final Claude revision failed; base report was used."
    if not openai_ok and not claude_ok and not grok_ok:
        return "all_models_failed", "No model returned usable content."
    if openai_ok and claude_ok and grok_ok and revision_ok:
        return "full_success", "All model stages contributed successfully."
    return "partial_success", "The run completed with one or more degraded model stages."


def classify_provider_state(results):
    openai_item = results.get("openai") or {}
    claude_item = results.get("claude") or {}
    grok_item = results.get("grok") or {}

    openai_error = str(openai_item.get("error_message", "")).lower()
    claude_error = str(claude_item.get("error_message", "")).lower()
    grok_ok = bool(grok_item.get("content"))

    openai_quota = "insufficient_quota" in openai_error or "429" in openai_error
    claude_credits = "credit balance is too low" in claude_error

    if openai_quota and claude_credits and grok_ok:
        return "grok_only_fallback", "Anthropic out of credits and OpenAI quota exceeded; Grok completed the run."
    if claude_credits and openai_quota:
        return "dual_provider_failure", "Anthropic credits depleted and OpenAI quota exceeded."
    if claude_credits:
        return "anthropic_out_of_credits", "Claude failed due to low API credits."
    if openai_quota:
        return "openai_quota_exceeded", "OpenAI failed due to quota/billing limits."
    return "full_stack_ok", "Primary providers available."


def send_backend_alert(location_id, contact_id, report_type, run_status, run_reason, provider_status, provider_reason, pdf_url=""):
    if not ALERT_WEBHOOK_URL:
        return

    if run_status == "full_success" and provider_status == "full_stack_ok":
        return

    payload = {
        "location_id": location_id or "",
        "contact_id": contact_id or "",
        "report_type": report_type or "",
        "run_status": run_status,
        "run_reason": run_reason,
        "provider_status": provider_status,
        "provider_reason": provider_reason,
        "pdf_url": pdf_url or "",
        "message": f"AI Underwriter alert | {run_status} | {provider_status} | {run_reason} | {provider_reason}"
    }

    try:
        r = requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=15)
        print(f"Backend alert status: {r.status_code}")
    except Exception as e:
        print(f"Backend alert error: {e}")

# ── GOOGLE SHEET WEBHOOK ───────────────────────────────────────────
def send_run_to_gsheet_webhook(location_id, contact_id, report_type, cost_data, pdf_url="", run_status="", run_reason="", provider_status="", provider_reason=""):
    if not GSHEET_WEBHOOK_URL:
        print("GSHEET_WEBHOOK_URL not set, skipping Google Sheet webhook")
        return

    payload = {
        "location_id": location_id or "",
        "contact_id": contact_id or "",
        "report_type": report_type or "",
        "total_cost": cost_data.get("total_cost", 0),
        "pdf_url": pdf_url or "",
        "openai_prompt_tokens": cost_data.get("openai_prompt_tokens", 0),
        "openai_completion_tokens": cost_data.get("openai_completion_tokens", 0),
        "openai_cost": cost_data.get("openai_cost", 0),
        "claude_input_tokens": cost_data.get("claude_input_tokens", 0),
        "claude_output_tokens": cost_data.get("claude_output_tokens", 0),
        "claude_cost": cost_data.get("claude_cost", 0),
        "grok_prompt_tokens": cost_data.get("grok_prompt_tokens", 0),
        "grok_completion_tokens": cost_data.get("grok_completion_tokens", 0),
        "grok_cost": cost_data.get("grok_cost", 0),
        "final_revision_input_tokens": cost_data.get("final_revision_input_tokens", 0),
        "final_revision_output_tokens": cost_data.get("final_revision_output_tokens", 0),
        "final_revision_cost": cost_data.get("final_revision_cost", 0),
        "run_status": run_status or "",
        "run_reason": run_reason or "",
        "provider_status": provider_status or "",
        "provider_reason": provider_reason or "",
    }

    try:
        r = requests.post(GSHEET_WEBHOOK_URL, json=payload, timeout=20)
        print(f"Google Sheet webhook status: {r.status_code}")
        print(f"Google Sheet webhook response: {r.text}")
    except Exception as e:
        print(f"Google Sheet webhook error: {e}")

# ── PROMPTS ────────────────────────────────────────────────────────
DETAILED_SYSTEM_PROMPT = """ROLE: You are a senior commercial underwriter at Fundara.
Return a lender-ready markdown underwriting report with these exact sections:
## Section 0: Decision Snapshot
## Section 1: SCORECARD
## Section 2: Deal Sheet
## Section 3: Monthly Ledger Table
## Section 4: Portfolio Metrics
## Section 5: Business Info
## Section 6: Bank Info
## Section 7: Red Flags
## Section 8: About the Business
## Section 9: Notice-Only
## Section 10: Online Presence
## Section 11: Failure & Scope
Never mention AI or tools. Use exact figures where visible."""

DETAILED_USER_PROMPT = """Create the full detailed Fundara underwriting report now.

Use only the statement evidence below.

BANK STATEMENTS:
{combined_text}
"""

QUICK_SYSTEM_PROMPT = """ROLE: You are a senior Fundara underwriter creating a fast screening memo.
Return ONLY:
## Section 0: Decision Snapshot
## Section 2: Deal Sheet
Never mention AI or tools. Be concise but decisive."""

QUICK_USER_PROMPT = """Create the quick Fundara underwriting screen now.

Use only the statement evidence below.

BANK STATEMENTS:
{combined_text}
"""

# ── MODEL CALLS ────────────────────────────────────────────────────
def get_prompts(report_type):
    if report_type == "Quick":
        return QUICK_SYSTEM_PROMPT, QUICK_USER_PROMPT
    return DETAILED_SYSTEM_PROMPT, DETAILED_USER_PROMPT


def analyze_with_openai(combined_text, report_type):
    try:
        system_prompt, user_prompt_template = get_prompts(report_type)
        max_tokens = 2200 if report_type == "Quick" else 5500

        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "gpt-4.1",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt_template.format(combined_text=combined_text)},
            ],
            "max_tokens": max_tokens,
        }

        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            json=payload,
            headers=headers,
            timeout=60,
        )
        data = r.json()
        print(f"OpenAI response status: {r.status_code}")

        if "choices" not in data:
            print(f"OpenAI error response: {data}")
            return {
                "content": None,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "cost": 0,
                "provider": "openai",
                "error_message": json.dumps(data),
            }

        usage = data.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        cost = calc_cost(prompt_tokens, completion_tokens, OPENAI_INPUT_PER_M, OPENAI_OUTPUT_PER_M)

        return {
            "content": data["choices"][0]["message"]["content"],
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "cost": cost,
            "provider": "openai",
            "error_message": "",
        }
    except Exception as openai_err:
        print(f"OpenAI exception: {openai_err}")
        return {
            "content": None,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "cost": 0,
            "provider": "openai",
            "error_message": str(openai_err),
        }


def analyze_with_claude(combined_text, report_type):
    try:
        system_prompt, user_prompt_template = get_prompts(report_type)
        max_tokens = 2200 if report_type == "Quick" else 5500

        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt_template.format(combined_text=combined_text)}],
        )

        input_tokens = getattr(message.usage, "input_tokens", 0)
        output_tokens = getattr(message.usage, "output_tokens", 0)
        cost = calc_cost(input_tokens, output_tokens, ANTHROPIC_INPUT_PER_M, ANTHROPIC_OUTPUT_PER_M)

        return {
            "content": message.content[0].text,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": cost,
            "provider": "claude",
            "error_message": "",
        }
    except Exception as claude_err:
        print(f"Claude exception: {claude_err}")
        return {
            "content": None,
            "input_tokens": 0,
            "output_tokens": 0,
            "cost": 0,
            "provider": "claude",
            "error_message": str(claude_err),
        }


def analyze_with_grok(combined_text, report_type):
    try:
        system_prompt, user_prompt_template = get_prompts(report_type)
        max_tokens = 2200 if report_type == "Quick" else 5500

        headers = {
            "Authorization": f"Bearer {GROK_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "grok-3-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt_template.format(combined_text=combined_text)},
            ],
            "max_tokens": max_tokens,
        }

        r = requests.post(
            "https://api.x.ai/v1/chat/completions",
            json=payload,
            headers=headers,
            timeout=60,
        )
        data = r.json()
        print(f"Grok response status: {r.status_code}")

        if "choices" not in data:
            print(f"Grok error response: {data}")
            return {
                "content": None,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "cost": 0,
                "provider": "grok",
                "error_message": json.dumps(data),
            }

        usage = data.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        cost = calc_cost(prompt_tokens, completion_tokens, GROK_INPUT_PER_M, GROK_OUTPUT_PER_M)

        return {
            "content": data["choices"][0]["message"]["content"],
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "cost": cost,
            "provider": "grok",
            "error_message": "",
        }
    except Exception as grok_err:
        print(f"Grok exception: {grok_err}")
        return {
            "content": None,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "cost": 0,
            "provider": "grok",
            "error_message": str(grok_err),
        }


def choose_base_report(results):
    candidates = []
    for key in ("claude", "openai", "grok"):
        item = results.get(key)
        if item and item.get("content") and len(item["content"].strip()) > 100:
            candidates.append(item)
    if not candidates:
        return None
    return max(candidates, key=lambda x: len(x["content"]))


def revise_final_with_claude(base_report, report2, report3, report_type):
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        revision_prompt = f"""You are the final senior underwriting editor at Fundara.
Keep the BASE REPORT as the backbone.
Improve it with valid facts from reviewer drafts.
Do not mention AI or drafting process.

BASE REPORT:
{base_report}

REVIEWER REPORT 2:
{report2 or "No reviewer report available."}

REVIEWER REPORT 3:
{report3 or "No reviewer report available."}
"""

        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=2600 if report_type == "Quick" else 6500,
            messages=[{"role": "user", "content": revision_prompt}],
        )

        input_tokens = getattr(message.usage, "input_tokens", 0)
        output_tokens = getattr(message.usage, "output_tokens", 0)
        cost = calc_cost(input_tokens, output_tokens, ANTHROPIC_INPUT_PER_M, ANTHROPIC_OUTPUT_PER_M)

        return {
            "content": message.content[0].text,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": cost,
            "error_message": "",
        }
    except Exception as merge_err:
        print(f"Final revision exception: {merge_err}")
        return {
            "content": base_report,
            "input_tokens": 0,
            "output_tokens": 0,
            "cost": 0,
            "error_message": str(merge_err),
        }


def generate_multi_model_report(combined_text, report_type):
    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(analyze_with_openai, combined_text, report_type): "openai",
            executor.submit(analyze_with_claude, combined_text, report_type): "claude",
            executor.submit(analyze_with_grok, combined_text, report_type): "grok",
        }

        for future, key in list(futures.items()):
            try:
                results[key] = future.result(timeout=MODEL_TIMEOUT)
            except FuturesTimeoutError:
                print(f"{key} model timeout after {MODEL_TIMEOUT}s")
                results[key] = {
                    "content": None,
                    "cost": 0,
                    "error_message": f"{key} timeout after {MODEL_TIMEOUT}s",
                }
            except Exception as err:
                print(f"{key} future failed: {err}")
                results[key] = {
                    "content": None,
                    "cost": 0,
                    "error_message": str(err),
                }

    base_item = choose_base_report(results)
    if not base_item:
        return None, results, {}, "", "", "", ""

    base_report = base_item["content"]

    others = []
    for key in ("claude", "openai", "grok"):
        item = results.get(key)
        if item and item.get("content") and item["content"] != base_report:
            others.append(item["content"])

    reviewer2 = others[0] if len(others) > 0 else None
    reviewer3 = others[1] if len(others) > 1 else None

    final_revision = revise_final_with_claude(base_report, reviewer2, reviewer3, report_type)

    cost_data = {
        "openai_prompt_tokens": (results.get("openai") or {}).get("prompt_tokens", 0),
        "openai_completion_tokens": (results.get("openai") or {}).get("completion_tokens", 0),
        "openai_cost": (results.get("openai") or {}).get("cost", 0),
        "claude_input_tokens": (results.get("claude") or {}).get("input_tokens", 0),
        "claude_output_tokens": (results.get("claude") or {}).get("output_tokens", 0),
        "claude_cost": (results.get("claude") or {}).get("cost", 0),
        "grok_prompt_tokens": (results.get("grok") or {}).get("prompt_tokens", 0),
        "grok_completion_tokens": (results.get("grok") or {}).get("completion_tokens", 0),
        "grok_cost": (results.get("grok") or {}).get("cost", 0),
        "final_revision_input_tokens": final_revision.get("input_tokens", 0),
        "final_revision_output_tokens": final_revision.get("output_tokens", 0),
        "final_revision_cost": final_revision.get("cost", 0),
    }

    cost_data["total_cost"] = round(
        cost_data["openai_cost"] +
        cost_data["claude_cost"] +
        cost_data["grok_cost"] +
        cost_data["final_revision_cost"],
        6
    )

    run_status, run_reason = classify_run(results, final_revision)
    provider_status, provider_reason = classify_provider_state(results)

    print(f"RUN COSTS: {json.dumps(cost_data)}")
    print(f"RUN STATUS: {run_status} | {run_reason}")
    print(f"PROVIDER STATUS: {provider_status} | {provider_reason}")

    return final_revision["content"], results, cost_data, run_status, run_reason, provider_status, provider_reason

# ── GHL PUSH ───────────────────────────────────────────────────────
def push_to_ghl(contact_id, report, api_key, pdf_url=""):
    try:
        url = f"https://services.leadconnectorhq.com/contacts/{contact_id}"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Version": "2021-07-28",
            "Content-Type": "application/json"
        }
        custom_fields = [{"key": "ai_underwriting_analysis", "value": report}]
        if pdf_url:
            custom_fields.append({"key": "ai_underwriting_analysis_pdf", "value": pdf_url})
        payload = {"customFields": custom_fields}
        r = requests.put(url, json=payload, headers=headers, timeout=45)
        print(f"GHL push status: {r.status_code}")
        return r.status_code
    except Exception as ghl_err:
        print(f"GHL push error: {ghl_err}")
        return 500

# ── ROUTES ─────────────────────────────────────────────────────────
@app.route("/analyze", methods=["POST"])
def analyze():
    try:
        data = request.json or {}
        contact_id = data.get("contact_id")
        location_id = data.get("location_id")
        ghl_key = data.get("ghl_api_key") or GHL_API_KEY

        raw_report_type = (data.get("report_type") or "").strip().lower()
        report_type = "Quick" if raw_report_type in ("quick", "quick report") else "Detailed"

        statement_urls = []
        for i in range(1, 11):
            url = data.get(f"bank_statement_{i}")
            if url and str(url).strip().lower() not in {"null", "", "undefined"}:
                statement_urls.append((i, url))
            elif url is not None:
                print(f"Skipping bad statement field bank_statement_{i}: {url}")

        if not statement_urls:
            return jsonify({"error": "No valid bank statements found"}), 400

        combined_text = ""
        successful_statements = 0

        for idx, url in statement_urls:
            print(f"Downloading statement {idx}")
            pdf_bytes = download_pdf(url, ghl_key)
            if not pdf_bytes:
                print(f"Statement {idx} skipped after download failure")
                continue

            print(f"Extracting statement {idx}")
            text = extract_text(pdf_bytes)
            if text.strip():
                combined_text += f"\n--- BANK STATEMENT {idx} ---\n{text}\n"
                successful_statements += 1
            else:
                print(f"Statement {idx} had no extractable text")

        if not combined_text.strip():
            return jsonify({"error": "Could not extract text from any valid PDFs"}), 400

        print(f"Generating {report_type} report using {successful_statements} statement(s)")
        final_report, model_results, cost_data, run_status, run_reason, provider_status, provider_reason = generate_multi_model_report(combined_text, report_type)

        if not final_report:
            return jsonify({
                "error": f"Failed to generate {report_type} report",
                "model_results_present": {k: bool(v and v.get('content')) for k, v in model_results.items()} if model_results else {},
            }), 500

        print("Converting final report to PDF")
        pdf_url = ""
        pdf_bytes = convert_to_pdf(final_report)
        if pdf_bytes:
            pdf_url = upload_to_r2(pdf_bytes, contact_id) or ""

        save_run_cost(location_id, contact_id, report_type, cost_data, pdf_url, run_status, run_reason, provider_status, provider_reason)
        send_run_to_gsheet_webhook(location_id, contact_id, report_type, cost_data, pdf_url, run_status, run_reason, provider_status, provider_reason)
        send_backend_alert(location_id, contact_id, report_type, run_status, run_reason, provider_status, provider_reason, pdf_url)
        status = push_to_ghl(contact_id, final_report, ghl_key, pdf_url)

        return jsonify({
            "success": True,
            "report_type": report_type,
            "ghl_update_status": status,
            "contact_id": contact_id,
            "location_id": location_id,
            "pdf_url": pdf_url,
            "cost_data": cost_data,
            "run_status": run_status,
            "run_reason": run_reason,
            "provider_status": provider_status,
            "provider_reason": provider_reason,
            "successful_statements": successful_statements,
        })
    except Exception as e:
        print(f"/analyze fatal error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"})


init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
