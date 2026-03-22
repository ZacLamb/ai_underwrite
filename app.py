import os
import io
import re
import uuid
import json
import requests
import pdfplumber
import boto3

from flask import Flask, request, jsonify
from anthropic import Anthropic
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
from reportlab.lib import colors
from concurrent.futures import ThreadPoolExecutor, as_completed

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

LOGO_URL = "https://assets.cdn.filesafe.space/HD59NWC1biIA31IHm1y8/media/69a4925b753f150a68663d79.png"

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


def _flag_card(severity, title, detail):
    sev_map = {"CRITICAL": RED, "HIGH": ORANGE, "MEDIUM": YELLOW, "LOW": GREEN}
    sc = sev_map.get(severity.upper(), TG)

    badge = Table(
        [[Paragraph(
            severity.upper(),
            ParagraphStyle("fb", fontName="Helvetica-Bold", fontSize=7, textColor=BG, alignment=TA_CENTER),
        )]],
        colWidths=[0.65 * inch],
    )
    badge.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), sc),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))

    content = Table([
        [Paragraph(
            f"<b>{_parse_md_bold(title)}</b>",
            ParagraphStyle("ft", fontName="Helvetica-Bold", fontSize=9, textColor=TW, leading=12),
        )],
        [Spacer(1, 3)],
        [Paragraph(_parse_md_bold(detail), STY["body"])],
    ], colWidths=[6.1 * inch])
    content.setStyle(TableStyle([
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))

    card = Table([[badge, content]], colWidths=[0.7 * inch, 6.3 * inch])
    card.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), CARD),
        ("BOX", (0, 0), (-1, -1), 0.35, BORDER),
        ("LINEBEFORE", (0, 0), (0, -1), 4, sc),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (1, 0), (1, 0), 10),
        ("RIGHTPADDING", (1, 0), (1, 0), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
    ]))
    return KeepTogether([card, Spacer(1, 5)])


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

        if stripped in ("---", "***", "___"):
            story.append(HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER))
            story.append(Spacer(1, 5))
            i += 1
            continue

        if stripped.startswith("- ") or stripped.startswith("* "):
            text = _parse_md_bold(stripped[2:])
            flag_match = re.match(
                r"\*\*(CRITICAL|HIGH|MEDIUM|LOW)\*\*[:\s–-]+(.+?)(?:[–-]+|:)(.+)",
                stripped[2:],
                re.IGNORECASE,
            )
            if flag_match:
                sev, ftitle, fdetail = flag_match.groups()
                story.append(_flag_card(sev, ftitle.strip(), fdetail.strip()))
                i += 1
                continue

            story.append(Paragraph(f"• {text}", STY["bullet"]))
            story.append(Spacer(1, 2))
            i += 1
            continue

        num_match = re.match(r"^(\d+)\.\s+(.+)", stripped)
        if num_match:
            num, text = num_match.groups()
            story.append(Paragraph(f"<b>{num}.</b>  {_parse_md_bold(text)}", STY["bullet"]))
            story.append(Spacer(1, 2))
            i += 1
            continue

        bold_only = re.match(r"^\*\*(.+)\*\*$", stripped)
        if bold_only:
            story.append(Paragraph(f"<b>{bold_only.group(1)}</b>", STY["body_b"]))
            story.append(Spacer(1, 3))
            i += 1
            continue

        kv_match = re.match(r"^\*\*(.+?)\*\*[:\s]+(.+)", stripped)
        if kv_match:
            key, val = kv_match.groups()
            val_clean = _parse_md_bold(val)
            val_upper = val.upper()
            if any(k in val_upper for k in ("DECLINE", "NEGATIVE", "CRITICAL", "FAILED")):
                val_markup = f'<font color="#E53935"><b>{val_clean}</b></font>'
            elif any(k in val_upper for k in ("HIGH RISK", "UNSUSTAINABLE", "OVERDRAFT")):
                val_markup = f'<font color="#F57C00"><b>{val_clean}</b></font>'
            elif any(k in val_upper for k in ("APPROVE", "PASS", "LOW RISK", "CONDITIONAL APPROVAL")):
                val_markup = f'<font color="#43A047"><b>{val_clean}</b></font>'
            else:
                val_markup = val_clean

            story.append(Paragraph(
                f'<b><font color="#90A4AE">{key}:</font></b>  {val_markup}',
                STY["body"],
            ))
            story.append(Spacer(1, 3))
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
                logo_img_draw = Image(io.BytesIO(logo_resp.content), width=1.6 * inch, height=0.38 * inch)
                logo_img_flow = Image(io.BytesIO(logo_resp.content), width=2.4 * inch, height=0.56 * inch)
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
            ContentType="application/pdf",
        )
        url = f"{R2_PUBLIC_URL}/{filename}"
        print(f"PDF uploaded to R2: {url}")
        return url
    except Exception as r2_err:
        print(f"R2 upload error: {r2_err}")
        return None


def download_pdf(url, api_key):
    try:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        response = requests.get(url, headers=headers, timeout=45)
        if response.status_code == 200:
            return response.content
        print(f"Failed to download PDF: {response.status_code}")
        return None
    except Exception as download_err:
        print(f"Download error: {download_err}")
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

# ── PROMPTS ────────────────────────────────────────────────────────
DETAILED_SYSTEM_PROMPT = """ROLE: You are a senior commercial underwriter at Fundara with 50 years of experience reviewing small-business bank statements, especially construction, trades, restaurants, trucking, and MCA-heavy files.

NON-NEGOTIABLE RULES
- Never mention AI, models, tools, systems, technology, or uncertainty about being an AI.
- Present all findings as Fundara AI Underwriting analysis.
- Be decisive. Do not hedge unnecessarily.
- Use exact figures from the statements whenever possible.
- If a figure cannot be supported from the statements, write "Not visible in reviewed statements" instead of guessing.
- Do not use placeholders like TBD unless the requested field truly is not visible.
- The report must read like a veteran underwriter who reviewed every line.

STYLE TARGET
The report must feel forensic, credit-focused, and lender-ready:
- dense but readable
- specific counterparties and named payees
- explicit underwriting logic
- clear approval/decline rationale
- conservative interpretation of ambiguous items

FORMAT
Markdown only. No intro, no outro, no code fences.
Use exactly these sections and this order:

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

GLOBAL FORMATTING RULES
- USD must be whole-dollar or 2-decimal currency with commas
- percentages to 1 decimal when needed
- mask account numbers
- use tables whenever appropriate
- one blank line between sections
- do not skip sections
- do not write generic filler

SECTION REQUIREMENTS

Section 0: Decision Snapshot
- Max 450 words
- First line must be exactly: **RECOMMENDATION: APPROVE**, **RECOMMENDATION: CONDITIONAL APPROVAL**, or **RECOMMENDATION: DECLINE**
- Then provide a tight underwriting narrative with exact figures
- Must include:
  - total reviewed deposits
  - average monthly deposits
  - ending balance trend
  - overdraft / low-balance behavior
  - existing debt burden / MCA burden
  - deposit concentration observations
  - final underwriting conclusion
- End with a plain-English lender conclusion

Section 1: SCORECARD
Use a 5-column table:
| Category | Weight | Score | Weighted Score | Notes |
- Categories:
  Deposits, Net Cash Flow, Ledger Balance, Overdraft Days, MCA Density, Tax Compliance, Vendor Concentration, Seasonality, New Capital, Statement Integrity, TOTAL SCORE
- Score scale: 1 best, 5 weakest
- EVERY row must include a Notes explanation with specific evidence
- TOTAL SCORE row must include a rating band and underwriting meaning

Section 2: Deal Sheet
Use a 2-column table:
| Field | Value |
Populate as many of these as visible:
Applicant Name, Business Name, DBA, Business Address, Mailing Address, Business Type, Account Number, Bank, Account Status, Statement Period Reviewed, Opening Balance, Closing Balance, Total Reviewed Deposits, Total Reviewed Withdrawals, Total Checks Cleared, Total Service Fees, Net Cash Flow, Monthly Average Deposits, Monthly Average Withdrawals, Average Ledger Balance, Overdraft Fees, Overdraft Days Count, Primary Income Source, Largest Single Deposit, Largest Single Withdrawal, Daily Min Balance, Daily Max Balance, MCA Presence, Credit Card Exposure, Other Financing Exposure, Utility Expense, Insurance, Telecom, Business Registration, Tax ID / EIN, Business License, Years in Operation, Employees, Requested Funding Amount, Recommended Loan Amount, Term, APR, Monthly Payment, Collateral Status, Fundara Recommendation
- If not visible, write "Not visible in reviewed statements"
- Do not leave blank cells

Section 3: Monthly Ledger Table
Use a comprehensive monthly table with:
| Month | Opening Balance | Deposits | Withdrawals | Checks Cleared | Service Fees | Ending Balance | Avg Ledger Balance | Overdraft Days | Low Days (<$100) |
- Include every reviewed month plus a total row
- After the table, add 4-6 bullet observations with exact trends

Section 4: Portfolio Metrics
Use one main table:
| Metric | Value | Assessment |
Must include:
- Total Reviewed Deposits
- Total Reviewed Withdrawals
- Average Monthly Deposits
- Average Monthly Withdrawals
- Monthly Net Cash Flow
- Deposit Mix
- Top Deposit Sources
- Top 5 Expense Sinks
- MCA Position Detail
- Credit Burden
- Liquidity Pattern
- Revenue Concentration
- Cash Usage Pattern
Assess each metric like an underwriter, not a data dump.

Section 5: Business Info
Use a 2-column table of business identity facts supported by the statements.

Section 6: Bank Info
Use a 2-column table with bank/account observations.

Section 7: Red Flags
Use a 4-column table:
| Severity | Flag | Evidence | Impact |
- Severity must be CRITICAL / HIGH / MEDIUM / LOW
- Include only real red flags supported by statement evidence
- Prefer specific anomalies:
  - chronic overdrafts
  - MCA stacking
  - out-of-state address mismatch
  - repetitive identical checks
  - returned items
  - chargebacks
  - heavy ATM/cash activity
  - deposit concentration
  - informal P2P revenue
  - timing crisis despite healthy deposits

Section 8: About the Business
Write 1 substantive paragraph describing what the business appears to do, how it likely operates, and how money appears to move through the account.

Section 9: Notice-Only
Write a short operational/lender note. Not filler.

Section 10: Online Presence
If not visible from statements, say so plainly. Do not invent websites or social media.

Section 11: Failure & Scope
Write 2 concise bullets:
- Failure Risk
- Scope for Improvement

QUALITY BAR
This report must be decisive, evidence-heavy, complete, and lender-grade.
"""

DETAILED_USER_PROMPT = """Create the full detailed Fundara underwriting report now.

Use only the statement evidence below.

BANK STATEMENTS:
{combined_text}
"""

QUICK_SYSTEM_PROMPT = """ROLE: You are a senior Fundara underwriter creating a fast screening memo from bank statements.

NON-NEGOTIABLE RULES
- Never mention AI, models, tools, or technology.
- Present all findings as Fundara AI Underwriting analysis.
- Be concise but decisive.
- Use exact numbers from the statements whenever possible.
- If not visible, write "Not visible in reviewed statements".
- No placeholders like TBD.

OUTPUT FORMAT
Markdown only.
Return ONLY these two sections in this exact order:

## Section 0: Decision Snapshot
## Section 2: Deal Sheet

SECTION 0 REQUIREMENTS
- 180-300 words
- First line must be exactly one of:
  **RECOMMENDATION: APPROVE**
  **RECOMMENDATION: CONDITIONAL APPROVAL**
  **RECOMMENDATION: DECLINE**
- Must include:
  - total reviewed deposits
  - average monthly deposits
  - ending/ledger condition
  - overdraft or low-balance condition
  - existing MCA / financing burden if present
  - one concise final underwriting conclusion

SECTION 2 REQUIREMENTS
Use exactly one table:
| Field | Value |

Include:
Applicant Name, Business Name, Business Type, Bank, Account Number, Statement Period Reviewed, Opening Balance, Closing Balance, Total Reviewed Deposits, Total Reviewed Withdrawals, Net Cash Flow, Monthly Average Deposits, Monthly Average Withdrawals, Average Ledger Balance, Overdraft Fees, Overdraft Days Count, Primary Income Source, Largest Single Deposit, Largest Single Withdrawal, MCA Presence, Other Financing Exposure, Requested Funding Amount, Recommended Loan Amount, Term, APR, Monthly Payment, Fundara Recommendation
"""

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
            timeout=180,
        )
        data = r.json()
        print(f"OpenAI response status: {r.status_code}")

        if "choices" not in data:
            print(f"OpenAI error response: {data}")
            return None

        return data["choices"][0]["message"]["content"]
    except Exception as openai_err:
        print(f"OpenAI exception: {openai_err}")
        return None


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
        return message.content[0].text
    except Exception as claude_err:
        print(f"Claude exception: {claude_err}")
        return None


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
            timeout=180,
        )
        data = r.json()
        print(f"Grok response status: {r.status_code}")

        if "choices" not in data:
            print(f"Grok error response: {data}")
            return None

        return data["choices"][0]["message"]["content"]
    except Exception as grok_err:
        print(f"Grok exception: {grok_err}")
        return None


def choose_base_report(results):
    ordered = [
        results.get("claude"),
        results.get("openai"),
        results.get("grok"),
    ]
    valid = [r for r in ordered if r and isinstance(r, str) and len(r.strip()) > 100]
    if not valid:
        return None
    return max(valid, key=len)


def revise_final_with_claude(base_report, report2, report3, report_type):
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)

        if report_type == "Quick":
            revision_prompt = f"""You are the final senior underwriting editor at Fundara.

You are revising a QUICK underwriting memo.

YOUR JOB
- Keep the BASE REPORT as the backbone
- Improve it using only clearly supported details from the two reviewer drafts
- Preserve or improve specificity
- Do NOT make it longer than necessary
- Return ONLY these two sections:
  - ## Section 0: Decision Snapshot
  - ## Section 2: Deal Sheet

NON-NEGOTIABLE RULES
- Do not mention AI, tools, models, or drafting process
- Do not average away sharp underwriting judgment
- If reviewers disagree, choose the most conservative supportable figure
- Do not weaken recommendation language
- The final result must be as complete or better than the base report, never worse

BASE REPORT:
{base_report}

REVIEWER REPORT 2:
{report2 or "No reviewer report available."}

REVIEWER REPORT 3:
{report3 or "No reviewer report available."}

Return the final revised QUICK Fundara underwriting report now.
"""
            max_tokens = 2600
        else:
            revision_prompt = f"""You are the final senior underwriting editor at Fundara.

You are revising a DETAILED underwriting report.

YOUR JOB
- Keep the BASE REPORT as the backbone
- Upgrade it with any better facts, stronger red flags, tighter numbers, missing details, or stronger underwriting logic from the reviewer drafts
- Do NOT compress the report into a summary
- Do NOT simplify tables unless the base report is clearly wrong
- Preserve section structure and richness
- The final report must be at least as detailed as the strongest source draft

NON-NEGOTIABLE RULES
- Do not mention AI, tools, models, technology, or drafting process
- Do not remove specific named counterparties, exact figures, or concrete risk logic unless unsupported
- If drafts disagree, choose the most conservative supportable figure
- Ensure Section 1 keeps Notes detail
- Ensure Section 2 is fully populated with no blank values
- Ensure Section 3 and Section 4 stay dense and lender-grade
- Ensure Section 7 has severity, evidence, and impact
- Maintain decisive lender-ready tone

BASE REPORT:
{base_report}

REVIEWER REPORT 2:
{report2 or "No reviewer report available."}

REVIEWER REPORT 3:
{report3 or "No reviewer report available."}

Return the final revised DETAILED Fundara underwriting report now.
"""
            max_tokens = 6500

        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": revision_prompt}],
        )
        return message.content[0].text
    except Exception as merge_err:
        print(f"Final revision exception: {merge_err}")
        return base_report


def generate_multi_model_report(combined_text, report_type):
    results = {}

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_key = {
            executor.submit(analyze_with_openai, combined_text, report_type): "openai",
            executor.submit(analyze_with_claude, combined_text, report_type): "claude",
            executor.submit(analyze_with_grok, combined_text, report_type): "grok",
        }

        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                results[key] = future.result()
            except Exception as err:
                print(f"{key} future failed: {err}")
                results[key] = None

    base_report = choose_base_report(results)
    if not base_report:
        return None, results

    others = []
    for key in ("claude", "openai", "grok"):
        val = results.get(key)
        if val and val != base_report:
            others.append(val)

    reviewer2 = others[0] if len(others) > 0 else None
    reviewer3 = others[1] if len(others) > 1 else None

    final_report = revise_final_with_claude(base_report, reviewer2, reviewer3, report_type)
    return final_report, results

# ── GHL PUSH ───────────────────────────────────────────────────────
def push_to_ghl(contact_id, report, api_key, pdf_url=""):
    try:
        url = f"https://services.leadconnectorhq.com/contacts/{contact_id}"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Version": "2021-07-28",
            "Content-Type": "application/json",
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
        ghl_key = data.get("ghl_api_key") or GHL_API_KEY

        raw_report_type = (data.get("report_type") or "").strip().lower()
        if raw_report_type in ("quick", "quick report"):
            report_type = "Quick"
        else:
            report_type = "Detailed"

        statement_urls = []
        for i in range(1, 11):
            url = data.get(f"bank_statement_{i}")
            if url and str(url).lower() != "null":
                statement_urls.append((i, url))

        if not statement_urls:
            return jsonify({"error": "No bank statements found"}), 400

        combined_text = ""
        for idx, url in statement_urls:
            print(f"Downloading statement {idx}")
            pdf_bytes = download_pdf(url, ghl_key)
            if pdf_bytes:
                print(f"Extracting statement {idx}")
                text = extract_text(pdf_bytes)
                if text.strip():
                    combined_text += f"\n--- BANK STATEMENT {idx} ---\n{text}\n"

        if not combined_text.strip():
            return jsonify({"error": "Could not extract text from any PDFs"}), 400

        print(f"Generating {report_type} report")
        final_report, model_results = generate_multi_model_report(combined_text, report_type)

        if not final_report:
            return jsonify({
                "error": f"Failed to generate {report_type} report",
                "model_results_present": {k: bool(v) for k, v in model_results.items()} if model_results else {},
            }), 500

        print("Converting final report to PDF")
        pdf_url = ""
        pdf_bytes = convert_to_pdf(final_report)
        if pdf_bytes:
            pdf_url = upload_to_r2(pdf_bytes, contact_id) or ""

        status = push_to_ghl(contact_id, final_report, ghl_key, pdf_url)

        return jsonify({
            "success": True,
            "report_type": report_type,
            "ghl_update_status": status,
            "contact_id": contact_id,
            "pdf_url": pdf_url,
        })
    except Exception as e:
        print(f"/analyze fatal error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
