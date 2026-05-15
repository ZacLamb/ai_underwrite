import os
import io
import re
import uuid
import time
import threading
import requests
import pdfplumber
import boto3
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
from anthropic import Anthropic
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.colors import HexColor
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image,
    HRFlowable, Table, TableStyle, KeepTogether
)
from reportlab.lib import colors

app = Flask(__name__)

OPENAI_API_KEY       = os.environ.get("OPENAI_API_KEY")
ANTHROPIC_API_KEY    = os.environ.get("ANTHROPIC_API_KEY")
GROK_API_KEY         = os.environ.get("GROK_API_KEY")
R2_ACCESS_KEY_ID     = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID        = os.environ.get("R2_ACCOUNT_ID")
R2_PUBLIC_URL        = os.environ.get("R2_PUBLIC_URL")
GSHEET_WEBHOOK_URL   = os.environ.get("GSHEET_WEBHOOK_URL", "")
R2_BUCKET            = "fundara-reports"
LOGO_URL             = "https://assets.cdn.filesafe.space/HD59NWC1biIA31IHm1y8/media/69a4925b753f150a68663d79.png"

# Pricing per 1M tokens (as of 2025)
# claude-haiku-4-5: $0.80 input / $4.00 output
# gpt-4o-mini: $0.15 input / $0.60 output
# grok-3-mini: free tier / negligible
CLAUDE_INPUT_COST_PER_M  = 0.80
CLAUDE_OUTPUT_COST_PER_M = 4.00
OPENAI_INPUT_COST_PER_M  = 0.15
OPENAI_OUTPUT_COST_PER_M = 0.60
GROK_INPUT_COST_PER_M    = 0.0
GROK_OUTPUT_COST_PER_M   = 0.0

BG      = HexColor('#0D1B2A')
CARD    = HexColor('#152030')
ROW_ALT = HexColor('#1C2B3A')
HDR_BG  = HexColor('#1A3550')
BORDER  = HexColor('#2A3F55')
RED     = HexColor('#E53935')
ORANGE  = HexColor('#F57C00')
YELLOW  = HexColor('#F9A825')
GREEN   = HexColor('#43A047')
LBLUE   = HexColor('#64B5F6')
TW      = HexColor('#ECEFF1')
TG      = HexColor('#90A4AE')
TL      = HexColor('#B0BEC5')
W_PAGE  = 7.0 * inch

SYSTEM_PROMPT = """ROLE: Senior COMMERCIAL UNDERWRITER (50y; restaurants & MCA). Tool-driven extraction only; NEVER reveal internal reasoning; no approximations. If unparseable, state cause.

IMPORTANT: Never mention any AI companies, models, tools, or technologies in your output. Present all findings as Fundara AI Underwriting analysis only.
IMPORTANT: Do NOT include "fundara.co" or any URLs in your output.
IMPORTANT: Do NOT include any title lines like "MERGED FUNDARA UNDERWRITING REPORT" or "FUNDARA AI UNDERWRITING ANALYSIS REPORT". Start directly with ## Section 0: Decision Snapshot.

FORMAT (strict)
- Markdown only, PDF-ready. No charts/images.
- Start directly with ## Section 0: Decision Snapshot
- Use tables; minimal prose; unknown -> N/A.
- USD whole dollars with commas; % to 1 decimal.
- Mask account numbers.

BANK STATEMENTS
Monthly table:
- Deposits, withdrawals, ending bal
- Funding inflows >=5k
- Recurring MCA debits
- Daily balances: avg, NEG days, LOW days
- Deposit channel mix %
- Top-5 deposit sources & expense sinks

SCORING (1=Best,5=Weak)
Deposits 15%; Net Cash 10%; Ledger 10%; OD Days 10%;
MCA Density 15%; Tax Compliance 5%; Vendor Concentration 5%;
Seasonality 5%; New Capital 5%; Statement Integrity 5%.

OUTPUT ORDER
0) Decision Snapshot
1) SCORECARD
2) Deal Sheet
3) Monthly Ledger Table
4) Portfolio Metrics
5) Business Info
6) Bank Info
7) Red Flags
8) About the Business
9) Notice-Only
10) Online Presence
11) Failure & Scope"""

USER_PROMPT = """Render a clean, print-ready underwriting report. Start directly with ## Section 0: Decision Snapshot, then Sections 1-11.
- Markdown only; tables preferred; omit N/A rows; one blank line between sections.
- USD whole dollars with commas; percentages 1 decimal; mask account numbers.
- Do NOT include fundara.co or any URLs.
- Do NOT include any title line above Section 0. Start the report with ## Section 0: Decision Snapshot.
- Do NOT mention any AI tools, models, or companies. Present as Fundara AI analysis only.
- CRITICAL: Section 0 must have RECOMMENDATION: APPROVE or RECOMMENDATION: DECLINE or RECOMMENDATION: CONDITIONAL on its own line.
- CRITICAL: Section 1 Scorecard must include a Notes column explaining WHY each score was given with specific evidence. Keep notes concise — max 3 sentences per cell.
- CRITICAL: Section 2 Deal Sheet must be exhaustive — never leave fields as TBD.
- CRITICAL: Section 3 Monthly Ledger must include Opening Balance, Deposits, Withdrawals, Checks Cleared, Service Fees, Ending Balance, Avg Daily Balance, Overdraft Days, Low Balance Days per month.
- CRITICAL: Section 7 Red Flags must be a table with columns: Severity, Flag, Evidence, Impact.

Here are the bank statements:

{combined_text}"""

SYSTEM_PROMPT_QUICK = """ROLE: Senior COMMERCIAL UNDERWRITER (50y; MCA focus). Quick-scan mode.

IMPORTANT: Never mention any AI companies, models, tools, or technologies. Present all findings as Fundara AI Underwriting analysis only.
IMPORTANT: Do NOT include "fundara.co" or any URLs in your output.
IMPORTANT: Do NOT include any title lines. Start directly with ## Section 0 - Decision Snapshot.

FORMAT (strict)
- Markdown only. No charts/images.
- Output ONLY Section 0 and Section 2 as defined below.
- Use ## for section headings, ### for table headings.
- USD whole dollars with commas; % to 1 decimal.
- Mask account numbers.

## Section 0 - Decision Snapshot (<=300 words):
- Business name, bank, account masked
- Average monthly deposits
- Average daily balance
- Negative balance days per month
- Overdraft fees total
- MCA density (existing positions)
- Funding recommendation: Approve / Decline / Conditional
- Max recommended position and factor rate range
- One paragraph summary of risk

## Section 2 - Deal Sheet:
Output TWO separate 2-column markdown tables.

### Table 1 - Business Info:
| Field | Value |
|-------|-------|
rows for: Business Name, DBA, Owner, Bank, Account Type, Review Period

### Table 2 - Financial Summary & Decision:
| Field | Value |
|-------|-------|
rows for: Avg Monthly Deposits, Avg Daily Balance, Total Deposits, Negative Days (avg/month), Overdraft Fees, Existing MCA Positions, Recommended Position, Recommended Factor Rate, Decision

Use ONLY these two 2-column tables. Do NOT create a wide multi-column table."""

USER_PROMPT_QUICK = """Render a quick broker decision report with ONLY Section 0 (Decision Snapshot) and Section 2 (Deal Sheet).
- Start directly with ## Section 0 - Decision Snapshot. Do NOT include any title above it.
- Do NOT include fundara.co or any URLs.
- Markdown only; one blank line between sections.
- Use ## for section headings and ### for table headings.
- Section 0: bullet list format, concise.
- Section 2: TWO separate 2-column tables (Field | Value).
- Do NOT create a wide multi-column table.
- USD whole dollars with commas; percentages 1 decimal; mask account numbers.
- Do NOT mention any AI tools, models, or companies. Present as Fundara AI analysis only.

Here are the bank statements:

{combined_text}"""


def build_styles():
    s = {}
    def ps(name, **kw):
        base = dict(fontName='Helvetica', fontSize=9, textColor=TL, leading=13, spaceAfter=0, spaceBefore=0)
        base.update(kw)
        s[name] = ParagraphStyle(name, **base)
    ps('h1', fontName='Helvetica-Bold', fontSize=13, textColor=TW, spaceBefore=6, spaceAfter=4, leading=17)
    ps('h2', fontName='Helvetica-Bold', fontSize=10.5, textColor=LBLUE, spaceBefore=8, spaceAfter=4, leading=14)
    ps('h3', fontName='Helvetica-Bold', fontSize=9.5, textColor=TW, spaceBefore=5, spaceAfter=3, leading=13)
    ps('body', fontSize=8.5, textColor=TL, leading=12)
    ps('body_b', fontName='Helvetica-Bold', fontSize=8.5, textColor=TW, leading=12)
    ps('bullet', fontSize=8.5, textColor=TL, leading=12, leftIndent=12)
    ps('th', fontName='Helvetica-Bold', fontSize=8, textColor=TW, leading=11, alignment=TA_CENTER)
    ps('th_l', fontName='Helvetica-Bold', fontSize=8, textColor=TW, leading=11)
    ps('td', fontSize=8, textColor=TL, leading=11)
    ps('td_r', fontName='Helvetica-Bold', fontSize=8, textColor=RED, leading=11)
    ps('td_g', fontName='Helvetica-Bold', fontSize=8, textColor=GREEN, leading=11)
    ps('td_y', fontName='Helvetica-Bold', fontSize=8, textColor=YELLOW, leading=11)
    ps('td_b', fontName='Helvetica-Bold', fontSize=8, textColor=TW, leading=11)
    return s

STY = build_styles()

BASE_TS = TableStyle([
    ('BACKGROUND',    (0, 0), (-1,  0), HDR_BG),
    ('ROWBACKGROUNDS',(0, 1), (-1, -1), [CARD, ROW_ALT]),
    ('GRID',          (0, 0), (-1, -1), 0.35, BORDER),
    ('FONTNAME',      (0, 0), (-1,  0), 'Helvetica-Bold'),
    ('FONTSIZE',      (0, 0), (-1,  0), 8),
    ('TEXTCOLOR',     (0, 0), (-1,  0), TW),
    ('FONTNAME',      (0, 1), (-1, -1), 'Helvetica'),
    ('FONTSIZE',      (0, 1), (-1, -1), 8),
    ('TEXTCOLOR',     (0, 1), (-1, -1), TL),
    ('TOPPADDING',    (0, 0), (-1, -1), 5),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ('LEFTPADDING',   (0, 0), (-1, -1), 8),
    ('RIGHTPADDING',  (0, 0), (-1, -1), 8),
    ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
    ('ROWSPLITTING',  (0, 0), (-1, -1), 1),
])


def _draw_cover(canvas, doc):
    canvas.saveState()
    pw, ph = letter
    canvas.setFillColor(BG)
    canvas.rect(0, 0, pw, ph, fill=1, stroke=0)
    canvas.setFillColor(LBLUE)
    canvas.rect(0, ph - 5, pw, 5, fill=1, stroke=0)
    canvas.setFillColor(CARD)
    canvas.rect(0, 0, pw, 32, fill=1, stroke=0)
    canvas.setFillColor(TG)
    canvas.setFont('Helvetica', 7)
    canvas.drawString(0.55 * inch, 11, 'CONFIDENTIAL — Authorized Fundara Personnel Only')
    canvas.restoreState()


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
    canvas.setFont('Helvetica', 7.5)
    canvas.drawRightString(pw - 0.55 * inch, ph - 16, 'AI Underwriting Report  |  CONFIDENTIAL')
    canvas.drawRightString(pw - 0.55 * inch, ph - 28, 'Powered by Fundara')
    canvas.setStrokeColor(BORDER)
    canvas.setLineWidth(0.4)
    canvas.line(0.55 * inch, ph - 42, pw - 0.55 * inch, ph - 42)
    canvas.setFillColor(CARD)
    canvas.rect(0, 0, pw, 28, fill=1, stroke=0)
    canvas.setFillColor(TG)
    canvas.setFont('Helvetica', 7)
    canvas.drawString(0.55 * inch, 10, 'This report is generated by Fundara AI and is for internal use only. Not financial advice.')
    canvas.drawRightString(pw - 0.55 * inch, 10, f'Page {doc.page}')
    canvas.restoreState()


def _cell_color_style(text):
    t = text.strip().upper()
    if any(k in t for k in ('DECLINE', 'CRITICAL', 'NEGATIVE', 'FAILED', '-$')):
        return STY['td_r']
    if any(k in t for k in ('HIGH RISK', 'OVERDRAFT', 'UNSUSTAINABLE')):
        return STY['td_y']
    if any(k in t for k in ('APPROVE', 'PASS', 'GOOD', 'LOW RISK')):
        return STY['td_g']
    return STY['td']


def _parse_md_bold(text):
    return re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)


def _parse_table(lines):
    rows = []
    for line in lines:
        if re.match(r'^\|[-| :]+\|$', line.strip()):
            continue
        cells = [c.strip() for c in line.strip().strip('|').split('|')]
        rows.append(cells)
    if not rows:
        return None
    ncols = max(len(r) for r in rows)
    col_widths = [W_PAGE * 0.35, W_PAGE * 0.65] if ncols == 2 else [W_PAGE / ncols] * ncols
    flowable_rows = []
    for ri, row in enumerate(rows):
        while len(row) < ncols:
            row.append('')
        fcells = []
        for ci, cell in enumerate(row):
            clean = _parse_md_bold(cell)
            if ri == 0:
                sty = STY['th_l'] if ci == 0 else STY['th']
            else:
                sty = _cell_color_style(cell) if ci > 0 else STY['td_b']
            fcells.append(Paragraph(clean, sty))
        flowable_rows.append(fcells)
    t = Table(flowable_rows, colWidths=col_widths, repeatRows=1, splitByRow=True)
    t.setStyle(BASE_TS)
    return t


def _section_header(title):
    return [Spacer(1, 10), HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER), Spacer(1, 5), Paragraph(title, STY['h1'])]


def _decision_banner(text):
    t = text.upper()
    if 'DECLINE' in t:
        c = RED
        bg = HexColor('#150505')
    elif 'CONDITIONAL' in t:
        c = ORANGE
        bg = HexColor('#150A00')
    else:
        c = GREEN
        bg = HexColor('#051505')
    banner = Table([[Paragraph(text.strip(), ParagraphStyle('dec', fontName='Helvetica-Bold', fontSize=22, textColor=c, alignment=TA_CENTER, leading=28))]], colWidths=[W_PAGE])
    banner.setStyle(TableStyle([
        ('BACKGROUND', (0,0),(-1,-1), bg),
        ('BOX', (0,0),(-1,-1), 2, c),
        ('TOPPADDING', (0,0),(-1,-1), 14),
        ('BOTTOMPADDING', (0,0),(-1,-1), 14)
    ]))
    return banner


def _should_skip_line(stripped):
    s = stripped.upper()
    skip_patterns = [
        'FUNDARA.CO',
        'MERGED FUNDARA',
        'FUNDARA AI UNDERWRITING ANALYSIS REPORT',
        'FUNDARA AI UNDERWRITING REPORT',
    ]
    for p in skip_patterns:
        if s.startswith(p) or s == p:
            return True
    if re.match(r'^https?://', stripped):
        return True
    return False


def markdown_to_flowables(md):
    story = []
    lines = md.split('\n')
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

        if _should_skip_line(stripped):
            i += 1
            continue

        if stripped.startswith('|'):
            table_buf.append(stripped)
            i += 1
            continue
        else:
            flush_table()

        if stripped.startswith('## '):
            story += _section_header(stripped[3:].strip())
            i += 1; continue
        if stripped.startswith('### '):
            story.append(Paragraph(stripped[4:].strip(), STY['h2']))
            story.append(Spacer(1, 4))
            i += 1; continue
        if stripped.startswith('#### '):
            story.append(Paragraph(stripped[5:].strip(), STY['h3']))
            story.append(Spacer(1, 3))
            i += 1; continue
        if stripped.startswith('# '):
            story += _section_header(stripped[2:].strip())
            i += 1; continue
        if stripped in ('---', '***', '___'):
            story.append(HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER))
            story.append(Spacer(1, 5))
            i += 1; continue

        if stripped.startswith('- ') or stripped.startswith('* '):
            text = _parse_md_bold(stripped[2:])
            story.append(Paragraph(f'• {text}', STY['bullet']))
            story.append(Spacer(1, 2))
            i += 1; continue

        num_match = re.match(r'^(\d+)\.\s+(.+)', stripped)
        if num_match:
            num, text = num_match.groups()
            story.append(Paragraph(f'<b>{num}.</b>  {_parse_md_bold(text)}', STY['bullet']))
            story.append(Spacer(1, 2))
            i += 1; continue

        if re.match(r'^[*_]*(RECOMMENDATION:\s*(DECLINE|APPROVE|CONDITIONAL))[*_]*$', stripped, re.IGNORECASE):
            story.append(Spacer(1, 6))
            story.append(_decision_banner(re.sub(r'[*_]', '', stripped).upper()))
            story.append(Spacer(1, 10))
            i += 1; continue

        kv_match = re.match(r'^\*\*(.+?)\*\*[:\s]+(.+)', stripped)
        if kv_match:
            key, val = kv_match.groups()
            val_clean = _parse_md_bold(val)
            val_upper = val.upper()
            if any(k in val_upper for k in ('DECLINE', 'NEGATIVE', 'CRITICAL')):
                val_markup = f'<font color="#E53935"><b>{val_clean}</b></font>'
            elif any(k in val_upper for k in ('APPROVE', 'PASS', 'LOW RISK')):
                val_markup = f'<font color="#43A047"><b>{val_clean}</b></font>'
            else:
                val_markup = val_clean
            story.append(Paragraph(f'<b><font color="#90A4AE">{key}:</font></b>  {val_markup}', STY['body']))
            story.append(Spacer(1, 3))
            i += 1; continue

        text = _parse_md_bold(stripped)
        story.append(Paragraph(text, STY['body']))
        story.append(Spacer(1, 3))
        i += 1

    flush_table()
    return story


def convert_to_pdf(markdown_text, report_type="Detailed"):
    try:
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter,
            leftMargin=0.6*inch, rightMargin=0.6*inch,
            topMargin=0.75*inch, bottomMargin=0.55*inch,
            allowSplitting=1)

        logo_bytes = None
        logo_img_draw = None
        logo_img_flow = None
        try:
            r = requests.get(LOGO_URL, timeout=10)
            if r.status_code == 200:
                logo_bytes = r.content
                logo_img_draw = Image(io.BytesIO(logo_bytes), width=1.6*inch, height=0.38*inch)
                logo_img_flow = Image(io.BytesIO(logo_bytes), width=1.6*inch, height=0.38*inch)
                logo_img_flow.hAlign = 'LEFT'
        except Exception:
            pass

        report_label = "Quick Broker Report" if report_type == "Quick" else "AI Underwriting Report"
        story = []
        story.append(Spacer(1, 0.55*inch))
        if logo_img_flow:
            story.append(logo_img_flow)
            story.append(Spacer(1, 0.18*inch))
        story.append(HRFlowable(width=W_PAGE, thickness=2, color=LBLUE, spaceAfter=10))
        story.append(Paragraph(report_label, ParagraphStyle('ct', fontName='Helvetica-Bold', fontSize=26, textColor=TW, leading=31)))
        story.append(Paragraph('Powered by Fundara  |  Confidential', ParagraphStyle('cs', fontName='Helvetica', fontSize=10, textColor=TG, leading=14, spaceAfter=6)))
        story.append(HRFlowable(width=W_PAGE, thickness=0.5, color=BORDER, spaceAfter=16))
        story.append(Spacer(1, 0.1*inch))
        story += markdown_to_flowables(markdown_text)
        story.append(Spacer(1, 0.25*inch))
        story.append(HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER))
        story.append(Spacer(1, 6))
        story.append(Paragraph('This report is generated by Fundara AI and is for internal use only. Not financial advice.',
            ParagraphStyle('foot', fontName='Helvetica', fontSize=7, textColor=TG, alignment=TA_CENTER)))

        def on_first(canvas, doc):
            _draw_cover(canvas, doc)

        def on_later(canvas, doc):
            _draw_page(canvas, doc, logo_img_draw)

        doc.build(story, onFirstPage=on_first, onLaterPages=on_later)
        buffer.seek(0)
        return buffer.getvalue()
    except Exception as e:
        print(f"PDF error: {e}")
        return None


def get_r2_client():
    return boto3.client("s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto")


def upload_to_r2(pdf_bytes, contact_id):
    try:
        client = get_r2_client()
        filename = f"reports/{contact_id}_{uuid.uuid4().hex[:8]}.pdf"
        client.put_object(Bucket=R2_BUCKET, Key=filename, Body=pdf_bytes, ContentType="application/pdf")
        url = f"{R2_PUBLIC_URL}/{filename}"
        print(f"PDF uploaded to R2: {url}")
        return url
    except Exception as e:
        print(f"R2 upload error: {e}")
        return None


def download_pdf(url, api_key):
    for attempt in range(3):
        try:
            headers = {"Authorization": f"Bearer {api_key}"}
            r = requests.get(url, headers=headers, timeout=90)
            if r.status_code == 200:
                return r.content
            print(f"Download failed: {r.status_code}")
        except Exception as e:
            print(f"Download error attempt {attempt+1}: {e}")
            if attempt < 2:
                time.sleep(3)
    return None


def extract_text(pdf_bytes):
    try:
        text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            print(f"PDF opened successfully, pages={len(pdf.pages)}")
            for idx, page in enumerate(pdf.pages):
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
                    print(f"Page {idx+1}: text_chars={len(page_text)}, words={len(page_text.split())}, images={len(page.images)}")
        return text
    except Exception as e:
        print(f"Extract error: {e}")
        return ""


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
            print(f"Including PDF URL in GHL push: {pdf_url}")
        else:
            print("Warning: no PDF URL to push to GHL")
        payload = {"customFields": custom_fields}
        print(f"GHL payload custom_fields count: {len(custom_fields)}")
        r = requests.put(url, json=payload, headers=headers, timeout=30)
        print(f"GHL push status: {r.status_code}")
        print(f"GHL push response: {r.text[:300]}")
        return r.status_code
    except Exception as e:
        print(f"GHL push error: {e}")
        return 500


def save_to_gsheet(location_id, contact_id, report_type, pdf_url, cost_data):
    if not GSHEET_WEBHOOK_URL:
        print("No GSHEET_WEBHOOK_URL set, skipping")
        return
    try:
        payload = {
            "location_id": location_id,
            "contact_id": contact_id,
            "report_type": report_type,
            "pdf_url": pdf_url,
            "claude_input_tokens":  cost_data.get("claude_input_tokens", 0),
            "claude_output_tokens": cost_data.get("claude_output_tokens", 0),
            "claude_cost":          round(cost_data.get("claude_cost", 0), 6),
            "openai_input_tokens":  cost_data.get("openai_input_tokens", 0),
            "openai_output_tokens": cost_data.get("openai_output_tokens", 0),
            "openai_cost":          round(cost_data.get("openai_cost", 0), 6),
            "grok_input_tokens":    cost_data.get("grok_input_tokens", 0),
            "grok_output_tokens":   cost_data.get("grok_output_tokens", 0),
            "grok_cost":            round(cost_data.get("grok_cost", 0), 6),
            "merge_input_tokens":   cost_data.get("merge_input_tokens", 0),
            "merge_output_tokens":  cost_data.get("merge_output_tokens", 0),
            "merge_cost":           round(cost_data.get("merge_cost", 0), 6),
            "total_cost":           round(cost_data.get("total_cost", 0), 6),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        }
        r = requests.post(GSHEET_WEBHOOK_URL, json=payload, timeout=20)
        print(f"Google Sheet webhook status: {r.status_code}")
        print(f"Google Sheet webhook response: {r.text[:200]}")
        print(f"RUN COSTS: {payload}")
    except Exception as e:
        print(f"Google Sheet webhook error: {e}")


def analyze_with_openai(combined_text, system_prompt, user_prompt):
    cost_data = {"openai_input_tokens": 0, "openai_output_tokens": 0, "openai_cost": 0}
    try:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt.format(combined_text=combined_text)}
            ],
            "max_tokens": 8000
        }
        r = requests.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers, timeout=120)
        data = r.json()
        print(f"OpenAI response status: {r.status_code}")
        if "choices" not in data:
            print(f"OpenAI error: {data}")
            return "", cost_data
        usage = data.get("usage", {})
        input_tokens  = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)
        cost = (input_tokens / 1_000_000 * OPENAI_INPUT_COST_PER_M) + \
               (output_tokens / 1_000_000 * OPENAI_OUTPUT_COST_PER_M)
        cost_data = {"openai_input_tokens": input_tokens, "openai_output_tokens": output_tokens, "openai_cost": cost}
        return data["choices"][0]["message"]["content"], cost_data
    except Exception as e:
        print(f"OpenAI exception: {e}")
        return "", cost_data


def analyze_with_claude(combined_text, system_prompt, user_prompt):
    cost_data = {"claude_input_tokens": 0, "claude_output_tokens": 0, "claude_cost": 0}
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=8000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt.format(combined_text=combined_text)}]
        )
        input_tokens  = message.usage.input_tokens
        output_tokens = message.usage.output_tokens
        cost = (input_tokens / 1_000_000 * CLAUDE_INPUT_COST_PER_M) + \
               (output_tokens / 1_000_000 * CLAUDE_OUTPUT_COST_PER_M)
        cost_data = {"claude_input_tokens": input_tokens, "claude_output_tokens": output_tokens, "claude_cost": cost}
        return message.content[0].text, cost_data
    except Exception as e:
        print(f"Claude exception: {e}")
        return "", cost_data


def analyze_with_grok(combined_text, system_prompt, user_prompt):
    cost_data = {"grok_input_tokens": 0, "grok_output_tokens": 0, "grok_cost": 0}
    try:
        headers = {"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": "grok-3-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt.format(combined_text=combined_text)}
            ],
            "max_tokens": 8000
        }
        r = requests.post("https://api.x.ai/v1/chat/completions", json=payload, headers=headers, timeout=120)
        data = r.json()
        print(f"Grok response status: {r.status_code}")
        if "choices" not in data:
            print(f"Grok error: {data}")
            return "", cost_data
        usage = data.get("usage", {})
        input_tokens  = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)
        cost = (input_tokens / 1_000_000 * GROK_INPUT_COST_PER_M) + \
               (output_tokens / 1_000_000 * GROK_OUTPUT_COST_PER_M)
        cost_data = {"grok_input_tokens": input_tokens, "grok_output_tokens": output_tokens, "grok_cost": cost}
        return data["choices"][0]["message"]["content"], cost_data
    except Exception as e:
        print(f"Grok exception: {e}")
        return "", cost_data


def merge_reports(report1, report2, report3, report_type="Detailed"):
    merge_cost_data = {"merge_input_tokens": 0, "merge_output_tokens": 0, "merge_cost": 0}
    available = [r for r in [report1, report2, report3] if r and len(r) > 200]

    if not available:
        return "No analysis could be completed. All AI models failed. Please check API credits.", merge_cost_data

    if len(available) == 1:
        print("Only 1 analysis available, skipping merge")
        return available[0], merge_cost_data

    if report_type == "Quick":
        format_instruction = """Output ONLY Section 0 (Decision Snapshot) and Section 2 (Deal Sheet).
Use ## for section headings and ### for table headings.
Section 2 must use TWO separate 2-column tables (Field | Value format):
- ### Table 1 - Business Info
- ### Table 2 - Financial Summary & Decision
Do NOT create wide multi-column tables."""
    else:
        format_instruction = """Keep Sections 0-11 format using ## headings for each section.
CRITICAL — produce an EXHAUSTIVE, FORENSIC-LEVEL report:
- Section 0: Must include specific dollar amounts, named transaction sources, exact dates, and RECOMMENDATION on its own line
- Section 1: Scorecard must include a Notes column for EVERY criterion with specific evidence. Keep notes concise — max 3 sentences per cell.
- Section 2: Deal Sheet must be comprehensive — never leave fields as TBD
- Section 3: Monthly Ledger must include Opening Balance, Deposits, Withdrawals, Checks Cleared, Service Fees, Ending Balance, Avg Daily Balance, Overdraft Days, Low Balance Days per month
- Section 4: Portfolio Metrics must include deposit channel mix %, top-5 deposit sources with names and amounts, top-5 expense sinks
- Section 7: Red Flags must be a severity-tagged table with columns: Severity, Flag, Evidence, Impact
- Sections 8-11: Must contain substantive narrative
Use the most conservative figures across all analyses.
Name specific individuals, payees, and transaction patterns found in the statements."""

    analyses_text = ""
    for idx, r in enumerate(available, 1):
        analyses_text += f"\nANALYSIS {idx}:\n{r}\n"

    merge_prompt = f"""You are a senior underwriting editor at Fundara. Merge these underwriting analyses into ONE definitive report.

RULES:
- Never mention any AI companies, models, tools, or technologies
- Present all findings as Fundara AI Underwriting analysis
- Do NOT include "fundara.co" or any URLs
- Do NOT include any title line above Section 0. Start directly with ## Section 0
- RECOMMENDATION must appear on its own line in Section 0
- Use most conservative figures when analyses disagree
- {format_instruction}

{analyses_text}

Produce the final merged Fundara underwriting report starting with ## Section 0:"""

    # Try Claude first
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=8000,
            messages=[{"role": "user", "content": merge_prompt}]
        )
        input_tokens  = message.usage.input_tokens
        output_tokens = message.usage.output_tokens
        cost = (input_tokens / 1_000_000 * CLAUDE_INPUT_COST_PER_M) + \
               (output_tokens / 1_000_000 * CLAUDE_OUTPUT_COST_PER_M)
        merge_cost_data = {"merge_input_tokens": input_tokens, "merge_output_tokens": output_tokens, "merge_cost": cost}
        print("Merge completed by Claude")
        return message.content[0].text, merge_cost_data
    except Exception as e:
        print(f"Claude merge failed: {e}")

    # Fall back to OpenAI
    try:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": merge_prompt}], "max_tokens": 8000}
        r = requests.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers, timeout=120)
        data = r.json()
        if "choices" in data:
            usage = data.get("usage", {})
            input_tokens  = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            cost = (input_tokens / 1_000_000 * OPENAI_INPUT_COST_PER_M) + \
                   (output_tokens / 1_000_000 * OPENAI_OUTPUT_COST_PER_M)
            merge_cost_data = {"merge_input_tokens": input_tokens, "merge_output_tokens": output_tokens, "merge_cost": cost}
            print("Merge completed by OpenAI fallback")
            return data["choices"][0]["message"]["content"], merge_cost_data
    except Exception as e:
        print(f"OpenAI merge failed: {e}")

    # Fall back to Grok
    try:
        headers = {"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": "grok-3-mini", "messages": [{"role": "user", "content": merge_prompt}], "max_tokens": 8000}
        r = requests.post("https://api.x.ai/v1/chat/completions", json=payload, headers=headers, timeout=120)
        data = r.json()
        if "choices" in data:
            print("Merge completed by Grok fallback")
            return data["choices"][0]["message"]["content"], merge_cost_data
    except Exception as e:
        print(f"Grok merge failed: {e}")

    print("All merges failed, returning best single analysis")
    return max(available, key=len), merge_cost_data


def run_analysis(data, contact_id, location_id, ghl_key, report_type):
    try:
        system_prompt = SYSTEM_PROMPT_QUICK if report_type == "Quick" else SYSTEM_PROMPT
        user_prompt   = USER_PROMPT_QUICK   if report_type == "Quick" else USER_PROMPT

        statement_urls = []
        for i in range(1, 11):
            url = data.get(f"bank_statement_{i}")
            if url and url not in ("null", "undefined", "") and url.startswith("http"):
                statement_urls.append((i, url))

        if not statement_urls:
            print(f"No bank statements found for contact {contact_id}")
            return

        combined_text = ""
        for idx, url in statement_urls:
            print(f"Statement {idx} URL: {url}")
            print(f"Downloading statement {idx}")
            pdf_bytes = download_pdf(url, ghl_key)
            if pdf_bytes:
                print(f"Statement {idx} downloaded successfully, bytes={len(pdf_bytes)}")
                text = extract_text(pdf_bytes)
                if text:
                    combined_text += f"\n--- BANK STATEMENT {idx} ---\n{text}\n"
                    print(f"Statement {idx} added to combined_text")
                else:
                    print(f"Warning: no text extracted from statement {idx}")
            else:
                print(f"Warning: could not download statement {idx}, skipping")

        if not combined_text.strip():
            print(f"No text extracted from any statements for contact {contact_id}")
            return

        print(f"Generating {report_type} report")

        openai_result = ("", {"openai_input_tokens": 0, "openai_output_tokens": 0, "openai_cost": 0})
        claude_result = ("", {"claude_input_tokens": 0, "claude_output_tokens": 0, "claude_cost": 0})
        grok_result   = ("", {"grok_input_tokens": 0, "grok_output_tokens": 0, "grok_cost": 0})

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(analyze_with_openai, combined_text, system_prompt, user_prompt): "openai",
                executor.submit(analyze_with_claude, combined_text, system_prompt, user_prompt): "claude",
                executor.submit(analyze_with_grok,   combined_text, system_prompt, user_prompt): "grok",
            }
            for future in as_completed(futures):
                key = futures[future]
                result = future.result()
                if key == "openai":
                    openai_result = result
                elif key == "claude":
                    claude_result = result
                elif key == "grok":
                    grok_result = result

        final_report, merge_cost_data = merge_reports(
            openai_result[0],
            claude_result[0],
            grok_result[0],
            report_type
        )

        # Aggregate all costs
        cost_data = {}
        cost_data.update(openai_result[1])
        cost_data.update(claude_result[1])
        cost_data.update(grok_result[1])
        cost_data.update(merge_cost_data)
        cost_data["total_cost"] = (
            cost_data.get("openai_cost", 0) +
            cost_data.get("claude_cost", 0) +
            cost_data.get("grok_cost", 0) +
            cost_data.get("merge_cost", 0)
        )

        print(f"Converting final report to PDF")
        pdf_url = ""
        pdf_bytes = convert_to_pdf(final_report, report_type)
        if pdf_bytes:
            pdf_url = upload_to_r2(pdf_bytes, contact_id) or ""

        push_to_ghl(contact_id, final_report, ghl_key, pdf_url)
        save_to_gsheet(location_id, contact_id, report_type, pdf_url, cost_data)

        print(f"Analysis complete for contact {contact_id}")

    except Exception as e:
        print(f"run_analysis error for contact {contact_id}: {e}")


@app.route("/analyze", methods=["POST"])
def analyze():
    data = request.json or {}
    contact_id  = data.get("contact_id")
    location_id = data.get("location_id")
    ghl_key     = data.get("ghl_api_key")
    report_type = (data.get("report_type") or "Detailed").strip()

    print(f"Request — location: {location_id} | contact: {contact_id} | type: {report_type}")

    if not ghl_key:
        return jsonify({"error": "No GHL API key provided"}), 400
    if not contact_id:
        return jsonify({"error": "No contact ID provided"}), 400

    thread = threading.Thread(
        target=run_analysis,
        args=(data, contact_id, location_id, ghl_key, report_type)
    )
    thread.daemon = True
    thread.start()

    return jsonify({
        "success": True,
        "message": "Analysis started",
        "contact_id": contact_id,
        "report_type": report_type
    }), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
