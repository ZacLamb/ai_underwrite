import os
import io
import re
import uuid
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
    HRFlowable, Table, TableStyle, KeepTogether, PageBreak
)
from reportlab.lib import colors

app = Flask(__name__)

# ── ENV ────────────────────────────────────────────────────────────
OPENAI_API_KEY       = os.environ.get("OPENAI_API_KEY")
ANTHROPIC_API_KEY    = os.environ.get("ANTHROPIC_API_KEY")
GROK_API_KEY         = os.environ.get("GROK_API_KEY")
R2_ACCESS_KEY_ID     = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID        = os.environ.get("R2_ACCOUNT_ID")
R2_PUBLIC_URL        = os.environ.get("R2_PUBLIC_URL")
R2_BUCKET            = "fundara-reports"

LOGO_URL = "https://assets.cdn.filesafe.space/HD59NWC1biIA31IHm1y8/media/69a4925b753f150a68663d79.png"

# ── PALETTE ────────────────────────────────────────────────────────
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

# ── PROMPTS ────────────────────────────────────────────────────────
SYSTEM_PROMPT = """ROLE: Senior COMMERCIAL UNDERWRITER (50y; restaurants & MCA). Tool-driven extraction only; NEVER reveal internal reasoning; no approximations. If unparseable, state cause.

IMPORTANT: Never mention any AI companies, models, tools, or technologies in your output. Present all findings as Fundara AI Underwriting analysis only.

FORMAT (strict)
- Markdown only, PDF-ready. No charts/images.
- Put Section 0: Decision Snapshot FIRST (<=400 words).
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

USER_PROMPT = """Render a clean, print-ready underwriting report. Section 0 Decision Snapshot first (<=400 words), then Sections 1-11.
- Markdown only; tables preferred; omit N/A rows; one blank line between sections.
- USD whole dollars with commas; percentages 1 decimal; mask account numbers.
- Do NOT mention any AI tools, models, or companies. Present as Fundara AI analysis only.

Here are the bank statements:

{combined_text}"""

SYSTEM_PROMPT_QUICK = """ROLE: Senior COMMERCIAL UNDERWRITER (50y; MCA focus). Quick-scan mode. Extract only what is needed for a broker to make a fast funding decision. No approximations. If unparseable, state cause.

IMPORTANT: Never mention any AI companies, models, tools, or technologies. Present all findings as Fundara AI Underwriting analysis only.

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
| Business Name | ... |
| DBA | ... |
| Owner | ... |
| Bank | ... |
| Account Type | ... |
| Review Period | ... |

### Table 2 - Financial Summary & Decision:
| Field | Value |
|-------|-------|
| Avg Monthly Deposits | ... |
| Avg Daily Balance | ... |
| Total Deposits | ... |
| Negative Days (avg/month) | ... |
| Overdraft Fees | ... |
| Existing MCA Positions | ... |
| Recommended Position | ... |
| Recommended Factor Rate | ... |
| Decision | APPROVE or DECLINE or CONDITIONAL |

Use ONLY these two 2-column tables. Do NOT create a wide multi-column table."""

USER_PROMPT_QUICK = """Render a quick broker decision report with ONLY Section 0 (Decision Snapshot) and Section 2 (Deal Sheet).
- Markdown only; one blank line between sections.
- Use ## for section headings and ### for table headings.
- Section 0: bullet list format, concise.
- Section 2: TWO separate 2-column tables (Field | Value). First table for business info, second for financials and decision.
- Do NOT create a wide multi-column table.
- USD whole dollars with commas; percentages 1 decimal; mask account numbers.
- Do NOT mention any AI tools, models, or companies. Present as Fundara AI analysis only.

Here are the bank statements:

{combined_text}"""


# ── STYLES ─────────────────────────────────────────────────────────
def build_styles():
    s = {}
    def ps(name, **kw):
        base = dict(fontName='Helvetica', fontSize=9, textColor=TL,
                    leading=13, spaceAfter=0, spaceBefore=0)
        base.update(kw)
        s[name] = ParagraphStyle(name, **base)

    ps('h1', fontName='Helvetica-Bold', fontSize=13, textColor=TW,
       spaceBefore=6, spaceAfter=4, leading=17)
    ps('h2', fontName='Helvetica-Bold', fontSize=10.5, textColor=LBLUE,
       spaceBefore=8, spaceAfter=4, leading=14)
    ps('h3', fontName='Helvetica-Bold', fontSize=9.5, textColor=TW,
       spaceBefore=5, spaceAfter=3, leading=13)
    ps('body',   fontSize=8.5, textColor=TL, leading=12)
    ps('body_b', fontName='Helvetica-Bold', fontSize=8.5, textColor=TW, leading=12)
    ps('bullet', fontSize=8.5, textColor=TL, leading=12, leftIndent=12)
    ps('caption', fontSize=7.5, textColor=TG, leading=10,
       fontName='Helvetica-Oblique', alignment=TA_CENTER, spaceAfter=4)
    ps('center', fontSize=8.5, textColor=TL, leading=12, alignment=TA_CENTER)
    ps('th',   fontName='Helvetica-Bold', fontSize=8, textColor=TW,
       leading=11, alignment=TA_CENTER)
    ps('th_l', fontName='Helvetica-Bold', fontSize=8, textColor=TW, leading=11)
    ps('td',   fontSize=8, textColor=TL, leading=11)
    ps('td_c', fontSize=8, textColor=TL, leading=11, alignment=TA_CENTER)
    ps('td_b', fontName='Helvetica-Bold', fontSize=8, textColor=TW, leading=11)
    ps('td_r', fontName='Helvetica-Bold', fontSize=8, textColor=RED,   leading=11)
    ps('td_g', fontName='Helvetica-Bold', fontSize=8, textColor=GREEN, leading=11)
    ps('td_y', fontName='Helvetica-Bold', fontSize=8, textColor=YELLOW, leading=11)
    ps('td_o', fontName='Helvetica-Bold', fontSize=8, textColor=ORANGE, leading=11)
    return s

STY = build_styles()

BASE_TS = TableStyle([
    ('BACKGROUND',    (0, 0), (-1,  0),  HDR_BG),
    ('ROWBACKGROUNDS',(0, 1), (-1, -1),  [CARD, ROW_ALT]),
    ('GRID',          (0, 0), (-1, -1),  0.35, BORDER),
    ('FONTNAME',      (0, 0), (-1,  0),  'Helvetica-Bold'),
    ('FONTSIZE',      (0, 0), (-1,  0),  8),
    ('TEXTCOLOR',     (0, 0), (-1,  0),  TW),
    ('FONTNAME',      (0, 1), (-1, -1),  'Helvetica'),
    ('FONTSIZE',      (0, 1), (-1, -1),  8),
    ('TEXTCOLOR',     (0, 1), (-1, -1),  TL),
    ('TOPPADDING',    (0, 0), (-1, -1),  5),
    ('BOTTOMPADDING', (0, 0), (-1, -1),  5),
    ('LEFTPADDING',   (0, 0), (-1, -1),  8),
    ('RIGHTPADDING',  (0, 0), (-1, -1),  8),
    ('VALIGN',        (0, 0), (-1, -1),  'MIDDLE'),
])


# ── CANVAS CALLBACKS ────────────────────────────────────────────────
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
    canvas.drawRightString(pw - 0.55 * inch, ph - 16,
                           'AI Underwriting Report  |  CONFIDENTIAL')
    canvas.drawRightString(pw - 0.55 * inch, ph - 28,
                           'Powered by Fundara')
    canvas.setStrokeColor(BORDER)
    canvas.setLineWidth(0.4)
    canvas.line(0.55 * inch, ph - 42, pw - 0.55 * inch, ph - 42)
    canvas.setFillColor(CARD)
    canvas.rect(0, 0, pw, 28, fill=1, stroke=0)
    canvas.setFillColor(TG)
    canvas.setFont('Helvetica', 7)
    canvas.drawString(0.55 * inch, 10,
                      'This report is generated by Fundara AI and is for internal use only. Not financial advice.')
    canvas.drawRightString(pw - 0.55 * inch, 10, f'Page {doc.page}')
    canvas.restoreState()


def _draw_cover(canvas, doc, logo_bytes=None):
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
    canvas.drawString(0.55 * inch, 11,
                      'CONFIDENTIAL — Authorized Fundara Personnel Only')
    canvas.restoreState()


# ── PDF HELPERS ─────────────────────────────────────────────────────
def _cell_color_style(text):
    t = text.strip().upper()
    if any(k in t for k in ('DECLINE', 'CRITICAL', 'NEGATIVE', 'FAILED')):
        return STY['td_r']
    if any(k in t for k in ('HIGH RISK', 'OVERDRAFT', 'UNSUSTAINABLE')):
        return STY['td_o']
    if any(k in t for k in ('MEDIUM', 'CAUTION', 'WEAK')):
        return STY['td_y']
    if any(k in t for k in ('APPROVE', 'PASS', 'GOOD', 'LOW RISK', 'STRONG')):
        return STY['td_g']
    return STY['td']


def _parse_md_bold(text):
    return re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)


def _parse_table(table_lines):
    rows = []
    for line in table_lines:
        if re.match(r'^\|[-| :]+\|$', line.strip()):
            continue
        cells = [c.strip() for c in line.strip().strip('|').split('|')]
        rows.append(cells)
    if not rows:
        return None
    ncols = max(len(r) for r in rows)

    if ncols == 2:
        col_widths = [W_PAGE * 0.35, W_PAGE * 0.65]
    else:
        col_widths = [W_PAGE / ncols] * ncols

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
                sty = _cell_color_style(cell)
                if ci == 0:
                    sty = STY['td_b'] if ri % 2 == 0 else STY['td']
            fcells.append(Paragraph(clean, sty))
        flowable_rows.append(fcells)

    t = Table(flowable_rows, colWidths=col_widths, repeatRows=1)
    t.setStyle(BASE_TS)
    return t


def _section_header(title):
    return [
        Spacer(1, 10),
        HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER),
        Spacer(1, 5),
        Paragraph(title, STY['h1']),
    ]


def _flag_card(severity, title, detail):
    sev_map = {'CRITICAL': RED, 'HIGH': ORANGE, 'MEDIUM': YELLOW, 'LOW': GREEN}
    sc = sev_map.get(severity.upper(), TG)
    badge = Table([[Paragraph(severity.upper(),
        ParagraphStyle('fb', fontName='Helvetica-Bold', fontSize=7,
                       textColor=BG, alignment=TA_CENTER))]],
        colWidths=[0.65 * inch])
    badge.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,-1), sc),
        ('TOPPADDING',    (0,0),(-1,-1), 5),
        ('BOTTOMPADDING', (0,0),(-1,-1), 5),
    ]))
    content = Table([
        [Paragraph(f'<b>{_parse_md_bold(title)}</b>',
            ParagraphStyle('ft', fontName='Helvetica-Bold',
                           fontSize=9, textColor=TW, leading=12))],
        [Spacer(1, 3)],
        [Paragraph(_parse_md_bold(detail), STY['body'])],
    ], colWidths=[6.1 * inch])
    content.setStyle(TableStyle([
        ('TOPPADDING',    (0,0),(-1,-1), 2),
        ('BOTTOMPADDING', (0,0),(-1,-1), 2),
        ('LEFTPADDING',   (0,0),(-1,-1), 0),
        ('RIGHTPADDING',  (0,0),(-1,-1), 0),
    ]))
    card = Table([[badge, content]], colWidths=[0.7*inch, 6.3*inch])
    card.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,-1), CARD),
        ('BOX',           (0,0),(-1,-1), 0.35, BORDER),
        ('LINEBEFORE',    (0,0),(0,-1),  4, sc),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
        ('LEFTPADDING',   (1,0),(1,0),   10),
        ('RIGHTPADDING',  (1,0),(1,0),   8),
        ('TOPPADDING',    (0,0),(-1,-1), 7),
        ('BOTTOMPADDING', (0,0),(-1,-1), 7),
    ]))
    return KeepTogether([card, Spacer(1, 5)])


def _decision_banner(text):
    is_decline = 'DECLINE' in text.upper()
    c = RED if is_decline else GREEN
    bg = HexColor('#150505') if is_decline else HexColor('#051505')
    banner = Table([[Paragraph(text.strip(),
        ParagraphStyle('dec', fontName='Helvetica-Bold', fontSize=28,
                       textColor=c, alignment=TA_CENTER, leading=34))]],
        colWidths=[W_PAGE])
    banner.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,-1), bg),
        ('BOX',           (0,0),(-1,-1), 2.5, c),
        ('TOPPADDING',    (0,0),(-1,-1), 18),
        ('BOTTOMPADDING', (0,0),(-1,-1), 18),
    ]))
    return banner


def markdown_to_flowables(markdown_text):
    story = []
    lines = markdown_text.split('\n')
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

        if stripped.startswith('|'):
            table_buf.append(stripped)
            i += 1
            continue
        else:
            flush_table()

        if stripped.startswith('## '):
            story += _section_header(stripped[3:].strip())
            i += 1
            continue

        if stripped.startswith('### '):
            story.append(Paragraph(stripped[4:].strip(), STY['h2']))
            story.append(Spacer(1, 4))
            i += 1
            continue

        if stripped.startswith('#### '):
            story.append(Paragraph(stripped[5:].strip(), STY['h3']))
            story.append(Spacer(1, 3))
            i += 1
            continue

        if stripped.startswith('# '):
            story += _section_header(stripped[2:].strip())
            i += 1
            continue

        if stripped in ('---', '***', '___'):
            story.append(HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER))
            story.append(Spacer(1, 5))
            i += 1
            continue

        if stripped.startswith('- ') or stripped.startswith('* '):
            text = _parse_md_bold(stripped[2:])
            flag_match = re.match(
                r'\*\*(CRITICAL|HIGH|MEDIUM|LOW)\*\*[:\s–-]+(.+?)(?:[–-]+|:)(.+)',
                stripped[2:], re.IGNORECASE)
            if flag_match:
                sev, ftitle, fdetail = flag_match.groups()
                story.append(_flag_card(sev, ftitle.strip(), fdetail.strip()))
                i += 1
                continue
            story.append(Paragraph(f'• {text}', STY['bullet']))
            story.append(Spacer(1, 2))
            i += 1
            continue

        num_match = re.match(r'^(\d+)\.\s+(.+)', stripped)
        if num_match:
            num, text = num_match.groups()
            story.append(Paragraph(
                f'<b>{num}.</b>  {_parse_md_bold(text)}', STY['bullet']))
            story.append(Spacer(1, 2))
            i += 1
            continue

        if re.match(r'^[*_]*(DECLINE|APPROVE|APPROVED|DECLINED)[*_]*$',
                    stripped, re.IGNORECASE):
            story.append(Spacer(1, 6))
            story.append(_decision_banner(re.sub(r'[*_]', '', stripped).upper()))
            story.append(Spacer(1, 10))
            i += 1
            continue

        if re.match(r'^(SECTION|TABLE)\s+\d+', stripped, re.IGNORECASE):
            story.append(Spacer(1, 8))
            story.append(Paragraph(stripped.rstrip(':'), STY['h2']))
            story.append(Spacer(1, 4))
            i += 1
            continue

        bold_only = re.match(r'^\*\*(.+)\*\*$', stripped)
        if bold_only:
            story.append(Paragraph(f'<b>{bold_only.group(1)}</b>', STY['body_b']))
            story.append(Spacer(1, 3))
            i += 1
            continue

        kv_match = re.match(r'^\*\*(.+?)\*\*[:\s]+(.+)', stripped)
        if kv_match:
            key, val = kv_match.groups()
            val_clean = _parse_md_bold(val)
            val_upper = val.upper()
            if any(k in val_upper for k in ('DECLINE', 'NEGATIVE', 'CRITICAL', 'FAILED')):
                val_markup = f'<font color="#E53935"><b>{val_clean}</b></font>'
            elif any(k in val_upper for k in ('HIGH RISK', 'UNSUSTAINABLE')):
                val_markup = f'<font color="#F57C00"><b>{val_clean}</b></font>'
            elif any(k in val_upper for k in ('APPROVE', 'PASS', 'LOW RISK')):
                val_markup = f'<font color="#43A047"><b>{val_clean}</b></font>'
            else:
                val_markup = val_clean
            story.append(Paragraph(
                f'<b><font color="#90A4AE">{key}:</font></b>  {val_markup}',
                STY['body']))
            story.append(Spacer(1, 3))
            i += 1
            continue

        text = _parse_md_bold(stripped)
        if any(k in stripped.upper() for k in ('RECOMMENDATION: DECLINE', 'RECOMMENDATION: APPROVE')):
            is_dec = 'DECLINE' in stripped.upper()
            c_hex = '#E53935' if is_dec else '#43A047'
            story.append(Paragraph(
                f'<b><font color="{c_hex}">{text}</font></b>', STY['body_b']))
        else:
            story.append(Paragraph(text, STY['body']))
        story.append(Spacer(1, 3))
        i += 1

    flush_table()
    return story


def convert_to_pdf(markdown_text, report_type="Detailed"):
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

        logo_bytes = None
        logo_img_draw = None
        logo_img_flow = None
        try:
            logo_resp = requests.get(LOGO_URL, timeout=10)
            if logo_resp.status_code == 200:
                logo_bytes = logo_resp.content
                logo_img_draw = Image(io.BytesIO(logo_bytes),
                                      width=1.6 * inch, height=0.38 * inch)
                logo_img_flow = Image(io.BytesIO(logo_bytes),
                                      width=2.4 * inch, height=0.56 * inch)
                logo_img_flow.hAlign = 'LEFT'
        except Exception as logo_err:
            print(f"Logo load error: {logo_err}")

        report_label = "Quick Broker Report" if report_type == "Quick" else "AI Underwriting Report"

        story = []
        story.append(Spacer(1, 0.55 * inch))

        if logo_img_flow:
            story.append(logo_img_flow)
            story.append(Spacer(1, 0.18 * inch))

        story.append(HRFlowable(width=W_PAGE, thickness=2, color=LBLUE, spaceAfter=10))
        story.append(Paragraph(
            report_label,
            ParagraphStyle('cover_title', fontName='Helvetica-Bold',
                           fontSize=26, textColor=TW, leading=31)))
        story.append(Paragraph(
            'Powered by Fundara  |  Confidential',
            ParagraphStyle('cover_sub', fontName='Helvetica',
                           fontSize=10, textColor=TG, leading=14, spaceAfter=6)))
        story.append(HRFlowable(width=W_PAGE, thickness=0.5, color=BORDER, spaceAfter=16))
        story.append(Spacer(1, 0.1 * inch))

        story += markdown_to_flowables(markdown_text)

        story.append(Spacer(1, 0.25 * inch))
        story.append(HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER))
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            'This report is generated by Fundara AI and is for internal use only. Not financial advice.',
            ParagraphStyle('foot', fontName='Helvetica', fontSize=7,
                           textColor=TG, alignment=TA_CENTER)))

        def on_first(canvas, doc):
            _draw_cover(canvas, doc, logo_bytes)

        def on_later(canvas, doc):
            _draw_page(canvas, doc, logo_img_draw)

        doc.build(story, onFirstPage=on_first, onLaterPages=on_later)
        buffer.seek(0)
        return buffer.getvalue()

    except Exception as pdf_err:
        print(f"PDF conversion error: {pdf_err}")
        return None


# ── R2 ─────────────────────────────────────────────────────────────
def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto"
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
        print(f"R2 upload error: {str(r2_err)}")
        return None


# ── GHL ─────────────────────────────────────────────────────────────
def download_pdf(url, api_key):
    try:
        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.content
        print(f"Failed to download PDF: {response.status_code}")
        return None
    except Exception as download_err:
        print(f"Download error: {str(download_err)}")
        return None


def extract_text(pdf_bytes):
    try:
        text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text
    except Exception as extract_err:
        print(f"Extract error: {str(extract_err)}")
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
        payload = {"customFields": custom_fields}
        r = requests.put(url, json=payload, headers=headers, timeout=30)
        print(f"GHL push status: {r.status_code}")
        return r.status_code
    except Exception as ghl_err:
        print(f"GHL push error: {str(ghl_err)}")
        return 500


# ── AI ──────────────────────────────────────────────────────────────
def analyze_with_openai(combined_text, system_prompt, user_prompt):
    try:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt.format(combined_text=combined_text)}
            ],
            "max_tokens": 4000
        }
        r = requests.post("https://api.openai.com/v1/chat/completions",
                          json=payload, headers=headers, timeout=120)
        data = r.json()
        print(f"OpenAI response status: {r.status_code}")
        if "choices" not in data:
            print(f"OpenAI error response: {data}")
            return f"Analysis 1 error: {data}"
        return data["choices"][0]["message"]["content"]
    except Exception as openai_err:
        print(f"OpenAI exception: {str(openai_err)}")
        return f"Analysis 1 failed: {str(openai_err)}"


def analyze_with_claude(combined_text, system_prompt, user_prompt):
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=4000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt.format(combined_text=combined_text)}]
        )
        return message.content[0].text
    except Exception as claude_err:
        print(f"Claude exception: {str(claude_err)}")
        return f"Analysis 2 failed: {str(claude_err)}"


def analyze_with_grok(combined_text, system_prompt, user_prompt):
    try:
        headers = {"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": "grok-3-mini",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt.format(combined_text=combined_text)}
            ],
            "max_tokens": 4000
        }
        r = requests.post("https://api.x.ai/v1/chat/completions",
                          json=payload, headers=headers, timeout=120)
        data = r.json()
        print(f"Grok response status: {r.status_code}")
        if "choices" not in data:
            print(f"Grok error response: {data}")
            return f"Analysis 3 error: {data}"
        return data["choices"][0]["message"]["content"]
    except Exception as grok_err:
        print(f"Grok exception: {str(grok_err)}")
        return f"Analysis 3 failed: {str(grok_err)}"


def merge_reports(report1, report2, report3, report_type="Detailed"):
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)

        if report_type == "Quick":
            format_instruction = """Output ONLY Section 0 (Decision Snapshot) and Section 2 (Deal Sheet).
Use ## for section headings and ### for table headings.
Section 2 must use TWO separate 2-column tables (Field | Value format):
- ### Table 1 - Business Info
- ### Table 2 - Financial Summary & Decision
Do NOT create wide multi-column tables."""
        else:
            format_instruction = """Keep Sections 0-11 format using ## headings for each section.
CRITICAL — this merge must produce an EXHAUSTIVE, FORENSIC-LEVEL report:
- Section 0: Must include specific dollar amounts, named transaction sources, exact dates, and a clear RECOMMENDATION line
- Section 1: Scorecard must include a Notes column for EVERY criterion explaining WHY that score was given with specific evidence
- Section 2: Deal Sheet must be a comprehensive two-column table — never leave fields as TBD or N/A without explanation
- Section 3: Monthly Ledger must include Opening Balance, Deposits, Withdrawals, Checks Cleared, Service Fees, Ending Balance, Avg Daily Balance, Overdraft Days, Low Balance Days (<$100) for every month
- Section 4: Portfolio Metrics must include deposit channel mix %, top-5 deposit sources with names and amounts, top-5 expense sinks with payee names and amounts, MCA position detail table
- Section 7: Red Flags must be a severity-tagged table with columns: Severity, Flag, Evidence, Impact — flag SPECIFIC anomalies like identical check amounts, out-of-state addresses, round-trip transactions, MCA rebills disguised as deposits
- Sections 8-11: Must contain substantive narrative, not placeholder text
Use the most conservative figures across all three analyses.
Name specific individuals, payees, and transaction patterns found in the statements.
The final report must read like it was written by a 50-year veteran underwriter who read every line of every statement."""
        merge_prompt = f"""You are a senior underwriting editor at Fundara. Merge these three underwriting analyses into ONE definitive report.

IMPORTANT RULES:
- Never mention any AI companies, models, tools, or technologies
- Present all findings as Fundara AI Underwriting analysis
- Use most accurate/conservative figures when there are discrepancies
- If all three agree, use that figure with high confidence
- If two agree and one differs, use the majority and note the discrepancy
- Flag significant discrepancies in Red Flags section
- {format_instruction}

ANALYSIS 1:
{report1}

ANALYSIS 2:
{report2}

ANALYSIS 3:
{report3}

Produce the final merged Fundara underwriting report:"""

        message = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=4000,
            messages=[{"role": "user", "content": merge_prompt}]
        )
        return message.content[0].text
    except Exception as merge_err:
        print(f"Merge exception: {str(merge_err)}")
        return report1


# ── ROUTE ───────────────────────────────────────────────────────────
@app.route("/analyze", methods=["POST"])
def analyze():
    data = request.json

    contact_id = data.get("contact_id")
    location_id = data.get("location_id")
    ghl_key = data.get("ghl_api_key")
    report_type = data.get("report_type", "Detailed").strip()

    print(f"Request — location: {location_id} | contact: {contact_id} | type: {report_type}")

    if not ghl_key:
        return jsonify({"error": "No GHL API key provided"}), 400

    if not contact_id:
        return jsonify({"error": "No contact ID provided"}), 400

    if report_type == "Quick":
        system_prompt = SYSTEM_PROMPT_QUICK
        user_prompt = USER_PROMPT_QUICK
    else:
        system_prompt = SYSTEM_PROMPT
        user_prompt = USER_PROMPT

    statement_urls = []
    for i in range(1, 11):
        url = data.get(f"bank_statement_{i}")
        if url and url != "null":
            statement_urls.append((i, url))

    if not statement_urls:
        return jsonify({"error": "No bank statements found"}), 400

    combined_text = ""
    for idx, url in statement_urls:
        pdf_bytes = download_pdf(url, ghl_key)
        if pdf_bytes:
            text = extract_text(pdf_bytes)
            combined_text += f"\n--- BANK STATEMENT {idx} ---\n{text}\n"

    if not combined_text.strip():
        return jsonify({"error": "Could not extract text from any PDFs"}), 400

    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_key = {
            executor.submit(analyze_with_openai, combined_text, system_prompt, user_prompt): "analysis1",
            executor.submit(analyze_with_claude, combined_text, system_prompt, user_prompt): "analysis2",
            executor.submit(analyze_with_grok,   combined_text, system_prompt, user_prompt): "analysis3"
        }
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            results[key] = future.result()

    final_report = merge_reports(
        results.get("analysis1", ""),
        results.get("analysis2", ""),
        results.get("analysis3", ""),
        report_type
    )

    pdf_url = ""
    pdf_bytes = convert_to_pdf(final_report, report_type)
    if pdf_bytes:
        pdf_url = upload_to_r2(pdf_bytes, contact_id) or ""

    status = push_to_ghl(contact_id, final_report, ghl_key, pdf_url)

    return jsonify({
        "success": True,
        "ghl_update_status": status,
        "location_id": location_id,
        "contact_id": contact_id,
        "report_type": report_type,
        "pdf_url": pdf_url
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
