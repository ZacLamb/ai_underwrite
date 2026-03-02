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
GHL_API_KEY       = os.environ.get("GHL_API_KEY")
OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GROK_API_KEY      = os.environ.get("GROK_API_KEY")
R2_ACCESS_KEY_ID  = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID     = os.environ.get("R2_ACCOUNT_ID")
R2_PUBLIC_URL     = os.environ.get("R2_PUBLIC_URL")
R2_BUCKET         = "fundara-reports"

LOGO_URL = "https://assets.cdn.filesafe.space/HD59NWC1biIA31IHm1y8/media/69a4925b753f150a68663d79.png"

# ── PALETTE ────────────────────────────────────────────────────────
BG         = HexColor('#0D1B2A')
CARD       = HexColor('#152030')
ROW_ALT    = HexColor('#1C2B3A')
HDR_BG     = HexColor('#1A3550')
BORDER     = HexColor('#2A3F55')
RED        = HexColor('#E53935')
ORANGE     = HexColor('#F57C00')
YELLOW     = HexColor('#F9A825')
GREEN      = HexColor('#43A047')
LBLUE      = HexColor('#64B5F6')
TW         = HexColor('#ECEFF1')   # text white
TG         = HexColor('#90A4AE')   # text grey
TL         = HexColor('#B0BEC5')   # text light

W_PAGE     = 7.0 * inch            # usable content width

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
    ps('h3', fontName='Helvetica-Bold', fontSize=9.5,  textColor=TW,
       spaceBefore=5, spaceAfter=3, leading=13)
    ps('body',   fontSize=8.5, textColor=TL, leading=12)
    ps('body_b', fontName='Helvetica-Bold', fontSize=8.5, textColor=TW, leading=12)
    ps('bullet', fontSize=8.5, textColor=TL, leading=12, leftIndent=12)
    ps('caption',fontSize=7.5, textColor=TG, leading=10,
       fontName='Helvetica-Oblique', alignment=TA_CENTER, spaceAfter=4)
    ps('center', fontSize=8.5, textColor=TL, leading=12, alignment=TA_CENTER)

    # Table cells
    ps('th',  fontName='Helvetica-Bold', fontSize=8, textColor=TW,
       leading=11, alignment=TA_CENTER)
    ps('th_l',fontName='Helvetica-Bold', fontSize=8, textColor=TW, leading=11)
    ps('td',  fontSize=8, textColor=TL, leading=11)
    ps('td_c',fontSize=8, textColor=TL, leading=11, alignment=TA_CENTER)
    ps('td_b',fontName='Helvetica-Bold', fontSize=8, textColor=TW, leading=11)
    ps('td_r',fontName='Helvetica-Bold', fontSize=8, textColor=RED,  leading=11)
    ps('td_g',fontName='Helvetica-Bold', fontSize=8, textColor=GREEN, leading=11)
    ps('td_y',fontName='Helvetica-Bold', fontSize=8, textColor=YELLOW,leading=11)
    ps('td_o',fontName='Helvetica-Bold', fontSize=8, textColor=ORANGE,leading=11)
    return s

STY = build_styles()

# ── BASE TABLE STYLE ───────────────────────────────────────────────
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

    # Dark background
    canvas.setFillColor(BG)
    canvas.rect(0, 0, pw, ph, fill=1, stroke=0)

    # ── Header bar ──
    canvas.setFillColor(CARD)
    canvas.rect(0, ph - 42, pw, 42, fill=1, stroke=0)

    # Logo in header (if loaded)
    if logo_img:
        try:
            logo_img.drawOn(canvas, 0.55 * inch, ph - 36)
        except Exception:
            pass

    # Header text (right side)
    canvas.setFillColor(TG)
    canvas.setFont('Helvetica', 7.5)
    canvas.drawRightString(pw - 0.55 * inch, ph - 16,
                           'AI Underwriting Report  |  CONFIDENTIAL')
    canvas.drawRightString(pw - 0.55 * inch, ph - 28,
                           'Powered by Fundara')

    # Accent line under header
    canvas.setStrokeColor(BORDER)
    canvas.setLineWidth(0.4)
    canvas.line(0.55 * inch, ph - 42, pw - 0.55 * inch, ph - 42)

    # ── Footer bar ──
    canvas.setFillColor(CARD)
    canvas.rect(0, 0, pw, 28, fill=1, stroke=0)
    canvas.setFillColor(TG)
    canvas.setFont('Helvetica', 7)
    canvas.drawString(0.55 * inch, 10,
                      'This report is generated by Fundara AI and is for internal use only. Not financial advice.')
    canvas.drawRightString(pw - 0.55 * inch, 10, f'Page {doc.page}')

    canvas.restoreState()


def _draw_cover(canvas, doc, logo_img=None):
    canvas.saveState()
    pw, ph = letter
    canvas.setFillColor(BG)
    canvas.rect(0, 0, pw, ph, fill=1, stroke=0)
    # Top accent stripe
    canvas.setFillColor(LBLUE)
    canvas.rect(0, ph - 5, pw, 5, fill=1, stroke=0)
    # Footer
    canvas.setFillColor(CARD)
    canvas.rect(0, 0, pw, 32, fill=1, stroke=0)
    canvas.setFillColor(TG)
    canvas.setFont('Helvetica', 7)
    canvas.drawString(0.55 * inch, 11,
                      'CONFIDENTIAL — Authorized Fundara Personnel Only')
    canvas.restoreState()


# ── MARKDOWN → FLOWABLES ────────────────────────────────────────────
def _cell_color_style(text):
    """Pick a paragraph style for a table cell based on content."""
    t = text.strip().upper()
    # Severity / decision keywords
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
    """Convert **bold** markdown to ReportLab XML bold tags."""
    return re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)


def _parse_table(table_lines):
    """Parse a markdown table into a ReportLab Table flowable."""
    rows = []
    for i, line in enumerate(table_lines):
        # Skip separator rows (---|---|---)
        if re.match(r'^\|[-| :]+\|$', line.strip()):
            continue
        cells = [c.strip() for c in line.strip().strip('|').split('|')]
        rows.append(cells)

    if not rows:
        return None

    # Auto-distribute column widths
    ncols = max(len(r) for r in rows)
    col_w = W_PAGE / ncols

    flowable_rows = []
    for ri, row in enumerate(rows):
        # Pad short rows
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

    t = Table(flowable_rows, colWidths=[col_w] * ncols, repeatRows=1)
    t.setStyle(BASE_TS)
    return t


def _section_header(title):
    """Returns a styled section header with a divider line."""
    return [
        Spacer(1, 10),
        HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER),
        Spacer(1, 5),
        Paragraph(title, STY['h1']),
    ]


def _flag_card(severity, title, detail):
    """Styled red-flag card matching the report theme."""
    sev_map = {
        'CRITICAL': RED, 'HIGH': ORANGE,
        'MEDIUM': YELLOW, 'LOW': GREEN,
    }
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
    """Big DECLINE / APPROVE banner."""
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


def _inline_box(text, left_color):
    """Styled info/callout box with left accent."""
    box = Table([[Paragraph(_parse_md_bold(text), STY['body'])]], colWidths=[W_PAGE])
    box.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,-1), CARD),
        ('LINEBEFORE',    (0,0),(0,-1),  4, left_color),
        ('BOX',           (0,0),(-1,-1), 0.3, BORDER),
        ('LEFTPADDING',   (0,0),(-1,-1), 12),
        ('RIGHTPADDING',  (0,0),(-1,-1), 12),
        ('TOPPADDING',    (0,0),(-1,-1), 9),
        ('BOTTOMPADDING', (0,0),(-1,-1), 9),
    ]))
    return box


def markdown_to_flowables(markdown_text):
    """
    Convert AI-generated markdown report into dark-themed ReportLab flowables.
    Handles: headings, paragraphs, bullet lists, markdown tables, bold text,
             flag cards (CRITICAL/HIGH/MEDIUM), decision banners.
    """
    story = []
    lines = markdown_text.split('\n')

    i = 0
    table_buf = []          # buffer for collecting table lines
    current_section = ''

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

        # ── Blank line ──────────────────────────────────────────────
        if not stripped:
            flush_table()
            story.append(Spacer(1, 5))
            i += 1
            continue

        # ── Table row ───────────────────────────────────────────────
        if stripped.startswith('|'):
            table_buf.append(stripped)
            i += 1
            continue
        else:
            flush_table()

        # ── Headings ────────────────────────────────────────────────
        if stripped.startswith('# '):
            title = stripped[2:].strip()
            current_section = title.upper()
            story += _section_header(title)
            i += 1
            continue

        if stripped.startswith('## '):
            title = stripped[3:].strip()
            current_section = title.upper()
            story += _section_header(title)
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

        # ── Horizontal rule ─────────────────────────────────────────
        if stripped in ('---', '***', '___'):
            story.append(HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER))
            story.append(Spacer(1, 5))
            i += 1
            continue

        # ── Page break hint ─────────────────────────────────────────
        if stripped.lower() in ('<pagebreak>', '<!-- pagebreak -->'):
            story.append(PageBreak())
            i += 1
            continue

        # ── Bullet list item ────────────────────────────────────────
        if stripped.startswith('- ') or stripped.startswith('* '):
            text = _parse_md_bold(stripped[2:])

            # Check if it's a flag: "- **CRITICAL**: Title — detail"
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

        # ── Numbered list ───────────────────────────────────────────
        num_match = re.match(r'^(\d+)\.\s+(.+)', stripped)
        if num_match:
            num, text = num_match.groups()
            story.append(Paragraph(
                f'<b>{num}.</b>  {_parse_md_bold(text)}', STY['bullet']))
            story.append(Spacer(1, 2))
            i += 1
            continue

        # ── Decision banner (standalone DECLINE / APPROVE) ──────────
        if re.match(r'^[*_]*(DECLINE|APPROVE|APPROVED|DECLINED)[*_]*$',
                    stripped, re.IGNORECASE):
            story.append(Spacer(1, 6))
            story.append(_decision_banner(
                re.sub(r'[*_]', '', stripped).upper()))
            story.append(Spacer(1, 10))
            i += 1
            continue

        # ── Bold-only line (often a sub-label) ──────────────────────
        bold_only = re.match(r'^\*\*(.+)\*\*$', stripped)
        if bold_only:
            story.append(Paragraph(
                f'<b>{bold_only.group(1)}</b>', STY['body_b']))
            story.append(Spacer(1, 3))
            i += 1
            continue

        # ── Key: Value style lines ───────────────────────────────────
        kv_match = re.match(r'^\*\*(.+?)\*\*[:\s]+(.+)', stripped)
        if kv_match:
            key, val = kv_match.groups()
            # Colour the value if it contains risk keywords
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

        # ── Generic paragraph ────────────────────────────────────────
        text = _parse_md_bold(stripped)
        # Highlight whole lines that are risk decisions
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


# ── PDF BUILDER ────────────────────────────────────────────────────
def convert_to_pdf(markdown_text):
    try:
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            leftMargin=0.6 * inch,
            rightMargin=0.6 * inch,
            topMargin=0.75 * inch,    # leaves room for 42pt header bar
            bottomMargin=0.55 * inch, # leaves room for 28pt footer bar
        )

        # ── Try to load logo ──────────────────────────────────────────
        logo_img_draw = None   # for canvas drawing (small, in header bar)
        logo_img_flow = None   # for story (cover page)
        try:
            logo_resp = requests.get(LOGO_URL, timeout=10)
            if logo_resp.status_code == 200:
                logo_buf = io.BytesIO(logo_resp.content)
                # Canvas version (header) — fits in 42pt bar
                import copy
                logo_img_draw = Image(io.BytesIO(logo_resp.content),
                                      width=1.6 * inch, height=0.38 * inch)
                # Story version (cover) — larger
                logo_img_flow = Image(io.BytesIO(logo_resp.content),
                                      width=2.4 * inch, height=0.56 * inch)
                logo_img_flow.hAlign = 'LEFT'
        except Exception as logo_err:
            print(f"Logo load error: {logo_err}")

        story = []

        # ── Cover section ─────────────────────────────────────────────
        story.append(Spacer(1, 0.55 * inch))

        if logo_img_flow:
            story.append(logo_img_flow)
            story.append(Spacer(1, 0.18 * inch))

        story.append(HRFlowable(width=W_PAGE, thickness=2,
                                 color=LBLUE, spaceAfter=10))

        story.append(Paragraph(
            'AI Underwriting Report',
            ParagraphStyle('cover_title', fontName='Helvetica-Bold',
                           fontSize=26, textColor=TW, leading=31)))
        story.append(Paragraph(
            'Powered by Fundara  |  Confidential',
            ParagraphStyle('cover_sub', fontName='Helvetica',
                           fontSize=10, textColor=TG, leading=14, spaceAfter=6)))

        story.append(HRFlowable(width=W_PAGE, thickness=0.5,
                                 color=BORDER, spaceAfter=16))
        story.append(Spacer(1, 0.1 * inch))

        # ── Main content ──────────────────────────────────────────────
        story += markdown_to_flowables(markdown_text)

        # ── Footer disclaimer ─────────────────────────────────────────
        story.append(Spacer(1, 0.25 * inch))
        story.append(HRFlowable(width=W_PAGE, thickness=0.4, color=BORDER))
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            'This report is generated by Fundara AI and is for internal use only. Not financial advice.',
            ParagraphStyle('foot', fontName='Helvetica', fontSize=7,
                           textColor=TG, alignment=TA_CENTER)))

        # ── Build with page callbacks ─────────────────────────────────
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


# ═══════════════════════════════════════════════════════════════════
# Everything below is unchanged from original
# ═══════════════════════════════════════════════════════════════════

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


def download_pdf(url):
    try:
        headers = {"Authorization": f"Bearer {GHL_API_KEY}"}
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


def analyze_with_openai(combined_text):
    try:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT.format(combined_text=combined_text)}
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


def analyze_with_claude(combined_text):
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=4000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": USER_PROMPT.format(combined_text=combined_text)}]
        )
        return message.content[0].text
    except Exception as claude_err:
        print(f"Claude exception: {str(claude_err)}")
        return f"Analysis 2 failed: {str(claude_err)}"


def analyze_with_grok(combined_text):
    try:
        headers = {"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": "grok-3-mini",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT.format(combined_text=combined_text)}
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


def merge_reports(report1, report2, report3):
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        merge_prompt = f"""You are a senior underwriting editor at Fundara. Merge these three underwriting analyses into ONE definitive report.

IMPORTANT RULES:
- Never mention any AI companies, models, tools, or technologies
- Present all findings as Fundara AI Underwriting analysis
- Use most accurate/conservative figures when there are discrepancies
- If all three agree, use that figure with high confidence
- If two agree and one differs, use the majority and note the discrepancy
- Flag significant discrepancies in Red Flags section
- Keep Sections 0-11 format
- The final report should read as a single cohesive professional document

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


@app.route("/analyze", methods=["POST"])
def analyze():
    data = request.json
    contact_id = data.get("contact_id")

    statement_urls = []
    for i in range(1, 11):
        url = data.get(f"bank_statement_{i}")
        if url and url != "null":
            statement_urls.append((i, url))

    if not statement_urls:
        return jsonify({"error": "No bank statements found"}), 400

    combined_text = ""
    for idx, url in statement_urls:
        pdf_bytes = download_pdf(url)
        if pdf_bytes:
            text = extract_text(pdf_bytes)
            combined_text += f"\n--- BANK STATEMENT {idx} ---\n{text}\n"

    if not combined_text.strip():
        return jsonify({"error": "Could not extract text from any PDFs"}), 400

    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_key = {
            executor.submit(analyze_with_openai, combined_text): "analysis1",
            executor.submit(analyze_with_claude, combined_text): "analysis2",
            executor.submit(analyze_with_grok, combined_text): "analysis3"
        }
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            results[key] = future.result()

    final_report = merge_reports(
        results.get("analysis1", ""),
        results.get("analysis2", ""),
        results.get("analysis3", "")
    )

    pdf_url = ""
    pdf_bytes = convert_to_pdf(final_report)
    if pdf_bytes:
        pdf_url = upload_to_r2(pdf_bytes, contact_id) or ""

    status = push_to_ghl(contact_id, final_report, pdf_url)

    return jsonify({
        "success": True,
        "ghl_update_status": status,
        "contact_id": contact_id,
        "pdf_url": pdf_url
    })


def push_to_ghl(contact_id, report, pdf_url=""):
    try:
        url = f"https://services.leadconnectorhq.com/contacts/{contact_id}"
        headers = {
            "Authorization": f"Bearer {GHL_API_KEY}",
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


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
