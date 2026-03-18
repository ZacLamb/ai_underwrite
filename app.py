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
GHL_API_KEY          = os.environ.get("GHL_API_KEY")
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
SYSTEM_PROMPT_FULL = """ROLE: Senior COMMERCIAL UNDERWRITER (50y; restaurants & MCA). Tool-driven extraction only; NEVER reveal internal reasoning; no approximations. If unparseable, state cause.

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

USER_PROMPT_FULL = """Render a clean, print-ready underwriting report. Section 0 Decision Snapshot first (<=400 words), then Sections 1-11.
- Markdown only; tables preferred; omit N/A rows; one blank line between sections.
- USD whole dollars with commas; percentages 1 decimal; mask account numbers.
- Do NOT mention any AI tools, models, or companies. Present as Fundara AI analysis only.

Here are the bank statements:

{combined_text}"""

SYSTEM_PROMPT_QUICK = """ROLE: Senior COMMERCIAL UNDERWRITER (50y; MCA focus). Extract only what is needed for a broker-facing quick decision summary. Be concise and direct.

IMPORTANT: Never mention any AI companies, models, tools, or technologies. Present all findings as Fundara AI analysis only.

FORMAT:
- Markdown only
- Section 0: Decision Snapshot (<=300 words) — include overall recommendation, key risk flags, and funding eligibility
- Section 2: Deal Sheet — business name, DBA, owner, bank, account type, avg monthly deposits, avg daily balance, negative days, MCA density, existing positions
- USD whole dollars with commas; % to 1 decimal; mask account numbers
- Be direct — brokers need fast actionable information only"""

USER_PROMPT_QUICK = """Generate a quick broker-facing underwriting summary with ONLY Section 0 (Decision Snapshot) and Section 2 (Deal Sheet).

Keep it concise and actionable. No fluff. Brokers need to know:
1. Should we fund this? Why or why not?
2. Key numbers at a glance

Do NOT mention any AI tools, models, or companies. Present as Fundara AI analysis only.

Here are the bank statements:

{combined_text}"""

MERGE_PROMPT_FULL = """You are a senior underwriting editor at Fundara. Merge these three underwriting analyses into ONE definitive full report.

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

MERGE_PROMPT_QUICK = """You are a senior underwriting editor at Fundara. Merge these three quick broker summaries into ONE definitive quick summary.

IMPORTANT RULES:
- Never mention any AI companies, models, tools, or technologies
- Present all findings as Fundara AI analysis
- Use most conservative figures when there are discrepancies
- Output ONLY Section 0 (Decision Snapshot) and Section 2 (Deal Sheet)
- Keep it concise and broker-friendly — no fluff
- Single cohesive professional document

ANALYSIS 1:
{report1}

ANALYSIS 2:
{report2}

ANALYSIS 3:
{report3}

Produce the final merged Fundara quick broker summary:"""


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


# ── CANVAS CALLBACKS ───────────────────────────────────────────────
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


def _draw_cover(canvas, doc, logo_img=None, quick=False):
    canvas.saveState()
    pw, ph = letter
    canvas.setFillColor(BG)
    canvas.rect(0, 0, pw, ph, fill=1, stroke=0)
    accent = ORANGE if quick else LBLUE
    canvas.setFillColor(accent)
    canvas.rect(0, ph - 5, pw, 5, fill=1, stroke=0)
    canvas.setFillColor(CARD)
    canvas.rect(0, 0, pw, 32, fill=1, stroke=0)
    canvas.setFillColor(TG)
    canvas.setFont('Helvetica', 7)
    canvas.drawString(0.55 * inch, 11,
                      'CONFIDENTIAL — Authorized Fundara Personnel Only')
    canvas.restoreState()


# ── TABLE HELPERS ──────────────────────────────────────────────────
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
        if re.match(r'^\|
