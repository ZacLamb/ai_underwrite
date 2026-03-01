import os
import io
import requests
import pdfplumber
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
from anthropic import Anthropic
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, HRFlowable
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT

app = Flask(__name__)

GHL_API_KEY = os.environ.get("GHL_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GROK_API_KEY = os.environ.get("GROK_API_KEY")

LOGO_URL = "https://assets.cdn.filesafe.space/HD59NWC1biIA31IHm1y8/media/69a4925b753f150a68663d79.png"

SYSTEM_PROMPT = """ROLE: Senior COMMERCIAL UNDERWRITER (50y; restaurants & MCA). Tool-driven extraction only; NEVER reveal internal reasoning; no approximations. If unparseable, state cause.

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

Here are the bank statements:

{combined_text}"""


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
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT.format(combined_text=combined_text)}
            ],
            "max_tokens": 4000
        }
        r = requests.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers, timeout=120)
        data = r.json()
        print(f"OpenAI response status: {r.status_code}")
        if "choices" not in data:
            print(f"OpenAI error response: {data}")
            return f"OpenAI error: {data}"
        return data["choices"][0]["message"]["content"]
    except Exception as openai_err:
        print(f"OpenAI exception: {str(openai_err)}")
        return f"OpenAI failed: {str(openai_err)}"


def analyze_with_claude(combined_text):
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": USER_PROMPT.format(combined_text=combined_text)}]
        )
        return message.content[0].text
    except Exception as claude_err:
        print(f"Claude exception: {str(claude_err)}")
        return f"Claude failed: {str(claude_err)}"


def analyze_with_grok(combined_text):
    try:
        headers = {"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "model": "grok-2",
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT.format(combined_text=combined_text)}
            ],
            "max_tokens": 4000
        }
        r = requests.post("https://api.x.ai/v1/chat/completions", json=payload, headers=headers, timeout=120)
        data = r.json()
        print(f"Grok response status: {r.status_code}")
        if "choices" not in data:
            print(f"Grok error response: {data}")
            return f"Grok error: {data}"
        return data["choices"][0]["message"]["content"]
    except Exception as grok_err:
        print(f"Grok exception: {str(grok_err)}")
        return f"Grok failed: {str(grok_err)}"


def merge_reports(gpt_report, claude_report, grok_report):
    try:
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        merge_prompt = f"""You are a senior underwriting editor. Merge these three underwriting reports into ONE definitive report.
- Use most accurate/conservative figures when there are discrepancies
- If all three agree, use that figure with high confidence
- If two agree and one differs, use the majority and note the discrepancy
- Flag significant discrepancies in Red Flags section
- Keep Sections 0-11 format

GPT-4 REPORT:
{gpt_report}

CLAUDE REPORT:
{claude_report}

GROK REPORT:
{grok_report}

Produce the final merged underwriting report:"""

        message = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4000,
            messages=[{"role": "user", "content": merge_prompt}]
        )
        return message.content[0].text
    except Exception as merge_err:
        print(f"Merge exception: {str(merge_err)}")
        return gpt_report


def convert_to_pdf(markdown_text):
    try:
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=0.75 * inch,
            leftMargin=0.75 * inch,
            topMargin=0.75 * inch,
            bottomMargin=0.75 * inch
        )

        styles = getSampleStyleSheet()

        heading1_style = ParagraphStyle(
            'CustomH1',
            parent=styles['Heading1'],
            fontSize=14,
            textColor=colors.HexColor('#1a1a2e'),
            spaceAfter=6,
            spaceBefore=12,
            borderPad=4,
        )
        heading2_style = ParagraphStyle(
            'CustomH2',
            parent=styles['Heading2'],
            fontSize=11,
            textColor=colors.HexColor('#16213e'),
            spaceAfter=4,
            spaceBefore=8,
        )
        normal_style = ParagraphStyle(
            'CustomNormal',
            parent=styles['Normal'],
            fontSize=9,
            leading=14,
            textColor=colors.HexColor('#333333'),
        )
        bullet_style = ParagraphStyle(
            'CustomBullet',
            parent=styles['Normal'],
            fontSize=9,
            leading=14,
            leftIndent=20,
            textColor=colors.HexColor('#333333'),
        )
        code_style = ParagraphStyle(
            'CustomCode',
            parent=styles['Code'],
            fontSize=7.5,
            leading=11,
            fontName='Courier',
            textColor=colors.HexColor('#333333'),
        )

        story = []

        # Add logo
        try:
            logo_response = requests.get(LOGO_URL, timeout=10)
            if logo_response.status_code == 200:
                logo_buffer = io.BytesIO(logo_response.content)
                logo = Image(logo_buffer, width=2 * inch, height=0.6 * inch)
                logo.hAlign = 'LEFT'
                story.append(logo)
                story.append(Spacer(1, 0.1 * inch))
        except Exception as logo_err:
            print(f"Logo error: {str(logo_err)}")

        # Header bar
        story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1a1a2e')))
        story.append(Spacer(1, 0.05 * inch))

        title_style = ParagraphStyle(
            'Title',
            parent=styles['Normal'],
            fontSize=18,
            textColor=colors.HexColor('#1a1a2e'),
            fontName='Helvetica-Bold',
            alignment=TA_LEFT,
            spaceAfter=4,
        )
        story.append(Paragraph("AI Underwriting Report", title_style))

        subtitle_style = ParagraphStyle(
            'Subtitle',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#888888'),
            alignment=TA_LEFT,
            spaceAfter=8,
        )
        story.append(Paragraph("Powered by Fundara | Confidential", subtitle_style))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#cccccc')))
        story.append(Spacer(1, 0.15 * inch))

        # Parse markdown
        for line in markdown_text.split('\n'):
            stripped = line.strip()
            if not stripped:
                story.append(Spacer(1, 0.08 * inch))
            elif stripped.startswith('## '):
                story.append(Spacer(1, 0.05 * inch))
                story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#dddddd')))
                story.append(Paragraph(stripped[3:], heading1_style))
            elif stripped.startswith('### '):
                story.append(Paragraph(stripped[4:], heading2_style))
            elif stripped.startswith('- '):
                text = stripped[2:].replace('**', '<b>', 1).replace('**', '</b>', 1)
                story.append(Paragraph(f"• {text}", bullet_style))
            elif stripped.startswith('|'):
                story.append(Paragraph(stripped, code_style))
            elif stripped.startswith('**') and stripped.endswith('**'):
                story.append(Paragraph(f"<b>{stripped[2:-2]}</b>", normal_style))
            else:
                text = stripped.replace('**', '<b>', 1).replace('**', '</b>', 1)
                story.append(Paragraph(text, normal_style))

        # Footer
        story.append(Spacer(1, 0.2 * inch))
        story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#cccccc')))
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=7,
            textColor=colors.HexColor('#aaaaaa'),
            alignment=TA_CENTER,
        )
        story.append(Paragraph("This report is generated by Fundara AI and is for internal use only. Not financial advice.", footer_style))

        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()
    except Exception as pdf_err:
        print(f"PDF conversion error: {str(pdf_err)}")
        return None


def upload_pdf_to_ghl(contact_id, pdf_bytes):
    try:
        url = f"https://services.leadconnectorhq.com/contacts/{contact_id}/files"
        headers = {
            "Authorization": f"Bearer {GHL_API_KEY}",
            "Version": "2021-07-28"
        }
        files = {
            "file": ("underwriting_report.pdf", pdf_bytes, "application/pdf")
        }
        data = {
            "fieldKey": "ai_underwriting_analysis_pdf"
        }
        r = requests.post(url, headers=headers, files=files, data=data, timeout=30)
        print(f"GHL PDF upload status: {r.status_code} - {r.text}")

        if r.status_code != 200:
            # Try alternate endpoint
            url2 = f"https://services.leadconnectorhq.com/contacts/{contact_id}"
            headers2 = {
                "Authorization": f"Bearer {GHL_API_KEY}",
                "Version": "2021-07-28",
                "Content-Type": "application/json"
            }
            import base64
            pdf_b64 = base64.b64encode(pdf_bytes).decode('utf-8')
            payload = {
                "customFields": [{
                    "key": "ai_underwriting_analysis_pdf",
                    "value": pdf_b64
                }]
            }
            r2 = requests.put(url2, json=payload, headers=headers2, timeout=30)
            print(f"GHL PDF fallback status: {r2.status_code} - {r2.text}")
            return r2.status_code

        return r.status_code
    except Exception as upload_err:
        print(f"PDF upload error: {str(upload_err)}")
        return 500


def push_to_ghl(contact_id, report):
    try:
        url = f"https://services.leadconnectorhq.com/contacts/{contact_id}"
        headers = {
            "Authorization": f"Bearer {GHL_API_KEY}",
            "Version": "2021-07-28",
            "Content-Type": "application/json"
        }
        payload = {"customFields": [{"key": "ai_underwriting_analysis", "value": report}]}
        r = requests.put(url, json=payload, headers=headers, timeout=30)
        print(f"GHL push status: {r.status_code}")
        return r.status_code
    except Exception as ghl_err:
        print(f"GHL push error: {str(ghl_err)}")
        return 500


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
            executor.submit(analyze_with_openai, combined_text): "gpt",
            executor.submit(analyze_with_claude, combined_text): "claude",
            executor.submit(analyze_with_grok, combined_text): "grok"
        }
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            results[key] = future.result()

    final_report = merge_reports(
        results.get("gpt", ""),
        results.get("claude", ""),
        results.get("grok", "")
    )

    status = push_to_ghl(contact_id, final_report)

    pdf_bytes = convert_to_pdf(final_report)
    pdf_status = 0
    if pdf_bytes:
        pdf_status = upload_pdf_to_ghl(contact_id, pdf_bytes)

    return jsonify({
        "success": True,
        "ghl_update_status": status,
        "pdf_upload_status": pdf_status,
        "contact_id": contact_id
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
