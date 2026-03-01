import os
import io
import requests
import pdfplumber
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed
from anthropic import Anthropic

app = Flask(__name__)

GHL_API_KEY = os.environ.get("GHL_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GROK_API_KEY = os.environ.get("GROK_API_KEY")

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
            "model": "grok-2-latest",
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
        print(f"GHL push status: {r.status_code} - {r.text}")
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

    return jsonify({"success": True, "ghl_update_status": status, "contact_id": contact_id})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
