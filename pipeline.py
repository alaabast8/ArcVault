"""
ArcVault Intake & Triage Pipeline
==================================
Event-driven ingestion pipeline. The inbox/ folder is watched for new
per-message JSON files (msg_NNN.json). When a file appears, it is read,
classified, routed, escalated if needed, summarised, and appended to
output/output_records.json — all without restarting the script.

Design principle: classification/extraction/summarization is delegated to an
LLM; routing and escalation are deterministic Python logic, NOT left to the
model — auditable, testable, and not subject to prompt drift.

Usage:
  python pipeline.py            # watch mode — runs until Ctrl-C
  python pipeline.py --batch    # batch mode — process inbox once, then exit

In production, the same pattern applies to an S3 event trigger, an email
webhook, or a message-queue consumer instead of a local filesystem watch.
"""

import argparse
import json
import re
import os
import time
from datetime import datetime, timezone
from google import genai
from google.genai import types, errors
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Load environment variables from .env file if it exists
env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                parts = line.strip().split("=", 1)
                if len(parts) == 2:
                    os.environ[parts[0].strip()] = parts[1].strip()

# Initialize Gemini Client
client = genai.Client()

# ---------------------------------------------------------------------------
# STEP 1 — INGESTION
# ---------------------------------------------------------------------------
# Each message arrives as an individual JSON file dropped into inbox/.
# In watch mode a watchdog Observer fires on_created for each new *.json file.
# In production this same trigger could be an S3 PutObject event, an email
# webhook, or a message-queue consumer — the downstream pipeline is unchanged.

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INBOX_DIR = os.path.join(BASE_DIR, "inbox")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "output_records.json")


def load_single_message(filepath: str) -> dict:
    """Read one per-message JSON file and normalise it for the pipeline."""
    with open(filepath, "r", encoding="utf-8") as f:
        msg = json.load(f)
    # Map 'content' -> 'raw_message' to match downstream expectations
    return {
        "id": msg["id"],
        "source": msg["source"],
        "raw_message": msg["content"],
    }


def append_record(record: dict) -> None:
    """Append one processed record to output_records.json (thread-safe enough
    for single-threaded watchdog callbacks; production would use a lock)."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    existing: list = []
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
    existing.append(record)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)


def _print_record_summary(r: dict) -> None:
    flag = "ESCALATED" if r["routing"]["escalation_flag"] else "standard"
    print(f"  #{r['id']:<2} [{r['classification']['category']:<20}] "
          f"priority={r['classification']['priority']:<6} "
          f"conf={r['classification']['confidence']:.2f} "
          f"-> {r['routing']['destination_queue']:<24} ({flag})")


def _ingest_file(filepath: str) -> bool:
    """Load, process, persist, and summarise a single message file. Returns True if successful."""
    filename = os.path.basename(filepath)
    print(f"\n[INGESTED] New message detected: {filename} -> processing...")
    try:
        msg = load_single_message(filepath)
        record = process_message(msg)
        append_record(record)
        _print_record_summary(record)
        print(f"  -> Written to {OUTPUT_PATH}")
        return True
    except Exception as exc:
        print(f"  [ERROR] Failed to process {filename}: {exc}")
        return False


class TriageEventHandler(FileSystemEventHandler):
    """Watchdog handler: fires _ingest_file() for every new *.json file."""

    def on_created(self, event):
        if event.is_directory:
            return
        if not event.src_path.endswith(".json"):
            return
        # Brief pause to let the OS finish writing the file before we read it
        time.sleep(0.2)
        _ingest_file(event.src_path)


def run_watch() -> None:
    """Start the watchdog observer and block until Ctrl-C."""
    os.makedirs(INBOX_DIR, exist_ok=True)
    handler = TriageEventHandler()
    observer = Observer()
    observer.schedule(handler, path=INBOX_DIR, recursive=False)
    observer.start()
    print(f"Watching {INBOX_DIR}/ for new messages... (Ctrl-C to stop)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping watcher...")
    finally:
        observer.stop()
        observer.join()
        print("Watcher stopped.")


def run_batch() -> None:
    """Process every *.json file currently in inbox/ once, then exit."""
    files = sorted(
        f for f in os.listdir(INBOX_DIR)
        if f.endswith(".json") and f != "messages.json"
    )
    if not files:
        print("No message files found in inbox/. Nothing to process.")
        return
    print(f"Batch mode: processing {len(files)} file(s) from {INBOX_DIR}/\n")
    success_count = 0
    for fname in files:
        if _ingest_file(os.path.join(INBOX_DIR, fname)):
            success_count += 1
    print(f"\nDone. {success_count} message(s) written to {OUTPUT_PATH}")

CATEGORIES = ["Bug Report", "Feature Request", "Billing Issue", "Technical Question", "Incident/Outage"]
PRIORITIES = ["Low", "Medium", "High"]

# ---------------------------------------------------------------------------
# STEP 2 & 3 — CLASSIFICATION + ENRICHMENT (single LLM call, see prompts.md)
# ---------------------------------------------------------------------------

CLASSIFY_ENRICH_PROMPT_TEMPLATE = """You are a triage assistant for ArcVault, a B2B software company.
Given the raw customer message below, return ONLY a JSON object (no prose, no markdown fences)
with exactly this shape:

{{
  "category": one of ["Bug Report", "Feature Request", "Billing Issue", "Technical Question", "Incident/Outage"],
  "priority": one of ["Low", "Medium", "High"],
  "confidence": a float between 0 and 1 representing your certainty in the category assignment,
  "core_issue": a single sentence describing the core issue,
  "entities": {{
      "account_ids": [...],
      "invoice_numbers": [...],
      "error_codes": [...],
      "dollar_amounts": [...],
      "other": [...]
  }},
  "urgency_signal": one short phrase describing what in the message signals urgency (or lack thereof)
}}

Classification Guidelines:
- "Technical Question": Inquiries about existing product capabilities, configuration, setup, or how to do something (e.g., questions phrased as "is there a way to do X?", "can we do X?", "how do I set up X?").
- "Feature Request": Suggestions, requests, or demands for new features, new products, or product improvements that are currently missing from the platform.

Priority Guidelines:
- "High": Active outages, security/data issues, or problems blocking business-critical operations for the customer right now.
- "Medium": A real functional problem or financial discrepancy affecting one customer/account, with no explicit deadline.
- "Low": Feature requests, pre-sales/evaluation questions, or anything the customer frames as non-blocking.

Source: {source}
Message: "{message}"
"""



MODEL_NAME = "gemini-3.1-flash-lite"


def call_llm(prompt: str, msg_id: int = None, is_json: bool = False) -> str | dict:
    """Calls the live Gemini API. gemini-3.1-flash-lite is the only model used
    (no model fallback logic — there is nothing to fall back to). Automatically
    retries on transient errors: 429 rate limits and 5xx server errors, using
    the retry-after hint from the API when available, otherwise exponential
    backoff."""
    config = None
    if is_json:
        config = types.GenerateContentConfig(
            response_mime_type="application/json"
        )

    # Sleep to avoid hitting the free tier rate limit (RPM limit)
    time.sleep(4.0)

    retries = 5
    backoff = 6.0
    live_response = None

    for attempt in range(retries):
        try:
            live_response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config=config,
            )
            break
        except (errors.ClientError, errors.ServerError, Exception) as e:
            is_retryable = False
            sleep_time = backoff

            if isinstance(e, errors.ClientError) and (e.code == 429 or "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e)):
                # Rate-limited — always retryable; use the API's suggested
                # retry-after delay when it provides one.
                is_retryable = True
                match = re.search(r"retry in ([\d\.]+)s", str(e))
                if match:
                    sleep_time = float(match.group(1)) + 1.0  # Add a small buffer
            elif isinstance(e, errors.ServerError) and (e.code in (500, 502, 503, 504) or any(c in str(e) for c in ("503", "502", "UNAVAILABLE", "INTERNAL"))):
                # Transient server-side errors (5xx) are always retryable
                is_retryable = True
            elif "ConnectTimeout" in e.__class__.__name__ or "timeout" in str(e).lower() or "10060" in str(e):
                is_retryable = True
                sleep_time = backoff

            if is_retryable:
                print(f"  [Retryable Error] {e.__class__.__name__}: {str(e)[:120]}... Sleeping for {sleep_time:.2f}s before retrying (Attempt {attempt+1}/{retries})...")
                time.sleep(sleep_time)
                backoff *= 1.5
            else:
                raise

    if live_response is None:
        raise RuntimeError(f"All {retries} retries exhausted without a successful response.")

    text = live_response.text.strip()
    if is_json:
        # Strip markdown fences if Gemini still generates them despite JSON mime_type.
        if text.startswith("```json"):
            text = text[len("```json"):]
        elif text.startswith("```"):
            text = text[len("```"):]
        if text.endswith("```"):
            text = text[:-len("```")]
        return json.loads(text.strip())

    return text


def classify_and_enrich(msg: dict) -> dict:
    prompt = CLASSIFY_ENRICH_PROMPT_TEMPLATE.format(source=msg["source"], message=msg["raw_message"])
    result = call_llm(prompt, msg["id"], is_json=True)
    assert result["category"] in CATEGORIES
    assert result["priority"] in PRIORITIES
    assert 0.0 <= result["confidence"] <= 1.0
    return result


# ---------------------------------------------------------------------------
# STEP 4 — ROUTING DECISION (deterministic, auditable)
# ---------------------------------------------------------------------------

ROUTING_MAP = {
    "Bug Report": "Engineering",
    "Incident/Outage": "Engineering",
    "Feature Request": "Product",
    "Billing Issue": "Billing",
    "Technical Question": "IT/Security",  # SSO/auth/security-flavored questions land here;
                                           # a real system would sub-route pre-sales vs. security.
}

CONFIDENCE_ESCALATION_THRESHOLD = 0.70
BILLING_ESCALATION_DOLLAR_THRESHOLD = 500.00

ESCALATION_KEYWORDS = [
    "outage", "down for all users", "down for everyone", "security breach",
    "data breach", "everyone is down", "critical", "urgent", "cannot access at all",
]


def parse_dollar_amounts(strings):
    amounts = []
    for s in strings:
        cleaned = re.sub(r"[^0-9.]", "", s)
        try:
            amounts.append(float(cleaned))
        except ValueError:
            continue
    return amounts


def determine_escalation(record: dict, raw_message: str) -> tuple:
    """Returns (escalation_flag: bool, reason: str|None)."""
    reasons = []

    if record["confidence"] < CONFIDENCE_ESCALATION_THRESHOLD:
        reasons.append(f"Low model confidence ({record['confidence']:.2f} < {CONFIDENCE_ESCALATION_THRESHOLD})")

    lowered = raw_message.lower()
    for kw in ESCALATION_KEYWORDS:
        if kw in lowered:
            reasons.append(f"Escalation keyword matched: '{kw}'")
            break

    if record["category"] == "Incident/Outage" and "multiple users" in lowered:
        reasons.append("Outage explicitly affecting multiple users")

    if record["category"] == "Billing Issue":
        amounts = parse_dollar_amounts(record["entities"].get("dollar_amounts", []))
        if len(amounts) >= 2:
            discrepancy = abs(max(amounts) - min(amounts))
            if discrepancy > BILLING_ESCALATION_DOLLAR_THRESHOLD:
                reasons.append(f"Billing discrepancy ${discrepancy:.2f} exceeds ${BILLING_ESCALATION_DOLLAR_THRESHOLD} threshold")

    if reasons:
        return True, "; ".join(reasons)
    return False, None


def route(record: dict, raw_message: str) -> dict:
    escalation_flag, escalation_reason = determine_escalation(record, raw_message)
    standard_destination = ROUTING_MAP.get(record["category"], "General/Unclassified")

    if escalation_flag:
        destination = "Escalation/Human Review"
        secondary_destination = standard_destination  # still tag where it *would* have gone
    else:
        destination = standard_destination
        secondary_destination = None

    return {
        "destination_queue": destination,
        "secondary_queue": secondary_destination,
        "escalation_flag": escalation_flag,
        "escalation_reason": escalation_reason,
    }


# ---------------------------------------------------------------------------
# STEP 5 — HUMAN-READABLE SUMMARY (second, small LLM call)
# ---------------------------------------------------------------------------

SUMMARY_PROMPT_TEMPLATE = """Write a 2-3 sentence summary of this support request for the {queue} team.
Be factual and specific (mention any IDs, amounts, or error codes). No preamble, just the summary.

Category: {category}
Priority: {priority}
Core issue: {core_issue}
Raw message: "{raw_message}"
"""




def summarize(record: dict, raw_message: str, destination_queue: str) -> str:
    prompt = SUMMARY_PROMPT_TEMPLATE.format(
        queue=destination_queue,
        category=record["category"],
        priority=record["priority"],
        core_issue=record["core_issue"],
        raw_message=raw_message
    )
    return call_llm(prompt, msg_id=record["_id"], is_json=False)


# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def process_message(msg: dict) -> dict:
    classification_enrichment = classify_and_enrich(msg)
    classification_enrichment["_id"] = msg["id"]

    routing = route(classification_enrichment, msg["raw_message"])
    summary = summarize(classification_enrichment, msg["raw_message"], routing["destination_queue"])

    record = {
        "id": msg["id"],
        "source": msg["source"],
        "raw_message": msg["raw_message"],
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "classification": {
            "category": classification_enrichment["category"],
            "priority": classification_enrichment["priority"],
            "confidence": classification_enrichment["confidence"],
        },
        "enrichment": {
            "core_issue": classification_enrichment["core_issue"],
            "entities": classification_enrichment["entities"],
            "urgency_signal": classification_enrichment["urgency_signal"],
        },
        "routing": routing,
        "human_readable_summary": summary,
    }
    return record


def main():
    parser = argparse.ArgumentParser(description="ArcVault Triage Pipeline")
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Process all existing inbox files once and exit (default: watch mode)",
    )
    args = parser.parse_args()

    if args.batch:
        run_batch()
    else:
        run_watch()


if __name__ == "__main__":
    main()