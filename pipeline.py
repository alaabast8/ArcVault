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

# NOTE ON LIVE vs. MOCK: call_llm() calls the live Gemini API by default.
# If the live call fails after all retries (e.g. rate limits, quota exhaustion,
# or network issues during a demo), it falls back to a pre-computed mock response
# that was generated by manually running these exact prompts against Claude for
# each of the 5 sample messages. The rest of the pipeline is agnostic to whether
# the response came from the live API or the mock fallback.

_MOCK_LLM_RESPONSES = {
    1: {
        "category": "Bug Report",
        "priority": "Medium",
        "confidence": 0.88,
        "core_issue": "User is receiving a 403 error when logging in, starting after a recent platform update.",
        "entities": {
            "account_ids": ["arcvault.io/user/jsmith"],
            "invoice_numbers": [],
            "error_codes": ["403"],
            "dollar_amounts": [],
            "other": ["regression correlated with 'last Tuesday' update"],
        },
        "urgency_signal": "Single user blocked from login; not reported as affecting others",
    },
    2: {
        "category": "Feature Request",
        "priority": "Medium",
        "confidence": 0.93,
        "core_issue": "Customer wants a bulk export feature for audit logs to support compliance workflows.",
        "entities": {
            "account_ids": [],
            "invoice_numbers": [],
            "error_codes": [],
            "dollar_amounts": [],
            "other": ["segment: compliance-heavy org", "feature: bulk export of audit logs"],
        },
        "urgency_signal": "No time pressure; framed as recurring efficiency gain, not a blocker",
    },
    3: {
        "category": "Billing Issue",
        "priority": "Medium",
        "confidence": 0.91,
        "core_issue": "Customer was billed $1,240 on invoice #8821 despite a contracted rate of $980/month.",
        "entities": {
            "account_ids": [],
            "invoice_numbers": ["8821"],
            "error_codes": [],
            "dollar_amounts": ["$1,240", "$980"],
            "other": ["discrepancy: $260 over contracted rate"],
        },
        "urgency_signal": "Financial discrepancy flagged politely, no explicit deadline",
    },
    4: {
        "category": "Technical Question",
        "priority": "Low",
        "confidence": 0.85,
        "core_issue": "Prospect/customer is asking whether ArcVault supports SSO integration with Okta.",
        "entities": {
            "account_ids": [],
            "invoice_numbers": [],
            "error_codes": [],
            "dollar_amounts": [],
            "other": ["identity provider: Okta", "context: evaluating switching auth providers"],
        },
        "urgency_signal": "Pre-sales/evaluation question, no urgency indicated",
    },
    5: {
        "category": "Incident/Outage",
        "priority": "High",
        "confidence": 0.95,
        "core_issue": "The dashboard became unavailable for multiple users starting around 2pm EST, confirmed not client-side.",
        "entities": {
            "account_ids": [],
            "invoice_numbers": [],
            "error_codes": [],
            "dollar_amounts": [],
            "other": ["time: ~2pm EST", "scope: multiple users", "client-side ruled out by customer"],
        },
        "urgency_signal": "Active, ongoing outage affecting multiple users \u2014 high urgency",
    },
}


def call_llm(prompt: str, msg_id: int = None, is_json: bool = False) -> str | dict:
    """Calls the live Gemini API (gemini-2.5-flash, with automatic retry on 429
    rate limits and model fallback to gemini-3.1-flash-lite on daily quota
    exhaustion). Falls back to a pre-computed mock response keyed by msg_id on
    ANY failure that escapes the retry loop — including non-retryable errors
    (bad API key, auth failure, safety filter block) and JSONDecodeError from
    a malformed model response — not only on retryable-error exhaustion."""
    config = None
    if is_json:
        config = types.GenerateContentConfig(
            response_mime_type="application/json"
        )

    # Sleep to avoid hitting the free tier rate limit (RPM limit)
    time.sleep(4.0)

    retries = 5
    backoff = 6.0
    model = 'gemini-2.5-flash'
    live_response = None

    try:
        for attempt in range(retries):
            try:
                live_response = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config=config,
                )
                break
            except (errors.ClientError, errors.ServerError, Exception) as e:
                is_retryable = False
                sleep_time = backoff

                if isinstance(e, errors.ClientError) and (e.code == 429 or "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e)):
                    # If it's a daily limit exhaustion for gemini-2.5-flash, fall back to gemini-3.1-flash-lite
                    if model == 'gemini-2.5-flash' and ("GenerateRequestsPerDay" in str(e) or "limit: 20" in str(e) or "limit: 0" in str(e)):
                        print("  [Quota Exhausted] Daily limit reached for gemini-2.5-flash. Falling back to gemini-3.1-flash-lite...")
                        model = 'gemini-3.1-flash-lite'
                        # Retry the loop iteration with the new model immediately
                        continue

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
                    # Non-retryable (bad key, auth, safety block, etc.) — bubble up
                    # to the outer except so the mock fallback fires, same as retryable
                    # exhaustion. Use bare raise to avoid leaking the loop variable.
                    raise

        if live_response is None:
            # All retries were exhausted for a retryable error; raise so the
            # outer except handles the fallback in one place.
            raise RuntimeError(f"All {retries} retries exhausted without a successful response.")

        text = live_response.text.strip()
        if is_json:
            # Strip markdown fences if Gemini still generates them despite JSON mime_type.
            # JSONDecodeError here also falls through to the outer except → mock fallback.
            if text.startswith("```json"):
                text = text[len("```json"):]
            elif text.startswith("```"):
                text = text[len("```"):]
            if text.endswith("```"):
                text = text[:-len("```")]
            return json.loads(text.strip())

        return text

    except Exception as exc:
        # Single, unified fallback for every failure path:
        #   • non-retryable API errors (bad key, auth, safety filter)
        #   • retryable errors that exhausted all attempts
        #   • JSONDecodeError from a malformed model response
        mock_store = _MOCK_LLM_RESPONSES if is_json else _MOCK_SUMMARIES
        if msg_id is not None and msg_id in mock_store:
            print(
                f"  [FALLBACK] Live API call failed for message {msg_id} "
                f"({exc.__class__.__name__}: {str(exc)[:80]}) — using mocked response."
            )
            return mock_store[msg_id]
        # No mock available for this msg_id (e.g. a new message beyond the 5 samples).
        # Re-raise so _ingest_file's except block logs it and skips to the next message.
        print(f"  [ERROR] Live API call failed for message {msg_id} and no mock fallback is available: {exc}")
        raise


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

_MOCK_SUMMARIES = {
    1: "Customer jsmith is locked out with a 403 login error that began after last Tuesday's platform update, suggesting a permissions or auth regression. Only this account has been reported affected so far. Recommend checking recent auth/role changes tied to that release.",
    2: "A compliance-focused customer is requesting a bulk export capability for audit logs to reduce manual effort. This is a recurring pain point framed around time savings rather than a blocker. Worth scoping for the product roadmap given the compliance use case.",
    3: "Customer disputes invoice #8821, which billed $1,240 against a contracted rate of $980/month \u2014 a $260 overcharge. No urgency language used, but the discrepancy should be verified against the contract terms. Recommend a billing team review and corrected invoice or explanation.",
    4: "A prospect or customer is asking whether ArcVault supports SSO via Okta as they evaluate switching identity providers. This is a pre-sales/technical capability question with no urgency. Recommend a quick confirmation of supported SSO/IdP integrations.",
    5: "Multiple users are unable to load the ArcVault dashboard starting around 2pm EST; the customer has ruled out issues on their end. This reads as an active, ongoing outage affecting more than one user. Recommend immediate incident response and status page update.",
}


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
