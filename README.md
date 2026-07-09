# ArcVault Customer Support Intake & Triage Pipeline

An automated support ticket intake, classification, and triage system. It uses **Google's Gemini API** for semantic enrichment (classifying issues and extracting entities) and **deterministic Python rules** for routing and safety-critical escalations.

---

## 📂 Directory Structure

```text
ArcVault/
├── inbox/
│   ├── msg_001.json           # Individual incoming customer messages
│   ├── msg_002.json
│   └── ...
├── output/
│   └── output_records.json    # Processed and routed tickets with metadata
├── .env                       # API Credentials (ignored by Git)
├── .env.example               # Template for setting up environment variables
├── .gitignore                 # Files excluded from version control
├── ARCHITECTURE.md            # In-depth architectural design, logic, and roadmap
├── pipeline.py                # Main pipeline orchestrator and logic (watch/batch modes)
├── prompts.md                 # Design rationale for Gemini API prompts
└── requirements.txt           # Python dependencies
```

---

## ⚡ Quick Start

### 1. Prerequisites
Make sure you have Python 3.9+ installed and run the following command to install the required dependencies:
```bash
pip install -r requirements.txt
```

### 2. Configuration
Copy the environment variables template and add your Gemini API key:
```bash
cp .env.example .env
```
Open `.env` in an editor and insert your key:
```ini
GEMINI_API_KEY=AIzaSy...your_actual_key...
```

### 3. Run the Pipeline

The pipeline supports two execution modes:

#### Watch Mode (Default)
Runs indefinitely and watches the `inbox/` folder for new `msg_NNN.json` files using the `watchdog` library:
```bash
python pipeline.py
```

#### Batch Mode
Processes all existing `*.json` files currently in `inbox/` once, then exits:
```bash
python pipeline.py --batch
```

Both modes process messages individually and append the enriched results to `output/output_records.json`.

---

## ⚙️ How it Works

1. **Ingestion**: Reads individual customer message files (`msg_NNN.json`) from `inbox/`. In default watch mode, a file-system watch (`watchdog` Observer) automatically triggers processing when a new message file is created.
2. **Classification & Enrichment**: Sends the ticket to the Gemini API using `gemini-3.1-flash-lite`. It extracts:
   - **Category**: One of `Bug Report`, `Feature Request`, `Billing Issue`, `Technical Question`, or `Incident/Outage`.
   - **Priority**: `Low`, `Medium`, or `High`.
   - **Entities**: Relevant identifiers like account IDs, error codes, invoice numbers, or dollar amounts.
3. **Deterministic Routing**: Maps categories to departments (e.g., `Bug Report` $\rightarrow$ `Engineering`) in Python code rather than letting the LLM decide. This prevents **prompt drift** and ensures auditability.
4. **Escalation Engine**: Automatically diverts high-risk tickets to the `Escalation/Human Review` queue if:
   - LLM confidence score is low ($< 0.70$).
   - Specific keywords match (e.g., `security breach`, `cannot access at all`).
   - A billing dispute discrepancy exceeds $\$500.00$.
   - A platform outage is explicitly reported as affecting multiple users.
5. **Summarization**: Prompts Gemini for a 2-3 sentence, fact-based summary customized for the target department handling the ticket.

---

## 📘 Design and Scaling
* For prompt details, tradeoffs, and proposed enhancements, see [prompts.md](prompts.md).
* For system architecture decisions, escalation math, and production-scaling strategies (e.g., event loops, deduplication, rate-limiting, Temporal orchestration), see [ARCHITECTURE.md](ARCHITECTURE.md).