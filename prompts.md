# ArcVault Triage Prompts

This document details the LLM prompts designed and implemented in the ArcVault Intake & Triage Pipeline.

## CLASSIFY_ENRICH_PROMPT_TEMPLATE

```
You are a triage assistant for ArcVault, a B2B software company.
Given the raw customer message below, return ONLY a JSON object (no prose, no markdown fences)
with exactly this shape:

{
  "category": one of ["Bug Report", "Feature Request", "Billing Issue", "Technical Question", "Incident/Outage"],
  "priority": one of ["Low", "Medium", "High"],
  "confidence": a float between 0 and 1 representing your certainty in the category assignment,
  "core_issue": a single sentence describing the core issue,
  "entities": {
      "account_ids": [...],
      "invoice_numbers": [...],
      "error_codes": [...],
      "dollar_amounts": [...],
      "other": [...]
  },
  "urgency_signal": one short phrase describing what in the message signals urgency (or lack thereof)
}

Classification Guidelines:
- "Technical Question": Inquiries about existing product capabilities, configuration, setup, or how to do something (e.g., questions phrased as "is there a way to do X?", "can we do X?", "how do I set up X?").
- "Feature Request": Suggestions, requests, or demands for new features, new products, or product improvements that are currently missing from the platform.

Priority Guidelines:
- "High": Active outages, security/data issues, or problems blocking business-critical operations for the customer right now.
- "Medium": A real functional problem or financial discrepancy affecting one customer/account, with no explicit deadline.
- "Low": Feature requests, pre-sales/evaluation questions, or anything the customer frames as non-blocking.

Source: {source}
Message: "{message}"
```

### Prompt Rationale, Tradeoffs, and Future Improvements
I designed this prompt to compress both classification (category, priority, confidence) and entity extraction (account IDs, invoice numbers, error codes, dollar amounts) into a single, unified LLM call. This choice trades off task isolation to minimize latency and API costs, which is highly efficient for real-time triage. To force the LLM to output predictable JSON, I specified the exact JSON schema structure inline and added targeted classification guidelines to reduce ambiguity at the boundaries (e.g., distinguishing between a "Technical Question" about Okta SSO setup vs. a missing "Feature Request" for log exports). To ensure reliability, the `call_llm` function invokes the Gemini API with `response_mime_type="application/json"`. If I had more time, I would improve robustness by implementing few-shot examples inside the prompt to guide edge cases, utilizing a strict validation library like Pydantic with Gemini's `response_schema` parameter to enforce type safety, and adding a schema verification/retry fallback step in the code in case of malformed output.

---

## SUMMARY_PROMPT_TEMPLATE

```
Write a 2-3 sentence summary of this support request for the {queue} team.
Be factual and specific (mention any IDs, amounts, or error codes). No preamble, just the summary.

Category: {category}
Priority: {priority}
Core issue: {core_issue}
Raw message: "{raw_message}"
```

### Prompt Rationale, Tradeoffs, and Future Improvements
This prompt is designed to generate a concise, context-aware ticket summary specifically tailored for the target department (passed dynamically via the `{queue}` variable, such as `Engineering` or `Billing`). By supplying the LLM with the structured classification categories, extracted entities, and core issues generated in the previous step, I minimized prompt complexity and kept input tokens low. The tradeoff here is a slight latency penalty from making a second sequential LLM call, but it ensures that the generated summary is grounded, objective, and contains critical tokens (like `jsmith`, `#8821`, or `403` error codes) without conversational fluff. In a production environment, I would improve this prompt by dynamically adjusting the tone and details based on the target queue (for example, providing a highly technical summary for `Engineering` but a financial/impact-focused summary for `Billing`) and implementing few-shot examples for each target queue to align summarizing styles with internal support guidelines.
