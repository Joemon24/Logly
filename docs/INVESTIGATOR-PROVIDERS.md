# Investigator providers

The Investigator routes follow-up questions to one of six topics: summary, source, hosts, related, gaps, or next. Python retrieves events, runs the existing rule predicates, and renders evidence citations. Provider output can select a topic only. It cannot supply incident facts, change rules, run tools, or access Elasticsearch.

| `LOGLY_INVESTIGATOR_PROVIDER` | Backend configuration |
| --- | --- |
| `deterministic` | Local keyword routing with no model calls |
| `ollama` | Set `LOGLY_OLLAMA_MODEL` to a model installed in local Ollama on loopback |
| `groq` | Set `GROQ_API_KEY` and optionally `GROQ_MODEL` in the backend environment |

If selection is unset, the existing `LOGLY_OLLAMA_MODEL` opt-in still works. Otherwise routing defaults to deterministic. A missing key, unavailable model, ineligible question, or invalid output triggers local fallback. An empty follow-up uses a local summary.

## Run Groq privately

The default is `openai/gpt-oss-20b`, which is listed in the [supported models](https://console.groq.com/docs/models) and [free-plan rate limits](https://console.groq.com/docs/rate-limits). The adapter uses the official [Chat Completions API](https://console.groq.com/docs/api-reference). Model availability and quota depend on the account. The backend and evaluation runner load `central-server-py/.env` regardless of the working directory. Existing process variables take precedence. The private `.env` is Git-ignored.

Install the pinned requirements using the existing virtual environment's interpreter. Put `LOGLY_INVESTIGATOR_PROVIDER=groq`, the private `GROQ_API_KEY`, and optionally `GROQ_MODEL` in `central-server-py/.env`. Use the root `.env.example` as a template. With Elasticsearch available, run from the repository root:

```sh
# Create .venv with python3 -m venv .venv if you do not already have a venv.
.venv/bin/python -m pip install -r central-server-py/requirements.txt
PYTHONPATH=central-server-py .venv/bin/python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

Start the dashboard from a separate terminal. React receives only the existing investigation response and a provider mode label. The static Simulation Lab has no provider configuration and makes no inference calls.

Groq receives the routing instruction and a normalized analyst question made only from approved routing words. Unknown words, numbers, identifiers, copied multiline logs, and credential-shaped text fail closed and route locally. This is a deliberately narrow input gate, not a general-purpose redaction service. Ask general questions and keep credentials and evidence excerpts out of the question field. Stored incident records, raw log text, event IDs, and Elasticsearch credentials are never added to the cloud request. The API key is used only in the HTTPS authorization header. The adapter ignores proxy environment configuration and disallows redirects.

Groq requests are limited to two concurrent inferences shared with Ollama, two attempts for transient transport or server failures, 1.75 seconds per attempt, a four-second total budget, 256 completion tokens, and an 8 KiB response. Rate limits trigger immediate local fallback without a retry. Provider failures do not change evidence retrieval or citation checks.

## Reproduce the routing evaluation

From `central-server-py`, use the same virtual environment and backend provider variables:

```sh
../.venv/bin/python -m experiments.evaluate_investigator
```

The runner reports the SHA-256 of the 40 labeled synthetic questions, end-to-end routing accuracy, provider-only accuracy, fallback frequency, per-question latency percentiles, and counts of terminal fallback reasons. It reports safe codes such as `http_429` or `timeout` and per-case IDs, expected topics, predicted topics, and routing modes. It excludes question text, credentials, and raw provider responses. Deterministic runs have no fallback. Providers without credentials or model configuration are marked skipped. Set `LOGLY_OLLAMA_MODEL` to include local Ollama. Without flags, the runner uses `LOGLY_INVESTIGATOR_PROVIDER`, or compares all three if it is unset. Repeat `--provider` to explicitly select multiple providers. Groq requests are paced for the documented free tier by default. The reported latency excludes pacing.

The dataset is a small routing smoke test. It does not measure incident detection accuracy, the completeness of evidence, or general investigation quality.

### Measured routing results

Run on macOS with a preloaded local Ollama model and the development Docker stack running. These are routing measurements on the checked-in questions, separate from the stack verification baseline.

| Provider | Accuracy including fallback | Fallback | Per-question latency p50 / p95 |
| --- | ---: | ---: | ---: |
| Deterministic | 25/40 (62.5%) | 0/40 | 0.002 / 0.003 ms |
| Ollama `qwen3.5:2b` | 25/40 (62.5%) | 1/40 (2.5%) | 454.567 / 1,294.989 ms |
| Groq `openai/gpt-oss-20b` | 36/40 (90.0%) | 0/40 | 521.230 / 772.461 ms |

Ollama returned an accepted topic on 39 questions, with 24 correct among those 39. It did not improve overall accuracy on that run. The separate live Groq run loaded the backend `.env` while running from `/tmp`. All 40 questions received an allowed topic, four were classified incorrectly, and no terminal provider errors or fallbacks were recorded. Recovered retries are not counted by this report. Mocked HTTP tests cover invalid output, missing keys, deadlines, rate limits, and fallback.

### Benchmark audit

- **Leakage check:** Each Groq call receives a fixed topic instruction and one normalized question. The evaluator compares the predicted topic with the case label after inference. The request carries no expected answer, case ID, examples from the dataset, earlier questions, or incident evidence. All 40 normalized prompts were unique. This checks the evaluator's request path. Provider training-set overlap cannot be verified.
- **Label distribution and wording:** Summary, source, hosts, and related have seven cases each. Gaps and next have six each. The majority-label baseline is 7/40 (17.5%). The questions were hand-written for this taxonomy, and five include the exact expected topic word. Near-paraphrases and the shared question vocabulary can still make the task easier. All 40 cases pass the cloud input gate, so this benchmark does not measure real-world cloud eligibility.
- **Ambiguous intent:** The benchmark assigns one expected topic per question, although an analyst may reasonably ask for overlapping kinds of help. The audit keeps the original labels and score. The audit run is useful for examining errors, but 40 synthetic questions are insufficient to claim broad routing reliability, production latency, or investigation quality.

### Case-level Groq rerun

The first run reported 36/40 but saved only aggregate results, so the identities of those four misses cannot be recovered. A separate run on the unchanged dataset and routing prompt recorded 35/40 (87.5%), with zero fallbacks or terminal provider errors and 515.911 ms p50 / 863.311 ms p95. The lower rerun score shows variation even with the same settings. The two results are reported separately.

| Case | Expected → observed | Assessment |
| --- | --- | --- |
| Q24 — How can I validate this finding? | `next` → `gaps` | Ambiguous. The wording asks for validation steps, but identifying missing evidence is a reasonable interpretation. |
| Q25 — What triggered this alert? | `summary` → `source` | Clear miss. The question asks why the alert fired and does not ask for an originating IP. |
| Q30 — Give me a short list of checks to perform. | `next` → `summary` | Clear miss. “Checks to perform” calls for actions. “Short list” only specifies the format. |
| Q34 — Did the same source appear in separate incidents? | `related` → `source` | Overlapping cues. The source is the join key, but comparing incidents is the requested task. |
| Q36 — Which logs should I check first? | `next` → `source` | Overlapping cues. The question asks for a review priority. A log source is not the network source covered by the `source` topic. |

Three of six `next` questions were missed in this rerun. The prompt and expected labels were kept fixed after reviewing the misses. Any future tuning should be evaluated against a separate set of independently labeled, realistic questions, including inputs that trigger the cloud gate and fall back locally.
