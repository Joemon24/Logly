"""Provider selection and question-only routing for the Investigator."""
import asyncio
from collections.abc import Callable
import json
import os
import re
from pathlib import Path
from threading import BoundedSemaphore
from typing import Protocol
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

import httpx
from dotenv import load_dotenv

BACKEND_ENV = Path(__file__).resolve().parents[1] / ".env"

TOPICS = ("summary", "source", "hosts", "related", "gaps", "next")
TOPIC_PROMPT = (
    "Classify an incident investigation question. Select summary for a general review; "
    "source for IP addresses; hosts for affected machines; related for other incidents; "
    "gaps for missing evidence or proof of compromise; next for recommended analyst actions."
)
DEFAULT_GROQ_MODEL = "openai/gpt-oss-20b"
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
GROQ_ATTEMPT_TIMEOUT = 1.75
GROQ_TOTAL_TIMEOUT = 4.0
GROQ_MAX_ATTEMPTS = 2
GROQ_RETRY_DELAY = 0.15
GROQ_RESPONSE_LIMIT = 8192

# Fail closed on identifiers, unknown words, copied logs, and credential-shaped
# input. Only these routing words can be serialized into a cloud question.
CLOUD_WORDS = frozenset("""
a about account across action actions activity address addresses affected after again alert all alone
also an analysis any appear are as asset assets at attack attacker attempt attempts before begin boxes
brief briefly can case cases cause check checks claims close clue clues come compare compromise
conclude conclusion confidence confirm confirmed connected connection connections
context correlate correlation could count data decide describe detail details
detect detected detection detections determine did direction do does earlier else endpoint
endpoints enough entry establish event events evidence exactly explain facts
fail failed failure failures finding first focus follow following for from further gap gaps general
get give happened has have help here history host hosts how identity in incident
incidents indicate information initial investigate investigation involved ip ips
i is it know known lead led linked list login logins logs look machine machines many match matters may
me missing more network next of on one origin originated other our outline
originate overview pattern pending perform prior proof prove proven raised reason recommendation
recommendations recommend record recorded records related relationship remains remote report review
risk rule same seen sequence separate server servers share short should show
similar since source sources spread ssh start started steps still summary
support suspect suspicious take target targets tell that the their them then
there these this those time timeline to trace traffic trigger triggered triage tied together
unknown unverified up us user validate verified verify view was we were what when
where whether which who why will with workstation workstations would you your
""".split())
CLOUD_QUESTION = re.compile(r"[a-zA-Z]+(?: +[a-zA-Z]+)*[?.]?")
QUESTION_STARTERS = frozenset("""
are can could describe did do does explain give has have how is outline
recommend review should show tell was were what when where which who why would
""".split())


MODEL_SLOTS = BoundedSemaphore(2)


class TopicProvider(Protocol):
    name: str
    model: str

    def topic(self, question: str) -> str: ...


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class OllamaTopicProvider:
    name = "ollama"

    def __init__(self, model):
        self.model = model
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def topic(self, question):
        if not MODEL_SLOTS.acquire(blocking=False):
            raise RoutingFailure("inference_busy")
        try:
            payload = {
                "model": self.model, "stream": False, "think": False,
                "options": {"num_predict": 16, "num_ctx": 1024, "temperature": 0},
                "format": {"type": "string", "enum": list(TOPICS)},
                "system": TOPIC_PROMPT,
                "prompt": question,
            }
            request = Request("http://127.0.0.1:11434/api/generate",
                              data=json.dumps(payload).encode(),
                              headers={"Content-Type": "application/json"})
            with self.opener.open(request, timeout=3) as response:
                body = json.loads(response.read(8192))
            return json.loads(body["response"])
        finally:
            MODEL_SLOTS.release()


class GroqTopicProvider:
    name = "groq"

    def __init__(self, api_key, model, *, transport=None):
        self._api_key = api_key
        self.model = model
        self._transport = transport

    def topic(self, question):
        if (not 1 <= len(question) <= 400 or not CLOUD_QUESTION.fullmatch(question)
                or self._api_key.casefold() in question.casefold()):
            raise RoutingFailure("ineligible_question")
        words = re.findall(r"[a-z]+", question.lower())
        if (len(words) < 2 or words[0] not in QUESTION_STARTERS
                or not all(word in CLOUD_WORDS for word in words)):
            raise RoutingFailure("ineligible_question")
        if not MODEL_SLOTS.acquire(blocking=False):
            raise RoutingFailure("inference_busy")
        try:
            return asyncio.run(self._route(" ".join(words)))
        finally:
            MODEL_SLOTS.release()

    async def _route(self, question):
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": TOPIC_PROMPT + " Reply with only the topic word."},
                {"role": "user", "content": question},
            ],
            "temperature": 0,
            "max_completion_tokens": 256,
            "stream": False,
        }
        if self.model in {"openai/gpt-oss-20b", "openai/gpt-oss-120b"}:
            payload.update(reasoning_effort="low", include_reasoning=False)
        headers = {"Authorization": f"Bearer {self._api_key}", "Accept-Encoding": "identity"}
        async with asyncio.timeout(GROQ_TOTAL_TIMEOUT):
            async with httpx.AsyncClient(timeout=GROQ_ATTEMPT_TIMEOUT, follow_redirects=False,
                                         trust_env=False, transport=self._transport) as client:
                for attempt in range(GROQ_MAX_ATTEMPTS):
                    try:
                        async with asyncio.timeout(GROQ_ATTEMPT_TIMEOUT):
                            body = await self._complete(client, payload, headers)
                        return self._topic_from_body(body)
                    except (TimeoutError, httpx.TimeoutException):
                        reason = "timeout"
                    except httpx.RequestError:
                        reason = "transport_error"
                    except RetryableGroqError as error:
                        reason = error.reason
                    if attempt < GROQ_MAX_ATTEMPTS - 1:
                        await asyncio.sleep(GROQ_RETRY_DELAY)
                raise RoutingFailure(reason) from None

    @staticmethod
    def _topic_from_body(body):
        try:
            topic = json.loads(body)["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError):
            raise RoutingFailure("invalid_response") from None
        if not isinstance(topic, str) or topic.strip() not in TOPICS:
            raise RoutingFailure("invalid_topic")
        return topic.strip()

    async def _complete(self, client, payload, headers):
        async with client.stream("POST", GROQ_ENDPOINT, json=payload, headers=headers) as response:
            if response.status_code in {408, 500, 502, 503, 504}:
                raise RetryableGroqError(f"http_{response.status_code}")
            if response.status_code != 200:
                # Fall back immediately on 429. A retry would compete for the
                # same quota, and Retry-After may exceed the request budget.
                raise RoutingFailure(f"http_{response.status_code}")
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                raise RoutingFailure("unexpected_encoding")
            body = bytearray()
            async for chunk in response.aiter_raw():
                if len(body) + len(chunk) > GROQ_RESPONSE_LIMIT:
                    raise RoutingFailure("response_too_large")
                body.extend(chunk)
            return body


class RoutingFailure(ValueError):
    """A safe diagnostic code that contains no request or response text."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class RetryableGroqError(RoutingFailure):
    pass


def configured_selection() -> str:
    load_dotenv(BACKEND_ENV, override=False, interpolate=False)
    return os.getenv("LOGLY_INVESTIGATOR_PROVIDER", "").strip().lower()


def configured_provider(selection: str | None = None) -> TopicProvider | None:
    """Resolve one backend provider without implicitly sending data to Groq."""
    environment_selection = configured_selection()
    if selection is None:
        selection = environment_selection
    ollama_model = os.getenv("LOGLY_OLLAMA_MODEL", "").strip()
    if selection == "groq":
        key = os.getenv("GROQ_API_KEY", "").strip()
        model = os.getenv("GROQ_MODEL", "").strip() or DEFAULT_GROQ_MODEL
        return GroqTopicProvider(key, model) if key else None
    if selection in {"", "ollama"} and ollama_model:
        return OllamaTopicProvider(ollama_model)
    return None


def choose_topic(
    question: str,
    provider: TopicProvider | None,
    *,
    on_fallback: Callable[[str], None] | None = None,
) -> tuple[str, str]:
    if not question:
        return "summary", "deterministic"
    if provider:
        try:
            topic = provider.topic(question)
            if isinstance(topic, str) and topic in TOPICS:
                return topic, provider.name
            reason = "invalid_topic"
        except RoutingFailure as error:
            reason = error.reason
        except TimeoutError:
            reason = "timeout"
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            reason = "provider_error"
        if on_fallback is not None:
            on_fallback(reason)
    for topic, terms in (
        ("related", ("related", "other incidents")),
        ("gaps", ("missing", "unknown", "gaps", "compromise", "proven")),
        ("hosts", ("host", "machine")),
        ("source", ("source", "address", " ip")),
        ("next", ("next", "recommend", "should")),
    ):
        if any(term in question.lower() for term in terms):
            return topic, "deterministic"
    return "summary", "deterministic"
