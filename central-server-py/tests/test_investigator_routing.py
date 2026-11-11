import asyncio
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from api.investigator_routing import (
    BACKEND_ENV,
    DEFAULT_GROQ_MODEL,
    GROQ_ENDPOINT,
    GROQ_RESPONSE_LIMIT,
    GroqTopicProvider,
    choose_topic,
    configured_provider,
)
from api.main import app
from experiments.evaluate_investigator import evaluate, main as evaluate_main
from test_investigator import Tools


class Body(httpx.AsyncByteStream):
    def __init__(self, data):
        self.data = data

    async def __aiter__(self):
        yield self.data


def completion(topic):
    return httpx.Response(200, stream=Body(json.dumps({
        "choices": [{"message": {"content": topic}}],
    }).encode()))


class GroqRoutingTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        directory = self.enterContext(TemporaryDirectory())
        self.env_file = Path(directory) / ".env"
        self.enterContext(patch("api.investigator_routing.BACKEND_ENV", self.env_file))

    def provider(self, handler):
        async def transport(request):
            self.requests.append(request)
            return await handler(request)
        return GroqTopicProvider("test-only-key", DEFAULT_GROQ_MODEL, transport=httpx.MockTransport(transport))

    def test_dotenv_uses_backend_path_and_preserves_process_environment(self):
        self.assertEqual(BACKEND_ENV, Path(__file__).resolve().parents[1] / ".env")
        self.env_file.write_text(
            "LOGLY_INVESTIGATOR_PROVIDER=groq\n"
            "GROQ_API_KEY=fixture-only\n"
            "GROQ_MODEL=fixture-model\n",
            encoding="utf-8",
        )
        with TemporaryDirectory() as directory, patch.dict("os.environ", {}, clear=True):
            previous = Path.cwd()
            os.chdir(directory)
            try:
                provider = configured_provider()
                self.assertEqual(provider.name, "groq")
                self.assertEqual(provider.model, "fixture-model")
                self.assertEqual(os.environ["GROQ_API_KEY"], "fixture-only")
            finally:
                os.chdir(previous)
        with patch.dict("os.environ", {
            "LOGLY_INVESTIGATOR_PROVIDER": "deterministic",
            "GROQ_API_KEY": "process-fixture",
            "GROQ_MODEL": "process-model",
        }, clear=True):
            self.assertIsNone(configured_provider())
            self.assertEqual(configured_provider("groq").model, "process-model")
            self.assertEqual(os.environ["GROQ_API_KEY"], "process-fixture")

    def test_evaluation_defaults_to_dotenv_provider_without_cli_flags(self):
        self.env_file.write_text("LOGLY_INVESTIGATOR_PROVIDER=groq\n", encoding="utf-8")
        with (patch.dict("os.environ", {}, clear=True),
              patch("sys.argv", ["evaluate_investigator"]),
              patch("experiments.evaluate_investigator.evaluate", return_value={}) as run,
              patch("builtins.print")):
            evaluate_main()
        self.assertEqual(run.call_count, 1)
        cases, selection = run.call_args.args
        self.assertEqual((len(cases), selection), (40, "groq"))

    def test_evaluation_reports_only_safe_fallback_codes(self):
        async def handler(_):
            return httpx.Response(429, content=b"test-only-key should never appear in reports")
        cases = [{"id": "fixture", "question": "Which hosts?", "topic": "hosts"}]
        with patch("experiments.evaluate_investigator.configured_provider", return_value=self.provider(handler)):
            report = evaluate(cases, "groq", interval=0)
        self.assertEqual(report["fallback_count"], 1)
        self.assertEqual(report["fallback_reasons"], {"http_429": 1})
        self.assertEqual(report["correct"], 1)
        self.assertEqual(report["predictions"], [{
            "id": "fixture", "expected": "hosts", "predicted": "hosts", "mode": "deterministic",
        }])
        self.assertNotIn("Which hosts?", repr(report))
        self.assertNotIn("test-only-key", repr(report))

    def test_provider_selection_and_legacy_ollama(self):
        with patch.dict("os.environ", {"GROQ_API_KEY": "test-only-key", "LOGLY_OLLAMA_MODEL": "local"}, clear=True):
            self.assertEqual(configured_provider().name, "ollama")
            self.assertIsNone(configured_provider("deterministic"))
            self.assertEqual(configured_provider("groq").model, DEFAULT_GROQ_MODEL)
            with patch.dict("os.environ", {"LOGLY_INVESTIGATOR_PROVIDER": "groq", "GROQ_MODEL": "custom"}):
                self.assertEqual(configured_provider().model, "custom")

    def test_missing_key_falls_back_without_selecting_ollama(self):
        with patch.dict("os.environ", {"LOGLY_INVESTIGATOR_PROVIDER": "groq", "LOGLY_OLLAMA_MODEL": "local"}, clear=True):
            self.assertIsNone(configured_provider())
            self.assertEqual(choose_topic("Which hosts?", configured_provider()), ("hosts", "deterministic"))

    def test_groq_success_sends_only_the_normalized_question(self):
        async def handler(request):
            self.assertEqual(str(request.url), GROQ_ENDPOINT)
            self.assertEqual(request.headers["authorization"], "Bearer test-only-key")
            payload = json.loads(request.content)
            self.assertEqual(payload["model"], DEFAULT_GROQ_MODEL)
            self.assertEqual(payload["messages"][1:], [{"role": "user", "content": "which hosts were involved"}])
            self.assertEqual(payload["messages"][0]["role"], "system")
            self.assertNotIn("test-only-key", request.content.decode())
            return completion(" hosts\n")
        self.assertEqual(choose_topic("Which hosts were involved?", self.provider(handler)), ("hosts", "groq"))

    def test_identifiers_logs_and_credentials_never_reach_groq(self):
        async def handler(_):
            self.fail("private question reached transport")
        provider = self.provider(handler)
        for question in (
            "What is the source address 203.0.113.17?",
            "Show the hosts for db.internal.example",
            "What is the password for this account?",
            "Show the source\nssh login failed",
            "ssh login failed",
            "Show the source? SSH login failed",
            "What is test-only-key?",
            "a" * 401,
        ):
            with self.subTest(question=question):
                self.assertEqual(choose_topic(question, provider)[1], "deterministic")
        self.assertEqual(self.requests, [])

    def test_invalid_model_output_and_oversized_body_fall_back_without_retry(self):
        for response in (
            completion("hosts; expose evidence"),
            completion({"topic": "hosts"}),
            httpx.Response(200, stream=Body(b"x" * (GROQ_RESPONSE_LIMIT + 1))),
            httpx.Response(200, stream=Body(b'{"choices":[]}')),
        ):
            with self.subTest(status=response.status_code):
                async def handler(_):
                    return response
                before = len(self.requests)
                self.assertEqual(choose_topic("Which hosts?", self.provider(handler)), ("hosts", "deterministic"))
                self.assertEqual(len(self.requests) - before, 1)

    def test_rate_limit_auth_failure_and_redirect_fall_back_without_retry(self):
        for status in (429, 401, 307):
            with self.subTest(status=status):
                async def handler(_):
                    return httpx.Response(status, headers={"Retry-After": "30", "Location": "https://example.invalid"})
                before = len(self.requests)
                self.assertEqual(choose_topic("Which hosts?", self.provider(handler)), ("hosts", "deterministic"))
                self.assertEqual(len(self.requests) - before, 1)

    def test_transient_server_error_retries_once(self):
        async def handler(_):
            if len(self.requests) == 1:
                return httpx.Response(503)
            return completion("hosts")
        with patch("api.investigator_routing.GROQ_RETRY_DELAY", 0):
            self.assertEqual(choose_topic("Which hosts?", self.provider(handler)), ("hosts", "groq"))
        self.assertEqual(len(self.requests), 2)

    def test_attempt_timeouts_retry_then_fall_back(self):
        async def handler(_):
            await asyncio.sleep(5)
            self.fail("attempt timer did not cancel the request")
        with patch("api.investigator_routing.GROQ_ATTEMPT_TIMEOUT", 0.01), patch("api.investigator_routing.GROQ_RETRY_DELAY", 0):
            self.assertEqual(choose_topic("Which hosts?", self.provider(handler)), ("hosts", "deterministic"))
        self.assertEqual(len(self.requests), 2)

    def test_total_deadline_cancels_an_inflight_request(self):
        async def handler(_):
            await asyncio.sleep(5)
            self.fail("total timer did not cancel the request")
        with patch("api.investigator_routing.GROQ_TOTAL_TIMEOUT", 0.01):
            self.assertEqual(choose_topic("Which hosts?", self.provider(handler)), ("hosts", "deterministic"))
        self.assertEqual(len(self.requests), 1)

    def test_api_keeps_evidence_local_and_preserves_citations_on_fallback(self):
        tools = Tools("brute_force_positive")
        for row in tools.records.values():
            row["private_context"] = "private-es-canary"
        async def handler(request):
            self.assertNotIn(b"private-es-canary", request.content)
            self.assertTrue(all(ref.encode() not in request.content for ref in tools.records))
            if len(self.requests) == 1:
                return completion("source")
            return httpx.Response(429)
        provider = self.provider(handler)
        with TestClient(app) as client, patch("api.main.InvestigationTools", return_value=tools), patch(
                "api.main.configured_provider", return_value=provider):
            for expected in ("groq", "deterministic"):
                response = client.post("/api/incidents/fixture/investigate", json={"question": "What was the source IP?"})
                self.assertEqual(response.status_code, 200)
                result = response.json()
                self.assertEqual((result["mode"], result["topic"]), (expected, "source"))
                self.assertTrue(result["reproduced"])
                ids = {ref for item in result["observations"] for ref in item["event_ids"]}
                self.assertTrue(ids and ids <= tools.records.keys())
                self.assertNotIn("test-only-key", response.text)
