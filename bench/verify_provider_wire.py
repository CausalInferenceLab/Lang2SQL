"""Capture the safe fields of the real HTTP request used by a recovery smoke."""

import importlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

import httpx

benchmark = sys.argv[1]
output = Path(sys.argv[sys.argv.index("--output") + 1])
send = httpx.AsyncClient.send
payloads = []


async def record(client, request, *args, **kwargs):
    if request.url.path.endswith("/chat/completions"):
        wire = json.loads(request.content)
        event = {
            "host": request.url.host,
            "model": wire["model"],
            "tool_choice": wire.get("tool_choice"),
            "max_output_tokens": wire.get(
                "max_completion_tokens", wire.get("max_tokens")
            ),
            "reasoning_effort": wire.get("reasoning_effort"),
        }
        payloads.append(event)
        result = await send(client, request, *args, **kwargs)
        event["http_status"] = result.status_code
        return result
    return await send(client, request, *args, **kwargs)


module = importlib.import_module(f"bench.{benchmark}.verify_recovery")
with patch.object(httpx.AsyncClient, "send", record):
    module.main()

result = json.loads(output.read_text())
result[0]["http_requests"] = payloads
output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
assert payloads and payloads[0]["tool_choice"] == {
    "type": "function",
    "function": {"name": "explore_schema"},
}
assert payloads[1]["tool_choice"] == "auto"
assert all(
    item["http_status"] == 200 and item["max_output_tokens"] == 4096
    for item in payloads
)
print("Verified forced tool selection in HTTP JSON and return to auto")
