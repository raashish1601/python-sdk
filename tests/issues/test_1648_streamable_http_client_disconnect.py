"""Regression test for issue #1648: client disconnects during POST body reads."""

import logging

import anyio
import pytest
from starlette.requests import Request
from starlette.types import Message, Scope

from mcp.server.streamable_http import StreamableHTTPServerTransport
from mcp.shared.message import SessionMessage

REQUEST_SCOPE: Scope = {
    "type": "http",
    "asgi": {"version": "3.0"},
    "http_version": "1.1",
    "method": "POST",
    "scheme": "http",
    "path": "/mcp",
    "raw_path": b"/mcp",
    "query_string": b"",
    "root_path": "",
    "headers": [
        (b"accept", b"application/json, text/event-stream"),
        (b"content-type", b"application/json"),
    ],
    "client": ("127.0.0.1", 12345),
    "server": ("127.0.0.1", 8000),
}


@pytest.mark.anyio
async def test_client_disconnect_during_body_read_does_not_return_500(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A mid-body disconnect should not be treated as a server-side internal error."""

    transport = StreamableHTTPServerTransport("/mcp")
    read_stream_writer, read_stream = anyio.create_memory_object_stream[SessionMessage | Exception](1)
    transport._read_stream_writer = read_stream_writer

    receive_messages: list[Message] = [
        {"type": "http.request", "body": b'{"jsonrpc": "2.0",', "more_body": True},
        {"type": "http.disconnect"},
    ]
    sent_messages: list[Message] = []

    async def receive() -> Message:
        return receive_messages.pop(0)

    async def send(message: Message) -> None:
        sent_messages.append(message)

    request = Request(REQUEST_SCOPE, receive)

    try:
        with caplog.at_level(logging.ERROR):
            await transport._handle_post_request(REQUEST_SCOPE, request, receive, send)

        assert sent_messages == []
        with pytest.raises(anyio.WouldBlock):
            read_stream.receive_nowait()
        assert "Error handling POST request" not in caplog.text
    finally:
        await read_stream_writer.aclose()
        await read_stream.aclose()
