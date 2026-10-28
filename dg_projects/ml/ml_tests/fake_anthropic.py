"""Stand-ins for the anthropic SDK client used by the feedback LLM adapters."""

from typing import Any


class FakeUsage:
    def __init__(self, input_tokens: int, output_tokens: int) -> None:
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens


class FakeMessage:
    def __init__(
        self,
        content: list[object],
        *,
        stop_reason: str = "end_turn",
        usage: FakeUsage | None = None,
    ) -> None:
        self.content = content
        self.stop_reason = stop_reason
        self.usage = usage


class FakeAnthropicClient:
    """Stands in for the anthropic SDK client: only messages.create is used."""

    def __init__(self, message: FakeMessage) -> None:
        self.requests: list[dict[str, Any]] = []

        def create(_self: object, **kwargs: Any) -> FakeMessage:
            self.requests.append(kwargs)
            return message

        self.messages = type("_Messages", (), {"create": create})()
