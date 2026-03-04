from dataclasses import dataclass, field


@dataclass
class MessageImage:
    url: str
    file: str


@dataclass
class ParsedMessage:
    text: str
    mentioned: bool
    images: list[MessageImage]
    reply_ids: list[str] = field(default_factory=list)
    forward_ids: list[str] = field(default_factory=list)


@dataclass
class PendingObservation:
    line: str
    normalized_text: str
    images: list[MessageImage]
    ts: float
    sender_name: str = ""
    sender_id: str = ""


@dataclass
class PrivatePairingRequest:
    code: str
    expires_at: float
