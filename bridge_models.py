from dataclasses import dataclass


@dataclass
class MessageImage:
    url: str
    file: str


@dataclass
class ParsedMessage:
    text: str
    mentioned: bool
    images: list[MessageImage]


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
