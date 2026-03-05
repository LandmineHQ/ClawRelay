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
    reply_blocks: list[str] = field(default_factory=list)
    forward_blocks: list[str] = field(default_factory=list)


@dataclass
class PendingObservation:
    line: str
    normalized_text: str
    images: list[MessageImage]
    ts: float
    sender_name: str = ""
    sender_id: str = ""
    message_id: str = ""
    reply_ids: list[str] = field(default_factory=list)
    forward_ids: list[str] = field(default_factory=list)
    reply_blocks: list[str] = field(default_factory=list)
    forward_blocks: list[str] = field(default_factory=list)


@dataclass
class PairingRequest:
    code: str
    expires_at: float
    requester_user_id: str = ""
    requester_message_type: str = ""


@dataclass
class PairingRecord:
    target_type: str
    target_id: str
    approved_by_user_id: str
    approved_at: int
