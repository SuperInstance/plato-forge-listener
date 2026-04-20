"""Forge listener — event listener with pattern filtering, replay, dead letter queue, multiplexing."""
import time
import re
from dataclasses import dataclass, field
from typing import Optional, Callable
from collections import defaultdict, deque
from enum import Enum

class EventSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class ForgeEvent:
    id: str
    event_type: str
    source: str
    payload: dict = field(default_factory=dict)
    severity: EventSeverity = EventSeverity.INFO
    timestamp: float = field(default_factory=time.time)
    room: str = ""
    metadata: dict = field(default_factory=dict)

@dataclass
class EventFilter:
    event_types: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    rooms: list[str] = field(default_factory=list)
    severities: list[EventSeverity] = field(default_factory=list)
    pattern: str = ""  # regex on payload
    min_severity: EventSeverity = EventSeverity.INFO

@dataclass
class ListenerConfig:
    max_buffer: int = 10000
    dead_letter_max: int = 1000
    replay_enabled: bool = True
    ack_timeout: float = 30.0

@dataclass
class Subscription:
    id: str
    filter: EventFilter
    handler: Callable
    created_at: float = field(default_factory=time.time)
    events_received: int = 0
    last_event: float = 0.0

class ForgeListener:
    def __init__(self, config: ListenerConfig = None):
        self.config = config or ListenerConfig()
        self._subscriptions: dict[str, Subscription] = {}
        self._event_log: deque = deque(maxlen=self.config.max_buffer)
        self._dead_letter: deque = deque(maxlen=self.config.dead_letter_max)
        self._pending_ack: dict[str, ForgeEvent] = {}
        self._stats = {"received": 0, "delivered": 0, "filtered": 0,
                      "dead_lettered": 0, "expired": 0}

    def subscribe(self, filter: EventFilter, handler: Callable, sub_id: str = "") -> str:
        sid = sub_id or f"sub-{len(self._subscriptions)}-{int(time.time())}"
        self._subscriptions[sid] = Subscription(id=sid, filter=filter, handler=handler)
        return sid

    def unsubscribe(self, sub_id: str) -> bool:
        return self._subscriptions.pop(sub_id, None) is not None

    def emit(self, event: ForgeEvent):
        self._stats["received"] += 1
        self._event_log.append(event)
        delivered = 0
        for sub in self._subscriptions.values():
            if self._matches(event, sub.filter):
                try:
                    sub.handler(event)
                    sub.events_received += 1
                    sub.last_event = event.timestamp
                    delivered += 1
                except Exception as e:
                    self._dead_letter.append({"event": event, "subscription": sub.id,
                                             "error": str(e), "timestamp": time.time()})
                    self._stats["dead_lettered"] += 1
        if delivered == 0:
            self._stats["filtered"] += 1
        else:
            self._stats["delivered"] += delivered

    def emit_simple(self, event_type: str, source: str, payload: dict = None,
                    severity: str = "info", room: str = ""):
        event = ForgeEvent(id=f"evt-{int(time.time()*1000)}", event_type=event_type,
                          source=source, payload=payload or {},
                          severity=EventSeverity(severity), room=room)
        self.emit(event)

    def _matches(self, event: ForgeEvent, filt: EventFilter) -> bool:
        if filt.event_types and event.event_type not in filt.event_types:
            return False
        if filt.sources and event.source not in filt.sources:
            return False
        if filt.rooms and event.room not in filt.rooms:
            return False
        if filt.severities and event.severity not in filt.severities:
            return False
        severity_order = {EventSeverity.INFO: 0, EventSeverity.WARNING: 1,
                         EventSeverity.ERROR: 2, EventSeverity.CRITICAL: 3}
        if severity_order.get(event.severity, 0) < severity_order.get(filt.min_severity, 0):
            return False
        if filt.pattern:
            payload_str = str(event.payload)
            if not re.search(filt.pattern, payload_str):
                return False
        return True

    def replay(self, event_type: str = "", since: float = 0.0, limit: int = 100) -> list[ForgeEvent]:
        events = list(self._event_log)
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        if since > 0:
            events = [e for e in events if e.timestamp >= since]
        return events[-limit:]

    def dead_letters(self, limit: int = 20) -> list[dict]:
        return list(self._dead_letter)[-limit:]

    def retry_dead_letters(self) -> int:
        count = 0
        while self._dead_letter:
            entry = self._dead_letter.popleft()
            event = entry["event"]
            sub = self._subscriptions.get(entry["subscription"])
            if sub:
                try:
                    sub.handler(event)
                    count += 1
                except:
                    self._dead_letter.append(entry)
                    break
        return count

    def subscriptions(self) -> list[dict]:
        return [{"id": s.id, "events_received": s.events_received,
                "last_event": s.last_event, "filter_types": s.filter.event_types}
                for s in self._subscriptions.values()]

    def recent_events(self, n: int = 20) -> list[ForgeEvent]:
        return list(self._event_log)[-n:]

    @property
    def stats(self) -> dict:
        return {**self._stats, "subscriptions": len(self._subscriptions),
                "buffer_usage": len(self._event_log),
                "dead_letters": len(self._dead_letter)}
