"""Forge listener — event-driven tile stream consumer with filtering and replay."""
import time
import json
from dataclasses import dataclass, field
from typing import Callable, Optional
from collections import defaultdict, deque
from enum import Enum

class EventType(Enum):
    TILE_CREATED = "tile_created"
    TILE_UPDATED = "tile_updated"
    TILE_DELETED = "tile_deleted"
    TILE_GHOSTED = "tile_ghosted"
    TILE_RESURRECTED = "tile_resurrected"
    ROOM_CHANGED = "room_changed"
    FORGE_TICK = "forge_tick"

@dataclass
class ForgeEvent:
    event_type: EventType
    tile_id: str = ""
    room: str = ""
    data: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0

@dataclass
class Subscription:
    id: str
    event_types: list[EventType]
    filter_room: str = ""
    filter_domain: str = ""
    callback: str = ""  # reference to registered handler
    active: bool = True
    events_received: int = 0
    last_event: float = 0.0

class ForgeListener:
    def __init__(self, buffer_size: int = 10000):
        self._buffer: deque = deque(maxlen=buffer_size)
        self._subscriptions: dict[str, Subscription] = {}
        self._handlers: dict[str, Callable] = {}
        self._sequence: int = 0
        self._stats = {"emitted": 0, "delivered": 0, "dropped": 0,
                      "replays": 0, "buffer_size": buffer_size}

    def emit(self, event_type: str, tile_id: str = "", room: str = "",
             data: dict = None) -> ForgeEvent:
        et = EventType(event_type)
        self._sequence += 1
        event = ForgeEvent(event_type=et, tile_id=tile_id, room=room,
                          data=data or {}, sequence=self._sequence)
        self._buffer.append(event)
        self._stats["emitted"] += 1
        # Deliver to matching subscriptions
        for sub in self._subscriptions.values():
            if not sub.active:
                continue
            if sub.event_types and et not in sub.event_types:
                continue
            if sub.filter_room and sub.filter_room != room:
                continue
            domain = data.get("domain", "") if data else ""
            if sub.filter_domain and sub.filter_domain != domain:
                continue
            sub.events_received += 1
            sub.last_event = time.time()
            self._stats["delivered"] += 1
            if sub.callback and sub.callback in self._handlers:
                try:
                    self._handlers[sub.callback](event)
                except Exception:
                    self._stats["dropped"] += 1
        return event

    def subscribe(self, event_types: list[str] = None, room: str = "",
                  domain: str = "", handler_name: str = "") -> Subscription:
        sub_id = f"sub-{len(self._subscriptions)}"
        types = [EventType(et) for et in event_types] if event_types else []
        sub = Subscription(id=sub_id, event_types=types, filter_room=room,
                          filter_domain=domain, callback=handler_name)
        self._subscriptions[sub_id] = sub
        return sub

    def register_handler(self, name: str, fn: Callable):
        self._handlers[name] = fn

    def unsubscribe(self, sub_id: str) -> bool:
        return self._subscriptions.pop(sub_id, None) is not None

    def pause(self, sub_id: str):
        sub = self._subscriptions.get(sub_id)
        if sub: sub.active = False

    def resume(self, sub_id: str):
        sub = self._subscriptions.get(sub_id)
        if sub: sub.active = True

    def replay(self, from_seq: int = 0, event_types: list[str] = None,
               room: str = "", limit: int = 100) -> list[ForgeEvent]:
        types = set(EventType(et) for et in event_types) if event_types else set()
        events = [e for e in self._buffer if e.sequence >= from_seq
                 and (not types or e.event_type in types)
                 and (not room or e.room == room)]
        self._stats["replays"] += 1
        return events[-limit:]

    def since(self, timestamp: float, limit: int = 100) -> list[ForgeEvent]:
        return [e for e in self._buffer if e.timestamp >= timestamp][-limit:]

    def latest(self, n: int = 20) -> list[ForgeEvent]:
        return list(self._buffer)[-n:]

    def room_events(self, room: str, n: int = 50) -> list[ForgeEvent]:
        return [e for e in self._buffer if e.room == room][-n:]

    def tile_events(self, tile_id: str, n: int = 50) -> list[ForgeEvent]:
        return [e for e in self._buffer if e.tile_id == tile_id][-n:]

    def clear_buffer(self):
        self._buffer.clear()

    @property
    def stats(self) -> dict:
        active_subs = sum(1 for s in self._subscriptions.values() if s.active)
        return {**self._stats, "subscriptions": len(self._subscriptions),
                "active_subscriptions": active_subs, "handlers": len(self._handlers),
                "sequence": self._sequence, "buffer_used": len(self._buffer)}
