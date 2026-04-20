"""Continuous fleet event listener with framing pipeline."""

from dataclasses import dataclass
from typing import Optional

@dataclass
class TraceEvent:
    event_type: str
    content: str
    priority: str = "P2"
    confidence: float = 0.5
    timestamp: float = 0.0
    source: str = ""

class ForgeListener:
    def __init__(self, gap_threshold: float = 0.7, max_batch: int = 1000):
        self.gap_threshold = gap_threshold
        self.max_batch = max_batch
        self._events: list[TraceEvent] = []
        self._gaps: list[dict] = []

    def classify(self, content: str, confidence: float = 0.5) -> str:
        c = content.lower()
        if any(k in c for k in ["error", "fail", "critical", "p0"]):
            return "P0"
        if any(k in c for k in ["warning", "important", "p1", "high"]):
            return "P1"
        return "P2"

    def frame(self, event: TraceEvent) -> dict:
        is_gap = event.confidence < self.gap_threshold
        framed = {"prompt": f"Context: {event.source}\nEvent: {event.event_type}",
                  "completion": event.content, "quality": event.confidence,
                  "priority": event.priority, "source": event.source,
                  "gap": is_gap}
        if is_gap:
            self._gaps.append({"content": event.content, "confidence": event.confidence,
                               "priority": event.priority})
        return framed

    def process(self, content: str, event_type: str = "message",
                confidence: float = 0.5, source: str = "") -> dict:
        import time
        priority = self.classify(content, confidence)
        event = TraceEvent(event_type=event_type, content=content, priority=priority,
                           confidence=confidence, timestamp=time.time(), source=source)
        self._events.append(event)
        if len(self._events) > self.max_batch:
            self._events = self._events[-self.max_batch:]
        return self.frame(event)

    def drain_batch(self, priority: str = None) -> list[dict]:
        if priority:
            events = [e for e in self._events if e.priority == priority]
            self._events = [e for e in self._events if e.priority != priority]
        else:
            events = self._events[:]
            self._events.clear()
        return [self.frame(e) for e in events]

    def drain_gaps(self) -> list[dict]:
        gaps = self._gaps[:]
        self._gaps.clear()
        return gaps

    @property
    def stats(self) -> dict:
        p_counts = {"P0": 0, "P1": 0, "P2": 0}
        for e in self._events:
            p_counts[e.priority] = p_counts.get(e.priority, 0) + 1
        return {"buffered": len(self._events), "gaps": len(self._gaps), "by_priority": p_counts}
