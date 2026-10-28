from typing import Callable, List, Any
from collections import defaultdict
import logging
import threading

class EventBus:
    def __init__(self):
        self._subscribers = defaultdict(set)
        self._lock = threading.Lock()

    def subscribe(self, event: str, func: Callable) -> None:
        with self._lock:
            self._subscribers[event].add(func)

    def emit(self, event: str, *args, **kwargs) -> List[Any]:
        results = []
        with self._lock:
            subscribers = tuple(self._subscribers[event])
        for f in subscribers:
            try:
                results.append(f(*args, **kwargs))
            except Exception:
                logging.getLogger(__name__).exception("Callback failed for event %s", event)
        return results
