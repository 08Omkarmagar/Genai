
import json
import re
import time
import random
from typing import Dict, List, Any

def parse_robust_json(text: str) -> Any | None:
    """Cleans markdown blocks and attempts to parse JSON from LLM output."""
    text = re.sub(r"```json|```", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:
                pass
    return None

def merge_dicts(a: Dict, b: Dict) -> Dict:
    """LangGraph reducer to merge dictionaries."""
    return {**(a or {}), **(b or {})}

def add_lists(a: List, b: List) -> List:
    """LangGraph reducer to concatenate lists."""
    return (a or []) + (b or [])

def human_delay(min_sec: float = 0.5, max_sec: float = 2.0):
    """Simple random sleep to mimic human behavior and avoid rate limits."""
    time.sleep(random.uniform(min_sec, max_sec))
