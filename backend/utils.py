"""
utils.py — Shared utility functions and LangGraph state reducers.

This module provides common helper functions used across the backend services,
including robust JSON parsing and state management utilities for LangGraph.
"""

import json
import re
import time
import random
from typing import Dict, List, Any

# ---------------------------------------------------------------------------
# LLM & Data Utilities
# ---------------------------------------------------------------------------

def parse_robust_json(text: str) -> Any | None:
    """
    Attempts to extract and parse JSON from potentially messy LLM responses.
    Handles markdown fences and conversational text surrounding the JSON block.
    """
    # Remove markdown code blocks if present
    text = re.sub(r"```json|```", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Fallback: Extract the first viable JSON-like object via regex
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try: return json.loads(match.group(0))
            except: pass
    return None

# ---------------------------------------------------------------------------
# LangGraph Reducers
# ---------------------------------------------------------------------------

def merge_dicts(a: Dict, b: Dict) -> Dict:
    """
    Reduces two dictionaries by merging them. 
    Used by LangGraph to consolidate results from parallel nodes.
    """
    return {**(a or {}), **(b or {})}

def add_lists(a: List, b: List) -> List:
    """
    Reduces two lists by concatenation.
    Used by LangGraph to accumulate errors or URLs across nodes.
    """
    return (a or []) + (b or [])

# ---------------------------------------------------------------------------
# Interaction Utilities
# ---------------------------------------------------------------------------

def human_delay(min_sec: float = 0.5, max_sec: float = 2.0):
    """Introduces a randomized delay to avoid server-side rate limits or IP bans."""
    time.sleep(random.uniform(min_sec, max_sec))
