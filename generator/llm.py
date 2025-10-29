# generator/llm.py
import os
import json
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from openai import OpenAI

MODEL = os.getenv("MDG_LLM_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
BASE_URL = os.getenv("OPENAI_BASE_URL")  # support OpenAI-compatible hosts
API_KEY = os.getenv("OPENAI_API_KEY")

class LLMError(RuntimeError):
    pass

class LLM:
    def __init__(self):
        if not API_KEY:
            raise LLMError("OPENAI_API_KEY not set. Set it or disable 'promptgen' domain.")
        self.client = OpenAI(api_key=API_KEY, base_url=BASE_URL)

    def complete_json(self, system: str, user: str, schema_hint: str, max_rows: int = 10) -> List[Dict[str, Any]]:
        """
        Returns a list[dict] JSON. We instruct the model to ONLY return JSON.
        """
        prompt = f"""
You are a data generator. Return STRICT JSON: an array of at most {max_rows} objects.
Schema hint (may be partial): {schema_hint}
User spec: {user}
"""
        resp = self.client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt.strip()},
            ],
            temperature=0.6,
            response_format={"type": "json_object"}  # toolformer json mode
        )
        try:
            content = resp.choices[0].message.content
            data = json.loads(content)
            # Accept either {"rows": [...]} or [...]
            if isinstance(data, dict) and "rows" in data:
                return data["rows"]
            if isinstance(data, list):
                return data
            # Last-ditch: look for 'data' or 'items'
            for k in ("data", "items"):
                if isinstance(data, dict) and k in data and isinstance(data[k], list):
                    return data[k]
            raise ValueError("Unexpected JSON shape")
        except Exception as e:
            raise LLMError(f"Bad JSON from model: {e}") from e
