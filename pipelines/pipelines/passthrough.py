"""
requirements: anthropic
"""

from typing import List, Optional, Union, Generator, Iterator
from pydantic import BaseModel
import anthropic
import os

class Pipeline:
    class Valves(BaseModel):
        pass

    def __init__(self):
        self.name = "test_passthrough"
        api_key = os.getenv("ANTHROPIC_API_KEY")
        print(f"API key loaded: {api_key[:10] if api_key else 'NOT FOUND'}")
        self.client = anthropic.Anthropic(api_key=api_key)


    async def on_startup(self):
        print("test_passthrough pipeline started")

    async def on_shutdown(self):
        print("test_passthrough pipeline stopped")

    async def inlet(self, body:dict, user: Optional[dict] = None) -> dict:
        print(f"Inlet received: {body}")
        return body

    async def outlet(self, body:dict, user: Optional[dict] = None) -> dict:
        print(f"Inlet received: {body}")
        return body
    
    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        clean_messages = [
            {"role": m["role"], "content": m["content"]}
            for m in messages
            if m.get("content")
        ]

        response = self.client.messages.create(
            model="claude-sonnet-4-20250514"    ,
            max_tokens=1024,
            messages=clean_messages
        )

        return response.content[0].text