"""
Abstract base class for LLM providers.
All providers must implement classify()
"""

from abc import ABC, abstractmethod
import json
import re
from config.general import TRANSACTION_CATEGORIES, TRANSACTION_SUBCATEGORIES
from database.transaction.table import TRANSACTION_TABLE_DDL
from prompt import TRANSACTION_PROMPT


class BaseLLMProvider(ABC):

    @abstractmethod
    async def classify_transactions(
        self,
        transactions: list[dict]
    ) -> dict:
        """
        Send a chat request.

        Args:
            messages: Conversation history in OpenAI message format.
            temperature: Sampling temperature.

        Returns:
            Dict with at minimum:
                "content": str — the LLM's test response
        """
        ...
    
    

    def build_transaction_prompt(self):
        category_block = """"""
        for cat in TRANSACTION_CATEGORIES:
            category_block += f"{cat}:\nDescription:{TRANSACTION_CATEGORIES[cat]}\nSubcategories:\n{'\n'.join(map(lambda x: '- ' + x, TRANSACTION_SUBCATEGORIES[cat]))}\n\n"

        return TRANSACTION_PROMPT.format(category_block=category_block,
                                         valid_pairs_block=_build_valid_pairs_block())
        

    def parse_llm_json_response(self, response):
        clean_content = re.sub(r'^```json\s*|```$', '', response.strip(), flags=re.MULTILINE)
        try:
            # This converts the string "[{...}]" into a real Python list of dicts
            parsed_data = json.loads(clean_content)
            return {"contents": parsed_data}
        except json.JSONDecodeError as e:
            print(f"Failed to parse LLM response: {e}")
            return {"contents": [], "error": str(e)}

def _build_valid_pairs_block() -> str:
        lines = []
        for cat, subs in TRANSACTION_SUBCATEGORIES.items():
            lines.append(f"{cat}: {' | '.join(subs)}")
        return "\n".join(lines)