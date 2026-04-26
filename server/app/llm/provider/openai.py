import openai as openai_lib
from llm.provider.base import BaseLLMProvider
from config.settings import settings


class OpenAIProvider(BaseLLMProvider):
    
    def _client(self) -> openai_lib.AsyncOpenAI:
        return openai_lib.AsyncOpenAI(api_key=settings.openai_api_key)
    
    async def classify_transactions(
        self,
        transactions: list[dict]
    ) -> dict:
        sys_prompt = self.build_transaction_prompt()
        messages = [{"role": "system", "content": sys_prompt}]
        messages.append({"role": "user", "content": str(transactions)})
        
        kwargs = {
            "model": settings.openai_model,
            "messages": messages,
        }

        response = await self._client().chat.completions.create(**kwargs)
        choice = response.choices[0]
        message = choice.message
        parsed_json = self.parse_llm_json_response(message.content)
        return parsed_json