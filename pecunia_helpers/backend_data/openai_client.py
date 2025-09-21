from openai import OpenAI, APIError, RateLimitError
from typing import List
import requests


class OpenAIEmbeddingModel:
    """
    Client For An OpenAI Embedding Model.
    """

    MODEL = "text-embedding-3-small"

    def __init__(self, api_key: str):
        self._client = OpenAI(api_key=api_key)

    def create_embedding(self, text: str) -> List[float]:
        """
        Generate a vector embedding from the given text

        Params:
            text: the string to be embedded

        Returns:
            a list of floats representing the vector embedding
        """
        try:
            transformed_text = text.replace("\n", " ")
            response = self._client.embeddings.create(
                input=[transformed_text], model=OpenAIEmbeddingModel.MODEL
            )
            return response.data[0].embedding

        except APIError as e:
            print(f"OpenAI API Error during embedding: {e}")
            raise
        except RateLimitError as e:
            print(f"OpenAI Rate Limit Error during embedding: {e}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Network error during OpenAI embedding call: {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred during OpenAI embedding: {e}")
            raise


class OpenAILLM:

    MODEL =  "gpt-4o-mini"
    MAX_TOKENS = 512
    SYSTEM_PROMPT = "You are a helpful AI financial companion to value investors in the Canadian stock market. Use the provided context to answer the user's query. For questions relating to specific companies, if the context does not contain the answer, respond with 'I don't know'. For typical questions that involve common sense or mundane conversation just answer naturally and restate your purpose. Be concise and to the point."
    
    def __init__(self, api_key: str):
        self._client = OpenAI(api_key=api_key)

    def generate_text(self, prompt: str) -> str:
        response = self._client.chat.completions.create(
            model=OpenAILLM.MODEL,
            messages=[
                {"role": "system", "content": OpenAILLM.SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            max_tokens=OpenAILLM.MAX_TOKENS,
        )
        return response.choices[0].message.content.strip()


