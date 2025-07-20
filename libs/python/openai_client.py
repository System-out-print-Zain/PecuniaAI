from openai import OpenAI, APIError, RateLimitError
from typing import List
import requests


class OpenAIEmbeddingModel:
    """
    Client For An OpenAI Embedding Model.
    """

    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        self._client = OpenAI(api_key=api_key)
        self._model = model

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
                input=[transformed_text], model=self._model
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
    pass
