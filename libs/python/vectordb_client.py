from pinecone import Pinecone, ServerlessSpec
from typing import List, Dict


class VectorDBClient:
    """
    Client For A VectorDB
    """

    DIMENSION = 1536  # For OpenAI's Model

    METADATA_FIELDS = {
        "source_file": False,
        "chunk_num": False,
        "section_title": False,
        "page_number": False,
        "content_preview": False,
        "company_ticker": False,
        "fiscal_year": False,
    }

    MAX_CONTENT_PREVIEW_LENGTH = 100

    def __init__(self, api_key: str):
        self._client = Pinecone(api_key=api_key)

        index_name = "sem-search-index"

        if index_name not in self._client.list_indexes().names():
            pc.create_index(
                name=index_name,
                dimension=VectorDBClient.DIMENSION,
                metric="cosine",
                spec=ServerlessSpec(),
            )

        self._index = self._client.Index(index_name)

    def upsert_vectors(self, vectors: List[Dict[str, Any]]):
        """
        Upserts the given list of vector into the DB.

        Args
        vectors: A list where each element has the following structure:
            id: A unique identifier for the vector. Typically derived from source file key and chunk number
            values: A list of floats representing the vector embedding
            metadata: A dictionary containing vector metadata fields. It has the following fields:
                source_file: S3 object key for the pdf document from which the vector was extracted
                chunk_num: The sequential chunk number of the text corresponding to the embedding
                section_title: the section name of the text chunk corresponding to the embedding
                page_number: the page number of the text chunk corresponding to the embedding
                content_preview: A prefix of the text chunk. It should have length <= 100
                company_ticker: The ticker symbol of the company the embedding relates to
                fiscal_year: The year of the text corresponding to the embedding

        Returns:
            True if successful. False otherwise
        """
        try:
            valid_vectors = []
            for i in range(len(vectors)):
                if self.validate_vector(
                    vectors[i]["id"], vectors[i]["values"], vectors[i]["metadata"]
                ):
                    valid_vectors.append(vectors[i])
                else:
                    print(f"Vector {i+1} is invalid.")

            self._index.upsert(vectors=valid_vectors)
            print(f"Upserted {len(valid_vectors)} to Pinecone.")

        except Exception as e:
            print(f"Error upserting vectors to Pinecone: {e}")
            raise

    def validate_vector(self, id: str, values: List[float], metadata: dict[str, Any]):
        """
        Validates the vector value to ensure it complies with the schema.

        Args
            id: A unique identifier for the vector. Typically derived from source file key and chunk number
            values: A list of floats representing the vector embedding
            metadata: A dictionary containing vector metadata fields.

        Returns:
            True if the arguments are valid. False otherwise.
        """
        try:
            if len(values) != self.DIMENSION:
                raise ValueError("Invalid vector length")

            if not id:
                raise ValueError("Empty id")

            missing_fields = self.METADATA_FIELDS - metadata.keys()
            if missing_fields:
                raise ValueError(
                    f"Missing required metadata fields: {', '.join(missing_fields)}"
                )

            content_preview = metadata.get("content_preview", "")
            if len(content_preview) > self.MAX_CONTENT_PREVIEW_LENGTH:
                metadata["content_preview"] = content_preview[
                    : self.MAX_CONTENT_PREVIEW_LENGTH
                ]

        except ValueError as e:
            print(f"Validation error with vector id {id}")
            return False
        except Exception as e:
            print(f"Error upserting vector id {id}")
            return False

    def semantic_search(
        self,
        query_embedding: List[float],
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Performs a semantic similarity search in the Pinecone index.
        """
        if not self.index:
            raise RuntimeError("Pinecone index not initialized.")
        try:
            response = self._index.query(
                vector=query_embedding,
                top_k=top_k,
                include_values=False,
                include_metadata=True,
                filter=filters,
            )
            results = []
            for match in response.matches:
                results.append(
                    {
                        "id": match.id,
                        "score": match.score,
                        "metadata": match.metadata,
                    }
                )
            return results
        except Exception as e:
            print(f"Error performing semantic search in Pinecone: {e}")
            raise
