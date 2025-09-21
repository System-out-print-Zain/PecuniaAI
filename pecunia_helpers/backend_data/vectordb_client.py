from pinecone import Pinecone, ServerlessSpec
from typing import List, Dict, Optional, Any

class VectorDBClient:
    """
    Client For A VectorDB
    """

    DIMENSION = 1536  # For OpenAI's Model

    METADATA_FIELDS = {
        "table": False,
        "source_file": False,
        "page_number": False,
        "company_name": False,
        "filing_date": False,
        "doc_type": False,
        "og_text": False
    }

    MAX_CONTENT_PREVIEW_LENGTH = 100

    INDEX_NAME = "pecunia-ai-index"

    def __init__(self, api_key: str):
        # Create Pinecone client instance
        self.pc = Pinecone(api_key=api_key)
        spec = ServerlessSpec(cloud="aws", region="us-east-1")

        # Create index if it doesn't exist
        if VectorDBClient.INDEX_NAME not in self.pc.list_indexes().names():
            self.pc.create_index(
                name=VectorDBClient.INDEX_NAME,
                dimension=self.DIMENSION,
                metric="cosine",
                spec=spec
            )

        # Connect to the index
        self._index = self.pc.Index(VectorDBClient.INDEX_NAME)

    def upsert_vectors(self, vectors: List[Dict[str, Any]], batch_size=50):
        """
        Upserts the given list of vector into the DB.
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

            for i in range(0, len(valid_vectors), batch_size):
                batch = valid_vectors[i:i+batch_size]
                self._index.upsert(vectors=batch)
            
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
            
            return True

        except ValueError as e:
            print(f"Validation error {e}")
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
        if not self._index:
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
