import libs.python.openai_client as openai_client  
import libs.python.vectordb_client as vectordb_client
import os
from dotenv import load_dotenv

load_dotenv()

embedding_model = openai_client.OpenAIEmbeddingModel(api_key=os.getenv("OPENAI_API_KEY"))
llm_model = openai_client.OpenAILLM(api_key=os.getenv("OPENAI_API_KEY"))
vector_db = vectordb_client.VectorDBClient(api_key=os.getenv("PINECONE_API_KEY"), index_name=vectordb_client.VectorDBClient.INDEX_NAME)

def run_rag_pipeline(query: str) -> str:
    """
    Run the RAG pipeline and return LLM response to augmented query.
    """
    embedded_query = embedding_model.create_embedding(query)
    sem_search_results = vector_db.semantic_search(embedded_query, top_k=5)

    text_results = [sem_search_results[i].metadata["og_text"] for i in range(len(sem_search_results))]

    augmented_query = "\n\nContext:\n" + "\n\n".join(text_results) + "\n\nUser Query: " + query
    return llm_model.generate_text(augmented_query)

