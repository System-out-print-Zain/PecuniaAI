import backend_data.openai_client as openai_client  
import backend_data.vectordb_client as vectordb_client
import os
import boto3
import pandas as pd
from io import StringIO
import json
from dotenv import load_dotenv

load_dotenv()

embedding_model = openai_client.OpenAIEmbeddingModel(api_key=os.getenv("OPENAI_API_KEY"))
llm_model = openai_client.OpenAILLM(api_key=os.getenv("OPENAI_API_KEY"))
vector_db = vectordb_client.VectorDBClient(api_key=os.getenv("PINECONE_API_KEY"))

def fetch_table(source_file: str) -> str:
    """
    Fetch the table (as CSV) from S3, convert to row-oriented JSON string.
    """
    BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    s3 = boto3.client("s3")
    
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=source_file)
    csv_data = obj["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(csv_data))
    
    json_table = {
        "table_name": source_file.split("/")[-1].replace(".csv", ""),
        "rows": df.to_dict(orient="records")
    }
    return json.dumps(json_table, indent=2)


def run_rag_pipeline(query: str) -> str:
    """
    Run the RAG pipeline and return LLM response to augmented query.
    """
    embedded_query = embedding_model.create_embedding(query)
    sem_search_results = vector_db.semantic_search(embedded_query, top_k=5)

    relevant_tables = [fetch_table(res["metadata"]["source_file"]) for res in sem_search_results if res["metadata"]["table"]]

    text_results = [sem_search_results[i]["metadata"]["og_text"] for i in range(len(sem_search_results))]

    augmented_query = "\n\nContext:\n" + "\n\n".join(text_results) + "\n\nData:\n" + "\n\n".join(relevant_tables) + "\n\nUser Query: " + query
    return llm_model.generate_text(augmented_query)

