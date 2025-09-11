"""
Document Processor

A crucial component in the ELT pipeline.
It's responsability is to retrieve unprocessed documents
from the S3 bucket, extract relevant tables and
text, run them through a vector embedding model, and
storing them in a vector DB.
"""

import boto3
import os
import re
import pdfplumber
import io
from dataclasses import dataclass
import pandas as pd
import wordninja
import uuid
from typing import Optional, List
from dotenv import load_dotenv

import libs.python.openai_client as openai_client  
import libs.python.vectordb_client as vectordb_client


@dataclass
class RawPdf:
    """
    A raw PDF object. Contains raw data and metadata.
    pdf_bytes: raw pdf data.
    key: s3 object key associated with the pdf.
    """

    def __init__(self, pdf_bytes: bytes, key: str, metadata: dict = None):
        self.pdf_bytes = pdf_bytes
        self.key = key
        self.metadata = metadata


@dataclass
class ParsedTable:
    """
    A parsed table object. Contains parsed data and metadata.
    table_data: 2D list representing the table.
    """
    def __init__(self, table: List, text_context: str, page_number: int):
        self.table = table
        self.text_context = text_context
        self.page_number = page_number
        self.s3_key = None  # To be set when uploaded to S3


@dataclass
class ParsedText:
    """
    A parsed text object. Contains parsed data and metadata.
    text: text content.
    page_number: page number of the text.
    """
    def __init__(self, text: str, page_number: int):
        self.text = text
        self.page_number = page_number

    
@dataclass
class ParsedPdf:
    """ 
    A parsed PDF object. Contains parsed data and metadata.
    pages: list of ParsedPage objects.
    metadata: metadata of the PDF.
    """

    def __init__(self, text_chunks: List[ParsedText], tables: List[ParsedTable], key: str, metadata: dict = None):
        self.text_chunks = text_chunks
        self.tables = tables
        self.key = key
        self.metadata = metadata


def retrieve_document_from_s3(s3_client, bucket_name: str, object_key: str):
    """
    Retrieve documents from S3 bucket.
    """
    # Notice: This keeps PDF data in memory. May need to be optimized.
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    pdf_bytes = response["Body"].read()
    metadata = response.get("Metadata", {})
    return RawPdf(pdf_bytes, object_key, metadata)


def iter_documents_from_prefix(s3_client, bucket_name: str, prefix: str):
    """
    Generator that yields RawPdf objects from all PDFs under the given bucket/prefix.
    """
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".pdf"):
                yield retrieve_document_from_s3(s3_client, bucket_name, key)


def merge_words_on_line(words, max_gap=3):
    """Merge letters or close words on the same line into proper words."""
    if not words:
        return ""

    words.sort(key=lambda w: w["x0"])
    merged = []
    current_word = words[0]["text"]
    last_x1 = words[0]["x1"]

    for w in words[1:]:
        if w["x0"] - last_x1 <= max_gap:
            current_word += w["text"]
        else:
            merged.append(current_word)
            current_word = w["text"]
        last_x1 = w["x1"]

    merged.append(current_word)
    return " ".join(merged)

def clean_heading_text(text: str) -> str:
    """Clean heading text: normalize, split CamelCase/numbers, and wordninja for stuck words."""
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', text)  # CamelCase
    text = re.sub(r'(?<=[0-9])(?=[A-Za-z])', ' ', text)  # Numbers
    words = []
    for token in text.split():
        words.extend(wordninja.split(token))
    return " ".join(words)

def should_augment_heading(heading_text: str) -> bool:
    """Heuristic: augment if heading is short, numeric, or generic."""
    heading_text = heading_text.strip()
    generic_words = ["table", "summary", "figure", "data", "report"]
    if len(heading_text.split()) <= 2:
        return True
    if any(word.lower() in heading_text.lower() for word in generic_words):
        return True
    if heading_text.isdigit():
        return True
    return False

def parse_tables(pdf: "RawPdf") -> List["ParsedTable"]:
    parsed_tables: List["ParsedTable"] = []

    with pdfplumber.open(io.BytesIO(pdf.pdf_bytes)) as pdf_doc:
        for page_number, page in enumerate(pdf_doc.pages, start=1):
            table_objs = page.find_tables()
            if not table_objs:
                continue

            words = page.extract_words(extra_attrs=["size", "fontname", "x0", "x1", "top"])

            for table_obj in table_objs:
                table = table_obj.extract()
                table_top = table_obj.bbox[1]

                # Find words above the table
                heading_candidates = [w for w in words if w["top"] < table_top]

                heading_text = ""
                if heading_candidates:
                    # Group words by line
                    lines = {}
                    for w in heading_candidates:
                        line_key = round(w["top"] / 2)
                        lines.setdefault(line_key, []).append(w)

                    # Sort lines by top (descending, closest to table first)
                    sorted_lines = sorted(lines.items(), key=lambda x: -x[0])

                    # Collect multiple lines until a significant font drop or line limit
                    selected_lines = []
                    last_avg_size = None
                    for _, line_words in sorted_lines:
                        avg_size = sum(w["size"] for w in line_words) / len(line_words)
                        if last_avg_size and avg_size < last_avg_size * 0.7:
                            break  # stop if font size drops significantly
                        selected_lines.append(line_words)
                        last_avg_size = avg_size
                        if len(selected_lines) >= 3:  # max 3 lines above table
                            break

                    # Merge words for each line and combine lines
                    heading_text = " ".join(merge_words_on_line(l) for l in reversed(selected_lines))

                # Fallback: first 800 chars above table
                if not heading_text:
                    above_table_text = " ".join(w["text"] for w in heading_candidates)
                    heading_text = above_table_text[:800]

                heading_text = clean_heading_text(heading_text)

                # Augment heading if needed
                if should_augment_heading(heading_text):
                    col_headers = table[0] if table else []
                    col_headers_text = " | ".join(str(h) for h in col_headers)
                    first_row = table[1] if len(table) > 1 else []
                    first_row_text = " | ".join(str(r) for r in first_row)
                    text_context = heading_text
                    if col_headers_text:
                        text_context += ". Columns: " + col_headers_text
                    if first_row_text:
                        text_context += ". Sample row: " + first_row_text
                else:
                    text_context = heading_text

                parsed_tables.append(
                    ParsedTable(
                        table=table,
                        text_context=text_context,
                        page_number=page_number,
                    )
                )

    return parsed_tables


def upload_tables_to_s3(s3_client, tables: List[ParsedTable], bucket_name: str, prefix: str, source_s3_key: str = None):
    """
    Upload parsed tables to S3 as CSV files.

    Args:
        tables: List of ParsedTable objects.
        bucket_name: S3 bucket to upload to.
        prefix: S3 key prefix (folder path) for the tables.
        doc_id: Document ID to use in file names.
    """
    if source_s3_key:
        import hashlib
        doc_id = hashlib.sha256(source_s3_key.encode()).hexdigest()[:16]
    else:
        doc_id = str(uuid.uuid4())

    for idx, table_obj in enumerate(tables, start=1):
        if len(table_obj.table) == 0:
            continue

        df = pd.DataFrame(table_obj.table[1:], columns=table_obj.table[0])

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        table_s3_key = f"{prefix}/{doc_id}_page{table_obj.page_number}_table{idx}.csv"

        table_obj.s3_key = table_s3_key  # Store the S3 key in the ParsedTable object

        s3_client.put_object(
            Bucket=bucket_name,
            Key=table_s3_key,
            Body=csv_buffer.getvalue()
        )

def looks_like_table(text: str, threshold: float = 0.4) -> bool:
    tokens = text.split()
    if not tokens:
        return False
    numeric_like = sum(1 for t in tokens if re.fullmatch(r"[\d,.\-%()]+", t))
    return numeric_like / len(tokens) > threshold


def split_text(text: str, max_chunk_size: int = 1000, overlap: int = 100) -> List[str]:
    text = re.sub(r"\s+", " ", text).strip()
    chunks = []
    text_length = len(text)

    if overlap >= max_chunk_size:
        raise ValueError("overlap must be smaller than max_chunk_size")

    start = 0
    while start < text_length:
        end = min(start + max_chunk_size, text_length)

        # Try to split at a sentence boundary
        split_point = text.rfind(".", start, end)
        if split_point == -1 or split_point <= start:
            split_point = end

        chunk = text[start:split_point].strip()
        if chunk and not looks_like_table(chunk):
            chunks.append(chunk)

        # Calculate new start so loop always moves forward
        next_start = split_point - overlap
        if next_start <= start:  # safety check to guarantee progress
            next_start = start + max_chunk_size
        start = next_start

    return chunks


def parse_text(pdf: RawPdf) -> List[ParsedText]:
    parsed_texts: List[ParsedText] = []

    with pdfplumber.open(io.BytesIO(pdf.pdf_bytes)) as pdf_doc:
        for page_number, page in enumerate(pdf_doc.pages, start=1):
            text = page.extract_text()

            if not text:
                text = ""

            chunks = split_text(text)

            for chunk in chunks:
                parsed_texts.append(
                ParsedText(
                    text=chunk,
                    page_number=page_number,
                )
            )

    return parsed_texts


def parse_pdf(pdf: RawPdf) -> ParsedPdf:
    """
    Parse a PDF file and return a list of ParsedPdf objects.
    """
    return ParsedPdf(
        text_chunks=parse_text(pdf),
        tables=parse_tables(pdf),
        metadata=pdf.metadata,
        key = pdf.key
    )
    

def upload_to_vector_db(pdf: ParsedPdf, embedding_model: openai_client.OpenAIEmbeddingModel, vector_db_client: vectordb_client.VectorDBClient):
    vectors = []
    # Upload text chunks
    for idx, chunk in enumerate(pdf.text_chunks, start=1):
        print("Processing text chunk:", chunk.text[:50], "...")
        if not chunk.text.strip():
            continue

        embedding = embedding_model.create_embedding(chunk.text)

        # Metadata (following your schema)
        metadata = {
            "table": False,
            "source_file": pdf.key,
            "page_number": chunk.page_number,
            "company_name": pdf.metadata.get("company"),
            "filing_date": pdf.metadata.get("filing_date"),
            "doc_type": pdf.metadata.get("doc_type"),
            "og_text": chunk.text,
        }

        vectors.append({
            "id": f"{pdf.key}_text_{idx}",
            "values": embedding,
            "metadata": metadata,
        })

    # Upload table chunks
    for idx, table in enumerate(pdf.tables, start=1):
        print("Processing table:", table.text_context[:50], "...")
        table_context = table.text_context or "Table data"
        embedding = embedding_model.create_embedding(table_context)

        metadata = {
            "table": True,
            "source_file": table.s3_key,
            "page_number": table.page_number,
            "company_name": pdf.metadata.get("company"),
            "filing_date": pdf.metadata.get("filing_date"),
            "doc_type": pdf.metadata.get("doc_type"),
            "og_text": table_context,
        }

        vectors.append({
            "id": f"{pdf.key}_table_{idx}",
            "values": embedding,
            "metadata": metadata,
        })
    
    # Upload all vectors to the vector DB
    vector_db_client.upsert_vectors(vectors)


if __name__ == "__main__":
    print("Starting PDF parsing test...")

    # --- Config ---
    S3_BUCKET = "pecunia-ai-document-storage"
    S3_DOC_PREFIX = "sedar-documents"
    S3_TABLE_PREFIX = "extracted-tables"

    load_dotenv()
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    PINECONE_API_KEY = os.environ["PINECONE_API_KEY"]

    # --- Clients --- 
    s3_client = boto3.client("s3")
    embedding_model = openai_client.OpenAIEmbeddingModel(api_key=OPENAI_API_KEY)
    vector_db_client = vectordb_client.VectorDBClient(api_key=PINECONE_API_KEY, index_name=vectordb_client.VectorDBClient.INDEX_NAME)

        # --- Process documents ---
    for raw_pdf in iter_documents_from_prefix(s3_client, S3_BUCKET, S3_DOC_PREFIX):
        print(f"Processing {raw_pdf.key}...")

        parsed_pdf = parse_pdf(raw_pdf)
        print("Done text and table parsing")

        # Upload table CSVs to S3 and update s3_key inside ParsedTable
        upload_tables_to_s3(s3_client, parsed_pdf.tables, S3_BUCKET, S3_TABLE_PREFIX, raw_pdf.key)
        print("Done uploading tables to S3")

        # Embed & push into Pinecone
        upload_to_vector_db(parsed_pdf, embedding_model, vector_db_client)

    print("✅ Finished processing all PDFs.")
