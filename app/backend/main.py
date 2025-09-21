from fastapi import FastAPI, HTTPException
from models import ChatRequest, ChatResponse
from services import run_rag_pipeline
from fastapi.middleware.cors import CORSMiddleware
from backend_data.openai_client import OpenAILLM

import tiktoken

encoding = tiktoken.encoding_for_model(OpenAILLM.MODEL)
MAX_TOKENS = OpenAILLM.MAX_TOKENS

app = FastAPI(title="PecuniaAI API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/chat-completion", response_model=ChatResponse)
async def chat_completion(request: ChatRequest):
    num_tokens = len(encoding.encode(request.query))
    
    if num_tokens > MAX_TOKENS:
        raise HTTPException(
            status_code=400,
            detail=f"Query too long. Max allowed is {MAX_TOKENS} tokens, but got {num_tokens}."
        )
    
    answer = run_rag_pipeline(request.query)
    return ChatResponse(answer=answer)
