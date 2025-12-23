import os
import uvicorn

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from agent import run_agent

REGION = os.environ["AWS_REGION"]
APP = FastAPI()

class QueryRequest(BaseModel):
   question: str
   thread_id: str


@APP.post("/api/chat")
def handle_chat(request_body: QueryRequest):
    # Get the question from the chat interface
    question = request_body.question
    if not question:
        return StreamingResponse("Please enter a question!", media_type="text/plain")

    # Get the session id from the chant interface
    # NOTE: This will be used later to supply the agent with chat history
    # thread_id = request_body.thread_id

    # Create the agent response to the question
    stream_generator = run_agent(question)
    
    return StreamingResponse(stream_generator, media_type="text/plain")

if __name__ == "__main__":
    uvicorn.run(APP, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))