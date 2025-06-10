# mypy: disable - error - code = "no-untyped-def,misc"
import pathlib
from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles
import fastapi.exceptions
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from agent import graph  # compiled LangGraph
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from langchain_core.messages import message_to_dict, messages_to_dict
import logging
import httpx
from typing import Any, Dict
from langchain_core.messages.base import BaseMessage
import pprint  # for better formatting
import uuid
import json

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

# Define the FastAPI app
app = FastAPI()


def create_frontend_router(build_dir="../frontend/dist"):
    """Creates a router to serve the React frontend.

    Args:
        build_dir: Path to the React build directory relative to this file.

    Returns:
        A Starlette application serving the frontend.
    """
    build_path = pathlib.Path(__file__).parent.parent.parent / build_dir
    
    static_files_path = build_path / "assets"  # Vite uses 'assets' subdir

    if not build_path.is_dir() or not (build_path / "index.html").is_file():
        print(
            f"WARN: Frontend build directory not found or incomplete at {build_path}. Serving frontend will likely fail."
        )
        # Return a dummy router if build isn't ready
        from starlette.routing import Route

        async def dummy_frontend(request):
            return Response(
                "Frontend not built. Run 'npm run build' in the frontend directory.",
                media_type="text/plain",
                status_code=503,
            )

        return Route("/{path:path}", endpoint=dummy_frontend)

    build_dir = pathlib.Path(build_dir)

    react = FastAPI(openapi_url="")
    react.mount(
        "/assets", StaticFiles(directory=static_files_path), name="static_assets"
    )

    @react.get("/{path:path}")
    async def handle_catch_all(request: Request, path: str):
        fp = build_path / path
        if not fp.exists() or not fp.is_file():
            fp = build_path / "index.html"
        return fastapi.responses.FileResponse(fp)

    return react


# Mount the frontend under /app to not conflict with the LangGraph API routes
# app.mount(
#     "/app",
#     create_frontend_router(),
#     name="frontend",
# )

# Allow requests from your frontend (e.g. localhost:3000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # or ["*"] for all origins (not safe in prod)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def serialize_item(item: Any) -> Any:
    try:
        # Handle LangChain Messages
        if isinstance(item, BaseMessage):
            return message_to_dict(item)["data"]    

        # Handle dictionaries recursively
        elif isinstance(item, dict):
            return {k: serialize_item(v) for k, v in item.items()}
        
        # Handle lists recursively
        elif isinstance(item, list):
            return [serialize_item(i) for i in item]
        
        # Handle tuples recursively
        # elif isinstance(item, tuple):
        #     return [serialize_item(i) for i in item]  # Convert tuple to list for JSON compatibility
    
        # Handle other types that might not be JSON serializable
        elif hasattr(item, '__dict__'):
            if hasattr(item, 'dict'):
                return item.dict()
            elif hasattr(item, 'model_dump'):
                return item.model_dump()
            else:
                return str(item)  # Fallback to string representation
        
        else:
            return item
            
    except Exception as e:
        print(f"Serialization error for {type(item)}: {e}")
        return str(item)

@app.post("/threads")
async def threads_handler(request: Request):
    body = await request.json()
    pprint.pprint(body)  # See what's actually sent
    message = body.get("messages")
    print("Received threads message:", message)
    unique_id = uuid.uuid4()

    return {"thread_id": unique_id}
    # return { "status": "ok" }
         
    
@app.post("/threads/{thread_id}/history")
def get_history(thread_id: str): 
    config = {"configurable": {"thread_id": thread_id}}

    history = graph.get_state_history(config)

    serializable_history = []
    for idx , checkpoint in enumerate(history):
        # print(f"history {idx}")
        # print(json.dumps(list(checkpoint), default=str))
        serializable_history.append({
            "values": serialize_item(checkpoint.values) if checkpoint.values is not None else {},  # The actual state data
            "checkpoint": serialize_item(checkpoint.config.get("configurable", {})) if checkpoint.config is not None else {},
            "metadata": serialize_item(checkpoint.metadata) if checkpoint.metadata is not None else {},
            "next": list(checkpoint.next) if checkpoint.next else [],
            "created_at": checkpoint.created_at,
            "parent_config": serialize_item(checkpoint.parent_config.get("configurable", {})) if checkpoint.parent_config is not None else {}
        })    
    # print(serializable_history)          
    return serializable_history

@app.post("/threads/{thread_id}/runs/stream")
async def stream_handler(thread_id: str, request: Request):
# @app.post("/runs/stream")
# async def stream_handler(request: Request):
    body = await request.json()
    pprint.pprint(body)  # See what's actually sent
    
    # # Start with user question
    state = body.get("input")
    id = state["messages"][0]["id"]
    stream_mode = ['messages' if item == 'messages-tuple' else item for item in body.get("stream_mode")]
    config = {"configurable": {"thread_id": thread_id}}

    async def event_stream():
        try:
            # print("***graph stream start***")
            counter = 0;
            for chunk_stream_mode, chunk in graph.stream(state, stream_mode=stream_mode, config=config):
                counter = counter + 1
                # print(f"counter {counter} [{chunk_stream_mode}]")    
                # print(chunk)        

                if (chunk_stream_mode == "messages"): 
                    message_chunk, metadata = chunk
        
                    # print("message_chunk")
                    serialize_chunk = [serialize_item(message_chunk)]

                    yield f"event: metadata\n"
                    yield f"data: {json.dumps(metadata, default=str)}\n\n"

                else:   
                    serialize_chunk = serialize_item(chunk)
                
                # print("after")
                # print(serialize_chunk)
                yield f"event: {chunk_stream_mode}\n"
                yield f"data: {json.dumps(serialize_chunk, default=str)}\n\n"
                # print(f"---end counter {counter} ")
            # print("***graph stream end***")
        except Exception as e:
            print("Encounter exception")
            print(json.dumps(e, default=str))
            yield f"event: error\n"
            yield f"data: {json.dumps(e, default=str)}\n\n"

    return StreamingResponse(event_stream())

    