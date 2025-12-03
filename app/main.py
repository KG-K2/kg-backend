from fastapi import FastAPI, HTTPException
from app.models import QueryRequest, SearchResponse
from app.services import run_custom_query, search_graph
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/run-query")
async def run_query(request: QueryRequest):
    try:
        query = request.query
        result = run_custom_query(query)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error running query: {e}")
    
@app.get("/search", response_model=SearchResponse)
async def search(q: str):
    """
    Endpoint untuk mencari Artist atau Artwork.
    Contoh penggunaan: /search?q=monet
    """
    if not q:
        raise HTTPException(status_code=400, detail="Search query cannot be empty")
    
    results = search_graph(q)
    return {"results": results}
