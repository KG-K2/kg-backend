from fastapi import FastAPI, HTTPException
from app.models import QueryRequest
from app.services import run_custom_query

app = FastAPI()

@app.post("/run-query")
async def run_query(request: QueryRequest):
    try:
        query = request.query
        result = run_custom_query(query)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error running query: {e}")