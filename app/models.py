from pydantic import BaseModel
from typing import Optional, List, Any

class QueryRequest(BaseModel):
    query: str

class SearchResult(BaseModel):
    id: int = None
    type: str
    label: str  # Bisa berisi nama artis atau judul karya
    score: float
    details: Optional[dict] = {}

class SearchResponse(BaseModel):
    results: List[SearchResult]