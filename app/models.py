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

# ... (kode QueryRequest dan SearchResult yang lama biarkan saja)

class ArtworkDetail(BaseModel):
    id: int
    title: str
    url: Optional[str] = None
    form: Optional[str] = None # misal: painting, sculpture
    location: Optional[str] = None
    type: str = "Artwork"

class ArtistDetail(BaseModel):
    id: int
    name: str
    bio: Optional[str] = None
    nationality: Optional[str] = None
    image: Optional[str] = None # Kalau nanti ada foto artis
    type: str = "Artist"
    artworks: List[ArtworkDetail] = [] # List karya dia

class ArtworkPageResponse(BaseModel):
    artwork: ArtworkDetail
    artist: Optional[ArtistDetail] = None # Info pembuatnya